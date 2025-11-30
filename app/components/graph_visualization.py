"""
Graph Visualization Component
Displays interactive network graph of YouTube videos
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import networkx as nx
import logging
from typing import Optional, Tuple
from components import render_video_link
from utils.youtube_helpers import youtube_url, short_youtube_url

logger = logging.getLogger(__name__)


def render_graph_visualization(mongo):
    """
    Render interactive graph visualization of video network.
    
    Args:
        mongo: MongoDBConnector instance
    """
    st.markdown("### 🕸️ Network Graph Visualization")
    
    # Configuration options
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        sample_size = st.selectbox(
            "Sample Size",
            options=[50, 100, 200, 500],
            index=1,
            help="Number of videos to sample for visualization"
        )
    
    with col2:
        layout_type = st.selectbox(
            "Layout Algorithm",
            options=["Spring", "Circular", "Kamada-Kawai", "Shell"],
            index=0,
            help="Graph layout algorithm"
        )
    
    with col3:
        color_by = st.selectbox(
            "Color By",
            options=["Degree", "Category", "Rate"],
            index=0,
            help="Node coloring scheme"
        )
    
    with col4:
        show_labels = st.checkbox(
            "Show Labels",
            value=False,
            help="Show video IDs on nodes"
        )
    
    # Generate graph button
    if st.button("🎨 Generate Graph", type="primary", width="stretch"):
        with st.spinner(f"Building network graph with {sample_size} nodes..."):
            try:
                graph_data = _build_graph_data(mongo, sample_size, color_by)
                
                if graph_data:
                    fig = _create_graph_visualization(
                        graph_data,
                        layout_type,
                        color_by,
                        show_labels
                    )
                    st.plotly_chart(fig, width='stretch')
                    
                    # Show statistics
                    _show_graph_stats(graph_data['graph'])
                else:
                    st.error("Could not generate graph. No data available.")
            except Exception as e:
                st.error(f"Error generating graph: {e}")
                logger.error(f"Graph generation error: {e}")


def _build_graph_data(mongo, sample_size: int, color_by: str) -> Optional[dict]:
    """
    Build graph data from MongoDB using a connectivity-aware sampling strategy.
    
    Args:
        mongo: MongoDBConnector instance
        sample_size: Number of nodes to sample
        color_by: Attribute to color nodes by
        
    Returns:
        Dictionary with graph and node attributes
    """
    try:
        videos_collection = mongo.get_collection('videos')
        edges_collection = mongo.get_collection('edges')
        
        # Strategy: Find videos with high degree (many connections) for better visualization
        # Group by src to find videos that reference many others
        high_degree_videos = list(edges_collection.aggregate([
            {"$group": {
                "_id": "$src",
                "out_degree": {"$sum": 1}
            }},
            {"$sort": {"out_degree": -1}},
            {"$limit": sample_size // 2}  # Get top connected videos
        ]))
        
        seed_video_ids = [v['_id'] for v in high_degree_videos if v.get('_id')]
        
        if not seed_video_ids:
            logger.warning("No high-degree videos found, using random sample")
            # Fallback to random edge sample
            edge_sample = list(edges_collection.aggregate([
                {"$sample": {"size": sample_size * 2}},
                {"$project": {"src": 1, "dst": 1, "_id": 0}}
            ]))
        else:
            # Get all edges involving these high-degree videos
            edge_sample = list(edges_collection.find(
                {"$or": [
                    {"src": {"$in": seed_video_ids}},
                    {"dst": {"$in": seed_video_ids}}
                ]},
                {"src": 1, "dst": 1, "_id": 0}
            ).limit(sample_size * 5))  # Get plenty of edges
        
        if not edge_sample:
            logger.warning("No edges found in database, falling back to random video sample")
            # Fallback: sample videos randomly
            video_sample = list(videos_collection.aggregate([
                {"$sample": {"size": sample_size}},
                {"$project": {"_id": 1, "category": 1, "uploader": 1}}
            ]))
            video_ids = [v['_id'] for v in video_sample]
            video_map = {v['_id']: v for v in video_sample}
            edge_sample = []
        else:
            # Collect unique video IDs from edges
            video_ids = set()
            for edge in edge_sample:
                if edge.get('src'):
                    video_ids.add(edge['src'])
                if edge.get('dst'):
                    video_ids.add(edge['dst'])
            
            # Limit to sample_size
            video_ids = list(video_ids)[:sample_size]
            
            # Get video info for these IDs
            video_sample = list(videos_collection.find(
                {"_id": {"$in": video_ids}},
                {"_id": 1, "category": 1, "uploader": 1}
            ))
            video_map = {v['_id']: v for v in video_sample}
            
            # Filter edges to only include our sampled videos
            edge_sample = [
                e for e in edge_sample
                if e.get('src') in video_ids and e.get('dst') in video_ids
            ]
        
        logger.info(f"Sampled {len(video_ids)} videos with {len(edge_sample)} edges")
        
        # Get latest snapshot data for these videos for additional attributes
        snapshots_collection = mongo.get_collection('video_snapshots')
        latest_snapshots = {}
        for video_id in video_ids:
            snapshot = snapshots_collection.find_one(
                {"video_id": video_id},
                {"video_id": 1, "rate": 1, "views": 1, "ratings": 1},
                sort=[("crawl_id", -1)]
            )
            if snapshot:
                latest_snapshots[video_id] = snapshot
        
        # Build NetworkX graph
        G = nx.DiGraph()
        
        # Add edges first (this will auto-create nodes)
        edges_added = 0
        for edge in edge_sample:
            if edge.get('src') and edge.get('dst'):
                G.add_edge(edge['src'], edge['dst'])
                edges_added += 1
        
        logger.info(f"Added {edges_added} edges to graph with {len(G.nodes())} nodes")
        
        # If no edges but we have videos, add them as isolated nodes
        if edges_added == 0 and video_ids:
            for video_id in video_ids:
                G.add_node(video_id)
            logger.warning("No edges in graph - all nodes are isolated")
        
        # Now set attributes for all nodes in the graph
        for node in G.nodes():
            video = video_map.get(node, {})
            snapshot = latest_snapshots.get(node, {})
            G.nodes[node]['category'] = video.get('category', 'Unknown')
            G.nodes[node]['rating'] = snapshot.get('rate', 0)
            G.nodes[node]['views'] = snapshot.get('views', 0)
            G.nodes[node]['ratings_count'] = snapshot.get('ratings', 0)
            G.nodes[node]['uploader'] = video.get('uploader', 'Unknown')
        
        # Calculate degree for coloring
        if color_by == "Degree":
            degrees = dict(G.degree())
            nx.set_node_attributes(G, degrees, 'degree')
        
        return {
            'graph': G,
            'video_map': video_map,
            'color_by': color_by
        }
        
    except Exception as e:
        logger.error(f"Error building graph data: {e}")
        return None


def _create_graph_visualization(
    graph_data: dict,
    layout_type: str,
    color_by: str,
    show_labels: bool
) -> go.Figure:
    """
    Create interactive Plotly graph visualization.
    
    Args:
        graph_data: Dictionary containing graph and attributes
        layout_type: Layout algorithm to use
        color_by: Attribute to color nodes by
        show_labels: Whether to show node labels
        
    Returns:
        Plotly figure object
    """
    G = graph_data['graph']
    
    # Calculate layout
    if layout_type == "Spring":
        pos = nx.spring_layout(G, k=0.5, iterations=50)
    elif layout_type == "Circular":
        pos = nx.circular_layout(G)
    elif layout_type == "Kamada-Kawai":
        pos = nx.kamada_kawai_layout(G)
    else:  # Shell
        pos = nx.shell_layout(G)
    
    # Create edge trace
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
    
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines',
        showlegend=False
    )
    
    # Create node trace
    node_x = []
    node_y = []
    node_text = []
    node_color = []
    node_size = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        # Get node attributes
        attrs = G.nodes[node]
        degree = G.degree(node)
        
        # Build hover text
        hover_text = f"<b>Video:</b> {node[:20]}<br>"
        hover_text += f"<b>Category:</b> {attrs.get('category', 'Unknown')}<br>"
        hover_text += f"<b>Rate:</b> {attrs.get('rating', 0):.2f}<br>"
        hover_text += f"<b>Views:</b> {attrs.get('views', 0):,}<br>"
        hover_text += f"<b>Uploader:</b> {attrs.get('uploader', 'Unknown')[:20]}<br>"
        hover_text += f"<b>Degree:</b> {degree}<br>"
        hover_text += f"<b>In-degree:</b> {G.in_degree(node)}<br>"
        hover_text += f"<b>Out-degree:</b> {G.out_degree(node)}"
        node_text.append(hover_text)
        
        # Color by attribute
        if color_by == "Degree":
            node_color.append(degree)
        elif color_by == "Rate":
            node_color.append(attrs.get('rating', 0))
        else:  # Category
            # Map categories to numbers for coloring
            categories = list(set([G.nodes[n].get('category', 'Unknown') for n in G.nodes()]))
            node_color.append(categories.index(attrs.get('category', 'Unknown')))
        
        # Size by degree
        node_size.append(max(10, min(30, degree * 3)))
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text' if show_labels else 'markers',
        hovertext=node_text,
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='Viridis' if color_by != "Category" else 'Set3',
            color=node_color,
            size=node_size,
            colorbar=dict(
                thickness=15,
                title=dict(text=color_by, side='right'),
                xanchor='left'
            ),
            line=dict(width=2, color='white')
        ),
        text=[node[:8] for node in G.nodes()] if show_labels else None,
        textposition="top center",
        textfont=dict(size=8),
        showlegend=False
    )
    
    # Create figure
    fig = go.Figure(data=[edge_trace, node_trace])
    
    fig.update_layout(
        title=dict(
            text=f"YouTube Video Network Graph ({len(G.nodes())} nodes, {len(G.edges())} edges)",
            x=0.5,
            xanchor='center'
        ),
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )
    
    return fig


def _show_graph_stats(G: nx.DiGraph):
    """
    Display graph statistics.
    
    Args:
        G: NetworkX graph
    """
    st.markdown("#### Graph Statistics")
    
    if len(G.edges()) == 0:
        st.warning("⚠️ No edges found in this sample. Try a larger sample size or the videos may not be connected.")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Nodes", len(G.nodes()))
    
    with col2:
        st.metric("Edges", len(G.edges()))
    
    with col3:
        density = nx.density(G)
        st.metric("Density", f"{density:.4f}")
    
    with col4:
        try:
            # For directed graphs, check weak connectivity
            if nx.is_weakly_connected(G):
                components = 1
            else:
                components = nx.number_weakly_connected_components(G)
            st.metric("Components", components)
        except:
            st.metric("Components", "N/A")
    
    with col5:
        avg_degree = sum(dict(G.degree()).values()) / len(G.nodes()) if len(G.nodes()) > 0 else 0
        st.metric("Avg Degree", f"{avg_degree:.2f}")
    
    # Additional insights
    with st.expander("📊 Detailed Network Analysis"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top 5 Most Connected Nodes (by degree):**")
            degree_dict = dict(G.degree())
            top_nodes = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:5]
            for node, degree in top_nodes:
                # show clickable link for each node
                st.markdown(f"- [{node[:20]}...]({youtube_url(node)}) → {degree} connections")

        with col2:
            st.markdown("**Network Properties:**")
            st.text(f"Average In-Degree: {sum(dict(G.in_degree()).values()) / len(G.nodes()):.2f}")
            st.text(f"Average Out-Degree: {sum(dict(G.out_degree()).values()) / len(G.nodes()):.2f}")
            
            # Check if graph is strongly connected
            try:
                is_strongly = nx.is_strongly_connected(G)
                st.text(f"Strongly Connected: {'Yes' if is_strongly else 'No'}")
            except:
                st.text("Strongly Connected: N/A")
            
            # Reciprocity (what fraction of edges are bidirectional)
            try:
                reciprocity = nx.reciprocity(G)
                st.text(f"Reciprocity: {reciprocity:.4f}")
            except:
                st.text("Reciprocity: N/A")
