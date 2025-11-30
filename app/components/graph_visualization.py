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
    Build graph data from MongoDB.
    
    Args:
        mongo: MongoDBConnector instance
        sample_size: Number of nodes to sample
        color_by: Attribute to color nodes by
        
    Returns:
        Dictionary with graph and node attributes
    """
    try:
        # Get sample of videos
        videos_collection = mongo.get_collection('videos')
        edges_collection = mongo.get_collection('edges')
        
        # Sample videos
        video_sample = list(videos_collection.aggregate([
            {"$sample": {"size": sample_size}},
            {"$project": {
                "_id": 1,
                "category": 1,
                "uploader": 1
            }}
        ]))
        
        if not video_sample:
            return None
        
        video_ids = [v['_id'] for v in video_sample]
        video_map = {v['_id']: v for v in video_sample}
        
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
        
        # Get edges for these videos
        edges = list(edges_collection.find({
            "$or": [
                {"src": {"$in": video_ids}},
                {"dst": {"$in": video_ids}}
            ]
        }, {"src": 1, "dst": 1, "_id": 0}))
        
        # Filter edges to only include videos in our sample
        edges = [
            e for e in edges
            if e['src'] in video_ids and e['dst'] in video_ids
        ]
        
        # Build NetworkX graph
        G = nx.DiGraph()
        
        # Add nodes with attributes
        for video_id in video_ids:
            video = video_map.get(video_id, {})
            snapshot = latest_snapshots.get(video_id, {})
            G.add_node(
                video_id,
                category=video.get('category', 'Unknown'),
                rating=snapshot.get('rate', 0),
                views=snapshot.get('views', 0),
                ratings_count=snapshot.get('ratings', 0),
                uploader=video.get('uploader', 'Unknown')
            )
        
        # Add edges
        for edge in edges:
            G.add_edge(edge['src'], edge['dst'])
        
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
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='rgba(240,240,240,0.9)',
        height=700
    )
    
    return fig


def _show_graph_stats(G: nx.DiGraph):
    """
    Display graph statistics.
    
    Args:
        G: NetworkX graph
    """
    st.markdown("#### Graph Statistics")
    
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
                st.text(f"{node[:20]}... → {degree} connections")
        
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
