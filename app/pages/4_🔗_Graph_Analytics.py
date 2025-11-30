"""
Graph Analytics Page
Advanced graph analysis using Spark GraphFrames for large-scale network analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
from typing import Optional
import logging
import sys
from pathlib import Path
from graphframes import GraphFrame

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

from utils import get_mongo_connector, get_spark_connector
from utils.youtube_helpers import youtube_url, short_youtube_url

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Graph Analytics - YouTube Analyzer",
    page_icon="🔗",
    layout="wide"
)

# Initialize connections - use shared singleton instances
mongo = get_mongo_connector()
spark_conn = get_spark_connector()


# use youtube helpers from utils for links

# Header
st.title("🔗 Graph Analytics")
st.markdown("Advanced network analysis using Spark GraphFrames for large-scale graph mining")

# Tabs for different analyses
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 PageRank Analysis",
    "🏘️ Community Detection", 
    "🎯 Centrality Metrics",
    "🕸️ Network Visualization"
])

# ===================== TAB 1: PageRank Analysis =====================
with tab1:
    st.markdown("### 📊 PageRank Analysis")
    st.markdown("Identify the most influential videos in the network based on link structure.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        pagerank_limit = st.number_input(
            "Sample Size (edges)",
            min_value=1000,
            max_value=1000000,
            value=50000,
            step=1000,
            help="Number of edges to analyze"
        )
    
    with col2:
        pagerank_reset = st.slider(
            "Reset Probability",
            min_value=0.01,
            max_value=0.5,
            value=0.15,
            step=0.01,
            help="Probability of jumping to random node (damping factor = 1 - reset)"
        )
    
    with col3:
        pagerank_iter = st.number_input(
            "Max Iterations",
            min_value=5,
            max_value=50,
            value=10,
            step=5,
            help="Maximum PageRank iterations"
        )
    
    if st.button("🚀 Run PageRank Analysis", type="primary", key="pagerank_btn"):
        with st.spinner("Running PageRank algorithm on video network..."):
            try:
                # Load graph data from MongoDB using Spark
                spark = spark_conn.spark
                
                # Load edges
                edges_df = spark.read.format("mongodb") \
                    .option("database", "youtube_analytics") \
                    .option("collection", "edges") \
                    .load() \
                    .limit(pagerank_limit) \
                    .select("src", "dst")
                
                edge_count = edges_df.count()
                st.info(f"Loaded {edge_count:,} edges for analysis")
                
                # Get unique vertices from edges
                src_vertices = edges_df.select("src").distinct().withColumnRenamed("src", "id")
                dst_vertices = edges_df.select("dst").distinct().withColumnRenamed("dst", "id")
                vertices_df = src_vertices.union(dst_vertices).distinct()
                
                vertex_count = vertices_df.count()
                st.info(f"Identified {vertex_count:,} unique videos")

                # Ensure DataFrames are explicitly bound to the current Spark session.
                # Some MongoDB connector DataFrames can lose their Spark context reference,
                # so recreate DataFrames from their RDDs to guarantee session attachment.
                try:
                    # materialize, cache and recreate
                    edges_df.cache()
                    edges_df.count()
                    edges_df = spark.createDataFrame(edges_df.rdd, schema=edges_df.schema)
                except Exception:
                    logger.exception("Failed to rebind edges_df to active Spark session; continuing with original DataFrame")

                try:
                    vertices_df.cache()
                    vertices_df.count()
                    vertices_df = spark.createDataFrame(vertices_df.rdd, schema=vertices_df.schema)
                except Exception:
                    logger.exception("Failed to rebind vertices_df to active Spark session; continuing with original DataFrame")

                logger.info(f"PageRank: spark app: {spark.sparkContext.appName}")
                logger.info(f"PageRank: vertices_df.sparkSession == spark: {getattr(vertices_df, 'sparkSession', None) == spark}")
                logger.info(f"PageRank: edges_df.sparkSession == spark: {getattr(edges_df, 'sparkSession', None) == spark}")

                # Create GraphFrame - it will extract SparkSession from DataFrames
                from graphframes import GraphFrame
                g = GraphFrame(vertices_df, edges_df)
                
                # Run PageRank
                with st.spinner(f"Computing PageRank (max {pagerank_iter} iterations)..."):
                    results = g.pageRank(resetProbability=pagerank_reset, maxIter=pagerank_iter)
                    
                    # Get top videos by PageRank
                    top_videos = results.vertices.orderBy("pagerank", ascending=False).limit(50)
                    top_videos_pd = top_videos.toPandas()
                    
                    # Enrich with video metadata
                    video_ids = top_videos_pd['id'].tolist()
                    videos_collection = mongo.get_collection('videos')
                    video_info = {
                        v['_id']: v for v in videos_collection.find(
                            {"_id": {"$in": video_ids}},
                            {"_id": 1, "category": 1, "uploader": 1}
                        )
                    }
                    
                    top_videos_pd['category'] = top_videos_pd['id'].map(
                        lambda x: video_info.get(x, {}).get('category', 'Unknown')
                    )
                    top_videos_pd['uploader'] = top_videos_pd['id'].map(
                        lambda x: video_info.get(x, {}).get('uploader', 'Unknown')
                    )
                    
                    # Add direct URLs for each video and show quick links
                    top_videos_pd['url'] = top_videos_pd['id'].map(youtube_url)
                    top_videos_pd['short_url'] = top_videos_pd['id'].map(short_youtube_url)
                
                # Display results
                st.success(f"✅ PageRank completed successfully!")
                
                # Visualization: Top videos by PageRank
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 🏆 Top 20 Most Influential Videos")
                    fig_top = px.bar(
                        top_videos_pd.head(20),
                        x='pagerank',
                        y='id',
                        orientation='h',
                        color='category',
                        hover_data=['uploader'],
                        labels={'pagerank': 'PageRank Score', 'id': 'Video ID'},
                        title="Videos with Highest PageRank Scores"
                    )
                    fig_top.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig_top, use_container_width=True)
                    # No quick links list; video IDs are clickable in the table below
                
                with col2:
                    st.markdown("#### 📊 PageRank Distribution")
                    fig_dist = px.histogram(
                        top_videos_pd,
                        x='pagerank',
                        nbins=30,
                        labels={'pagerank': 'PageRank Score', 'count': 'Number of Videos'},
                        title="PageRank Score Distribution (Top 50)"
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)
                    
                    st.markdown("#### 📂 PageRank by Category")
                    category_scores = top_videos_pd.groupby('category')['pagerank'].agg(['mean', 'count']).reset_index()
                    category_scores = category_scores.sort_values('mean', ascending=False)
                    
                    fig_cat = px.bar(
                        category_scores,
                        x='mean',
                        y='category',
                        orientation='h',
                        text='count',
                        labels={'mean': 'Average PageRank', 'category': 'Category', 'count': 'Videos'},
                        title="Average PageRank by Category"
                    )
                    fig_cat.update_traces(texttemplate='%{text} videos', textposition='outside')
                    st.plotly_chart(fig_cat, use_container_width=True)
                
                # Data table
                st.markdown("#### 📋 Top 50 Videos by PageRank")
                display_df = top_videos_pd[['id', 'pagerank', 'category', 'uploader']].copy()
                display_df.columns = ['Video ID', 'PageRank Score', 'Category', 'Uploader']
                # Keep the full id for the link, show full clickable id text
                display_df['Video'] = display_df['Video ID'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
                display_df['Uploader'] = display_df['Uploader'].str[:30]
                display_df['PageRank Score'] = display_df['PageRank Score'].round(6)
                display_df = display_df[['Video', 'PageRank Score', 'Category', 'Uploader']]
                st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"Error running PageRank: {e}")
                logger.error(f"PageRank error: {e}", exc_info=True)

# ===================== TAB 2: Community Detection =====================
with tab2:
    st.markdown("### 🏘️ Community Detection")
    st.markdown("Identify clusters of highly connected videos using Label Propagation algorithm.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        community_limit = st.number_input(
            "Sample Size (edges)",
            min_value=1000,
            max_value=500000,
            value=50000,
            step=1000,
            help="Number of edges to analyze",
            key="community_limit"
        )
    
    with col2:
        max_iterations = st.number_input(
            "Max Iterations",
            min_value=3,
            max_value=20,
            value=5,
            help="Maximum iterations for label propagation",
            key="community_iter"
        )
    
    if st.button("🔍 Detect Communities", type="primary", key="community_btn"):
        with st.spinner("Running community detection algorithm..."):
            try:
                spark = spark_conn.spark
                
                # Load graph data
                edges_df = spark.read.format("mongodb") \
                    .option("database", "youtube_analytics") \
                    .option("collection", "edges") \
                    .load() \
                    .limit(community_limit) \
                    .select("src", "dst")
                
                src_vertices = edges_df.select("src").distinct().withColumnRenamed("src", "id")
                dst_vertices = edges_df.select("dst").distinct().withColumnRenamed("dst", "id")
                vertices_df = src_vertices.union(dst_vertices).distinct()
                
                # Rebind DataFrames to the Spark session to avoid GraphFrame initialization issues
                try:
                    edges_df.cache()
                    edges_df.count()
                    edges_df = spark.createDataFrame(edges_df.rdd, schema=edges_df.schema)
                except Exception:
                    logger.exception("Failed to rebind edges_df to active Spark session in community detection; continuing with original DataFrame")

                try:
                    vertices_df.cache()
                    vertices_df.count()
                    vertices_df = spark.createDataFrame(vertices_df.rdd, schema=vertices_df.schema)
                except Exception:
                    logger.exception("Failed to rebind vertices_df to active Spark session in community detection; continuing with original DataFrame")

                logger.info(f"Community Detection: spark app: {spark.sparkContext.appName}")
                logger.info(f"Community Detection: vertices_df.sparkSession == spark: {getattr(vertices_df, 'sparkSession', None) == spark}")
                logger.info(f"Community Detection: edges_df.sparkSession == spark: {getattr(edges_df, 'sparkSession', None) == spark}")

                from graphframes import GraphFrame
                g = GraphFrame(vertices_df, edges_df)
                
                # Run Label Propagation Algorithm
                result = g.labelPropagation(maxIter=max_iterations)
                communities = result.groupBy("label").count().orderBy("count", ascending=False)
                communities_pd = communities.toPandas()
                
                st.success(f"✅ Found {len(communities_pd)} communities!")
                
                # Visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 📊 Community Size Distribution")
                    fig_sizes = px.bar(
                        communities_pd.head(20),
                        x='label',
                        y='count',
                        labels={'label': 'Community ID', 'count': 'Number of Videos'},
                        title="Top 20 Largest Communities"
                    )
                    st.plotly_chart(fig_sizes, use_container_width=True)
                
                with col2:
                    st.markdown("#### 🥧 Community Size Categories")
                    size_bins = pd.cut(
                        communities_pd['count'],
                        bins=[0, 10, 50, 100, 500, float('inf')],
                        labels=['Tiny (1-10)', 'Small (11-50)', 'Medium (51-100)', 'Large (101-500)', 'Huge (500+)']
                    )
                    size_dist = size_bins.value_counts().reset_index()
                    size_dist.columns = ['Size Category', 'Number of Communities']
                    
                    fig_pie = px.pie(
                        size_dist,
                        values='Number of Communities',
                        names='Size Category',
                        title="Distribution of Community Sizes"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                # Statistics
                st.markdown("#### 📈 Community Statistics")
                stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                
                with stat_col1:
                    st.metric("Total Communities", len(communities_pd))
                with stat_col2:
                    st.metric("Largest Community", f"{communities_pd['count'].max():,} videos")
                with stat_col3:
                    st.metric("Average Size", f"{communities_pd['count'].mean():.1f} videos")
                with stat_col4:
                    st.metric("Median Size", f"{communities_pd['count'].median():.0f} videos")
                
                # Get sample videos from largest communities
                st.markdown("#### 🔍 Sample Videos from Top Communities")
                top_communities = result.filter(
                    result.label.isin([int(x) for x in communities_pd.head(5)['label'].tolist()])
                ).limit(100).toPandas()
                
                # Enrich with metadata
                video_ids = top_communities['id'].tolist()
                videos_collection = mongo.get_collection('videos')
                video_info = {
                    v['_id']: v for v in videos_collection.find(
                        {"_id": {"$in": video_ids}},
                        {"_id": 1, "category": 1, "uploader": 1}
                    )
                }
                # (No display_df is defined in this block; ensure community video URLs are added below)
                
                top_communities['category'] = top_communities['id'].map(
                    lambda x: video_info.get(x, {}).get('category', 'Unknown')
                )
                # Add URL fields for the selected community videos
                top_communities['url'] = top_communities['id'].map(youtube_url)
                top_communities['short_url'] = top_communities['id'].map(short_youtube_url)
                
                # Category distribution per community
                community_cats = top_communities.groupby(['label', 'category']).size().reset_index(name='count')
                fig_community_cats = px.bar(
                    community_cats,
                    x='label',
                    y='count',
                    color='category',
                    labels={'label': 'Community ID', 'count': 'Number of Videos'},
                    title="Category Distribution in Top 5 Communities",
                    barmode='stack'
                )
                st.plotly_chart(fig_community_cats, use_container_width=True)

                # Show clickable list of sample videos from communities as a table (no extra UI)
                if 'id' in top_communities.columns:
                    top_communities['Video'] = top_communities['id'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
                    display_df = top_communities[['Video', 'label', 'category']].copy()
                    display_df.columns = ['Video', 'Community ID', 'Category']
                    st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"Error detecting communities: {e}")
                logger.error(f"Community detection error: {e}", exc_info=True)

# ===================== TAB 3: Centrality Metrics =====================
with tab3:
    st.markdown("### 🎯 Centrality Metrics")
    st.markdown("Measure video importance using various centrality algorithms.")
    
    centrality_limit = st.number_input(
        "Sample Size (edges)",
        min_value=1000,
        max_value=100000,
        value=30000,
        step=1000,
        help="Number of edges to analyze",
        key="centrality_limit"
    )
    
    if st.button("📊 Calculate Centrality Metrics", type="primary", key="centrality_btn"):
        with st.spinner("Calculating centrality metrics..."):
            try:
                spark = spark_conn.spark
                
                # Load graph data
                edges_df = spark.read.format("mongodb") \
                    .option("database", "youtube_analytics") \
                    .option("collection", "edges") \
                    .load() \
                    .limit(centrality_limit) \
                    .select("src", "dst")
                
                edges_pd = edges_df.toPandas()
                
                # Use NetworkX for centrality calculations (more reliable than GraphFrames)
                G = nx.DiGraph()
                G.add_edges_from(zip(edges_pd['src'], edges_pd['dst']))
                
                st.info(f"Analyzing network: {len(G.nodes())} nodes, {len(G.edges())} edges")
                
                # Calculate various centrality metrics
                with st.spinner("Computing degree centrality..."):
                    in_degree = dict(G.in_degree())
                    out_degree = dict(G.out_degree())
                
                with st.spinner("Computing betweenness centrality (this may take a while)..."):
                    betweenness = nx.betweenness_centrality(G, k=min(100, len(G.nodes())))
                
                with st.spinner("Computing closeness centrality..."):
                    try:
                        closeness = nx.closeness_centrality(G)
                    except:
                        closeness = {}
                
                # Compile results
                centrality_data = []
                for node in G.nodes():
                    centrality_data.append({
                        'video_id': node,
                        'in_degree': in_degree.get(node, 0),
                        'out_degree': out_degree.get(node, 0),
                        'total_degree': in_degree.get(node, 0) + out_degree.get(node, 0),
                        'betweenness': betweenness.get(node, 0),
                        'closeness': closeness.get(node, 0)
                    })
                
                centrality_df = pd.DataFrame(centrality_data)
                
                # Get video metadata
                video_ids = centrality_df.nlargest(100, 'total_degree')['video_id'].tolist()
                videos_collection = mongo.get_collection('videos')
                video_info = {
                    v['_id']: v for v in videos_collection.find(
                        {"_id": {"$in": video_ids}},
                        {"_id": 1, "category": 1, "uploader": 1}
                    )
                }
                
                # Display results
                st.success("✅ Centrality metrics calculated!")
                
                # Visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 🏆 Top Videos by Degree Centrality")
                    top_degree = centrality_df.nlargest(20, 'total_degree').copy()
                    top_degree['category'] = top_degree['video_id'].map(
                        lambda x: video_info.get(x, {}).get('category', 'Unknown')
                    )
                    
                    fig_degree = px.bar(
                        top_degree,
                        x='total_degree',
                        y='video_id',
                        orientation='h',
                        color='category',
                        labels={'total_degree': 'Total Degree', 'video_id': 'Video ID'},
                        title="Highest Degree Centrality"
                    )
                    fig_degree.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig_degree, use_container_width=True)
                    # Show clickable links in a simple table (no extra UI widgets)
                    if 'video_id' in top_degree.columns:
                        top_degree['Video'] = top_degree['video_id'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
                        display_top = top_degree[['Video', 'total_degree', 'category']].copy()
                        display_top.columns = ['Video', 'Total Degree', 'Category']
                        st.write(display_top.to_html(escape=False, index=False), unsafe_allow_html=True)
                
                with col2:
                    st.markdown("#### 🌉 Top Videos by Betweenness Centrality")
                    top_between = centrality_df.nlargest(20, 'betweenness').copy()
                    top_between['category'] = top_between['video_id'].map(
                        lambda x: video_info.get(x, {}).get('category', 'Unknown')
                    )
                    
                    fig_between = px.bar(
                        top_between,
                        x='betweenness',
                        y='video_id',
                        orientation='h',
                        color='category',
                        labels={'betweenness': 'Betweenness Centrality', 'video_id': 'Video ID'},
                        title="Highest Betweenness (Bridge Videos)"
                    )
                    fig_between.update_layout(height=600, yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig_between, use_container_width=True)
                
                # Scatter plot: Degree vs Betweenness
                st.markdown("#### 🎯 Centrality Comparison")
                top_100 = centrality_df.nlargest(100, 'total_degree').copy()
                top_100['category'] = top_100['video_id'].map(
                    lambda x: video_info.get(x, {}).get('category', 'Unknown')
                )
                
                fig_scatter = px.scatter(
                    top_100,
                    x='total_degree',
                    y='betweenness',
                    color='category',
                    size='total_degree',
                    hover_data=['video_id'],
                    labels={'total_degree': 'Degree Centrality', 'betweenness': 'Betweenness Centrality'},
                    title="Degree vs Betweenness Centrality (Top 100 videos)"
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error calculating centrality: {e}")
                logger.error(f"Centrality error: {e}", exc_info=True)

# ===================== TAB 4: Network Visualization =====================
with tab4:
    st.markdown("### 🕸️ Interactive Network Visualization")
    st.markdown("Visualize graph structure with insights from GraphFrames analysis.")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        vis_sample_size = st.selectbox(
            "Sample Size",
            options=[50, 100, 200, 500],
            index=1,
            help="Number of edges to visualize",
            key="vis_sample"
        )
    
    with col2:
        vis_layout = st.selectbox(
            "Layout Algorithm",
            options=["Spring", "Circular", "Kamada-Kawai", "Shell"],
            index=0,
            key="vis_layout"
        )
    
    with col3:
        vis_color = st.selectbox(
            "Color By",
            options=["Degree", "Category", "PageRank"],
            index=0,
            key="vis_color"
        )
    
    with col4:
        vis_labels = st.checkbox(
            "Show Labels",
            value=False,
            key="vis_labels"
        )
    
    if st.button("🎨 Generate Visualization", type="primary", key="vis_btn"):
        with st.spinner(f"Building network graph with {vis_sample_size} edges..."):
            try:
                # Sample edges from MongoDB
                edges_collection = mongo.get_collection('edges')
                edge_sample = list(edges_collection.aggregate([
                    {"$sample": {"size": vis_sample_size * 3}},
                    {"$project": {"src": 1, "dst": 1, "_id": 0}}
                ]))
                
                # Build NetworkX graph
                G = nx.DiGraph()
                for edge in edge_sample:
                    if edge.get('src') and edge.get('dst'):
                        G.add_edge(edge['src'], edge['dst'])
                
                # Limit to top connected nodes
                degrees = dict(G.degree())
                top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:vis_sample_size]
                top_node_ids = [n[0] for n in top_nodes]
                G = G.subgraph(top_node_ids).copy()
                
                st.info(f"Visualizing {len(G.nodes())} nodes and {len(G.edges())} edges")
                
                # Get node attributes
                videos_collection = mongo.get_collection('videos')
                video_info = {
                    v['_id']: v for v in videos_collection.find(
                        {"_id": {"$in": list(G.nodes())}},
                        {"_id": 1, "category": 1, "uploader": 1}
                    )
                }
                
                # Calculate PageRank if needed
                if vis_color == "PageRank":
                    pagerank_scores = nx.pagerank(G, max_iter=20)
                else:
                    pagerank_scores = {}
                
                # Set node attributes
                for node in G.nodes():
                    G.nodes[node]['category'] = video_info.get(node, {}).get('category', 'Unknown')
                    G.nodes[node]['uploader'] = video_info.get(node, {}).get('uploader', 'Unknown')
                    G.nodes[node]['degree'] = G.degree(node)
                    G.nodes[node]['pagerank'] = pagerank_scores.get(node, 0)
                
                # Calculate layout
                if vis_layout == "Spring":
                    pos = nx.spring_layout(G, k=0.5, iterations=50)
                elif vis_layout == "Circular":
                    pos = nx.circular_layout(G)
                elif vis_layout == "Kamada-Kawai":
                    pos = nx.kamada_kawai_layout(G)
                else:
                    pos = nx.shell_layout(G)
                
                # Create Plotly visualization
                edge_x, edge_y = [], []
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
                
                # Node trace
                node_x, node_y, node_text, node_color, node_size = [], [], [], [], []
                
                for node in G.nodes():
                    x, y = pos[node]
                    node_x.append(x)
                    node_y.append(y)
                    
                    attrs = G.nodes[node]
                    degree = attrs['degree']
                    
                    hover_text = f"<b>Video:</b> {node[:20]}<br>"
                    hover_text += f"<b>Category:</b> {attrs['category']}<br>"
                    hover_text += f"<b>Uploader:</b> {attrs['uploader'][:20]}<br>"
                    hover_text += f"<b>Degree:</b> {degree}<br>"
                    hover_text += f"<b>In-degree:</b> {G.in_degree(node)}<br>"
                    hover_text += f"<b>Out-degree:</b> {G.out_degree(node)}"
                    if vis_color == "PageRank":
                        hover_text += f"<br><b>PageRank:</b> {attrs['pagerank']:.6f}"
                    hover_text += f"<br><b>URL:</b> {short_youtube_url(node)}"
                    node_text.append(hover_text)
                    
                    # Color by selected attribute
                    if vis_color == "Degree":
                        node_color.append(degree)
                    elif vis_color == "PageRank":
                        node_color.append(attrs['pagerank'])
                    else:  # Category
                        categories = list(set([G.nodes[n]['category'] for n in G.nodes()]))
                        node_color.append(categories.index(attrs['category']))
                    
                    node_size.append(max(10, min(30, degree * 2)))
                
                node_trace = go.Scatter(
                    x=node_x, y=node_y,
                    mode='markers+text' if vis_labels else 'markers',
                    hovertext=node_text,
                    hoverinfo='text',
                    marker=dict(
                        showscale=True,
                        colorscale='Viridis' if vis_color != "Category" else 'Set3',
                        color=node_color,
                        size=node_size,
                        colorbar=dict(
                            thickness=15,
                            title=dict(text=vis_color, side='right'),
                            xanchor='left'
                        ),
                        line=dict(width=2, color='white')
                    ),
                    text=[node[:8] for node in G.nodes()] if vis_labels else None,
                    textposition="top center",
                    textfont=dict(size=8),
                    showlegend=False
                )
                
                fig = go.Figure(data=[edge_trace, node_trace])
                fig.update_layout(
                    title=dict(
                        text=f"YouTube Video Network ({len(G.nodes())} nodes, {len(G.edges())} edges)",
                        x=0.5,
                        xanchor='center'
                    ),
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20, l=5, r=5, t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    height=700
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Graph statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Nodes", len(G.nodes()))
                with col2:
                    st.metric("Edges", len(G.edges()))
                with col3:
                    density = nx.density(G)
                    st.metric("Density", f"{density:.4f}")
                with col4:
                    avg_degree = sum(degrees.values()) / len(degrees) if degrees else 0
                    st.metric("Avg Degree", f"{avg_degree:.2f}")
                # Node selection UI removed; video IDs are now clickable where they are shown as links
                
            except Exception as e:
                st.error(f"Error generating visualization: {e}")
                logger.error(f"Visualization error: {e}", exc_info=True)

# Footer
st.markdown("---")
st.markdown("*Graph Analytics powered by Apache Spark GraphFrames and NetworkX*")
