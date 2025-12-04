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
import time
from datetime import datetime
from pathlib import Path
from graphframes import GraphFrame

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

from utils import get_mongo_connector, get_spark_connector
from utils.youtube_helpers import youtube_url, short_youtube_url
from components import render_video_link, render_algorithm_performance

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Graph Analytics - YouTube Analyzer",
    page_icon="link",
    layout="wide"
)


# --- Cached compute functions for heavy operations ---
@st.cache_data(ttl=3600)
def _compute_pagerank(limit: int, reset: float, max_iter: int):
    """Compute PageRank and return top 50 videos as pandas DataFrame and perf metrics."""
    try:
        spark_conn_local = get_spark_connector()
        try:
            from graphframes import GraphFrame
        except Exception as e:
            raise RuntimeError("GraphFrames not available or failed to import: " + str(e))

        spark = spark_conn_local.spark
        edges_df = spark.read.format("mongodb") \
            .option("database", "youtube_analytics") \
            .option("collection", "edges") \
            .load() \
            .limit(limit) \
            .select("src", "dst")

        src_vertices = edges_df.select("src").distinct().withColumnRenamed("src", "id")
        dst_vertices = edges_df.select("dst").distinct().withColumnRenamed("dst", "id")
        vertices_df = src_vertices.union(dst_vertices).distinct()

        # Try to rebind to session; may be needed in cluster scenarios
        try:
            edges_df.cache(); edges_df.count(); edges_df = spark.createDataFrame(edges_df.rdd, schema=edges_df.schema)
        except Exception:
            logger.exception("Failed to rebind edges_df")
        try:
            vertices_df.cache(); vertices_df.count(); vertices_df = spark.createDataFrame(vertices_df.rdd, schema=vertices_df.schema)
        except Exception:
            logger.exception("Failed to rebind vertices_df")

        g = GraphFrame(vertices_df, edges_df)
        start_time = time.time()
        results = g.pageRank(resetProbability=reset, maxIter=max_iter)
        pagerank_time = time.time() - start_time

        top_videos = results.vertices.orderBy("pagerank", ascending=False).limit(50)
        top_videos_pd = top_videos.toPandas()

        # Enrich with video metadata via Mongo
        mongo_local = get_mongo_connector()
        videos_collection = mongo_local.get_collection('videos')
        video_ids = top_videos_pd['id'].tolist()
        video_info = {v['_id']: v for v in videos_collection.find({"_id": {"$in": video_ids}}, {"_id": 1, "category": 1, "uploader": 1})}

        top_videos_pd['category'] = top_videos_pd['id'].map(lambda x: video_info.get(x, {}).get('category', 'Unknown'))
        top_videos_pd['uploader'] = top_videos_pd['id'].map(lambda x: video_info.get(x, {}).get('uploader', 'Unknown'))
        top_videos_pd['url'] = top_videos_pd['id'].map(youtube_url)
        top_videos_pd['short_url'] = top_videos_pd['id'].map(short_youtube_url)

        perf = {
            'execution_time': pagerank_time,
            'rows_processed': limit,
            'rows_returned': len(top_videos_pd),
            'query_type': 'PageRank Analysis',
            'timestamp': datetime.now()
        }

        return top_videos_pd, perf
    except Exception as e:
        logger.exception("PageRank computation failed: %s", e)
        raise


@st.cache_data(ttl=3600)
def _compute_communities(limit: int, max_iter: int):
    """Compute community detection and return communities summary and sample top communities."""
    try:
        spark_conn_local = get_spark_connector()
        try:
            from graphframes import GraphFrame
        except Exception as e:
            raise RuntimeError("GraphFrames not available or failed to import: " + str(e))

        spark = spark_conn_local.spark
        edges_df = spark.read.format("mongodb") \
            .option("database", "youtube_analytics") \
            .option("collection", "edges") \
            .load() \
            .limit(limit) \
            .select("src", "dst")

        src_vertices = edges_df.select("src").distinct().withColumnRenamed("src", "id")
        dst_vertices = edges_df.select("dst").distinct().withColumnRenamed("dst", "id")
        vertices_df = src_vertices.union(dst_vertices).distinct()

        try:
            edges_df.cache(); edges_df.count(); edges_df = spark.createDataFrame(edges_df.rdd, schema=edges_df.schema)
        except Exception:
            logger.exception("Failed to rebind edges_df for communities")
        try:
            vertices_df.cache(); vertices_df.count(); vertices_df = spark.createDataFrame(vertices_df.rdd, schema=vertices_df.schema)
        except Exception:
            logger.exception("Failed to rebind vertices_df for communities")

        g = GraphFrame(vertices_df, edges_df)
        community_start = time.time()
        result = g.labelPropagation(maxIter=max_iter)
        communities = result.groupBy("label").count().orderBy("count", ascending=False)
        communities_pd = communities.toPandas()
        community_time = time.time() - community_start

        # Sample top communities
        top_labels = communities_pd.head(5)['label'].tolist() if not communities_pd.empty else []
        top_communities = result.filter(result.label.isin([int(x) for x in top_labels])).limit(100).toPandas()

        # Enrich metadata for sample videos
        mongo_local = get_mongo_connector()
        video_ids = top_communities['id'].tolist() if 'id' in top_communities.columns else []
        videos_collection = mongo_local.get_collection('videos')
        video_info = {v['_id']: v for v in videos_collection.find({"_id": {"$in": video_ids}}, {"_id": 1, "category": 1, "uploader": 1})}

        perf = {
            'execution_time': community_time,
            'rows_processed': limit,
            'rows_returned': len(communities_pd),
            'query_type': 'Community Detection',
            'timestamp': datetime.now()
        }

        return communities_pd, top_communities, video_info, perf
    except Exception as e:
        logger.exception("Community computation failed: %s", e)
        raise


@st.cache_data(ttl=3600)
def _compute_centrality(limit: int, top_k: int = 100):
    """Compute centrality metrics focusing on top_k nodes; returns pandas DataFrames and perf metrics."""
    try:
        spark_conn_local = get_spark_connector()
        spark = spark_conn_local.spark
        edges_df = spark.read.format("mongodb") \
            .option("database", "youtube_analytics") \
            .option("collection", "edges") \
            .load() \
            .limit(limit) \
            .select("src", "dst")

        # Compute in/out degree in Spark
        out_deg = edges_df.groupBy("src").count().withColumnRenamed("src", "video_id").withColumnRenamed("count", "out_degree")
        in_deg = edges_df.groupBy("dst").count().withColumnRenamed("dst", "video_id").withColumnRenamed("count", "in_degree")
        deg_df = out_deg.join(in_deg, on="video_id", how="full_outer").na.fill(0)
        deg_df = deg_df.withColumn("total_degree", deg_df.out_degree + deg_df.in_degree).orderBy("total_degree", ascending=False).limit(top_k)
        deg_pd = deg_df.toPandas()

        # Use NetworkX for centrality on top_k subgraph
        import networkx as nx
        G = nx.DiGraph()
        # Build edges only for top nodes to avoid large memory use
        top_nodes = set(deg_pd['video_id'].tolist())
        # fetch edges from Mongo to use subgraph
        edges_collection = get_mongo_connector().get_collection('edges')
        # Query only edges where both src and dst in top_nodes
        cursor = edges_collection.find({"$and": [{"src": {"$in": list(top_nodes)}}, {"dst": {"$in": list(top_nodes)}}]}, {"_id": 0, "src": 1, "dst": 1})
        for e in cursor:
            if e.get('src') and e.get('dst'):
                G.add_edge(e['src'], e['dst'])

        # compute centrality metrics - sampling for large graphs
        betweenness = nx.betweenness_centrality(G, k=min(100, len(G.nodes()))) if len(G.nodes()) > 0 else {}
        closeness = nx.closeness_centrality(G) if len(G.nodes()) > 0 else {}

        centrality_data = []
        for node in G.nodes():
            in_d = G.in_degree(node)
            out_d = G.out_degree(node)
            centrality_data.append({
                'video_id': node,
                'in_degree': in_d,
                'out_degree': out_d,
                'total_degree': in_d + out_d,
                'betweenness': betweenness.get(node, 0),
                'closeness': closeness.get(node, 0)
            })

        centrality_df = pd.DataFrame(centrality_data)

        # Enrich with video metadata
        video_ids = centrality_df.nlargest(100, 'total_degree')['video_id'].tolist() if not centrality_df.empty else []
        videos_collection = get_mongo_connector().get_collection('videos')
        video_info = {v['_id']: v for v in videos_collection.find({"_id": {"$in": video_ids}}, {"_id": 1, "category": 1, "uploader": 1})}

        perf = {
            'execution_time': 0,  # approximate; not the actual wall time here
            'rows_processed': limit,
            'rows_returned': len(centrality_df),
            'query_type': 'Centrality Analysis',
            'timestamp': datetime.now()
        }
        return centrality_df, video_info, perf
    except Exception as e:
        logger.exception("Centrality computation failed: %s", e)
        raise


@st.cache_data(ttl=1800)
def _sample_edges_for_visualization(sample_size: int):
    """Return sampled edges and video metadata for visualization page."""
    try:
        mongo_local = get_mongo_connector()
        edges_collection = mongo_local.get_collection('edges')
        edge_sample = list(edges_collection.aggregate([
            {"$sample": {"size": sample_size * 3}},
            {"$project": {"src": 1, "dst": 1, "_id": 0}}
        ]))
        video_ids = list({e.get('src') for e in edge_sample if e.get('src')} | {e.get('dst') for e in edge_sample if e.get('dst')})
        videos_collection = mongo_local.get_collection('videos')
        video_info = {v['_id']: v for v in videos_collection.find({"_id": {"$in": video_ids}}, {"_id": 1, "category": 1, "uploader": 1})}
        return edge_sample, video_info
    except Exception as e:
        logger.exception("Edge sampling failed: %s", e)
        raise


# Initialize connections - use shared singleton instances
mongo = get_mongo_connector()
spark_conn = get_spark_connector()


# use youtube helpers from utils for links

# Header
st.title("Graph Analytics")
st.markdown("Advanced network analysis using Spark GraphFrames for large-scale graph mining")

# Tabs for different analyses
tab1, tab2, tab3, tab4 = st.tabs([
    "PageRank Analysis",
    "Community Detection", 
    "Centrality Metrics",
    "Network Visualization"
])

# ===================== TAB 1: PageRank Analysis =====================
with tab1:
    st.markdown("### PageRank Analysis")
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
    
    if st.button("Run PageRank Analysis", type="primary", key="pagerank_btn"):
        with st.spinner("Running PageRank algorithm on video network..."):
            try:
                # Use cached PageRank computation
                top_videos_pd, pagerank_perf = _compute_pagerank(pagerank_limit, pagerank_reset, pagerank_iter)
                total_time = pagerank_perf.get('execution_time', 0)
                pagerank_performance = {
                    'execution_time': total_time,
                    'rows_processed': pagerank_limit,
                    'rows_returned': len(top_videos_pd),
                    'query_type': 'PageRank Analysis',
                    'timestamp': datetime.now(),
                    'additional_metrics': {
                        'algorithm': 'GraphFrames PageRank',
                        'max_iterations': pagerank_iter,
                        'reset_probability': pagerank_reset,
                        'vertices_analyzed': len(top_videos_pd),
                        'pagerank_computation_time': pagerank_perf.get('execution_time', 0)
                    }
                }
                
                # Store results in session state and trigger reload so the session-state renderer shows them
                st.session_state['pagerank_results'] = {
                    'top_videos': top_videos_pd,
                    'performance': pagerank_performance,
                    'sample_size': pagerank_limit
                }
                st.success("PageRank completed successfully! Results saved and will render below.")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error running PageRank: {e}")
                logger.error(f"PageRank error: {e}", exc_info=True)
    
    # Display results if available in session state
    if 'pagerank_results' in st.session_state:
        results = st.session_state['pagerank_results']
        top_videos_pd = results['top_videos']
        pagerank_performance = results['performance']
        
        # Display performance metrics
        render_algorithm_performance(pagerank_performance)
        st.divider()
        
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
            st.plotly_chart(fig_top, width='stretch', key='pagerank_top_result')
        
        with col2:
            st.markdown("#### 📊 PageRank Distribution")
            fig_dist = px.histogram(
                top_videos_pd,
                x='pagerank',
                nbins=30,
                labels={'pagerank': 'PageRank Score', 'count': 'Number of Videos'},
                title="PageRank Score Distribution (Top 50)"
            )
            st.plotly_chart(fig_dist, width='stretch', key='pagerank_dist_result')
            
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
            st.plotly_chart(fig_cat, width='stretch', key='pagerank_cat_result')
        
        # Data table
        st.markdown("#### 📋 Top 50 Videos by PageRank")
        display_df = top_videos_pd[['id', 'pagerank', 'category', 'uploader']].copy()
        display_df.columns = ['Video ID', 'PageRank Score', 'Category', 'Uploader']
        display_df['Video'] = display_df['Video ID'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
        display_df['Uploader'] = display_df['Uploader'].str[:30]
        display_df['PageRank Score'] = display_df['PageRank Score'].round(6)
        display_df = display_df[['Video', 'PageRank Score', 'Category', 'Uploader']]
        st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)

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
                communities_pd, top_communities, video_info, community_perf = _compute_communities(community_limit, max_iterations)
                community_performance = {
                    'execution_time': community_perf.get('execution_time', 0),
                    'rows_processed': community_limit,
                    'rows_returned': len(communities_pd),
                    'query_type': 'Community Detection',
                    'timestamp': datetime.now(),
                    'additional_metrics': {
                        'algorithm': 'Label Propagation',
                        'max_iterations': max_iterations,
                        'communities_found': len(communities_pd),
                        'computation_time': community_perf.get('execution_time', 0),
                        'largest_community': communities_pd['count'].max() if not communities_pd.empty else 0
                    }
                }
                
                # Save results in session state and trigger rerun so render uses the session-state branch
                st.session_state['community_results'] = {
                    'communities': communities_pd,
                    'top_communities': top_communities,
                    'community_cats': None,
                    'performance': community_performance,
                    'video_info': video_info
                }
                st.success(f"Found {len(communities_pd)} communities! Results saved and will render below.")
                st.rerun()
                # (Results stored above and renderer will display them via session state)
                
            except Exception as e:
                st.error(f"Error detecting communities: {e}")
                logger.error(f"Community detection error: {e}", exc_info=True)
    
    # Display results if available in session state
    if 'community_results' in st.session_state:
        results = st.session_state['community_results']
        communities_pd = results['communities']
        community_performance = results['performance']
        # Ensure community categories are available (compute if not present)
        community_cats = results.get('community_cats')
        top_communities = results.get('top_communities')
        if community_cats is None and top_communities is not None and 'id' in top_communities.columns:
            community_cats = top_communities.groupby(['label', 'category']).size().reset_index(name='count')
        
        # Display performance metrics
        render_algorithm_performance(community_performance)
        st.divider()
        
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
            st.plotly_chart(fig_sizes, width='stretch', key='community_sizes_result')
        
        with col2:
            st.markdown("#### 🥧 Community Size Categories")
            size_bins = pd.cut(
                communities_pd['count'],
                bins=[0, 10, 50, 100, 500, float('inf')],
                labels=['Tiny (1-10)', 'Small (11-50)', 'Medium (51-100)', 'Large (101-500)', 'Huge (500+)']
            )
            size_dist = size_bins.value_counts().reset_index(name='count')
            size_dist.columns = ['Size Category', 'Number of Communities']
            
            fig_pie = px.pie(
                size_dist,
                values='Number of Communities',
                names='Size Category',
                title="Distribution of Community Sizes"
            )
            st.plotly_chart(fig_pie, width='stretch', key='community_pie_result')
        
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
        
        # Show sample videos from top communities
        st.markdown("#### 🔍 Sample Videos from Top Communities")
        top_communities = results['top_communities']
        community_cats = results['community_cats']
        
        # Category distribution per community
        fig_community_cats = px.bar(
            community_cats,
            x='label',
            y='count',
            color='category',
            labels={'label': 'Community ID', 'count': 'Number of Videos'},
            title="Category Distribution in Top 5 Communities",
            barmode='stack'
        )
        st.plotly_chart(fig_community_cats, width='stretch', key='community_cats_result')

        # Show clickable list of sample videos
        if 'id' in top_communities.columns:
            display_df = top_communities[['Video', 'label', 'category']].copy()
            display_df.columns = ['Video', 'Community ID', 'Category']
            st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)

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
                # Compute centrality with caching; results are returned as smaller pandas DataFrames
                centrality_df, video_info, centrality_perf = _compute_centrality(centrality_limit, top_k=100)
                total_time = centrality_perf.get('execution_time', 0)
                centrality_performance = {
                    'execution_time': total_time,
                    'rows_processed': centrality_limit,
                    'rows_returned': len(centrality_df),
                    'query_type': 'Centrality Analysis',
                    'timestamp': datetime.now(),
                    'additional_metrics': {
                        'library': 'NetworkX',
                        'nodes_analyzed': len(centrality_df),
                        'edges_analyzed': 0,
                        'degree_time': 0,
                        'betweenness_time': 0,
                        'closeness_time': 0,
                        'total_computation_time': centrality_perf.get('execution_time', 0)
                    }
                }
                
                # Display results
                st.success("Centrality metrics calculated!")
                
                # Display performance metrics
                render_algorithm_performance(centrality_performance)
                st.divider()
                
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
                st.session_state['centrality_results'] = {
                    'centrality_df': centrality_df,
                    'video_info': video_info,
                    'performance': centrality_performance
                }
                st.success("Centrality metrics calculated and saved. Rendering below.")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error calculating centrality: {e}")
                logger.error(f"Centrality error: {e}", exc_info=True)
    
    # Display results if available in session state
    if 'centrality_results' in st.session_state:
        results = st.session_state['centrality_results']
        centrality_df = results['centrality_df']
        video_info = results['video_info']
        centrality_performance = results['performance']
        
        # Display performance metrics
        render_algorithm_performance(centrality_performance)
        st.divider()
        
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
            st.plotly_chart(fig_degree, width='stretch', key='centrality_degree_result')
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
            st.plotly_chart(fig_between, width='stretch', key='centrality_between_result')
        
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
        st.plotly_chart(fig_scatter, width='stretch', key='centrality_scatter_result')

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
                start_time = time.time()
                
                # Sample edges using cached helper
                edge_sample, video_info = _sample_edges_for_visualization(vis_sample_size)
                
                # Build NetworkX graph
                graph_start = time.time()
                G = nx.DiGraph()
                for edge in edge_sample:
                    if edge.get('src') and edge.get('dst'):
                        G.add_edge(edge['src'], edge['dst'])
                
                # Limit to top connected nodes
                degrees = dict(G.degree())
                top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:vis_sample_size]
                top_node_ids = [n[0] for n in top_nodes]
                G = G.subgraph(top_node_ids).copy()
                graph_build_time = time.time() - graph_start
                
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
                    pagerank_start = time.time()
                    pagerank_scores = nx.pagerank(G, max_iter=20)
                    pagerank_time = time.time() - pagerank_start
                else:
                    pagerank_scores = {}
                    pagerank_time = 0
                
                # Set node attributes
                for node in G.nodes():
                    G.nodes[node]['category'] = video_info.get(node, {}).get('category', 'Unknown')
                    G.nodes[node]['uploader'] = video_info.get(node, {}).get('uploader', 'Unknown')
                    G.nodes[node]['degree'] = G.degree(node)
                    G.nodes[node]['pagerank'] = pagerank_scores.get(node, 0)
                
                # Calculate layout
                layout_start = time.time()
                if vis_layout == "Spring":
                    pos = nx.spring_layout(G, k=0.5, iterations=50)
                elif vis_layout == "Circular":
                    pos = nx.circular_layout(G)
                elif vis_layout == "Kamada-Kawai":
                    pos = nx.kamada_kawai_layout(G)
                else:
                    pos = nx.shell_layout(G)
                layout_time = time.time() - layout_start
                
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
                
                total_time = time.time() - start_time
                
                # Create performance metrics
                viz_performance = {
                    'execution_time': total_time,
                    'rows_processed': vis_sample_size * 3,  # Sampled edges
                    'rows_returned': len(G.nodes()),
                    'query_type': 'Network Visualization',
                    'timestamp': datetime.now(),
                    'additional_metrics': {
                        'layout_algorithm': vis_layout,
                        'color_attribute': vis_color,
                        'graph_build_time': graph_build_time,
                        'layout_time': layout_time,
                        'pagerank_time': pagerank_time,
                        'nodes_visualized': len(G.nodes()),
                        'edges_visualized': len(G.edges())
                    }
                }
                
                # Store visualization results in session state and refresh the page to show below
                density = nx.density(G)
                avg_degree = sum(degrees.values()) / len(degrees) if degrees else 0
                st.session_state['visualization_results'] = {
                    'fig': fig,
                    'graph_stats': {
                        'nodes': len(G.nodes()),
                        'edges': len(G.edges()),
                        'density': density,
                        'avg_degree': avg_degree
                    },
                    'performance': viz_performance
                }
                st.success("Visualization generated and saved. Rendering below.")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error generating visualization: {e}")
                logger.error(f"Visualization error: {e}", exc_info=True)
    
    # Display results if available in session state
    if 'visualization_results' in st.session_state:
        results = st.session_state['visualization_results']
        fig = results['fig']
        graph_stats = results['graph_stats']
        viz_performance = results['performance']
        
        # Display performance metrics
        render_algorithm_performance(viz_performance)
        st.divider()
        
        # Display the graph
        st.plotly_chart(fig, width='stretch', key='vis_fig_result')
        
        # Graph statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Nodes", graph_stats['nodes'])
        with col2:
            st.metric("Edges", graph_stats['edges'])
        with col3:
            st.metric("Density", f"{graph_stats['density']:.4f}")
        with col4:
            st.metric("Avg Degree", f"{graph_stats['avg_degree']:.2f}")

# Footer
st.markdown("---")
st.markdown("*Graph Analytics powered by Apache Spark GraphFrames and NetworkX*")
