"""
Network Statistics Page
Displays degree distribution and categorized statistics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Network Statistics - YouTube Analyzer",
    page_icon="📊",
    layout="wide"
)

# Header
st.title("📊 Network Statistics")
st.markdown("Analyze degree distribution and categorized video statistics")

# Tabs for different statistics
tab1, tab2 = st.tabs(["🔢 Degree Distribution", "📂 Categorized Statistics"])

with tab1:
    st.markdown("### Degree Distribution Analysis")
    st.markdown("Analyze in-degree, out-degree, and total degree distributions for the video network.")
    
    # Sample size selector
    col1, col2 = st.columns([3, 1])
    with col1:
        sample_size = st.slider(
            "Sample Size (edges)",
            min_value=1000,
            max_value=100000,
            value=50000,
            step=1000,
            help="Number of edges to sample for analysis"
        )
    with col2:
        run_analysis = st.button("🔄 Run Analysis", width="stretch", type="primary")
    
    if run_analysis:
        with st.spinner("Computing degree statistics..."):
            try:
                from utils import get_spark_connector
                from analytics import DegreeAnalytics
                
                # Initialize analytics
                spark_conn = get_spark_connector()
                degree_analytics = DegreeAnalytics(spark_conn)
                
                # Run analysis
                results = degree_analytics.run_full_analysis(sample_size=sample_size)
                
                # Store results in session state
                st.session_state['degree_results'] = results
                st.success("✅ Analysis complete!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Degree analysis error: {e}", exc_info=True)
    
    # Display results if available
    if 'degree_results' in st.session_state:
        results = st.session_state['degree_results']
        agg_stats = results['aggregate_stats']
        
        # Summary Statistics
        st.markdown("#### Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Avg Total Degree", f"{agg_stats['avg_degree']:.2f}")
        col2.metric("Max Degree", f"{agg_stats['max_degree']:,}")
        col3.metric("Min Degree", f"{agg_stats['min_degree']:,}")
        col4.metric("Std Dev", f"{agg_stats['std_degree']:.2f}")
        
        # Degree Distribution Histogram
        st.markdown("#### Degree Distribution")
        dist_data = results['distribution']
        if dist_data:
            df_dist = pd.DataFrame(dist_data)
            fig = px.bar(
                df_dist,
                x='degree',
                y='count',
                title='Video Degree Distribution',
                labels={'degree': 'Total Degree', 'count': 'Number of Videos'}
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No distribution data available")
        
        # Top 10 Videos by Degree
        st.markdown("#### Top 10 Videos by Degree")
        top_videos = results['top_videos']
        if top_videos:
            df_top = pd.DataFrame(top_videos)
            df_top.index = range(1, len(df_top) + 1)
            st.dataframe(
                df_top,
                column_config={
                    "id": "Video ID",
                    "total_degree": st.column_config.NumberColumn("Total Degree", format="%d"),
                    "in_degree": st.column_config.NumberColumn("In-Degree", format="%d"),
                    "out_degree": st.column_config.NumberColumn("Out-Degree", format="%d")
                },
                use_container_width=True
            )
        else:
            st.info("No top videos data available")
    else:
        st.info("👆 Click 'Run Analysis' to compute degree statistics")

with tab2:
    st.markdown("### Categorized Statistics")
    st.markdown("View video statistics grouped by category, length, and view count.")
    
    # Sample size selector
    col1, col2 = st.columns([3, 1])
    with col1:
        cat_sample_size = st.slider(
            "Sample Size (videos)",
            min_value=1000,
            max_value=100000,
            value=50000,
            step=1000,
            help="Number of videos to sample for analysis",
            key="cat_sample"
        )
    with col2:
        run_cat_analysis = st.button("🔄 Run Analysis", width="stretch", type="primary", key="cat_run")
    
    if run_cat_analysis:
        with st.spinner("Computing categorized statistics..."):
            try:
                from utils import get_spark_connector
                from analytics import CategoryAnalytics
                
                # Initialize analytics
                spark_conn = get_spark_connector()
                cat_analytics = CategoryAnalytics(spark_conn)
                
                # Run analysis
                results = cat_analytics.run_full_analysis(sample_size=cat_sample_size)
                
                # Store results in session state
                st.session_state['category_results'] = results
                st.success("✅ Analysis complete!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Category analysis error: {e}", exc_info=True)
    
    # Display results if available
    if 'category_results' in st.session_state:
        results = st.session_state['category_results']
        
        # Videos by Category
        st.markdown("#### Videos by Category")
        if results['by_category']:
            df_cat = pd.DataFrame(results['by_category'])
            # Show top 15 categories
            df_cat_top = df_cat.head(15)
            fig_cat = px.bar(
                df_cat_top,
                x='category',
                y='count',
                title='Top 15 Video Categories',
                labels={'category': 'Category', 'count': 'Number of Videos'}
            )
            fig_cat.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("No category data available")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Length Distribution")
            if results['by_length']:
                df_length = pd.DataFrame(results['by_length'])
                fig_length = px.pie(
                    df_length,
                    names='bucket',
                    values='count',
                    title='Videos by Duration'
                )
                st.plotly_chart(fig_length, use_container_width=True)
            else:
                st.info("No length data available")
        
        with col2:
            st.markdown("#### View Count Distribution")
            if results['by_views']:
                df_views = pd.DataFrame(results['by_views'])
                fig_views = px.pie(
                    df_views,
                    names='bucket',
                    values='count',
                    title='Videos by View Count'
                )
                st.plotly_chart(fig_views, use_container_width=True)
            else:
                st.info("No view count data available")
    else:
        st.info("👆 Click 'Run Analysis' to compute categorized statistics")

st.markdown("---")
st.markdown("### 💾 Save Results to Archives")
st.markdown("Save your analysis results to the `youtube_analytics_archives` database with timestamps.")

col1, col2 = st.columns(2)

with col1:
    if st.button("Save Degree Stats to MongoDB", disabled='degree_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['degree_results']
            
            # Get archives database
            archives_db = mongo.client['youtube_analytics_archives']
            
            # Create timestamp for collection names
            now = datetime.now()
            date_str = now.strftime("%m-%d-%Y")
            time_str = now.strftime("%H:%M:%S")
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            # Get sample size from session or use default
            edges_count = results['aggregate_stats'].get('num_vertices', 'unknown')
            
            # Save aggregate stats
            agg_collection_name = f"degree_aggregate_stats_{timestamp_str}_{edges_count}_vertices"
            agg_collection = archives_db[agg_collection_name]
            agg_collection.insert_one({
                'type': 'aggregate_statistics',
                'created_date': date_str,
                'created_time': time_str,
                'sample_size': sample_size,
                **results['aggregate_stats']
            })
            
            # Save top videos
            top_collection_name = f"degree_top_videos_{timestamp_str}_{edges_count}_vertices"
            top_collection = archives_db[top_collection_name]
            if results['top_videos']:
                for video in results['top_videos']:
                    video['created_date'] = date_str
                    video['created_time'] = time_str
                top_collection.insert_many(results['top_videos'])
            
            # Save distribution
            dist_collection_name = f"degree_distribution_{timestamp_str}_{edges_count}_vertices"
            dist_collection = archives_db[dist_collection_name]
            if results['distribution']:
                for dist in results['distribution']:
                    dist['created_date'] = date_str
                    dist['created_time'] = time_str
                dist_collection.insert_many(results['distribution'])
            
            st.success(f"✅ Degree statistics saved to `youtube_analytics_archives`!")
            st.info(f"""
            **Saved Collections:**
            - `{agg_collection_name}`
            - `{top_collection_name}`
            - `{dist_collection_name}`
            """)
            logger.info(f"Degree statistics saved to youtube_analytics_archives at {date_str} {time_str}")
        except Exception as e:
            st.error(f"Error saving degree stats: {e}")
            logger.error(f"Error saving degree stats: {e}", exc_info=True)

with col2:
    if st.button("Save Category Stats to MongoDB", disabled='category_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['category_results']
            
            # Get archives database
            archives_db = mongo.client['youtube_analytics_archives']
            
            # Create timestamp for collection names
            now = datetime.now()
            date_str = now.strftime("%m-%d-%Y")
            time_str = now.strftime("%H:%M:%S")
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            saved_collections = []
            
            # Save category distribution
            if results['by_category']:
                cat_collection_name = f"category_distribution_{timestamp_str}_{cat_sample_size}_videos"
                cat_collection = archives_db[cat_collection_name]
                for cat in results['by_category']:
                    cat['created_date'] = date_str
                    cat['created_time'] = time_str
                cat_collection.insert_many(results['by_category'])
                saved_collections.append(cat_collection_name)
            
            # Save length distribution
            if results['by_length']:
                len_collection_name = f"length_distribution_{timestamp_str}_{cat_sample_size}_videos"
                len_collection = archives_db[len_collection_name]
                for length in results['by_length']:
                    length['created_date'] = date_str
                    length['created_time'] = time_str
                len_collection.insert_many(results['by_length'])
                saved_collections.append(len_collection_name)
            
            # Save view distribution
            if results['by_views']:
                view_collection_name = f"view_distribution_{timestamp_str}_{cat_sample_size}_videos"
                view_collection = archives_db[view_collection_name]
                for view in results['by_views']:
                    view['created_date'] = date_str
                    view['created_time'] = time_str
                view_collection.insert_many(results['by_views'])
                saved_collections.append(view_collection_name)
            
            st.success(f"✅ Category statistics saved to `youtube_analytics_archives`!")
            st.info(f"""
            **Saved Collections:**
            {chr(10).join([f'- `{c}`' for c in saved_collections])}
            """)
            logger.info(f"Category statistics saved to youtube_analytics_archives at {date_str} {time_str}")
        except Exception as e:
            st.error(f"Error saving category stats: {e}")
            logger.error(f"Error saving category stats: {e}", exc_info=True)
