"""
Top-K Queries Page
Find top k categories, most viewed videos, and highest rated videos.
Uses TopKAnalytics module for Spark queries.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Top-K Queries - YouTube Analyzer",
    page_icon="🔍",
    layout="wide"
)

# Header
st.title("🔍 Top-K Queries")
st.markdown("Find the most popular videos, categories, and highest rated content")

# Tabs for different query types
tab1, tab2, tab3 = st.tabs(["📂 Top Categories", "👀 Most Viewed Videos", "⭐ Highest Rated Videos"])

# ============== TAB 1: Top K Categories ==============
with tab1:
    st.markdown("### Top K Categories by Video Count")
    st.markdown("Find the categories with the most uploaded videos.")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        k_categories = st.slider(
            "Number of Categories (K)",
            min_value=5,
            max_value=50,
            value=10,
            step=5,
            help="Number of top categories to retrieve",
            key="k_cat"
        )
    with col2:
        run_cat_query = st.button("🔍 Find Top Categories", type="primary", key="run_cat")
    
    if run_cat_query:
        with st.spinner(f"Finding top {k_categories} categories..."):
            try:
                from utils import get_spark_connector
                from analytics import TopKAnalytics
                
                spark_conn = get_spark_connector()
                topk = TopKAnalytics(spark_conn)
                
                results = topk.get_top_k_categories(k_categories)
                
                st.session_state['top_categories_results'] = {
                    'data': results,
                    'k': k_categories
                }
                st.success(f"✅ Found top {k_categories} categories!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Top categories query error: {e}", exc_info=True)
    
    # Display results
    if 'top_categories_results' in st.session_state:
        results = st.session_state['top_categories_results']
        data = results['data']
        
        if data:
            df = pd.DataFrame(data)
            df.index = range(1, len(df) + 1)
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.markdown("#### Results Table")
                st.dataframe(
                    df,
                    column_config={
                        "category": "Category",
                        "video_count": st.column_config.NumberColumn("Video Count", format="%d")
                    },
                    use_container_width=True
                )
            
            with col2:
                st.markdown("#### Distribution Chart")
                fig = px.bar(
                    df,
                    x='category',
                    y='video_count',
                    title=f'Top {results["k"]} Categories by Video Count',
                    labels={'category': 'Category', 'video_count': 'Number of Videos'}
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data found")
    else:
        st.info("👆 Click 'Find Top Categories' to run the query")


# ============== TAB 2: Most Viewed Videos ==============
with tab2:
    st.markdown("### Top K Most Viewed Videos")
    st.markdown("Find the videos with the highest view counts.")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        k_views = st.slider(
            "Number of Videos (K)",
            min_value=5,
            max_value=100,
            value=10,
            step=5,
            help="Number of top viewed videos to retrieve",
            key="k_views"
        )
    with col2:
        run_views_query = st.button("🔍 Find Most Viewed", type="primary", key="run_views")
    
    if run_views_query:
        with st.spinner(f"Finding top {k_views} most viewed videos..."):
            try:
                from utils import get_spark_connector
                from analytics import TopKAnalytics
                
                spark_conn = get_spark_connector()
                topk = TopKAnalytics(spark_conn)
                
                results = topk.get_top_k_most_viewed(k_views)
                
                st.session_state['top_viewed_results'] = {
                    'data': results,
                    'k': k_views
                }
                st.success(f"✅ Found top {k_views} most viewed videos!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Top viewed query error: {e}", exc_info=True)
    
    # Display results
    if 'top_viewed_results' in st.session_state:
        results = st.session_state['top_viewed_results']
        data = results['data']
        
        if data:
            df = pd.DataFrame(data)
            df.index = range(1, len(df) + 1)
            
            st.markdown("#### Results Table")
            st.dataframe(
                df,
                column_config={
                    "video_id": "Video ID",
                    "views": st.column_config.NumberColumn("Views", format="%d"),
                    "category": "Category",
                    "rating": st.column_config.NumberColumn("Rating", format="%.2f"),
                    "num_ratings": st.column_config.NumberColumn("# Ratings", format="%d")
                },
                use_container_width=True
            )
            
            st.markdown("#### Views Distribution")
            fig = px.bar(
                df.head(20),
                x='video_id',
                y='views',
                color='category',
                title=f'Top {min(20, results["k"])} Videos by View Count',
                labels={'video_id': 'Video ID', 'views': 'Views'}
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No view data found")
    else:
        st.info("👆 Click 'Find Most Viewed' to run the query")


# ============== TAB 3: Highest Rated Videos ==============
with tab3:
    st.markdown("### Top K Highest Rated Videos")
    st.markdown("Find the videos with the highest ratings.")
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        k_rated = st.slider(
            "Number of Videos (K)",
            min_value=5,
            max_value=100,
            value=10,
            step=5,
            help="Number of top rated videos to retrieve",
            key="k_rated"
        )
    with col2:
        min_ratings = st.slider(
            "Minimum Ratings Required",
            min_value=0,
            max_value=1000,
            value=100,
            step=50,
            help="Minimum number of ratings required (filters out videos with few ratings)",
            key="min_ratings"
        )
    with col3:
        run_rated_query = st.button("🔍 Find Top Rated", type="primary", key="run_rated")
    
    if run_rated_query:
        with st.spinner(f"Finding top {k_rated} highest rated videos..."):
            try:
                from utils import get_spark_connector
                from analytics import TopKAnalytics
                
                spark_conn = get_spark_connector()
                topk = TopKAnalytics(spark_conn)
                
                results = topk.get_top_k_highest_rated(k_rated, min_ratings)
                
                st.session_state['top_rated_results'] = {
                    'data': results,
                    'k': k_rated,
                    'min_ratings': min_ratings
                }
                st.success(f"✅ Found top {k_rated} highest rated videos (min {min_ratings} ratings)!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Top rated query error: {e}", exc_info=True)
    
    # Display results
    if 'top_rated_results' in st.session_state:
        results = st.session_state['top_rated_results']
        data = results['data']
        
        if data:
            df = pd.DataFrame(data)
            df.index = range(1, len(df) + 1)
            
            st.markdown("#### Results Table")
            st.dataframe(
                df,
                column_config={
                    "video_id": "Video ID",
                    "rating": st.column_config.NumberColumn("Rating", format="%.2f"),
                    "num_ratings": st.column_config.NumberColumn("# Ratings", format="%d"),
                    "views": st.column_config.NumberColumn("Views", format="%d"),
                    "category": "Category"
                },
                use_container_width=True
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### Rating Distribution")
                fig = px.bar(
                    df.head(20),
                    x='video_id',
                    y='rating',
                    color='category',
                    title=f'Top {min(20, results["k"])} Videos by Rating',
                    labels={'video_id': 'Video ID', 'rating': 'Rating'}
                )
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### Rating vs Views")
                fig2 = px.scatter(
                    df,
                    x='views',
                    y='rating',
                    color='category',
                    size='num_ratings',
                    hover_data=['video_id'],
                    title='Rating vs Views Correlation',
                    labels={'views': 'Views', 'rating': 'Rating'}
                )
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No rated videos found with the specified criteria")
    else:
        st.info("👆 Click 'Find Top Rated' to run the query")


# ============== Save Results Section ==============
st.markdown("---")
st.markdown("### 💾 Save Results to Archives")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("Save Top Categories", disabled='top_categories_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['top_categories_results']
            
            archives_db = mongo.client['youtube_analytics_archives']
            now = datetime.now()
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            collection_name = f"top_{results['k']}_categories_{timestamp_str}"
            collection = archives_db[collection_name]
            
            # Make copies to avoid modifying session state
            data_to_save = []
            for item in results['data']:
                save_item = item.copy()
                save_item['created_date'] = now.strftime("%m-%d-%Y")
                save_item['created_time'] = now.strftime("%H:%M:%S")
                save_item['k_value'] = results['k']
                data_to_save.append(save_item)
            
            collection.insert_many(data_to_save)
            st.success(f"✅ Saved to `{collection_name}`!")
            
        except Exception as e:
            st.error(f"Error: {e}")

with col2:
    if st.button("Save Most Viewed", disabled='top_viewed_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['top_viewed_results']
            
            archives_db = mongo.client['youtube_analytics_archives']
            now = datetime.now()
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            collection_name = f"top_{results['k']}_most_viewed_{timestamp_str}"
            collection = archives_db[collection_name]
            
            # Make copies to avoid modifying session state
            data_to_save = []
            for item in results['data']:
                save_item = item.copy()
                save_item['created_date'] = now.strftime("%m-%d-%Y")
                save_item['created_time'] = now.strftime("%H:%M:%S")
                save_item['k_value'] = results['k']
                data_to_save.append(save_item)
            
            collection.insert_many(data_to_save)
            st.success(f"✅ Saved to `{collection_name}`!")
            
        except Exception as e:
            st.error(f"Error: {e}")

with col3:
    if st.button("Save Top Rated", disabled='top_rated_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['top_rated_results']
            
            archives_db = mongo.client['youtube_analytics_archives']
            now = datetime.now()
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            collection_name = f"top_{results['k']}_highest_rated_{timestamp_str}"
            collection = archives_db[collection_name]
            
            # Make copies to avoid modifying session state
            data_to_save = []
            for item in results['data']:
                save_item = item.copy()
                save_item['created_date'] = now.strftime("%m-%d-%Y")
                save_item['created_time'] = now.strftime("%H:%M:%S")
                save_item['k_value'] = results['k']
                save_item['min_ratings_filter'] = results['min_ratings']
                data_to_save.append(save_item)
            
            collection.insert_many(data_to_save)
            st.success(f"✅ Saved to `{collection_name}`!")
            
        except Exception as e:
            st.error(f"Error: {e}")
