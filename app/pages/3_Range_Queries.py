"""
Range Queries Page
Filter videos by category, duration, views, and other criteria.
Uses RangeQueryAnalytics module for Spark queries.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List
import logging
import sys
from pathlib import Path
from components import render_video_link
from utils.youtube_helpers import youtube_url, short_youtube_url, add_youtube_links_to_df

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Range Queries - YouTube Analyzer",
    page_icon="🎯",
    layout="wide"
)

# Header
st.title("🎯 Range Queries")
st.markdown("Filter videos by category, duration, views, and other criteria")

# Tabs for different query types
tab1, tab2 = st.tabs(["⏱️ Duration Range", "👀 Views Range"])

# ============== TAB 1: Duration Range Query ==============
with tab1:
    st.markdown("### Filter Videos by Duration")
    st.markdown("Find all videos in a specific category with duration within a specified range.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Get categories for dropdown
        try:
            from utils import get_mongo_connector
            mongo = get_mongo_connector()
            categories = mongo.db['videos'].distinct('category')
            categories = sorted([c for c in categories if c])  # Filter out None/empty
        except:
            categories = ['Music', 'Entertainment', 'Sports', 'News', 'Comedy', 'Film', 'Education']
        
        selected_category = st.selectbox(
            "Select Category",
            options=categories,
            index=0 if categories else None,
            help="Choose a video category to filter",
            key="duration_category"
        )
    
    with col2:
        duration_range = st.slider(
            "Duration Range (seconds)",
            min_value=0,
            max_value=3600,
            value=(200, 500),
            step=10,
            help="Select minimum and maximum video duration in seconds",
            key="duration_range"
        )
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        max_results = st.slider(
            "Maximum Results",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            help="Limit the number of results returned",
            key="duration_max"
        )
    with col3:
        run_duration_query = st.button("🔍 Search", type="primary", key="run_duration")
    
    if run_duration_query:
        with st.spinner(f"Finding {selected_category} videos with duration {duration_range[0]}-{duration_range[1]} seconds..."):
            try:
                from utils import get_spark_connector
                from analytics import RangeQueryAnalytics
                
                spark_conn = get_spark_connector()
                range_analytics = RangeQueryAnalytics(spark_conn)
                
                results = range_analytics.query_by_duration(
                    category=selected_category,
                    min_duration=duration_range[0],
                    max_duration=duration_range[1],
                    max_results=max_results
                )
                
                st.session_state['duration_range_results'] = {
                    'data': results,
                    'category': selected_category,
                    'min_duration': duration_range[0],
                    'max_duration': duration_range[1]
                }
                st.success(f"✅ Found {len(results)} videos!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Duration range query error: {e}", exc_info=True)
    
    # Display results
    if 'duration_range_results' in st.session_state:
        results = st.session_state['duration_range_results']
        data = results['data']
        
        if data:
            df = pd.DataFrame(data)
            df.index = range(1, len(df) + 1)
            # Add URLs for quick access
            if 'video_id' in df.columns:
                df['url'] = df['video_id'].map(youtube_url)
                df['short_url'] = df['video_id'].map(short_youtube_url)
            # Add URLs for the results for quick links
            if 'video_id' in df.columns:
                df['url'] = df['video_id'].map(youtube_url)
                df['short_url'] = df['video_id'].map(short_youtube_url)
            
            st.markdown(f"#### Results: {results['category']} videos ({results['min_duration']}-{results['max_duration']} sec)")
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # Convert video_id column to clickable links and render as HTML table
                if 'video_id' in df.columns:
                    df['Video'] = df['video_id'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
                    display_df = df[['Video', 'duration_formatted', 'views', 'rating']].copy()
                    display_df.columns = ['Video', 'Duration', 'Views', 'Rating']
                    st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)
                else:
                    st.dataframe(
                        df[['duration_formatted', 'views', 'rating']],
                        column_config={
                            "duration_formatted": "Duration",
                            "views": st.column_config.NumberColumn("Views", format="%d"),
                            "rating": st.column_config.NumberColumn("Rating", format="%.2f")
                        },
                        use_container_width=True
                    )
            
            with col2:
                fig = px.histogram(
                    df,
                    x='duration_sec',
                    nbins=20,
                    title=f'Duration Distribution ({results["category"]})',
                    labels={'duration_sec': 'Duration (seconds)', 'count': 'Number of Videos'}
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No videos found matching the criteria")
    else:
        st.info("👆 Select a category and duration range, then click 'Search'")


# ============== TAB 2: Views Range Query ==============
with tab2:
    st.markdown("### Filter Videos by View Count")
    st.markdown("Find all videos with view counts within a specified range.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Get categories for dropdown
        try:
            from utils import get_mongo_connector
            mongo = get_mongo_connector()
            categories = mongo.db['videos'].distinct('category')
            categories = ['All Categories'] + sorted([c for c in categories if c])
        except:
            categories = ['All Categories', 'Music', 'Entertainment', 'Sports', 'News', 'Comedy']
        
        views_category = st.selectbox(
            "Select Category (optional)",
            options=categories,
            index=0,
            help="Filter by category or select 'All Categories'",
            key="views_category"
        )
    
    with col2:
        views_range = st.slider(
            "Views Range",
            min_value=0,
            max_value=10_000_000,
            value=(10_000, 1_000_000),
            step=10_000,
            format="%d",
            help="Select minimum and maximum view count",
            key="views_range"
        )
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        views_max_results = st.slider(
            "Maximum Results",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            help="Limit the number of results returned",
            key="views_max"
        )
    with col3:
        run_views_query = st.button("🔍 Search", type="primary", key="run_views_range")
    
    if run_views_query:
        with st.spinner(f"Finding videos with {views_range[0]:,}-{views_range[1]:,} views..."):
            try:
                from utils import get_spark_connector
                from analytics import RangeQueryAnalytics
                
                spark_conn = get_spark_connector()
                range_analytics = RangeQueryAnalytics(spark_conn)
                
                results = range_analytics.query_by_views(
                    min_views=views_range[0],
                    max_views=views_range[1],
                    category=views_category if views_category != 'All Categories' else None,
                    max_results=views_max_results
                )
                
                st.session_state['views_range_results'] = {
                    'data': results,
                    'category': views_category,
                    'min_views': views_range[0],
                    'max_views': views_range[1]
                }
                st.success(f"✅ Found {len(results)} videos!")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                logger.error(f"Views range query error: {e}", exc_info=True)
    
    # Display results
    if 'views_range_results' in st.session_state:
        results = st.session_state['views_range_results']
        data = results['data']
        
        if data:
            df = pd.DataFrame(data)
            df.index = range(1, len(df) + 1)
            
            category_text = results['category'] if results['category'] != 'All Categories' else 'all categories'
            st.markdown(f"#### Results: Videos in {category_text} with {results['min_views']:,}-{results['max_views']:,} views")
            
            # Convert video_id to clickable links in the results table
            if 'video_id' in df.columns:
                df['Video'] = df['video_id'].map(lambda v: f'<a href="{youtube_url(v)}" target="_blank">{v}</a>')
                display_df = df.copy()
                col_order = ['Video'] + [c for c in df.columns if c not in ['video_id', 'Video']]
                display_df = display_df[col_order]
                st.write(display_df.to_html(escape=False, index=False), unsafe_allow_html=True)
            else:
                st.dataframe(
                    df,
                    column_config={
                        "views": st.column_config.NumberColumn("Views", format="%d"),
                        "rating": st.column_config.NumberColumn("Rating", format="%.2f"),
                        "num_ratings": st.column_config.NumberColumn("# Ratings", format="%d"),
                        "category": "Category",
                        "duration_sec": st.column_config.NumberColumn("Duration (s)", format="%d")
                    },
                    use_container_width=True
                )
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.histogram(
                    df,
                    x='views',
                    nbins=20,
                    color='category',
                    title='Views Distribution',
                    labels={'views': 'Views', 'count': 'Number of Videos'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Category breakdown
                cat_counts = df['category'].value_counts().reset_index()
                cat_counts.columns = ['category', 'count']
                fig2 = px.pie(
                    cat_counts,
                    names='category',
                    values='count',
                    title='Category Distribution in Results'
                )
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No videos found matching the criteria")
    else:
        st.info("👆 Select criteria and click 'Search'")


# ============== Save Results Section ==============
st.markdown("---")
st.markdown("### 💾 Save Results to Archives")

col1, col2 = st.columns(2)

with col1:
    if st.button("Save Duration Query", disabled='duration_range_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['duration_range_results']
            
            archives_db = mongo.client['youtube_analytics_archives']
            now = datetime.now()
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            collection_name = f"range_query_duration_{results['category']}_{results['min_duration']}-{results['max_duration']}sec_{timestamp_str}"
            collection = archives_db[collection_name]
            
            # Make copies to avoid modifying session state
            data_to_save = []
            for item in results['data']:
                save_item = item.copy()
                save_item['created_date'] = now.strftime("%m-%d-%Y")
                save_item['created_time'] = now.strftime("%H:%M:%S")
                save_item['query_type'] = 'duration_range'
                save_item['filter_category'] = results['category']
                save_item['filter_min_duration'] = results['min_duration']
                save_item['filter_max_duration'] = results['max_duration']
                data_to_save.append(save_item)
            
            if data_to_save:
                collection.insert_many(data_to_save)
                st.success(f"✅ Saved {len(data_to_save)} results!")
            else:
                st.warning("No data to save")
            
        except Exception as e:
            st.error(f"Error: {e}")

with col2:
    if st.button("Save Views Query", disabled='views_range_results' not in st.session_state):
        try:
            from utils import get_mongo_connector
            from datetime import datetime
            
            mongo = get_mongo_connector()
            results = st.session_state['views_range_results']
            
            archives_db = mongo.client['youtube_analytics_archives']
            now = datetime.now()
            timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
            
            collection_name = f"range_query_views_{results['min_views']}-{results['max_views']}_{timestamp_str}"
            collection = archives_db[collection_name]
            
            # Make copies to avoid modifying session state
            data_to_save = []
            for item in results['data']:
                save_item = item.copy()
                save_item['created_date'] = now.strftime("%m-%d-%Y")
                save_item['created_time'] = now.strftime("%H:%M:%S")
                save_item['query_type'] = 'views_range'
                save_item['filter_category'] = results['category']
                save_item['filter_min_views'] = results['min_views']
                save_item['filter_max_views'] = results['max_views']
                data_to_save.append(save_item)
            
            if data_to_save:
                collection.insert_many(data_to_save)
                st.success(f"✅ Saved {len(data_to_save)} results!")
            else:
                st.warning("No data to save")
            
        except Exception as e:
            st.error(f"Error: {e}")
