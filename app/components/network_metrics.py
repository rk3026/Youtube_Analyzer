"""
Network Metrics Component
Displays network analysis visualizations
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import logging

logger = logging.getLogger(__name__)


def render_network_metrics(mongo):
    """
    Render network metrics and visualizations.
    
    Args:
        mongo: MongoDBConnector instance
    """
    st.markdown("### 🕸️ Network Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        _render_degree_distribution(mongo)
    
    with col2:
        _render_rating_distribution(mongo)
    
    with col3:
        _render_view_stats(mongo)


def _render_degree_distribution(mongo):
    """Render degree distribution scatter plot."""
    st.markdown("#### Degree Distribution")
    with st.spinner("Loading degree data..."):
        try:
            degree_collection = mongo.get_collection('degree_statistics')
            sample_size = 100
            degree_data = list(degree_collection.aggregate([
                {"$sample": {"size": sample_size}},
                {"$project": {"video_id": 1, "in_degree": 1, "out_degree": 1}}
            ]))
            
            if degree_data:
                df = pd.DataFrame(degree_data)
                
                fig = px.scatter(
                    df,
                    x='in_degree',
                    y='out_degree',
                    title=f'In-Degree vs Out-Degree (Sample: {len(df)})',
                    labels={'in_degree': 'In-Degree', 'out_degree': 'Out-Degree'},
                    opacity=0.6,
                    color_discrete_sequence=['#FF4B4B']
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No degree data available")
        except Exception as e:
            st.error(f"Could not load degree data: {e}")
            logger.error(f"Degree chart error: {e}")


def _render_rating_distribution(mongo):
    """Render rating distribution bar chart."""
    st.markdown("#### Rating Distribution")
    with st.spinner("Loading rating data..."):
        try:
            videos_collection = mongo.get_collection('videos')
            pipeline = [
                {"$match": {"rating": {"$exists": True, "$ne": None}}},
                {"$bucket": {
                    "groupBy": "$rating",
                    "boundaries": [0, 1, 2, 3, 4, 5, 6],
                    "default": "Other",
                    "output": {"count": {"$sum": 1}}
                }}
            ]
            ratings = list(videos_collection.aggregate(pipeline))
            
            if ratings:
                df = pd.DataFrame(ratings)
                df['_id'] = df['_id'].apply(lambda x: f"{x}-{x+1}" if isinstance(x, (int, float)) else str(x))
                
                fig = px.bar(
                    df,
                    x='_id',
                    y='count',
                    title='Video Ratings',
                    labels={'_id': 'Rating Range', 'count': 'Number of Videos'},
                    color='count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No rating data available")
        except Exception as e:
            st.error(f"Could not load rating data: {e}")
            logger.error(f"Rating chart error: {e}")


def _render_view_stats(mongo):
    """Render view count statistics."""
    st.markdown("#### View Count Stats")
    with st.spinner("Loading view data..."):
        try:
            videos_collection = mongo.get_collection('videos')
            pipeline = [
                {"$match": {"views": {"$exists": True, "$gt": 0}}},
                {"$group": {
                    "_id": None,
                    "avg_views": {"$avg": "$views"},
                    "max_views": {"$max": "$views"},
                    "min_views": {"$min": "$views"},
                    "total_views": {"$sum": "$views"}
                }}
            ]
            stats = list(videos_collection.aggregate(pipeline))
            
            if stats:
                stat = stats[0]
                st.metric("Total Views", f"{stat.get('total_views', 0):,.0f}")
                st.metric("Average Views", f"{stat.get('avg_views', 0):,.0f}")
                st.metric("Max Views", f"{stat.get('max_views', 0):,.0f}")
            else:
                st.info("No view data available")
        except Exception as e:
            st.error(f"Could not load view stats: {e}")
            logger.error(f"View stats error: {e}")
