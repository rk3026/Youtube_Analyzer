"""
Visual Analytics Component
Renders charts and visualizations for quick insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import logging

logger = logging.getLogger(__name__)


def render_visual_analytics(mongo):
    """
    Render visual analytics charts.
    
    Args:
        mongo: MongoDBConnector instance
    """
    st.markdown("### 📈 Quick Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        _render_category_distribution(mongo)
    
    with col2:
        _render_top_uploaders(mongo)


def _render_category_distribution(mongo):
    """Render category distribution pie chart."""
    st.markdown("#### Category Distribution")
    with st.spinner("Loading category data..."):
        try:
            videos_collection = mongo.get_collection('videos')
            pipeline = [
                {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ]
            categories = list(videos_collection.aggregate(pipeline))
            
            if categories:
                df = pd.DataFrame(categories)
                df.columns = ['Category', 'Count']
                df['Category'] = df['Category'].fillna('Unknown')
                
                fig = px.pie(
                    df, 
                    values='Count', 
                    names='Category',
                    title='Top 10 Video Categories',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                fig.update_layout(height=400, showlegend=True)
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No category data available")
        except Exception as e:
            st.error(f"Could not load category data: {e}")
            logger.error(f"Category chart error: {e}")


def _render_top_uploaders(mongo):
    """Render top uploaders bar chart."""
    st.markdown("#### Top Content Creators")
    with st.spinner("Loading uploader data..."):
        try:
            videos_collection = mongo.get_collection('videos')
            pipeline = [
                {"$match": {"uploader": {"$exists": True}}},
                {"$group": {
                    "_id": "$uploader",
                    "video_count": {"$sum": 1}
                }},
                {"$sort": {"video_count": -1}},
                {"$limit": 10}
            ]
            uploaders = list(videos_collection.aggregate(pipeline))
            
            if uploaders:
                df = pd.DataFrame(uploaders)
                df.columns = ['Uploader', 'Video Count']
                
                fig = px.bar(
                    df,
                    x='Video Count',
                    y='Uploader',
                    orientation='h',
                    title='Top 10 Content Creators',
                    color='Video Count',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(height=400, showlegend=False)
                fig.update_yaxes(categoryorder='total ascending')
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("No uploader data available")
        except Exception as e:
            st.error(f"Could not load uploader data: {e}")
            logger.error(f"Uploader chart error: {e}")
