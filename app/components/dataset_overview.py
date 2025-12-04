"""
Dataset Overview Component
Displays collection statistics and metrics
"""

import streamlit as st
import logging

logger = logging.getLogger(__name__)


def render_dataset_overview(mongo):
    """
    Render dataset overview with collection statistics.
    
    Args:
        mongo: MongoDBConnector instance
    """
    st.markdown("### 📊 Dataset Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        videos_stats = mongo.get_collection_stats('videos')
        st.metric(
            label="📹 Total Videos",
            value=f"{videos_stats['count']:,}" if videos_stats['exists'] else "N/A"
        )
    
    with col2:
        edges_stats = mongo.get_collection_stats('edges')
        st.metric(
            label="🔗 Total Edges",
            value=f"{edges_stats['count']:,}" if edges_stats['exists'] else "N/A"
        )
    
    with col3:
        snapshots_stats = mongo.get_collection_stats('video_snapshots')
        st.metric(
            label="📸 Snapshots",
            value=f"{snapshots_stats['count']:,}" if snapshots_stats['exists'] else "N/A"
        )
    
    with col4:
        _render_avg_degree(mongo)


def _render_avg_degree(mongo):
    """Render average degree metric."""
    try:
        degree_stats = mongo.get_collection('degree_statistics')
        avg_degree = degree_stats.aggregate([
            {"$group": {"_id": None, "avg": {"$avg": "$total_degree"}}}
        ])
        avg_val = next(avg_degree, {}).get('avg', 0)
        st.metric(
            label="📊 Avg Degree",
            value=f"{avg_val:.2f}" if avg_val else "N/A"
        )
    except:
        st.metric(label="📊 Avg Degree", value="N/A")
