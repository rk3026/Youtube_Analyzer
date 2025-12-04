"""
YouTube Analyzer - Main Application Entry Point
Multi-page Streamlit application for YouTube network analytics.
"""

import streamlit as st
import logging
import sys
from pathlib import Path

# Add current directory to path for imports
app_dir = Path(__file__).parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Import components
from components import (
    render_connection_status,
    render_dataset_overview,
    render_visual_analytics,
    render_instructions
)
from utils import get_mongo_connector
import json


# --- Lightweight Mongo query cache helpers (Home-page-only) ---
@st.cache_data(ttl=600)
def _cached_get_collection_stats(uri: str, db_name: str, collection_name: str):
    """Cached wrapper around connector.get_collection_stats.

    Use the connector created by `get_mongo_connector()` to keep a single client
    instance across calls.
    """
    try:
        mongo2 = get_mongo_connector()
        # ensure same db if different connectors are used
        col = mongo2.get_collection(collection_name)
        count = col.count_documents({})
        sample = col.find_one()
        fields = list(sample.keys()) if sample else []

        return {
            'name': collection_name,
            'count': count,
            'fields': fields,
            'exists': True
        }
    except Exception as e:
        return {
            'name': collection_name,
            'count': 0,
            'fields': [],
            'exists': False,
            'error': str(e)
        }


@st.cache_data(ttl=600)
def _cached_aggregate(uri: str, db_name: str, collection_name: str, pipeline):
    """Cached wrapper around collection.aggregate.

    The pipeline is passed directly to the function so Streamlit can build
    a cache key from it.
    """
    try:
        mongo2 = get_mongo_connector()
        col = mongo2.get_collection(collection_name)
        # Convert to list (materialize) because cursor objects are not hashable
        results = list(col.aggregate(pipeline))
        return results
    except Exception as e:
        # Raise to surface errors in logs / UI as before
        raise


@st.cache_data(ttl=600)
def _cached_count_documents(uri: str, db_name: str, collection_name: str, filter_doc):
    try:
        mongo2 = get_mongo_connector()
        col = mongo2.get_collection(collection_name)
        return col.count_documents(filter_doc)
    except Exception as e:
        raise


class _CachedCollection:
    def __init__(self, base_collection, mongo_connector):
        self._base = base_collection
        self._mongo = mongo_connector

    def aggregate(self, pipeline):
        return _cached_aggregate(self._mongo.uri, self._mongo.database_name, self._base.name, pipeline)

    def count_documents(self, filter_doc):
        return _cached_count_documents(self._mongo.uri, self._mongo.database_name, self._base.name, filter_doc)

    def find_one(self, *args, **kwargs):
        # very small result - cache via wrapper by using cached_get_collection_stats
        return self._base.find_one(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._base, name)


class _CachedMongo:
    def __init__(self, base_mongo):
        self._base = base_mongo
        self.uri = base_mongo.uri
        self.database_name = base_mongo.database_name
        self.connection_type = base_mongo.connection_type

    def get_collection_stats(self, collection_name: str):
        return _cached_get_collection_stats(self.uri, self.database_name, collection_name)

    def get_collection(self, collection_name: str):
        base_collection = self._base.get_collection(collection_name)
        return _CachedCollection(base_collection, self._base)

    def get_all_collections(self):
        return self._base.get_all_collections()

    def test_connection(self):
        return self._base.test_connection()

    # Proxy any attribute access to the base connector
    def __getattr__(self, name):
        return getattr(self._base, name)


# Page configuration
st.set_page_config(
    page_title="YouTube Network Analyzer",
    page_icon="📺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4B4B;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #FF4B4B;
    }
    .stAlert {
        margin-top: 1rem;
    }
    div[data-testid="stSidebarNav"] {
        padding-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Application Header
st.markdown('<div class="main-header">📺 YouTube Network Analyzer</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Big Data Analytics Platform for YouTube Video Networks</div>', unsafe_allow_html=True)

# Main content
st.markdown("## 🏠 Welcome")

# Add a small control for refreshing cached data only (doesn't restart connectors)
col_refresh, _ = st.columns([1, 5])
with col_refresh:
    if st.button("🔁 Refresh Cached Data"):
        st.cache_data.clear()
        st.rerun()

# Connection status section
render_connection_status()

# Dataset Overview and Analytics
st.markdown("---")

try:
    mongo = get_mongo_connector()
    cached_mongo = _CachedMongo(mongo)

    # Dataset overview metrics (use cached_mongo - home page only cache wrapper)
    render_dataset_overview(cached_mongo)
    
    # Visual Analytics Section
    st.markdown("---")
    render_visual_analytics(cached_mongo)

except Exception as e:
    st.warning("⚠️ Could not load dataset statistics. Please check your MongoDB connection.")
    logger.error(f"Error loading dataset overview: {e}")

# Instructions
st.markdown("---")
render_instructions()

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem;'>
    <p><strong>YouTube Network Analyzer</strong></p>
    <p>Built with Streamlit, PySpark, and MongoDB</p>
</div>
""", unsafe_allow_html=True)
