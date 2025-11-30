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

# Sidebar is intentionally left to Streamlit's default page navigation

# Main content
st.markdown("## 🏠 Welcome")

# Connection status section
render_connection_status()

# Dataset Overview and Analytics
st.markdown("---")

try:
    mongo = get_mongo_connector()
    
    # Dataset overview metrics
    render_dataset_overview(mongo)
    
    # Visual Analytics Section
    st.markdown("---")
    render_visual_analytics(mongo)

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
