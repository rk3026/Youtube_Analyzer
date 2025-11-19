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

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/200x80/FF4B4B/FFFFFF?text=YouTube+Analyzer", width='stretch')
    st.markdown("---")
    st.markdown("### Navigation")
    st.markdown("""
    Welcome to the YouTube Analyzer! Use the pages above to explore:
    
    - 🏠 **Home** - Overview and connection status
    - 📊 **Network Statistics** - Degree distribution and metrics
    - 🔍 **Top-K Queries** - Most popular videos and categories
    - 🎯 **Range Queries** - Filter videos by criteria
    - 🕸️ **Pattern Search** - Find network patterns
    - 💡 **Influence Analysis** - PageRank and influence metrics
    - ⚙️ **Settings** - Configure connections
    """)
    st.markdown("---")
    st.markdown("### About")
    st.markdown("""
    **CPTS 415 - Big Data**  
    **Milestone 4 - GUI Prototype**
    
    **Team:**
    - Ross Kugler
    - Huy (Harry) Ky
    - Ben Bordon
    """)

# Main content
st.markdown("## 🏠 Welcome")

# Connection status section
st.markdown("### Connection Status")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### MongoDB Connection")
    with st.spinner("Testing MongoDB connection..."):
        try:
            from utils import get_mongo_connector
            mongo = get_mongo_connector()
            status = mongo.test_connection()
            
            if status['status'] == 'connected':
                st.success("✅ Connected")
                st.info(f"""
                **Connection Type:** {status['connection_type']}  
                **Database:** {status['database']}  
                **Collections:** {status['collections_count']}  
                **Server Version:** {status['server_version']}
                """)
            else:
                st.error(f"❌ Connection Failed: {status.get('error', 'Unknown error')}")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            logger.error(f"MongoDB connection error: {e}")

with col2:
    st.markdown("#### Spark Connection")
    with st.spinner("Testing Spark session..."):
        try:
            from utils import get_spark_connector
            spark_conn = get_spark_connector()
            info = spark_conn.get_session_info()
            test = spark_conn.test_connection()
            
            if info['status'] == 'active' and test['test_passed']:
                st.success("✅ Active")
                st.info(f"""
                **Connection Type:** {info['connection_type']}  
                **Spark Version:** {info['spark_version']}  
                **Master:** {info['master']}  
                **Parallelism:** {info['default_parallelism']}
                """)
            else:
                st.error(f"❌ Session Issue: {test.get('error', 'Unknown error')}")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            logger.error(f"Spark connection error: {e}")

# Dataset Overview
st.markdown("---")
st.markdown("### Dataset Overview")

try:
    mongo = get_mongo_connector()
    
    # Get collection statistics
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
        # Calculate average degree (if degree_statistics collection exists)
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

except Exception as e:
    st.warning("⚠️ Could not load dataset statistics. Please check your MongoDB connection.")
    logger.error(f"Error loading dataset overview: {e}")

# Quick Actions
st.markdown("---")
st.markdown("### Quick Actions")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🔄 Refresh Connections", type="primary", use_container_width=True):
        st.cache_resource.clear()
        st.rerun()

with col2:
    if st.button("📊 View Network Stats", type="primary", use_container_width=True):
        st.switch_page("pages/1_📊_Network_Statistics.py")

with col3:
    if st.button("💡 Run PageRank", type="primary", use_container_width=True):
        st.switch_page("pages/5_💡_Influence_Analysis.py")

# Instructions
st.markdown("---")
st.markdown("### Getting Started")

with st.expander("📖 How to Use This Application"):
    st.markdown("""
    #### Navigation
    Use the sidebar menu to navigate between different analytics features:
    
    1. **Network Statistics** - View degree distributions and categorized statistics
    2. **Top-K Queries** - Find top videos by views, ratings, or categories
    3. **Range Queries** - Filter videos using multiple criteria
    4. **Pattern Search** - Discover network motifs and patterns
    5. **Influence Analysis** - Compute PageRank scores and analyze influence
    6. **Settings** - Configure MongoDB and Spark connections
    
    #### Configuration
    - The application uses configuration from `.env` files and environment variables
    - Use environment variables to customize your setup if needed
    
    #### Performance Tips
    - Start with smaller sample sizes for faster results
    - Results are cached to improve performance
    - Clear cache using the "Refresh Connections" button if needed
    
    #### Troubleshooting
    If you encounter connection issues:
    1. Check MongoDB is running and accessible
    2. Verify Java 17 is installed for Spark
    3. Review configuration in `.env` file or environment variables
    4. Check the logs for detailed error messages
    """)

with st.expander("⚙️ Configuration Details"):
    st.markdown("""
    #### Current Configuration
    
    The application is using settings from `.env` files and environment variables.
    
    **MongoDB Configuration:**
    - Connection type determined by environment variables
    - Supports local, Atlas, and custom connections
    - Database and collections specified in code defaults
    
    **Spark Configuration:**
    - Connection type determined by SPARK_MASTER environment variable
    - Supports local mode, standalone cluster, and custom setups
    - Memory and parallelism settings in code defaults
    
    **To modify settings:**
    1. Create a `.env` file (copy from .env.example)
    2. Or set environment variables (MONGODB_URI, SPARK_MASTER, etc.)
    3. Restart the application to apply changes
    """)
    
    # Show current active configuration
    try:
        from config import get_config
        config = get_config()
        
        st.markdown("**Active MongoDB Config:**")
        mongo_cfg = config.get_mongodb_config()
        st.json({
            'connection_type': mongo_cfg['connection_type'],
            'database': mongo_cfg['database'],
            'collections': mongo_cfg['collections']
        })
        
        st.markdown("**Active Spark Config:**")
        spark_cfg = config.get_spark_config()
        st.json({
            'connection_type': spark_cfg['connection_type'],
            'app_name': spark_cfg['app_name'],
            'packages': spark_cfg['packages'][:2] + ['...']  # Show first 2 packages
        })
    except Exception as e:
        st.warning("Could not load configuration details")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem;'>
    <p><strong>YouTube Network Analyzer</strong> | CPTS 415 Big Data Project | Milestone 4</p>
    <p>Built with Streamlit, PySpark, and MongoDB</p>
</div>
""", unsafe_allow_html=True)
