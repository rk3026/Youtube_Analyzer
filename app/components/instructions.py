"""
Instructions Component
Renders help documentation and configuration details
"""

import streamlit as st


def render_instructions():
    """Render getting started instructions and configuration details."""
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
        - Set `MONGODB_URI` environment variable to configure MongoDB connection
        - Default: `mongodb://localhost:27017/` 
        - Spark runs in local mode with 4GB memory
        
        #### Performance Tips
        - Start with smaller sample sizes for faster results
        - Results are cached to improve performance
        - Clear cache using the "Refresh Connections" button if needed
        
        #### Troubleshooting
        If you encounter connection issues:
        1. Check MongoDB is running and accessible
        2. Verify Java 17 is installed for Spark
        3. Set `MONGODB_URI` environment variable if using remote MongoDB
        4. Check the logs for detailed error messages
        """)
    
    with st.expander("⚙️ Configuration Details"):
        st.markdown("""
        #### Current Configuration
        
        The application uses simple environment variables for configuration.
        
        **MongoDB Configuration:**
        - Set `MONGODB_URI` environment variable (default: `mongodb://localhost:27017/`)
        - Database: `youtube_analytics`
        
        **Spark Configuration:**
        - Runs in local mode: `local[*]`
        - Memory: 4GB driver and executor
        - Includes MongoDB connector and GraphFrames packages
        
        **To modify settings:**
        1. Set the `MONGODB_URI` environment variable to your MongoDB connection string
        2. Restart the application to apply changes
        """)
        
        _render_active_configuration()


def _render_active_configuration():
    """Render current active configuration."""
    try:
        from utils import get_mongo_connector, get_spark_connector
        
        mongo = get_mongo_connector()
        spark_conn = get_spark_connector()
        
        st.markdown("**Active MongoDB Config:**")
        st.json({
            'connection_type': mongo.connection_type,
            'database': mongo.database_name,
            'uri': '***' if 'password' in mongo.uri or '@' in mongo.uri else mongo.uri
        })
        
        st.markdown("**Active Spark Config:**")
        st.json({
            'connection_type': spark_conn.connection_type,
            'app_name': spark_conn.app_name,
            'master': 'local[*]'
        })
    except Exception as e:
        st.warning(f"Could not load configuration details: {e}")
