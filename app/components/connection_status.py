"""
Connection Status Component
Displays MongoDB and Spark connection information
"""

import streamlit as st
import logging

logger = logging.getLogger(__name__)


def render_connection_status():
    """Render MongoDB and Spark connection status."""
    st.markdown("### Connection Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        _render_mongodb_status()
    
    with col2:
        _render_spark_status()


def _render_mongodb_status():
    """Render MongoDB connection status."""
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


def _render_spark_status():
    """Render Spark connection status."""
    st.markdown("#### Spark Connection")
    with st.spinner("Checking Spark session..."):
        try:
            from utils import get_spark_connector
            spark_conn = get_spark_connector()
            
            # Access the spark property to trigger lazy initialization
            spark_session = spark_conn.spark
            info = spark_conn.get_session_info()
            
            if info.get('status') == 'active':
                st.success("✅ Active")
                st.info(f"""
                **Connection Type:** {info.get('connection_type', 'unknown')}  
                **Spark Version:** {info.get('spark_version', 'unknown')}  
                **Master:** {info.get('master', 'unknown')}  
                **Parallelism:** {info.get('default_parallelism', 'unknown')}
                """)
            else:
                error_msg = info.get('error', 'Session not initialized')
                st.warning(f"⚠️ {error_msg}")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            logger.error(f"Spark connection error: {e}")
