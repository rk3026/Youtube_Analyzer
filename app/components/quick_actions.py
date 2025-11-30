"""
Quick Actions Component
Renders action buttons for navigation and operations
"""

import streamlit as st


def render_quick_actions():
    """Render quick action buttons."""
    st.markdown("### Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Refresh Connections", type="primary", width="stretch"):
            st.cache_resource.clear()
            st.rerun()
    
    with col2:
        if st.button("📊 View Network Stats", type="primary", width="stretch"):
            st.switch_page("pages/1_📊_Network_Statistics.py")
    
    with col3:
        if st.button("💡 Run PageRank", type="primary", width="stretch"):
            st.switch_page("pages/5_💡_Influence_Analysis.py")
