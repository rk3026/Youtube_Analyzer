"""
Pattern Search Page
Find subgraph patterns - related video pairs in the recommendation network.
Uses DataFrame joins instead of GraphFrames to avoid SparkSession issues.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
app_dir = Path(__file__).parent.parent
if str(app_dir) not in sys.path:
    sys.path.insert(0, str(app_dir))

logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Pattern Search - YouTube Analyzer",
    page_icon="🔗",
    layout="wide"
)

# Header
st.title("🔗 Pattern Search")
st.markdown("Find subgraph patterns in the YouTube recommendation network")

st.markdown("""
### About Pattern Search
This analysis finds patterns of connected videos in the recommendation network:
- **Related Pairs**: `(a)→(b)` - Video A recommends Video B
- **Video Chains**: `(a)→(b)→(c)` - Recommendation chains
- **Common Recommendations**: `(a)→(c)←(b)` - Videos that share recommendations

These patterns help identify recommendation clusters and content relationships.
""")

# Configuration
st.markdown("### Search Configuration")

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
    
    selected_category = st.selectbox(
        "Filter by Category",
        options=categories,
        index=0,
        help="Find patterns where videos are in this category",
        key="pattern_category"
    )

with col2:
    pattern_type = st.selectbox(
        "Pattern Type",
        options=[
            "Related Pairs (a→b)",
            "Video Chains (a→b→c)",
            "Common Recommendations (a→c←b)"
        ],
        index=0,
        help="Type of subgraph pattern to search for",
        key="pattern_type"
    )

col3, col4 = st.columns(2)

with col3:
    max_results = st.slider(
        "Maximum Results",
        min_value=10,
        max_value=100,
        value=50,
        step=10,
        help="Limit the number of patterns returned",
        key="pattern_max"
    )

with col4:
    sample_size = st.slider(
        "Edge Sample Size",
        min_value=10000,
        max_value=100000,
        value=50000,
        step=10000,
        help="Number of edges to sample (higher = slower but more comprehensive)",
        key="pattern_sample"
    )

run_search = st.button("🔍 Find Patterns", type="primary")

if run_search:
    with st.spinner("Searching for patterns (this may take a moment)..."):
        try:
            from utils import get_spark_connector
            from analytics.pattern_search import PatternSearch
            
            spark_conn = get_spark_connector()
            pattern_search = PatternSearch(spark_conn)
            
            category = selected_category if selected_category != 'All Categories' else None
            
            if "Related Pairs" in pattern_type:
                results = pattern_search.find_related_video_pairs(
                    category=category,
                    max_results=max_results,
                    sample_size=sample_size
                )
                result_type = "pairs"
            elif "Video Chains" in pattern_type:
                results = pattern_search.find_video_chains(
                    category=category,
                    chain_length=2,
                    max_results=max_results
                )
                result_type = "chains"
            else:  # Common Recommendations
                results = pattern_search.find_common_recommendations(
                    category=category,
                    max_results=max_results
                )
                result_type = "common"
            
            st.session_state['pattern_results'] = {
                'data': results,
                'category': selected_category,
                'pattern_type': pattern_type,
                'result_type': result_type
            }
            st.success(f"✅ Found {len(results)} results!")
            
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            logger.error(f"Pattern search error: {e}", exc_info=True)

# Display results
if 'pattern_results' in st.session_state:
    results = st.session_state['pattern_results']
    data = results['data']
    result_type = results['result_type']
    
    if data:
        df = pd.DataFrame(data)
        df.index = range(1, len(df) + 1)
        
        category_text = results['category'] if results['category'] != 'All Categories' else 'all categories'
        st.markdown(f"#### {results['pattern_type']} ({category_text})")
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.markdown("##### Results Table")
            
            if result_type == "pairs":
                st.dataframe(
                    df,
                    column_config={
                        "source_video": "Source Video (a)",
                        "related_video": "Related Video (b)"
                    },
                    use_container_width=True
                )
            elif result_type == "chains":
                st.dataframe(
                    df,
                    column_config={
                        "video_a": "Video A",
                        "video_b": "Video B",
                        "video_c": "Video C"
                    },
                    use_container_width=True
                )
            else:  # common
                st.dataframe(
                    df,
                    column_config={
                        "video_a": "Video A",
                        "video_b": "Video B",
                        "common_target": "Common Target (c)"
                    },
                    use_container_width=True
                )
        
        with col2:
            st.markdown("##### Pattern Visualization")
            
            if result_type == "pairs":
                # Count how many times each video appears as source
                source_counts = df['source_video'].value_counts().head(10).reset_index()
                source_counts.columns = ['video_id', 'outgoing_links']
                
                # Shorten video IDs for display
                source_counts['short_id'] = source_counts['video_id'].str[:12] + '...'
                
                fig = px.bar(
                    source_counts,
                    y='short_id',
                    x='outgoing_links',
                    orientation='h',
                    title='Top Source Videos by Outgoing Links',
                    labels={'short_id': 'Video', 'outgoing_links': 'Recommendations'},
                    color='outgoing_links',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(
                    showlegend=False,
                    coloraxis_showscale=False,
                    yaxis={'categoryorder': 'total ascending'},
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                
            elif result_type == "chains":
                # Show distribution of middle videos in chains
                middle_counts = df['video_b'].value_counts().head(10).reset_index()
                middle_counts.columns = ['video_id', 'chain_count']
                
                # Shorten video IDs for display
                middle_counts['short_id'] = middle_counts['video_id'].str[:12] + '...'
                
                fig = px.bar(
                    middle_counts,
                    y='short_id',
                    x='chain_count',
                    orientation='h',
                    title='Most Common Bridge Videos (B in A→B→C)',
                    labels={'short_id': 'Video', 'chain_count': 'Chain Count'},
                    color='chain_count',
                    color_continuous_scale='Greens'
                )
                fig.update_layout(
                    showlegend=False,
                    coloraxis_showscale=False,
                    yaxis={'categoryorder': 'total ascending'},
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                
            else:  # common
                # Show most commonly targeted videos
                target_counts = df['common_target'].value_counts().head(10).reset_index()
                target_counts.columns = ['video_id', 'recommendation_count']
                
                # Shorten video IDs for display
                target_counts['short_id'] = target_counts['video_id'].str[:12] + '...'
                
                fig = px.bar(
                    target_counts,
                    y='short_id',
                    x='recommendation_count',
                    orientation='h',
                    title='Most Commonly Recommended Videos',
                    labels={'short_id': 'Video', 'recommendation_count': 'Times Recommended'},
                    color='recommendation_count',
                    color_continuous_scale='Oranges'
                )
                fig.update_layout(
                    showlegend=False,
                    coloraxis_showscale=False,
                    yaxis={'categoryorder': 'total ascending'},
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No patterns found matching the criteria")
else:
    st.info("👆 Select options and click 'Find Patterns' to search")


# ============== Save Results Section ==============
st.markdown("---")
st.markdown("### 💾 Save Results to Archives")

if st.button("Save Pattern Results", disabled='pattern_results' not in st.session_state):
    try:
        from utils import get_mongo_connector
        from datetime import datetime
        
        mongo = get_mongo_connector()
        results = st.session_state['pattern_results']
        
        archives_db = mongo.client['youtube_analytics_archives']
        now = datetime.now()
        timestamp_str = now.strftime("%m-%d-%Y_%H-%M-%S")
        
        cat_suffix = results['category'].replace(' ', '_') if results['category'] != 'All Categories' else 'all'
        pattern_suffix = results['result_type']
        collection_name = f"pattern_{pattern_suffix}_{cat_suffix}_{timestamp_str}"
        collection = archives_db[collection_name]
        
        # Make copies to avoid modifying session state
        data_to_save = []
        for item in results['data']:
            save_item = item.copy()
            save_item['created_date'] = now.strftime("%m-%d-%Y")
            save_item['created_time'] = now.strftime("%H:%M:%S")
            save_item['filter_category'] = results['category']
            save_item['pattern_type'] = results['pattern_type']
            data_to_save.append(save_item)
        
        if data_to_save:
            collection.insert_many(data_to_save)
            st.success(f"✅ Saved {len(data_to_save)} results to `{collection_name}`!")
        else:
            st.warning("No data to save")
        
    except Exception as e:
        st.error(f"Error: {e}")
