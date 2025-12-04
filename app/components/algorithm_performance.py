"""
Algorithm Performance Component
Reusable UI component for displaying query/algorithm performance metrics.
"""

import streamlit as st
from typing import Dict, Any, Optional
import plotly.graph_objects as go
from datetime import datetime


def render_algorithm_performance(
    performance_data: Dict[str, Any],
    title: str = "Algorithm Performance",
    show_details: bool = True
) -> None:
    """
    Render algorithm performance metrics in a clean, informative UI.
    
    Args:
        performance_data: Dictionary containing performance metrics:
            - execution_time: Query execution time in seconds (required)
            - rows_processed: Number of rows processed (optional)
            - rows_returned: Number of rows returned (optional)
            - query_type: Type of query executed (optional)
            - timestamp: When the query was executed (optional)
            - additional_metrics: Dict of any additional metrics (optional)
        title: Title for the performance section
        show_details: Whether to show detailed breakdown
    """
    if not performance_data:
        return
    
    # Main performance container
    with st.container():
        st.markdown(f"### {title}")
        
        # Key metrics in columns
        cols = st.columns(4)
        
        # Execution Time
        exec_time = performance_data.get('execution_time', 0)
        with cols[0]:
            st.metric(
                label="Execution Time",
                value=f"{exec_time:.3f}s",
                help="Total query execution time"
            )
        
        # Rows Processed
        rows_processed = performance_data.get('rows_processed')
        if rows_processed is not None:
            with cols[1]:
                st.metric(
                    label="Rows Processed",
                    value=f"{rows_processed:,}",
                    help="Number of rows scanned/processed"
                )
        
        # Rows Returned
        rows_returned = performance_data.get('rows_returned')
        if rows_returned is not None:
            with cols[2]:
                st.metric(
                    label="Rows Returned",
                    value=f"{rows_returned:,}",
                    help="Number of rows returned in result"
                )
        
        # Throughput
        if rows_processed and exec_time > 0:
            throughput = rows_processed / exec_time
            with cols[3]:
                st.metric(
                    label="Throughput",
                    value=f"{throughput:,.0f} rows/s",
                    help="Processing speed"
                )
        
        # Detailed breakdown (expandable)
        if show_details:
            with st.expander("Performance Details", expanded=False):
                detail_cols = st.columns(2)
                
                with detail_cols[0]:
                    # Query Information
                    st.markdown("**Query Information**")
                    query_type = performance_data.get('query_type', 'Unknown')
                    st.text(f"Type: {query_type}")
                    
                    timestamp = performance_data.get('timestamp')
                    if timestamp:
                        if isinstance(timestamp, str):
                            st.text(f"Executed: {timestamp}")
                        else:
                            st.text(f"Executed: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Selectivity (if applicable)
                    if rows_processed and rows_returned:
                        selectivity = (rows_returned / rows_processed) * 100
                        st.text(f"Selectivity: {selectivity:.2f}%")
                
                with detail_cols[1]:
                    # Additional Metrics
                    additional = performance_data.get('additional_metrics', {})
                    if additional:
                        st.markdown("**Additional Metrics**")
                        for key, value in additional.items():
                            if isinstance(value, float):
                                st.text(f"{key}: {value:.3f}")
                            elif isinstance(value, int):
                                st.text(f"{key}: {value:,}")
                            else:
                                st.text(f"{key}: {value}")



def render_comparison_performance(
    performance_history: list[Dict[str, Any]],
    title: str = "Performance History"
) -> None:
    """
    Render performance comparison across multiple query executions.
    
    Args:
        performance_history: List of performance_data dictionaries
        title: Title for the comparison section
    """
    if not performance_history or len(performance_history) < 2:
        return
    
    with st.container():
        st.markdown(f"### {title}")
        
        # Extract data
        labels = [f"Run {i+1}" for i in range(len(performance_history))]
        exec_times = [p.get('execution_time', 0) for p in performance_history]
        
        # Create bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=labels,
                y=exec_times,
                text=[f"{t:.3f}s" for t in exec_times],
                textposition='auto',
            )
        ])
        
        fig.update_layout(
            title="Execution Time Comparison",
            xaxis_title="Query Run",
            yaxis_title="Execution Time (seconds)",
            height=300,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Stats
        avg_time = sum(exec_times) / len(exec_times)
        min_time = min(exec_times)
        max_time = max(exec_times)
        
        stat_cols = st.columns(3)
        with stat_cols[0]:
            st.metric("Average", f"{avg_time:.3f}s")
        with stat_cols[1]:
            st.metric("Best", f"{min_time:.3f}s")
        with stat_cols[2]:
            st.metric("Worst", f"{max_time:.3f}s")
