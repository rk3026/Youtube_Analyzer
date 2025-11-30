"""
Component to render a consistent YouTube video link across the UI.
"""
import streamlit as st


def render_video_link(video_id: str, label: str | None = None, short: bool = False):
    """Render a clickable YouTube link in the UI.

    Args:
        video_id: YouTube video ID
        label: Optional label to display for the link. If not provided, the ID is used.
        short: Whether to show the shortened URL text instead of the full URL.
    """
    if not video_id:
        st.write("No video ID")
        return

    if short:
        url = f"https://youtu.be/{video_id}"
    else:
        url = f"https://www.youtube.com/watch?v={video_id}"

    text = label or video_id
    # render as anchor
    st.markdown(f'<a href="{url}" target="_blank">{text}</a>', unsafe_allow_html=True)
