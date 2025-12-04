"""
YouTube helpers: utilities to create links and attach URLs to DataFrames.
"""
from typing import Any
import pandas as pd


def youtube_url(video_id: str) -> str:
    """Return a canonical YouTube URL for a video id."""
    return f"https://www.youtube.com/watch?v={video_id}"


def short_youtube_url(video_id: str) -> str:
    """Return a shortened YouTube URL for sharing."""
    return f"https://youtu.be/{video_id}"


def add_youtube_links_to_df(df: pd.DataFrame, id_col: str = 'id', url_col: str = 'url', short_col: str = 'short_url') -> pd.DataFrame:
    """Return a new DataFrame with URL columns added for the given video id column.

    Args:
        df: Input DataFrame with video IDs
        id_col: Column name that contains the video ID
        url_col: Name of the URL column to add
        short_col: Name of the short URL column to add

    Returns:
        DataFrame copy with `url_col` and `short_col` added
    """
    if id_col not in df.columns:
        return df

    df = df.copy()
    df[url_col] = df[id_col].map(youtube_url)
    df[short_col] = df[id_col].map(short_youtube_url)
    return df
