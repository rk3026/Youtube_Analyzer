"""
Range Queries Analytics Module.
Provides queries for filtering videos by duration, views, and custom criteria.
"""

import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc

logger = logging.getLogger(__name__)


class RangeQueryAnalytics:
    """Modular range query analytics for YouTube video data."""
    
    def __init__(self, spark_connector):
        """
        Initialize Range Query analytics.
        
        Args:
            spark_connector: SparkConnector instance from utils
        """
        self.spark_connector = spark_connector
        self.spark = spark_connector.spark
    
    def query_by_duration(
        self,
        category: str,
        min_duration: int,
        max_duration: int,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Find all videos in a category with duration within a specified range.
        
        Args:
            category: Video category to filter
            min_duration: Minimum duration in seconds
            max_duration: Maximum duration in seconds
            max_results: Maximum number of results to return
            
        Returns:
            List of dictionaries with video details
        """
        logger.info(f"Finding {category} videos with duration {min_duration}-{max_duration} sec...")
        
        snapshots_df = self.spark_connector.load_collection_from_mongo('video_snapshots')
        
        filtered = (
            snapshots_df
            .filter(
                (col('category') == category) &
                (col('length_sec').between(min_duration, max_duration))
            )
            .select('video_id', 'length_sec', 'views', 'rate', 'category')
            .limit(max_results)
            .collect()
        )
        
        results = []
        for row in filtered:
            duration_sec = int(row['length_sec']) if row['length_sec'] else 0
            results.append({
                'video_id': row['video_id'],
                'duration_sec': duration_sec,
                'duration_formatted': f"{duration_sec // 60}:{duration_sec % 60:02d}",
                'views': int(row['views']) if row['views'] else 0,
                'rating': float(row['rate']) if row['rate'] else 0.0,
                'category': row['category']
            })
        
        logger.info(f"Found {len(results)} videos")
        return results
    
    def query_by_views(
        self,
        min_views: int,
        max_views: int,
        category: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Find all videos with view counts within a specified range.
        
        Args:
            min_views: Minimum view count
            max_views: Maximum view count
            category: Optional category filter (None = all categories)
            max_results: Maximum number of results to return
            
        Returns:
            List of dictionaries with video details
        """
        logger.info(f"Finding videos with {min_views:,}-{max_views:,} views...")
        
        snapshots_df = self.spark_connector.load_collection_from_mongo('video_snapshots')
        
        # Build filter
        base_filter = col('views').between(min_views, max_views)
        if category and category != 'All Categories':
            base_filter = base_filter & (col('category') == category)
        
        filtered = (
            snapshots_df
            .filter(base_filter)
            .orderBy(desc('views'))
            .select('video_id', 'views', 'rate', 'ratings', 'category', 'length_sec')
            .limit(max_results)
            .collect()
        )
        
        results = []
        for row in filtered:
            results.append({
                'video_id': row['video_id'],
                'views': int(row['views']) if row['views'] else 0,
                'rating': float(row['rate']) if row['rate'] else 0.0,
                'num_ratings': int(row['ratings']) if row['ratings'] else 0,
                'category': row['category'],
                'duration_sec': int(row['length_sec']) if row['length_sec'] else 0
            })
        
        logger.info(f"Found {len(results)} videos")
        return results
