"""
Top-K Queries Analytics Module.
Provides queries for finding top K categories, most viewed videos, and highest rated videos.
"""

import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc

logger = logging.getLogger(__name__)


class TopKAnalytics:
    """Modular Top-K query analytics for YouTube video data."""
    
    def __init__(self, spark_connector):
        """
        Initialize Top-K analytics.
        
        Args:
            spark_connector: SparkConnector instance from utils
        """
        self.spark_connector = spark_connector
        self.spark = spark_connector.spark
    
    def get_top_k_categories(self, k: int = 10) -> List[Dict[str, Any]]:
        """
        Find top K categories by number of videos uploaded.
        
        Args:
            k: Number of top categories to retrieve
            
        Returns:
            List of dictionaries with category and video_count
        """
        logger.info(f"Finding top {k} categories by video count...")
        
        videos_df = self.spark_connector.load_collection_from_mongo('videos')
        
        top_categories = (
            videos_df
            .groupBy('category')
            .agg(count('*').alias('video_count'))
            .orderBy(desc('video_count'))
            .limit(k)
            .collect()
        )
        
        results = []
        for row in top_categories:
            results.append({
                'category': row['category'],
                'video_count': int(row['video_count'])
            })
        
        logger.info(f"Found {len(results)} categories")
        return results
    
    def get_top_k_most_viewed(self, k: int = 10) -> List[Dict[str, Any]]:
        """
        Find top K videos by view count.
        Uses video_snapshots collection which contains view data.
        
        Args:
            k: Number of top videos to retrieve
            
        Returns:
            List of dictionaries with video details
        """
        logger.info(f"Finding top {k} most viewed videos...")
        
        snapshots_df = self.spark_connector.load_collection_from_mongo('video_snapshots')
        
        top_viewed = (
            snapshots_df
            .orderBy(desc('views'))
            .select('video_id', 'views', 'category', 'rate', 'ratings')
            .limit(k)
            .collect()
        )
        
        results = []
        for row in top_viewed:
            results.append({
                'video_id': row['video_id'],
                'views': int(row['views']) if row['views'] else 0,
                'category': row['category'],
                'rating': float(row['rate']) if row['rate'] else 0.0,
                'num_ratings': int(row['ratings']) if row['ratings'] else 0
            })
        
        logger.info(f"Found {len(results)} most viewed videos")
        return results
    
    def get_top_k_highest_rated(self, k: int = 10, min_ratings: int = 100) -> List[Dict[str, Any]]:
        """
        Find top K videos by rating.
        Uses video_snapshots collection which contains rating data.
        
        Args:
            k: Number of top videos to retrieve
            min_ratings: Minimum number of ratings required (filters out low-sample videos)
            
        Returns:
            List of dictionaries with video details
        """
        logger.info(f"Finding top {k} highest rated videos (min {min_ratings} ratings)...")
        
        snapshots_df = self.spark_connector.load_collection_from_mongo('video_snapshots')
        
        top_rated = (
            snapshots_df
            .filter(col('ratings') >= min_ratings)
            .orderBy(desc('rate'))
            .select('video_id', 'rate', 'ratings', 'views', 'category')
            .limit(k)
            .collect()
        )
        
        results = []
        for row in top_rated:
            results.append({
                'video_id': row['video_id'],
                'rating': float(row['rate']) if row['rate'] else 0.0,
                'num_ratings': int(row['ratings']) if row['ratings'] else 0,
                'views': int(row['views']) if row['views'] else 0,
                'category': row['category']
            })
        
        logger.info(f"Found {len(results)} highest rated videos")
        return results
