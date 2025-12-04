"""
Category Statistics Analytics Module.
Computes categorized video statistics (by category, length, views).
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, desc, when

logger = logging.getLogger(__name__)


class CategoryAnalytics:
    """Modular category and bucket statistics computation."""
    
    def __init__(self, spark_connector):
        """
        Initialize category analytics.
        
        Args:
            spark_connector: SparkConnector instance from utils
        """
        self.spark_connector = spark_connector
        self.spark = spark_connector.spark
    
    def compute_category_distribution(self, videos: DataFrame) -> list:
        """
        Compute video count by category.
        
        Args:
            videos: DataFrame with video metadata
            
        Returns:
            List of dictionaries with category and count
        """
        logger.info("Computing category distribution...")
        
        if "category" not in videos.columns:
            logger.warning("'category' column not found in videos")
            return []
        
        category_freq = (
            videos
            .groupBy("category")
            .agg(count("*").alias("num_videos"))
            .orderBy(desc("num_videos"))
            .limit(15)  # Get top 15 categories
            .collect()
        )
        
        results = []
        for row in category_freq:
            results.append({
                'category': row['category'],
                'count': int(row['num_videos'])
            })
        
        logger.info(f"Found top {len(results)} categories")
        return results
    
    def compute_length_distribution(self, videos: DataFrame) -> list:
        """
        Compute video count by length buckets.
        
        Args:
            videos: DataFrame with video metadata
            
        Returns:
            List of dictionaries with bucket and count
        """
        logger.info("Computing length distribution...")
        
        if "length_sec" not in videos.columns:
            logger.warning("'length_sec' column not found in videos")
            return []
        
        # Define length buckets
        length_bucket = (
            when(col("length_sec") <= 120, "0-2m")
            .when(col("length_sec") <= 300, "2-5m")
            .when(col("length_sec") <= 600, "5-10m")
            .when(col("length_sec") <= 1200, "10-20m")
            .otherwise(">20m")
        )
        
        length_dist = (
            videos
            .select(length_bucket.alias("length_bucket"))
            .groupBy("length_bucket")
            .agg(count("*").alias("num_videos"))
            .orderBy("length_bucket")
            .collect()
        )
        
        results = []
        for row in length_dist:
            results.append({
                'bucket': row['length_bucket'],
                'count': int(row['num_videos'])
            })
        
        logger.info(f"Computed {len(results)} length buckets")
        return results
    
    def compute_view_distribution(self, videos: DataFrame) -> list:
        """
        Compute video count by view count buckets.
        
        Args:
            videos: DataFrame with video metadata
            
        Returns:
            List of dictionaries with bucket and count
        """
        logger.info("Computing view count distribution...")
        
        # Check for view count column (multiple possible names)
        view_col = None
        if "view_count" in videos.columns:
            view_col = col("view_count")
        elif "views" in videos.columns:
            view_col = col("views")
        else:
            logger.warning("No view count column found in videos")
            return []
        
        # Define view count buckets
        view_bucket = (
            when(view_col <= 1_000, "0-1K")
            .when(view_col <= 10_000, "1K-10K")
            .when(view_col <= 100_000, "10K-100K")
            .when(view_col <= 1_000_000, "100K-1M")
            .when(view_col <= 10_000_000, "1M-10M")
            .otherwise(">10M")
        )
        
        view_dist = (
            videos
            .select(view_bucket.alias("view_bucket"))
            .groupBy("view_bucket")
            .agg(count("*").alias("num_videos"))
            .orderBy("view_bucket")
            .collect()
        )
        
        results = []
        for row in view_dist:
            results.append({
                'bucket': row['view_bucket'],
                'count': int(row['num_videos'])
            })
        
        logger.info(f"Computed {len(results)} view count buckets")
        return results
    
    def run_full_analysis(self, sample_size: int = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Run complete category analysis pipeline.
        
        Args:
            sample_size: Optional sample size for videos (None = use all)
            
        Returns:
            Tuple of (results dict, performance metrics dict)
        """
        logger.info("Starting full category analysis...")
        start_time = time.time()
        
        # Load videos
        videos = self.spark_connector.load_collection_from_mongo('videos')
        
        # Sample if requested
        if sample_size:
            total_videos = videos.count()
            if sample_size < total_videos:
                fraction = sample_size / total_videos
                videos = videos.sample(withReplacement=False, fraction=fraction)
                logger.info(f"Sampled {sample_size:,} videos from {total_videos:,} total")
        
        # Compute distributions from videos collection
        category_dist = self.compute_category_distribution(videos)
        length_dist = self.compute_length_distribution(videos)
        
        # Load video_snapshots for view data (has 'views' field)
        try:
            snapshots = self.spark_connector.load_collection_from_mongo('video_snapshots')
            if sample_size:
                total_snapshots = snapshots.count()
                if sample_size < total_snapshots:
                    fraction = sample_size / total_snapshots
                    snapshots = snapshots.sample(withReplacement=False, fraction=fraction)
            view_dist = self.compute_view_distribution(snapshots)
        except Exception as e:
            logger.warning(f"Could not load video_snapshots for view data: {e}")
            view_dist = []
        
        execution_time = time.time() - start_time
        
        results = {
            'by_category': category_dist,
            'by_length': length_dist,
            'by_views': view_dist
        }
        
        performance = {
            'execution_time': execution_time,
            'rows_processed': videos.count() if sample_size is None else sample_size,
            'rows_returned': len(category_dist) + len(length_dist) + len(view_dist),
            'query_type': 'Category Analysis',
            'timestamp': datetime.now(),
            'additional_metrics': {
                'sample_size': sample_size,
                'analysis_type': 'categorized_statistics'
            }
        }
        
        logger.info(f"Category analysis completed in {execution_time:.3f}s")
        return results, performance
    
    def save_to_mongo(self, results: Dict[str, list], collection_prefix: str = "categorized_statistics"):
        """
        Save categorized statistics to MongoDB.
        
        Args:
            results: Dictionary with analysis results
            collection_prefix: Prefix for collection names
        """
        logger.info(f"Saving categorized statistics to MongoDB with prefix '{collection_prefix}'...")
        
        for key, data in results.items():
            if not data:
                continue
            
            # Convert list of dicts to DataFrame
            df = self.spark.createDataFrame(data)
            
            collection_name = f"{collection_prefix}_{key}"
            df.write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("database", "youtube_analytics") \
                .option("collection", collection_name) \
                .save()
            
            logger.info(f"Saved {len(data)} records to '{collection_name}'")
        
        logger.info("All categorized statistics saved successfully")
