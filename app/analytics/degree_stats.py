"""
Degree Statistics Analytics Module.
Computes in-degree, out-degree, and total degree statistics for the YouTube network.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, min as spark_min, max as spark_max, stddev, count, desc

logger = logging.getLogger(__name__)


class DegreeAnalytics:
    """Modular degree statistics computation for YouTube video network."""
    
    def __init__(self, spark_connector):
        """
        Initialize degree analytics.
        
        Args:
            spark_connector: SparkConnector instance from utils
        """
        self.spark_connector = spark_connector
        self.spark = spark_connector.spark
    
    def compute_out_degrees(self, edges: DataFrame) -> DataFrame:
        """
        Compute out-degree for each video (number of outgoing edges).
        
        Args:
            edges: DataFrame with 'src' and 'dst' columns
            
        Returns:
            DataFrame with columns: id, outDegree
        """
        logger.info("Computing out-degrees...")
        
        out_degrees = (
            edges
            .select(col("src").alias("id"))
            .groupBy("id")
            .agg(count("*").alias("outDegree"))
        )
        
        # Cache for reuse but don't count here - it's expensive
        out_degrees.cache()
        logger.info("Out-degrees computed (cached)")
        
        return out_degrees
    
    def compute_in_degrees(self, edges: DataFrame) -> DataFrame:
        """
        Compute in-degree for each video (number of incoming edges).
        
        Args:
            edges: DataFrame with 'src' and 'dst' columns
            
        Returns:
            DataFrame with columns: id, inDegree
        """
        logger.info("Computing in-degrees...")
        
        in_degrees = (
            edges
            .select(col("dst").alias("id"))
            .groupBy("id")
            .agg(count("*").alias("inDegree"))
        )
        
        # Cache for reuse but don't count here - it's expensive
        in_degrees.cache()
        logger.info("In-degrees computed (cached)")
        
        return in_degrees
    
    def compute_degree_statistics(
        self, 
        out_degrees: DataFrame, 
        in_degrees: DataFrame
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Combine in/out degrees and compute aggregate statistics.
        
        Args:
            out_degrees: DataFrame with id, outDegree
            in_degrees: DataFrame with id, inDegree
            
        Returns:
            Tuple of (degree_stats DataFrame, aggregate_stats dict)
        """
        logger.info("Computing combined degree statistics...")
        
        # Outer join to include all vertices
        degree_stats = out_degrees.join(
            in_degrees, 
            on="id", 
            how="outer"
        ).na.fill(0)
        
        # Add total degree
        degree_stats = degree_stats.withColumn(
            "total_degree", 
            col("outDegree") + col("inDegree")
        )
        
        degree_stats.cache()
        
        # Compute aggregate statistics
        agg_result = degree_stats.agg(
            count("*").alias("num_vertices"),
            avg("total_degree").alias("avg_degree"),
            stddev("total_degree").alias("std_degree"),
            spark_min("total_degree").alias("min_degree"),
            spark_max("total_degree").alias("max_degree"),
            avg("outDegree").alias("avg_out_degree"),
            spark_max("outDegree").alias("max_out_degree"),
            avg("inDegree").alias("avg_in_degree"),
            spark_max("inDegree").alias("max_in_degree")
        ).collect()[0]
        
        agg_stats = {
            'num_vertices': agg_result['num_vertices'],
            'avg_degree': float(agg_result['avg_degree']) if agg_result['avg_degree'] else 0.0,
            'std_degree': float(agg_result['std_degree']) if agg_result['std_degree'] else 0.0,
            'min_degree': int(agg_result['min_degree']) if agg_result['min_degree'] else 0,
            'max_degree': int(agg_result['max_degree']) if agg_result['max_degree'] else 0,
            'avg_out_degree': float(agg_result['avg_out_degree']) if agg_result['avg_out_degree'] else 0.0,
            'max_out_degree': int(agg_result['max_out_degree']) if agg_result['max_out_degree'] else 0,
            'avg_in_degree': float(agg_result['avg_in_degree']) if agg_result['avg_in_degree'] else 0.0,
            'max_in_degree': int(agg_result['max_in_degree']) if agg_result['max_in_degree'] else 0
        }
        
        logger.info(f"Computed statistics for {agg_stats['num_vertices']:,} vertices")
        
        return degree_stats, agg_stats
    
    def get_top_k_by_degree(
        self, 
        degree_stats: DataFrame, 
        k: int = 10, 
        degree_type: str = "total"
    ) -> list:
        """
        Get top-k videos by degree.
        
        Args:
            degree_stats: DataFrame with degree columns
            k: Number of top videos to return
            degree_type: "total", "in", or "out"
            
        Returns:
            List of dictionaries with video info
        """
        logger.info(f"Getting top {k} videos by {degree_type} degree...")
        
        degree_col = {
            "total": "total_degree",
            "in": "inDegree",
            "out": "outDegree"
        }.get(degree_type, "total_degree")
        
        top_videos = (
            degree_stats
            .orderBy(desc(degree_col))
            .limit(k)
            .collect()
        )
        
        results = []
        for row in top_videos:
            results.append({
                'id': row['id'],
                'total_degree': int(row['total_degree']),
                'in_degree': int(row['inDegree']),
                'out_degree': int(row['outDegree'])
            })
        
        return results
    
    def get_degree_distribution(
        self, 
        degree_stats: DataFrame, 
        bins: int = 20
    ) -> list:
        """
        Get degree distribution histogram data.
        
        Args:
            degree_stats: DataFrame with degree columns
            bins: Maximum number of bins to return
            
        Returns:
            List of dictionaries with degree and count
        """
        logger.info(f"Computing degree distribution (max {bins} bins)...")
        
        distribution = (
            degree_stats
            .groupBy("total_degree")
            .agg(count("*").alias("num_videos"))
            .orderBy("total_degree")
            .limit(bins)
            .collect()
        )
        
        results = []
        for row in distribution:
            results.append({
                'degree': int(row['total_degree']),
                'count': int(row['num_videos'])
            })
        
        return results
    
    def run_full_analysis(
        self, 
        sample_size: int = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Run complete degree analysis pipeline.
        
        Args:
            sample_size: Optional sample size for edges (None = use all)
            
        Returns:
            Tuple of (results dict, performance metrics dict)
        """
        logger.info("Starting full degree analysis...")
        start_time = time.time()
        
        # Load data
        edges = self.spark_connector.load_collection_from_mongo('edges')
        
        # Sample if requested
        if sample_size:
            total_edges = edges.count()
            if sample_size < total_edges:
                fraction = sample_size / total_edges
                edges = edges.sample(withReplacement=False, fraction=fraction)
                logger.info(f"Sampled {sample_size:,} edges from {total_edges:,} total")
        
        # Compute degrees
        out_degrees = self.compute_out_degrees(edges)
        in_degrees = self.compute_in_degrees(edges)
        degree_stats, agg_stats = self.compute_degree_statistics(out_degrees, in_degrees)
        
        # Get additional insights
        top_10 = self.get_top_k_by_degree(degree_stats, k=10)
        distribution = self.get_degree_distribution(degree_stats, bins=50)
        
        execution_time = time.time() - start_time
        
        results = {
            'aggregate_stats': agg_stats,
            'top_videos': top_10,
            'distribution': distribution,
            'degree_stats_df': degree_stats  # For further analysis
        }
        
        performance = {
            'execution_time': execution_time,
            'rows_processed': edges.count() if sample_size is None else sample_size,
            'rows_returned': len(top_10) + len(distribution),
            'query_type': 'Degree Analysis',
            'timestamp': datetime.now(),
            'additional_metrics': {
                'sample_size': sample_size,
                'analysis_type': 'degree_distribution'
            }
        }
        
        logger.info(f"Degree analysis completed in {execution_time:.3f}s")
        return results, performance
    
    def save_to_mongo(self, degree_stats: DataFrame, collection: str = "degree_statistics"):
        """
        Save degree statistics to MongoDB.
        
        Args:
            degree_stats: DataFrame to save
            collection: Target collection name
        """
        logger.info(f"Saving degree statistics to MongoDB collection '{collection}'...")
        
        degree_stats.write \
            .format("mongodb") \
            .mode("overwrite") \
            .option("database", "youtube_analytics") \
            .option("collection", collection) \
            .save()
        
        logger.info("Degree statistics saved successfully")
