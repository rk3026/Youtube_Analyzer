#!/usr/bin/env python3
"""YouTube Network Degree & Categorized Stats
Computes per-video in/out/total degree, summary stats, degree histogram,
and category/length/view bucket counts from MongoDB using PySpark.
Designed for Windows local execution (Java 17, local[*], Mongo connector);
minimal dependencies; scalable via DataFrame groupBy/joins.
"""

import logging
import time
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min as spark_min, max as spark_max, count, desc, when

# Configuration
MONGO_URI = "mongodb://localhost:27017/youtube_analytics"
DATABASE_NAME = "youtube_analytics"

def setup_logging():
    """Configure logging for the degree computation pipeline"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('degree_computation.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class DegreeComputation:
    """Pipeline: load data -> degrees -> aggregates -> categorized stats -> optional save."""
    
    def __init__(self, mongo_uri: str, database_name: str):
        self.logger = setup_logging()
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.spark = None
        self.graph = None
        
    def initialize_spark(self):
        """Init Spark session (Mongo connector, Windows-friendly settings)."""
        self.logger.info("[INIT] Initializing Spark session...")
        
        # Set Java 17 as JAVA_HOME (compatible with Spark)
        java17_home = os.environ.get('JAVA_HOME', "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.16.8-hotspot")
        os.environ['JAVA_HOME'] = java17_home
        
        # Set HADOOP_HOME to avoid Windows issues
        hadoop_home = os.environ.get('HADOOP_HOME', "C:\\hadoop")
        os.environ['HADOOP_HOME'] = hadoop_home
        
        self.logger.info(f"[JAVA] Using Java 17: {java17_home}")
        self.logger.info(f"[HADOOP] Setting HADOOP_HOME: {hadoop_home}")
        
        # Windows-specific configuration
        os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
        os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)
        
    # Configure Spark session with Windows-friendly settings
    # Using compatible versions: Spark 3.5.x with Scala 2.12 (no GraphFrames needed)
        try:
            self.spark = SparkSession.builder \
                .appName("YouTube Network Degree Computation") \
                .config("spark.mongodb.input.uri", self.mongo_uri) \
                .config("spark.mongodb.output.uri", self.mongo_uri) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
                .config("spark.driver.memory", "3g") \
                .config("spark.executor.memory", "3g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "localhost") \
                .config("spark.ui.enabled", "false") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
                .config("spark.hadoop.mapreduce.application.classpath", "$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*") \
                .master("local[*]") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("[SUCCESS] Spark session initialized with Java 17")
            
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to initialize Spark: {e}")
            raise
        
    def load_graph_data(self):
        """Load vertices (_id->id + optional attrs) and edges (src,dst) from MongoDB."""
        self.logger.info("[LOAD] Loading graph data from MongoDB...")
        
        # Load vertices (videos)
        # Index benefit: MongoDB uses the _id index for efficient document retrieval
        self.logger.info("  Loading vertices from 'videos' collection...")
        vertices_df = self.spark.read \
            .format("mongo") \
            .option("uri", f"{self.mongo_uri}.videos") \
            .load()
        
        # Select core columns and include optional metrics (e.g., view count) if present
        vcols = vertices_df.columns
        optional_cols = []
        if "category" in vcols: optional_cols.append(col("category"))
        if "uploader" in vcols: optional_cols.append(col("uploader"))
        if "length_sec" in vcols: optional_cols.append(col("length_sec"))
        # try common view count field names
        if "view_count" in vcols:
            optional_cols.append(col("view_count"))
        elif "views" in vcols:
            optional_cols.append(col("views").alias("view_count"))

        vertices = vertices_df.select(col("_id").alias("id"), *optional_cols)
        
        vertex_count = vertices.count()
        self.logger.info(f"  Loaded {vertex_count:,} vertices")
        
        # Load edges
        # Index benefit: MongoDB uses src and dst indexes for fast edge retrieval
        self.logger.info("  Loading edges from 'edges' collection...")
        edges_df = self.spark.read \
            .format("mongo") \
            .option("uri", f"{self.mongo_uri}.edges") \
            .load()
        
        # Select and rename columns for edge list (src, dst)
        edges = edges_df.select(
            col("src"),
            col("dst")
        )
        
        edge_count = edges.count()
        self.logger.info(f"  Loaded {edge_count:,} edges")

        # Persist DataFrames on the instance for reuse
        self.vertices = vertices
        self.edges = edges
        return vertices, edges

    # PageRank intentionally excluded from this script per team division of work.

    def compute_categorized_statistics(self, vertices):
        """Category, length bucket, view bucket counts (if fields exist)."""
        self.logger.info("[COMPUTE] Computing categorized statistics...")
        start = time.time()

        results = {}

        # Category frequency
        if "category" in vertices.columns:
            category_freq = vertices.groupBy("category").agg(count("*").alias("num_videos")).orderBy(desc("num_videos"))
            results["by_category"] = category_freq
        else:
            self.logger.warning("[WARN] 'category' column not found; skipping category frequency.")

        # Length buckets
        if "length_sec" in vertices.columns:
            vb = when(col("length_sec") <= 120, "0-2m") \
                 .when(col("length_sec") <= 300, "2-5m") \
                 .when(col("length_sec") <= 600, "5-10m") \
                 .when(col("length_sec") <= 1200, "10-20m") \
                 .otherwise(">20m")
            length_buckets_df = vertices.select(vb.alias("length_bucket")) \
                .groupBy("length_bucket").agg(count("*").alias("num_videos")) \
                .orderBy("length_bucket")
            results["by_length_bucket"] = length_buckets_df
        else:
            self.logger.warning("[WARN] 'length_sec' column not found; skipping length buckets.")

        # View count buckets (if present)
        view_col = None
        if "view_count" in vertices.columns:
            view_col = col("view_count")
        elif "views" in vertices.columns:
            view_col = col("views")

        if view_col is not None:
            vbv = when(view_col <= 1_000, "0-1K") \
                  .when(view_col <= 10_000, "1K-10K") \
                  .when(view_col <= 100_000, "10K-100K") \
                  .when(view_col <= 1_000_000, "100K-1M") \
                  .when(view_col <= 10_000_000, "1M-10M") \
                  .otherwise(">10M")
            view_buckets_df = vertices.select(vbv.alias("view_bucket")) \
                .groupBy("view_bucket").agg(count("*").alias("num_videos")) \
                .orderBy("view_bucket")
            results["by_view_bucket"] = view_buckets_df
        else:
            self.logger.info("[INFO] No view count column found; skipping view buckets.")

        elapsed = time.time() - start
        self.logger.info(f"[SUCCESS] Categorized statistics computed in {elapsed:.2f}s")
        return results

    def display_categorized_results(self, cat_stats):
        self.logger.info("\n" + "="*70)
        self.logger.info("CATEGORIZED VIDEO STATISTICS")
        self.logger.info("="*70)

        if "by_category" in cat_stats:
            self.logger.info("\n[BY CATEGORY] Top 15")
            for row in cat_stats["by_category"].limit(15).collect():
                self.logger.info(f"  {row['category']}: {row['num_videos']:,}")

        if "by_length_bucket" in cat_stats:
            self.logger.info("\n[BY LENGTH BUCKET]")
            for row in cat_stats["by_length_bucket"].collect():
                self.logger.info(f"  {row['length_bucket']:>6}: {row['num_videos']:,}")

        if "by_view_bucket" in cat_stats:
            self.logger.info("\n[BY VIEW COUNT BUCKET]")
            for row in cat_stats["by_view_bucket"].collect():
                self.logger.info(f"  {row['view_bucket']:>8}: {row['num_videos']:,}")

    def save_categorized_results(self, cat_stats, output_collection_prefix="categorized_statistics"):
        self.logger.info(f"[SAVE] Saving categorized statistics to Mongo with prefix '{output_collection_prefix}'...")
        start = time.time()
        for key, df in cat_stats.items():
            df.write \
                .format("mongo") \
                .mode("overwrite") \
                .option("uri", f"{self.mongo_uri}.{output_collection_prefix}_{key}") \
                .save()
        self.logger.info(f"[SUCCESS] Categorized results saved in {time.time()-start:.2f}s")
    
    def compute_out_degrees(self):
        """outDegree = count edges by src."""
        self.logger.info("[COMPUTE] Computing out-degrees...")
        start_time = time.time()
        
        # Pure PySpark: count edges grouped by source as out-degree
        out_degrees = (
            self.edges
            .select(col("src").alias("id"))
            .groupBy("id")
            .agg(count("*").alias("outDegree"))
        )
        
        # Cache for reuse
        out_degrees.cache()
        out_degree_count = out_degrees.count()
        
        elapsed = time.time() - start_time
        self.logger.info(f"[SUCCESS] Computed out-degrees for {out_degree_count:,} vertices in {elapsed:.2f}s")
        
        return out_degrees
    
    def compute_in_degrees(self):
        """inDegree = count edges by dst."""
        self.logger.info("[COMPUTE] Computing in-degrees...")
        start_time = time.time()
        
        # Pure PySpark: count edges grouped by destination as in-degree
        in_degrees = (
            self.edges
            .select(col("dst").alias("id"))
            .groupBy("id")
            .agg(count("*").alias("inDegree"))
        )
        
        # Cache for reuse
        in_degrees.cache()
        in_degree_count = in_degrees.count()
        
        elapsed = time.time() - start_time
        self.logger.info(f"[SUCCESS] Computed in-degrees for {in_degree_count:,} vertices in {elapsed:.2f}s")
        
        return in_degrees
    
    def compute_degree_statistics(self, out_degrees, in_degrees):
        """Outer join degrees, fill 0, add total_degree, compute aggregates."""
        self.logger.info("[COMPUTE] Computing combined degree statistics...")
        start_time = time.time()
        
        # Join in-degrees and out-degrees
        # Use outer join to include vertices with only in-degrees or only out-degrees
        degree_stats = out_degrees.join(
            in_degrees, 
            on="id", 
            how="outer"
        ).na.fill(0)  # Fill missing values with 0
        
        # Compute total degree
        degree_stats = degree_stats.withColumn(
            "total_degree", 
            col("outDegree") + col("inDegree")
        )
        
        # Cache for multiple aggregations
        degree_stats.cache()
        
        # Compute aggregate statistics
        self.logger.info("  Computing aggregate statistics...")
        agg_stats = degree_stats.agg(
            avg("total_degree").alias("avg_degree"),
            spark_min("total_degree").alias("min_degree"),
            spark_max("total_degree").alias("max_degree"),
            avg("outDegree").alias("avg_out_degree"),
            spark_max("outDegree").alias("max_out_degree"),
            avg("inDegree").alias("avg_in_degree"),
            spark_max("inDegree").alias("max_in_degree")
        ).collect()[0]
        
        elapsed = time.time() - start_time
        self.logger.info(f"[SUCCESS] Computed degree statistics in {elapsed:.2f}s")
        
        return degree_stats, agg_stats
    
    def display_results(self, degree_stats, agg_stats):
        """Log aggregate stats, top 10, histogram."""
        self.logger.info("\n" + "="*70)
        self.logger.info("DEGREE DISTRIBUTION STATISTICS")
        self.logger.info("="*70)
        
        # Display aggregate statistics
        self.logger.info("\n[AGGREGATE STATISTICS]")
        self.logger.info(f"  Average Total Degree:    {agg_stats['avg_degree']:.2f}")
        self.logger.info(f"  Minimum Total Degree:    {agg_stats['min_degree']}")
        self.logger.info(f"  Maximum Total Degree:    {agg_stats['max_degree']}")
        self.logger.info(f"\n  Average Out-Degree:      {agg_stats['avg_out_degree']:.2f}")
        self.logger.info(f"  Maximum Out-Degree:      {agg_stats['max_out_degree']}")
        self.logger.info(f"\n  Average In-Degree:       {agg_stats['avg_in_degree']:.2f}")
        self.logger.info(f"  Maximum In-Degree:       {agg_stats['max_in_degree']}")
        
        # Display top 10 vertices by total degree
        self.logger.info("\n[TOP 10 VIDEOS BY TOTAL DEGREE]")
        top_degree_videos = degree_stats.orderBy(desc("total_degree")).limit(10)
        
        for row in top_degree_videos.collect():
            self.logger.info(f"  Video ID: {row['id']}")
            self.logger.info(f"    Total Degree: {row['total_degree']} (In: {row['inDegree']}, Out: {row['outDegree']})")
        
        # Display degree distribution (histogram)
        self.logger.info("\n[DEGREE DISTRIBUTION HISTOGRAM]")
        degree_distribution = degree_stats.groupBy("total_degree") \
            .agg(count("*").alias("num_videos")) \
            .orderBy("total_degree") \
            .limit(20)
        
        self.logger.info("  Degree | Number of Videos")
        self.logger.info("  " + "-"*30)
        for row in degree_distribution.collect():
            self.logger.info(f"  {row['total_degree']:6} | {row['num_videos']:,}")
        
        self.logger.info("\n" + "="*70)
    
    def save_results(self, degree_stats, output_collection="degree_statistics"):
        """Persist degree stats to MongoDB."""
        self.logger.info(f"[SAVE] Saving results to MongoDB collection '{output_collection}'...")
        start_time = time.time()
        
        # Write results to MongoDB
        degree_stats.write \
            .format("mongo") \
            .mode("overwrite") \
            .option("uri", f"{self.mongo_uri}.{output_collection}") \
            .save()
        
        elapsed = time.time() - start_time
        self.logger.info(f"[SUCCESS] Results saved in {elapsed:.2f}s")
    
    def run(self, save_to_mongo=True):
        """Run full pipeline; optionally save results."""
        self.logger.info("="*70)
        self.logger.info("YOUTUBE NETWORK DEGREE COMPUTATION PIPELINE")
        self.logger.info("="*70)
        
        pipeline_start = time.time()
        
        try:
            # Step 1: Initialize Spark
            self.initialize_spark()
            
            # Step 2: Load graph data from MongoDB
            vertices, edges = self.load_graph_data()
            
            # Step 3: Compute out-degrees
            out_degrees = self.compute_out_degrees()
            
            # Step 4: Compute in-degrees
            in_degrees = self.compute_in_degrees()
            
            # Step 5: Compute combined degree statistics
            degree_stats, agg_stats = self.compute_degree_statistics(out_degrees, in_degrees)
            
            # Step 6: Display results
            self.display_results(degree_stats, agg_stats)
            
            # Step 7: Categorized stats over videos
            cat_stats = self.compute_categorized_statistics(vertices)
            self.display_categorized_results(cat_stats)

            # Step 8: Save results to MongoDB (optional)
            if save_to_mongo:
                self.save_results(degree_stats)
                self.save_categorized_results(cat_stats)
            
            pipeline_elapsed = time.time() - pipeline_start
            self.logger.info(f"\n[COMPLETE] Total pipeline execution time: {pipeline_elapsed:.2f}s")
            
            return degree_stats, agg_stats
            
        except Exception as e:
            self.logger.error(f"[ERROR] Pipeline failed: {e}")
            raise
        
        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("[CLEANUP] Spark session stopped")

def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Compute degree statistics and categorized video statistics for YouTube network'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save results to MongoDB (display only)'
    )
    args = parser.parse_args()
    
    # Initialize and run degree computation
    computer = DegreeComputation(MONGO_URI, DATABASE_NAME)
    computer.run(save_to_mongo=not args.no_save)

if __name__ == "__main__":
    main()