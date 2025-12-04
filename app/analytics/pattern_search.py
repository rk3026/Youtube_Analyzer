"""
Pattern Search Module.
Find subgraph patterns (related video pairs) using DataFrame joins instead of GraphFrames.
This avoids SparkSession issues that occur with GraphFrames.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql.functions import col, desc

logger = logging.getLogger(__name__)


class PatternSearch:
    """Pattern search for YouTube video network using DataFrame operations."""
    
    def __init__(self, spark_connector):
        """
        Initialize Pattern Search.
        
        Args:
            spark_connector: SparkConnector instance from utils
        """
        self._spark_connector = spark_connector
        self._edges_df = None
        self._videos_df = None
    
    def _load_data(self):
        """Load edges and videos data from MongoDB."""
        if self._edges_df is None:
            self._edges_df = self._spark_connector.load_collection_from_mongo('edges')
            logger.info("Loaded edges collection")
        if self._videos_df is None:
            self._videos_df = self._spark_connector.load_collection_from_mongo('videos')
            logger.info("Loaded videos collection")
    
    def find_related_video_pairs(
        self,
        category: Optional[str] = None,
        max_results: int = 50,
        sample_size: int = 50000
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Find pairs of related videos (a)->(b) using DataFrame joins.
        
        This implements the motif pattern: (a)-[e]->(b)
        Where video 'a' recommends/links to video 'b'.
        
        Args:
            category: Optional category filter for source videos
            max_results: Maximum number of pairs to return
            sample_size: Number of edges to sample for efficiency
            
        Returns:
            Tuple of (results list, performance metrics dict)
        """
        logger.info(f"Finding related video pairs{f' in {category}' if category else ''}...")
        
        start_time = time.time()
        
        self._load_data()
        
        # Sample edges for memory efficiency
        edges_sample = self._edges_df.select('src', 'dst').limit(sample_size)
        edges_processed = sample_size
        
        if category:
            # Filter source videos by category
            # Join edges with videos to get source video category
            category_videos = self._videos_df.filter(
                col('category') == category
            ).select(col('_id').alias('video_id'))
            
            # Find edges where source is in the specified category
            filtered_edges = edges_sample.join(
                category_videos,
                edges_sample['src'] == category_videos['video_id'],
                'inner'
            ).select('src', 'dst')
        else:
            filtered_edges = edges_sample
        
        # Get the pairs
        pairs = filtered_edges.select(
            col('src').alias('source_video'),
            col('dst').alias('related_video')
        ).limit(max_results).collect()
        
        execution_time = time.time() - start_time
        
        results = []
        for row in pairs:
            results.append({
                'source_video': row['source_video'],
                'related_video': row['related_video']
            })
        
        performance = {
            'execution_time': execution_time,
            'rows_processed': edges_processed,
            'rows_returned': len(results),
            'query_type': 'Related Video Pairs Pattern',
            'timestamp': datetime.now(),
            'additional_metrics': {
                'category_filter': category if category else 'None',
                'sample_size': sample_size,
                'pattern': '(a)→(b)'
            }
        }
        
        logger.info(f"Found {len(results)} video pairs in {execution_time:.3f}s")
        return results, performance
    
    def find_video_chains(
        self,
        category: Optional[str] = None,
        chain_length: int = 2,
        max_results: int = 30
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Find chains of related videos: (a)->(b)->(c)
        
        This finds recommendation chains where video 'a' leads to 'b' leads to 'c'.
        
        Args:
            category: Optional category filter for the starting video
            chain_length: Length of chain (2 = a->b->c, 1 = a->b)
            max_results: Maximum number of chains to return
            
        Returns:
            Tuple of (results list, performance metrics dict)
        """
        logger.info(f"Finding video chains of length {chain_length}...")
        
        start_time = time.time()
        
        self._load_data()
        
        edges = self._edges_df.select('src', 'dst').limit(50000)
        edges_processed = 50000
        
        if chain_length == 1:
            # Simple pairs (a)->(b)
            return self.find_related_video_pairs(category, max_results)
        
        # For chains (a)->(b)->(c), do a self-join
        edges_ab = edges.select(
            col('src').alias('a'),
            col('dst').alias('b')
        )
        edges_bc = edges.select(
            col('src').alias('b2'),
            col('dst').alias('c')
        )
        
        # Join to find a->b->c patterns
        chains = edges_ab.join(
            edges_bc,
            edges_ab['b'] == edges_bc['b2'],
            'inner'
        ).select('a', 'b', 'c')
        
        if category:
            # Filter by starting video category
            category_videos = self._videos_df.filter(
                col('category') == category
            ).select(col('_id').alias('video_id'))
            
            chains = chains.join(
                category_videos,
                chains['a'] == category_videos['video_id'],
                'inner'
            ).select('a', 'b', 'c')
        
        chain_results = chains.limit(max_results).collect()
        
        execution_time = time.time() - start_time
        
        results = []
        for row in chain_results:
            results.append({
                'video_a': row['a'],
                'video_b': row['b'],
                'video_c': row['c']
            })
        
        performance = {
            'execution_time': execution_time,
            'rows_processed': edges_processed,
            'rows_returned': len(results),
            'query_type': 'Video Chain Pattern',
            'timestamp': datetime.now(),
            'additional_metrics': {
                'category_filter': category if category else 'None',
                'chain_length': chain_length,
                'pattern': '(a)→(b)→(c)',
                'join_operations': 1
            }
        }
        
        logger.info(f"Found {len(results)} video chains in {execution_time:.3f}s")
        return results, performance
    
    def find_common_recommendations(
        self,
        category: Optional[str] = None,
        min_common: int = 2,
        max_results: int = 30
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Find videos that share common recommendations.
        
        Pattern: (a)->(c)<-(b) where both 'a' and 'b' recommend 'c'.
        
        Args:
            category: Optional category filter
            min_common: Minimum number of common sources to include
            max_results: Maximum results to return
            
        Returns:
            Tuple of (results list, performance metrics dict)
        """
        logger.info(f"Finding common recommendations...")
        
        start_time = time.time()
        
        self._load_data()
        
        edges = self._edges_df.select('src', 'dst').limit(50000)
        edges_processed = 50000
        
        # Self-join on destination to find common targets
        edges_a = edges.select(
            col('src').alias('video_a'),
            col('dst').alias('common_target')
        )
        edges_b = edges.select(
            col('src').alias('video_b'),
            col('dst').alias('common_target2')
        )
        
        # Find pairs pointing to same target
        common = edges_a.join(
            edges_b,
            (edges_a['common_target'] == edges_b['common_target2']) & 
            (edges_a['video_a'] < edges_b['video_b']),  # Avoid duplicates
            'inner'
        ).select('video_a', 'video_b', 'common_target')
        
        if category:
            category_videos = self._videos_df.filter(
                col('category') == category
            ).select(col('_id').alias('video_id'))
            
            common = common.join(
                category_videos,
                common['common_target'] == category_videos['video_id'],
                'inner'
            ).select('video_a', 'video_b', 'common_target')
        
        results_data = common.limit(max_results).collect()
        
        execution_time = time.time() - start_time
        
        results = []
        for row in results_data:
            results.append({
                'video_a': row['video_a'],
                'video_b': row['video_b'],
                'common_target': row['common_target']
            })
        
        performance = {
            'execution_time': execution_time,
            'rows_processed': edges_processed,
            'rows_returned': len(results),
            'query_type': 'Common Recommendations Pattern',
            'timestamp': datetime.now(),
            'additional_metrics': {
                'category_filter': category if category else 'None',
                'min_common_threshold': min_common,
                'pattern': '(a)→(c)←(b)',
                'join_operations': 1
            }
        }
        
        logger.info(f"Found {len(results)} common recommendation patterns in {execution_time:.3f}s")
        return results, performance
