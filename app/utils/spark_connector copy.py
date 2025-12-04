"""
Simple Spark Connector for YouTube Analyzer.
Connects to Spark with minimal configuration.
"""

import os
import sys
from typing import Optional, Dict, Any
import logging
from pyspark.sql import SparkSession
import streamlit as st

logger = logging.getLogger(__name__)


class SparkConnector:
    """Simple Spark connector for the application."""
    
    def __init__(self, mongo_uri: Optional[str] = None):
        """
        Initialize Spark connector.
        
        Args:
            mongo_uri: MongoDB URI for Spark-MongoDB integration
        """
        self.app_name = 'YouTube Analytics'
        self.connection_type = 'local'
        
        # Get MongoDB URI
        if mongo_uri is None:
            mongo_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
        self.mongo_uri = mongo_uri
        
        # Spark session will be initialized on first use (lazy loading)
        self._spark: Optional[SparkSession] = None
        
        # Set up environment variables
        self._setup_environment()
        
        logger.info(f"Spark connector initialized")
    
    def _setup_environment(self):
        """Set up environment variables for Spark."""
        # Set Python executables for PySpark
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        # Set Java/Hadoop homes if not already set (use env vars or defaults)
        if 'JAVA_HOME' not in os.environ:
            default_java = r'C:\Program Files\Java\jdk-17'
            if os.path.exists(default_java):
                os.environ['JAVA_HOME'] = default_java
        
        if 'HADOOP_HOME' not in os.environ:
            default_hadoop = r'C:\hadoop'
            if os.path.exists(default_hadoop):
                os.environ['HADOOP_HOME'] = default_hadoop
    
    @property
    def spark(self) -> SparkSession:
        """Get Spark session, initializing if necessary and checking if it's still active."""
        if self._spark is None:
            self._create_session()
        else:
            # Check if the session is stopped and restart if needed
            try:
                # Try to access the SparkContext to verify it's active
                _ = self._spark.sparkContext.appName
            except Exception:
                # Session is stopped or invalid, recreate it
                logger.warning("Spark session was stopped or invalid, recreating...")
                self._spark = None
                self._create_session()
        return self._spark
    
    def _create_session(self):
        """Create Spark session with local clustering for scalability."""
        try:
            logger.info("Creating Spark session with local clustering...")
            
            # Detect available CPU cores for optimal parallelism
            import multiprocessing
            num_cores = multiprocessing.cpu_count()
            # Use all cores minus 1 to keep system responsive
            worker_cores = max(1, num_cores - 1)
            
            logger.info(f"Detected {num_cores} CPU cores, using {worker_cores} for Spark")
            
            # Build Spark session with clustering optimizations
            builder = (SparkSession.builder
                .appName(self.app_name)
                # Use all available cores for parallel processing
                .master(f'local[{worker_cores}]')
                
                # Memory configuration for clustering
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.memory.fraction", "0.8")  # 80% of heap for execution/storage
                .config("spark.memory.storageFraction", "0.3")  # 30% for cached data
                
                # Parallelism and partitioning for scalability
                .config("spark.default.parallelism", str(worker_cores * 2))
                .config("spark.sql.shuffle.partitions", str(worker_cores * 4))
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                
                # Optimize shuffle for local clustering
                .config("spark.shuffle.compress", "true")
                .config("spark.shuffle.spill.compress", "true")
                .config("spark.io.compression.codec", "snappy")
                
                # Dynamic resource allocation
                .config("spark.dynamicAllocation.enabled", "false")  # Not needed in local mode
                .config("spark.speculation", "true")  # Re-launch slow tasks
                .config("spark.speculation.multiplier", "1.5")
                
                # Network and execution settings
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.network.timeout", "800s")
                .config("spark.executor.heartbeatInterval", "120s")
                .config("spark.sql.broadcastTimeout", "600s")
                .config("spark.rpc.askTimeout", "600s")
                
                # Optimize for large datasets
                .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB per partition
                .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB broadcast threshold
                
                # MongoDB and package configuration
                .config("spark.jars.packages", 
                    "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,"
                    "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
                .config("spark.pyspark.python", sys.executable)
                .config("spark.pyspark.driver.python", sys.executable)
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .config("spark.mongodb.input.uri", self.mongo_uri)
                .config("spark.mongodb.output.uri", self.mongo_uri)
                
                # MongoDB read optimizations for clustering
                .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
                .config("spark.mongodb.input.partitionerOptions.partitionSizeMB", "64")
                .config("spark.mongodb.input.partitionerOptions.samplesPerPartition", "10")
            )
            
            # Create the session
            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")
            
            sc = self._spark.sparkContext
            logger.info(f"Spark session created (version {self._spark.version})")
            logger.info(f"Master: {sc.master}")
            logger.info(f"Default parallelism: {sc.defaultParallelism}")
            logger.info(f"Configured for scalable local clustering with {worker_cores} cores")
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            self._spark = None
            raise
    
    def get_session_info(self) -> Dict[str, Any]:
        """
        Get information about the current Spark session.
        
        Returns:
            Dictionary with Spark session information.
        """
        try:
            if self._spark is None:
                return {
                    'status': 'not_initialized',
                    'connection_type': self.connection_type,
                    'app_name': self.app_name
                }
            
            sc = self._spark.sparkContext
            
            return {
                'status': 'active',
                'connection_type': self.connection_type,
                'app_name': self.app_name,
                'spark_version': self._spark.version,
                'master': sc.master,
                'app_id': sc.applicationId,
                'default_parallelism': sc.defaultParallelism,
                'error': None
            }
        except Exception as e:
            logger.error(f"Error getting Spark session info: {e}")
            return {
                'status': 'error',
                'connection_type': self.connection_type,
                'error': str(e)
            }
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test Spark session by running a simple operation.
        
        Returns:
            Dictionary with test results.
        """
        try:
            # Try to create a simple DataFrame
            data = [(1, "test"), (2, "spark")]
            df = self.spark.createDataFrame(data, ["id", "value"])
            count = df.count()
            
            return {
                'status': 'healthy',
                'test_passed': True,
                'test_result': f"Successfully created and counted DataFrame ({count} rows)",
                'error': None
            }
        except Exception as e:
            logger.error(f"Spark connection test failed: {e}")
            return {
                'status': 'unhealthy',
                'test_passed': False,
                'error': str(e)
            }
    
    def load_collection_from_mongo(
        self,
        collection_name: str,
        database: str = 'youtube_analytics'
    ):
        """
        Load a MongoDB collection into a Spark DataFrame.
        
        Args:
            collection_name: Name of the collection to load
            database: Database name (defaults to youtube_analytics)
            
        Returns:
            Spark DataFrame
        """
        try:
            logger.info(f"Loading collection {collection_name} from MongoDB...")
            
            df = self.spark.read \
                .format("mongodb") \
                .option("database", database) \
                .option("collection", collection_name) \
                .load()
            
            # Don't call count() here - it's expensive and causes timeouts
            logger.info(f"Loaded collection {collection_name} (lazy evaluation)")
            return df
            
        except Exception as e:
            logger.error(f"Error loading collection {collection_name}: {e}")
            raise
    
    def stop(self):
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped")
    
    def restart(self):
        """Restart the Spark session."""
        logger.info("Restarting Spark session...")
        self.stop()
        self._create_session()


# Global Spark connector instance
_spark_connector: Optional[SparkConnector] = None


@st.cache_resource
def get_spark_session(_reload: bool = False) -> SparkSession:
    """
    Get the global Spark session (cached in Streamlit).
    
    Uses Streamlit's cache_resource to ensure single instance across reruns.
    
    Returns:
        SparkSession instance
    """
    connector = get_spark_connector()
    return connector.spark


@st.cache_resource
def get_spark_connector(_reload: bool = False) -> SparkConnector:
    """
    Get the global Spark connector instance (cached in Streamlit).
    
    Uses Streamlit's cache_resource to ensure single instance across reruns.
    The _reload parameter is prefixed with underscore to prevent it from
    affecting the cache key.
    
    Returns:
        SparkConnector instance
    """
    # Create a new connector - Streamlit's cache_resource ensures this
    # only runs once and reuses the same instance across reruns
    connector = SparkConnector()
    return connector
