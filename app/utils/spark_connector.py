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
        """Get Spark session, initializing if necessary."""
        if self._spark is None:
            self._create_session()
        return self._spark
    
    def _create_session(self):
        """Create Spark session with configured settings."""
        try:
            logger.info("Creating Spark session...")
            
            # Build Spark session
            builder = (SparkSession.builder
                .appName(self.app_name)
                .master('local[*]')
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.jars.packages", 
                    "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0,"
                    "graphframes:graphframes:0.8.4-spark3.5-s_2.12")
                .config("spark.pyspark.python", sys.executable)
                .config("spark.pyspark.driver.python", sys.executable)
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .config("spark.mongodb.input.uri", self.mongo_uri)
                .config("spark.mongodb.output.uri", self.mongo_uri)
            )
            
            # Create the session
            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")
            
            logger.info(f"Spark session created (version {self._spark.version})")
            
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
            
            logger.info(f"Loaded {collection_name}: {df.count()} rows")
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
def get_spark_session(reload: bool = False) -> SparkSession:
    """
    Get the global Spark session (cached in Streamlit).
    
    Args:
        reload: If True, recreate the Spark session
        
    Returns:
        SparkSession instance
    """
    global _spark_connector
    
    if _spark_connector is None or reload:
        _spark_connector = SparkConnector()
    
    return _spark_connector.spark


@st.cache_resource
def get_spark_connector(reload: bool = False) -> SparkConnector:
    """
    Get the global Spark connector instance (cached in Streamlit).
    
    Args:
        reload: If True, recreate the connector
        
    Returns:
        SparkConnector instance
    """
    global _spark_connector
    
    if _spark_connector is None or reload:
        _spark_connector = SparkConnector()
    
    return _spark_connector
