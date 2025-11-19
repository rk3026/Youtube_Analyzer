"""
Spark Connector with abstraction for different Spark configurations.
Supports local, standalone cluster, and custom Spark setups without hardcoding.
"""

from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
import logging
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import streamlit as st

# Add parent directory to path for absolute imports
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config

logger = logging.getLogger(__name__)


class SparkConfigurationStrategy(ABC):
    """Abstract base class for Spark configuration strategies."""
    
    @abstractmethod
    def configure_builder(
        self,
        builder: SparkSession.Builder,
        settings: Dict[str, Any]
    ) -> SparkSession.Builder:
        """
        Configure Spark session builder with strategy-specific settings.
        
        Args:
            builder: SparkSession.Builder instance
            settings: Strategy-specific settings
            
        Returns:
            Configured SparkSession.Builder
        """
        pass


class LocalSparkStrategy(SparkConfigurationStrategy):
    """Strategy for local Spark sessions."""
    
    def configure_builder(
        self,
        builder: SparkSession.Builder,
        settings: Dict[str, Any]
    ) -> SparkSession.Builder:
        master = settings.get('master', 'local[*]')
        driver_memory = settings.get('driver_memory', '4g')
        executor_memory = settings.get('executor_memory', '4g')
        
        return builder \
            .master(master) \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory)


class StandaloneSparkStrategy(SparkConfigurationStrategy):
    """Strategy for standalone Spark cluster connections."""
    
    def configure_builder(
        self,
        builder: SparkSession.Builder,
        settings: Dict[str, Any]
    ) -> SparkSession.Builder:
        master = settings.get('master', 'spark://localhost:7077')
        driver_memory = settings.get('driver_memory', '4g')
        executor_memory = settings.get('executor_memory', '4g')
        executor_instances = settings.get('executor_instances', 2)
        
        return builder \
            .master(master) \
            .config("spark.driver.memory", driver_memory) \
            .config("spark.executor.memory", executor_memory) \
            .config("spark.executor.instances", executor_instances)


class CustomSparkStrategy(SparkConfigurationStrategy):
    """Strategy for custom Spark configurations."""
    
    def configure_builder(
        self,
        builder: SparkSession.Builder,
        settings: Dict[str, Any]
    ) -> SparkSession.Builder:
        # Apply all settings from custom config
        master = settings.get('master', 'local[*]')
        builder = builder.master(master)
        
        # Apply any additional configs provided
        for key, value in settings.items():
            if key != 'master' and key.startswith('spark.'):
                builder = builder.config(key, value)
            elif key == 'driver_memory':
                builder = builder.config("spark.driver.memory", value)
            elif key == 'executor_memory':
                builder = builder.config("spark.executor.memory", value)
        
        return builder


class SparkConnector:
    """
    Spark connector with support for multiple configuration strategies.
    Provides abstraction layer between GUI and Spark sessions.
    """
    
    STRATEGIES = {
        'local': LocalSparkStrategy(),
        'standalone': StandaloneSparkStrategy(),
        'custom': CustomSparkStrategy()
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Spark connector.
        
        Args:
            config: Optional configuration dictionary. If None, loads from config file.
        """
        if config is None:
            config_loader = get_config()
            config = config_loader.get_spark_config()
        
        self.config = config
        self.connection_type = config['connection_type']
        self.app_name = config['app_name']
        
        # Get appropriate strategy
        self.strategy = self.STRATEGIES.get(self.connection_type)
        if not self.strategy:
            raise ValueError(f"Unknown Spark connection type: {self.connection_type}")
        
        # Spark session will be initialized on first use (lazy loading)
        self._spark: Optional[SparkSession] = None
        
        # Set up environment variables
        self._setup_environment(config.get('environment', {}))
        
        logger.info(f"Spark connector initialized with {self.connection_type} strategy")
    
    def _setup_environment(self, env_config: Dict[str, Any]):
        """Set up environment variables for Spark."""
        # Java Home
        if 'JAVA_HOME' in env_config and env_config['JAVA_HOME']:
            os.environ['JAVA_HOME'] = env_config['JAVA_HOME']
            logger.info(f"Set JAVA_HOME to {env_config['JAVA_HOME']}")
        
        # Hadoop Home
        if 'HADOOP_HOME' in env_config and env_config['HADOOP_HOME']:
            os.environ['HADOOP_HOME'] = env_config['HADOOP_HOME']
            logger.info(f"Set HADOOP_HOME to {env_config['HADOOP_HOME']}")
        
        # PySpark Python executables
        python_executable = env_config.get('PYSPARK_PYTHON') or sys.executable
        driver_python = env_config.get('PYSPARK_DRIVER_PYTHON') or sys.executable
        
        # Force set these environment variables (don't use setdefault)
        os.environ['PYSPARK_PYTHON'] = python_executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = driver_python
        logger.info(f"Set PYSPARK_PYTHON to {python_executable}")
        logger.info(f"Set PYSPARK_DRIVER_PYTHON to {driver_python}")
    
    @property
    def spark(self) -> SparkSession:
        """Get Spark session, initializing if necessary."""
        if self._spark is None:
            self._create_session()
        return self._spark
    
    def _create_session(self):
        """Create Spark session with configured settings."""
        try:
            logger.info(f"Creating Spark session using {self.connection_type} strategy...")
            
            # Start with base builder
            builder = SparkSession.builder.appName(self.app_name)
            
            # Apply strategy-specific configuration
            builder = self.strategy.configure_builder(
                builder,
                self.config['settings']
            )
            
            # Add Spark packages (MongoDB connector, GraphFrames, etc.)
            packages = self.config.get('packages', [])
            if packages:
                packages_str = ','.join(packages)
                builder = builder.config("spark.jars.packages", packages_str)
                logger.info(f"Loading packages: {packages_str}")
            
            # Add additional Spark configurations
            additional_conf = self.config.get('conf', {})
            for key, value in additional_conf.items():
                builder = builder.config(key, value)
            
            # Add Python paths to Spark config for better debugging
            builder = builder.config("spark.pyspark.python", os.environ.get('PYSPARK_PYTHON', sys.executable))
            builder = builder.config("spark.pyspark.driver.python", os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable))
            
            # Disable Arrow optimization which can cause Python worker crashes with Python 3.12
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
            builder = builder.config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            
            # Add MongoDB connection URI if available
            mongo_config = get_config().get_mongodb_config()
            # This allows Spark to read directly from MongoDB
            mongo_connector = None
            try:
                from .mongo_connector import get_mongo_connector
                mongo_connector = get_mongo_connector()
                builder = builder.config("spark.mongodb.input.uri", mongo_connector.uri)
                builder = builder.config("spark.mongodb.output.uri", mongo_connector.uri)
                logger.info("MongoDB URI configured for Spark")
            except Exception as e:
                logger.warning(f"Could not configure MongoDB URI for Spark: {e}")
            
            # Create the session
            self._spark = builder.getOrCreate()
            
            # Set log level to reduce noise
            self._spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session created successfully")
            logger.info(f"Spark version: {self._spark.version}")
            logger.info(f"Spark master: {self._spark.sparkContext.master}")
            
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
        database: Optional[str] = None
    ):
        """
        Load a MongoDB collection into a Spark DataFrame.
        
        Args:
            collection_name: Name of the collection to load
            database: Database name (uses config default if None)
            
        Returns:
            Spark DataFrame
        """
        try:
            if database is None:
                database = get_config().get_mongodb_config()['database']
            
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
