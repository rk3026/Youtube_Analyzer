"""
Configuration Loader for YouTube Analyzer.
Loads configuration from .env files and environment variables.
"""

import os
from typing import Dict, Any, Optional
import logging
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not available, continue without it
    pass

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Loads configuration from .env files and environment variables.
    Provides sensible defaults for all settings.
    """

    def __init__(self):
        """Initialize the configuration loader."""
        logger.info("Configuration loaded from .env and environment variables")

    def get_mongodb_config(self) -> Dict[str, Any]:
        """
        Get MongoDB configuration from environment variables.

        Returns:
            Dictionary with MongoDB connection settings.
        """
        # Determine connection type from environment
        mongo_uri = os.environ.get('MONGODB_URI')
        atlas_uri = os.environ.get('MONGODB_ATLAS_URI')

        if atlas_uri:
            connection_type = 'atlas'
            uri = atlas_uri
        elif mongo_uri:
            connection_type = 'custom'
            uri = mongo_uri
        else:
            connection_type = 'local'
            uri = 'mongodb://localhost:27017/youtube_analytics'

        # Build settings based on connection type
        if connection_type == 'local':
            settings = {
                'host': 'localhost',
                'port': 27017,
                'database': 'youtube_analytics'
            }
        elif connection_type == 'atlas':
            settings = {
                'connection_string': uri,
                'database': 'youtube_analytics'
            }
        else:  # custom
            settings = {
                'uri': uri,
                'database': 'youtube_analytics'
            }

        return {
            'connection_type': connection_type,
            'settings': settings,
            'database': 'youtube_analytics',
            'collections': ['videos', 'edges', 'video_snapshots', 'crawls'],
            'options': {
                'serverSelectionTimeoutMS': 5000,
                'connectTimeoutMS': 10000
            }
        }

    def get_spark_config(self) -> Dict[str, Any]:
        """
        Get Spark configuration from environment variables.

        Returns:
            Dictionary with Spark connection settings.
        """
        # Get Spark master from environment or default to local
        master = os.environ.get('SPARK_MASTER', 'local[*]')

        # Determine connection type based on master
        if master.startswith('spark://'):
            connection_type = 'standalone'
        elif master == 'local[*]' or master.startswith('local['):
            connection_type = 'local'
        else:
            connection_type = 'custom'

        # Build settings
        settings = {
            'master': master,
            'driver_memory': '4g',
            'executor_memory': '4g'
        }

        if connection_type == 'standalone':
            settings['executor_instances'] = 2

        return {
            'connection_type': connection_type,
            'app_name': 'YouTube Analytics',
            'settings': settings,
            'packages': [
                'org.mongodb.spark:mongo-spark-connector_2.12:10.5.0',
                'graphframes:graphframes:0.8.4-spark3.5-s_2.12'
            ],
            'conf': {
                'spark.sql.shuffle.partitions': 8,
                'spark.sql.adaptive.enabled': True,
                'spark.sql.adaptive.coalescePartitions.enabled': True,
                'spark.driver.host': 'localhost',
                'spark.driver.bindAddress': 'localhost'
            },
            'environment': {
                'JAVA_HOME': os.environ.get('JAVA_HOME', r'C:\Program Files\Java\jdk-17'),
                'HADOOP_HOME': os.environ.get('HADOOP_HOME', r'C:\hadoop'),
                'SPARK_HOME': os.environ.get('SPARK_HOME', r'C:\spark'),
                'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', sys.executable),
                'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', sys.executable)
            }
        }

    def get_app_config(self) -> Dict[str, Any]:
        """
        Get application settings.

        Returns:
            Dictionary with app configuration.
        """
        return {
            'default_sample_size': 50000,
            'max_sample_size': 1000000,
            'cache_enabled': True,
            'cache_ttl_seconds': 3600,
            'query_timeout_seconds': 300,
            'default_top_k': 10,
            'max_top_k': 100,
            'results_per_page': 50
        }

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key (supports dot notation).

        Args:
            key: Configuration key (e.g., 'mongodb.connection_type')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        # Get the appropriate config section
        if key.startswith('mongodb.'):
            config = self.get_mongodb_config()
            sub_key = key[8:]  # Remove 'mongodb.'
        elif key.startswith('spark.'):
            config = self.get_spark_config()
            sub_key = key[6:]  # Remove 'spark.'
        elif key.startswith('app.'):
            config = self.get_app_config()
            sub_key = key[4:]  # Remove 'app.'
        else:
            return default

        # Navigate the config dict
        keys = sub_key.split('.')
        value = config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value


# Global configuration instance
_config_instance: Optional[ConfigLoader] = None


def get_config(reload: bool = False) -> ConfigLoader:
    """
    Get the global configuration instance.

    Args:
        reload: If True, reload configuration from file

    Returns:
        ConfigLoader instance
    """
    global _config_instance

    if _config_instance is None or reload:
        _config_instance = ConfigLoader()

    return _config_instance
