"""
MongoDB Connector with abstraction for different connection methods.
Supports local, Atlas, and custom MongoDB connections without hardcoding.
"""

from typing import Optional, Dict, Any, List
from abc import ABC, abstractmethod
import logging
import sys
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import streamlit as st

# Add parent directory to path for absolute imports
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from config import get_config

logger = logging.getLogger(__name__)


class MongoDBConnectionStrategy(ABC):
    """Abstract base class for MongoDB connection strategies."""
    
    @abstractmethod
    def get_connection_uri(self, settings: Dict[str, Any]) -> str:
        """Build MongoDB connection URI from settings."""
        pass
    
    @abstractmethod
    def get_connection_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """Get connection options for MongoClient."""
        pass


class LocalMongoDBStrategy(MongoDBConnectionStrategy):
    """Strategy for local MongoDB connections."""
    
    def get_connection_uri(self, settings: Dict[str, Any]) -> str:
        host = settings.get('host', 'localhost')
        port = settings.get('port', 27017)
        username = settings.get('username')
        password = settings.get('password')
        
        if username and password:
            return f"mongodb://{username}:{password}@{host}:{port}/"
        else:
            return f"mongodb://{host}:{port}/"
    
    def get_connection_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'serverSelectionTimeoutMS': options.get('serverSelectionTimeoutMS', 5000),
            'connectTimeoutMS': options.get('connectTimeoutMS', 10000)
        }


class AtlasMongoDBStrategy(MongoDBConnectionStrategy):
    """Strategy for MongoDB Atlas (cloud) connections."""
    
    def get_connection_uri(self, settings: Dict[str, Any]) -> str:
        # Check if using environment variable
        if 'connection_string_env' in settings:
            import os
            env_var = settings['connection_string_env']
            uri = os.environ.get(env_var)
            if not uri:
                raise ValueError(f"Environment variable {env_var} not set for MongoDB Atlas connection")
            return uri
        
        # Use directly provided connection string
        return settings.get('connection_string', '')
    
    def get_connection_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'serverSelectionTimeoutMS': options.get('serverSelectionTimeoutMS', 10000),
            'connectTimeoutMS': options.get('connectTimeoutMS', 20000),
            'retryWrites': True
        }


class CustomMongoDBStrategy(MongoDBConnectionStrategy):
    """Strategy for custom MongoDB connections (e.g., specific IPs, custom setups)."""
    
    def get_connection_uri(self, settings: Dict[str, Any]) -> str:
        return settings.get('uri', 'mongodb://localhost:27017/')
    
    def get_connection_options(self, options: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'serverSelectionTimeoutMS': options.get('serverSelectionTimeoutMS', 5000),
            'connectTimeoutMS': options.get('connectTimeoutMS', 10000)
        }


class MongoDBConnector:
    """
    MongoDB connector with support for multiple connection strategies.
    Provides abstraction layer between GUI and database.
    """
    
    STRATEGIES = {
        'local': LocalMongoDBStrategy(),
        'atlas': AtlasMongoDBStrategy(),
        'custom': CustomMongoDBStrategy()
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize MongoDB connector.
        
        Args:
            config: Optional configuration dictionary. If None, loads from config file.
        """
        if config is None:
            config_loader = get_config()
            config = config_loader.get_mongodb_config()
        
        self.config = config
        self.connection_type = config['connection_type']
        self.database_name = config['database']
        self.collections = config.get('collections', [])
        
        # Get appropriate strategy
        self.strategy = self.STRATEGIES.get(self.connection_type)
        if not self.strategy:
            raise ValueError(f"Unknown connection type: {self.connection_type}")
        
        # Build connection URI and options
        self.uri = self.strategy.get_connection_uri(config['settings'])
        self.options = self.strategy.get_connection_options(config.get('options', {}))
        
        # Client will be initialized on first use (lazy loading)
        self._client: Optional[MongoClient] = None
        self._db = None
        
        logger.info(f"MongoDB connector initialized with {self.connection_type} strategy")
    
    @property
    def client(self) -> MongoClient:
        """Get MongoDB client, initializing if necessary."""
        if self._client is None:
            self._connect()
        return self._client
    
    @property
    def db(self):
        """Get MongoDB database."""
        if self._db is None:
            self._db = self.client[self.database_name]
        return self._db
    
    def _connect(self):
        """Establish MongoDB connection."""
        try:
            logger.info(f"Connecting to MongoDB using {self.connection_type} strategy...")
            self._client = MongoClient(self.uri, **self.options)
            # Test connection
            self._client.admin.command('ping')
            logger.info("MongoDB connection established successfully")
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self._client = None
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test MongoDB connection and return status.
        
        Returns:
            Dictionary with connection status information.
        """
        try:
            if self._client is None:
                self._connect()
            
            # Ping server
            self.client.admin.command('ping')
            
            # Get server info
            server_info = self.client.server_info()
            
            # Get database stats
            db_stats = self.db.command('dbStats')
            
            return {
                'status': 'connected',
                'connection_type': self.connection_type,
                'server_version': server_info.get('version', 'unknown'),
                'database': self.database_name,
                'collections_count': len(self.db.list_collection_names()),
                'data_size': db_stats.get('dataSize', 0),
                'error': None
            }
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return {
                'status': 'disconnected',
                'connection_type': self.connection_type,
                'database': self.database_name,
                'error': str(e)
            }
    
    def get_collection(self, collection_name: str):
        """
        Get a MongoDB collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            MongoDB collection object
        """
        return self.db[collection_name]
    
    def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """
        Get statistics for a collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            Dictionary with collection statistics
        """
        try:
            collection = self.get_collection(collection_name)
            count = collection.count_documents({})
            
            # Get sample document to show fields
            sample = collection.find_one()
            fields = list(sample.keys()) if sample else []
            
            return {
                'name': collection_name,
                'count': count,
                'fields': fields,
                'exists': True
            }
        except Exception as e:
            logger.error(f"Error getting stats for collection {collection_name}: {e}")
            return {
                'name': collection_name,
                'count': 0,
                'fields': [],
                'exists': False,
                'error': str(e)
            }
    
    def get_all_collections(self) -> List[str]:
        """
        Get list of all collection names in the database.
        
        Returns:
            List of collection names
        """
        try:
            return self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []
    
    def close(self):
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            logger.info("MongoDB connection closed")


# Global connector instance
_mongo_connector: Optional[MongoDBConnector] = None


@st.cache_resource
def get_mongo_connector(reload: bool = False) -> MongoDBConnector:
    """
    Get the global MongoDB connector instance (cached in Streamlit).
    
    Args:
        reload: If True, recreate the connector
        
    Returns:
        MongoDBConnector instance
    """
    global _mongo_connector
    
    if _mongo_connector is None or reload:
        _mongo_connector = MongoDBConnector()
    
    return _mongo_connector
