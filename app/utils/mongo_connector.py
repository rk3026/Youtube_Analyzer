"""
Simple MongoDB Connector for YouTube Analyzer.
Connects to MongoDB with minimal configuration.
"""

import os
from typing import Optional, Dict, Any, List
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import streamlit as st

logger = logging.getLogger(__name__)


class MongoDBConnector:
    """Simple MongoDB connector for the application."""
    
    def __init__(self, uri: Optional[str] = None, database: str = 'youtube_analytics'):
        """
        Initialize MongoDB connector.
        
        Args:
            uri: MongoDB connection URI (defaults to localhost or MONGODB_URI env var)
            database: Database name
        """
        # Get URI from parameter, environment variable, or default to localhost
        if uri is None:
            uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
        
        self.uri = uri
        self.database_name = database
        self.connection_type = 'atlas' if 'mongodb.net' in uri or 'mongodb+srv' in uri else 'local'
        
        # Client will be initialized on first use (lazy loading)
        self._client: Optional[MongoClient] = None
        self._db = None
        
        logger.info(f"MongoDB connector initialized ({self.connection_type})")
    
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
            logger.info(f"Connecting to MongoDB ({self.connection_type})...")
            self._client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
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
