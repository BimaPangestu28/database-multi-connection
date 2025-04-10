"""
Database service functions with caching
"""

from app.db import get_db_connector
from app.cache.redis_cache import redis_cache

@redis_cache
def execute_query_with_cache(connection_string, db_type, query, **kwargs):
    """
    Execute query with Redis cache support
    
    Args:
        connection_string (str): Database connection string
        db_type (str): Database type (fabric, postgres)
        query (str): SQL query to execute
        **kwargs: Additional parameters, including cache settings
        
    Returns:
        dict: Query results with status and metadata
    """
    try:
        # Get appropriate database connector
        connector = get_db_connector(connection_string, db_type)
        
        # Connect to database
        connector.connect()
        
        # Execute query
        result = connector.execute_query(query)
        
        # Disconnect
        connector.disconnect()
        
        return {
            "status": "success",
            "result": result,
            "query": query,
            "db_type": db_type,
            "cached": False  # This will be overwritten if returned from cache
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "query": query,
            "db_type": db_type,
            "cached": False
        }

@redis_cache
def get_ddl_with_cache(connection_string, db_type, object_name, object_type, **kwargs):
    """
    Get DDL with Redis cache support
    
    Args:
        connection_string (str): Database connection string
        db_type (str): Database type (fabric, postgres)
        object_name (str): Name of the database object
        object_type (str): Type of the database object (table, view, procedure, etc.)
        **kwargs: Additional parameters, including cache settings
        
    Returns:
        dict: DDL statement with status and metadata
    """
    try:
        print(f"Getting DDL for {object_name} of type {object_type} from {db_type} database")

        # Get appropriate database connector
        connector = get_db_connector(connection_string, db_type)
        
        # Connect to database
        connector.connect()
        
        # Get DDL
        ddl = connector.get_ddl(object_name, object_type)
        
        # Disconnect
        connector.disconnect()
        
        return {
            "status": "success",
            "ddl": ddl,
            "object_name": object_name,
            "object_type": object_type,
            "db_type": db_type,
            "cached": False  # This will be overwritten if returned from cache
        }
    except Exception as e:
        print(f"Error getting DDL: {str(e)}")

        return {
            "status": "error",
            "error": str(e),
            "object_name": object_name,
            "object_type": object_type,
            "db_type": db_type,
            "cached": False
        }