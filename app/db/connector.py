"""
Database connector factory
"""

from app.db.odbc_connector import ODBCConnector
from app.db.postgres_connector import PostgresConnector

def get_db_connector(connection_string, db_type):
    """
    Factory function to get the appropriate database connector
    
    Args:
        connection_string (str): Database connection string
        db_type (str): Database type (fabric, odbc, postgres, postgresql)
        
    Returns:
        BaseConnector: The appropriate database connector instance
        
    Raises:
        ValueError: If the database type is not supported
    """
    if db_type.lower() in ["fabric", "odbc", "microsoft_fabric"]:
        return ODBCConnector(connection_string)
    elif db_type.lower() in ["postgres", "postgresql"]:
        return PostgresConnector(connection_string)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")