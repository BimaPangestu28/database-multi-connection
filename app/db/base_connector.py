"""
Base database connector class
"""

class BaseConnector:
    """
    Base connector class for database operations
    All database connectors should inherit from this class
    """
    
    def __init__(self, connection_string):
        """
        Initialize the connector with a connection string
        
        Args:
            connection_string (str): Database connection string
        """
        self.connection_string = connection_string
        self.connection = None

    def connect(self):
        """
        Connect to the database
        
        Returns:
            bool: True if connection successful
            
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement connect method")

    def disconnect(self):
        """
        Disconnect from the database
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query):
        """
        Execute a query and return results
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            dict: Query results
            
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement execute_query method")

    def get_ddl(self, object_name, object_type):
        """
        Get DDL for a specific database object
        
        Args:
            object_name (str): Name of the database object
            object_type (str): Type of the database object (table, view, procedure, etc.)
            
        Returns:
            str: DDL statement for the object
            
        Raises:
            NotImplementedError: Subclasses must implement this method
        """
        raise NotImplementedError("Subclasses must implement get_ddl method")