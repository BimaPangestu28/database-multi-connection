"""
ODBC (Microsoft Fabric) connector implementation
"""

import pyodbc
from app.db.base_connector import BaseConnector

class ODBCConnector(BaseConnector):
    """
    ODBC connector for Microsoft Fabric and other ODBC-compatible databases
    """
    
    def connect(self):
        """
        Connect to the ODBC database
        
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            self.connection = pyodbc.connect(self.connection_string)
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ODBC database: {str(e)}")

    def execute_query(self, query):
        """
        Execute a query on the ODBC database
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            dict: Query results with columns, data, and row count
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            
            # Get column names if results exist
            columns = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]
            
            # Fetch all results
            results = []
            if cursor.description:  # Only try to fetch if there are results
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
            
            return {
                "columns": columns,
                "data": results,
                "row_count": len(results) if cursor.description else cursor.rowcount,
                "affected_rows": cursor.rowcount if not cursor.description else 0
            }
        finally:
            cursor.close()

    def get_ddl(self, object_name, object_type):
        """
        Get DDL for Microsoft Fabric/SQL Server objects
        
        Args:
            object_name (str): Name of the database object or '*' for all
            object_type (str): Type of the database object (table, view, procedure, etc.)
            
        Returns:
            str: DDL statement for the object(s)
        """
        object_type = object_type.lower()
        
        if object_type == "table":
            if object_name == '*':
                # Get all tables
                tables_query = """
                SELECT name
                FROM sys.tables
                ORDER BY name
                """
                
                tables_result = self.execute_query(tables_query)
                
                if not tables_result["data"]:
                    return "-- No tables found in database"
                
                # Build DDL for each table
                all_ddl = ""
                for table in tables_result["data"]:
                    table_name = table["name"]
                    table_ddl = self.get_ddl(table_name, "table")
                    all_ddl += f"\n\n{table_ddl}"
                
                return all_ddl.strip()
            else:
                # Query to get table DDL
                query = f"""
                SELECT 
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_nullable,
                    c.is_identity,
                    c.column_id
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                JOIN sys.tables tbl ON c.object_id = tbl.object_id
                WHERE tbl.name = '{object_name}'
                ORDER BY c.column_id
                """
                
                columns_info = self.execute_query(query)
                
                # Build CREATE TABLE statement
                ddl = f"CREATE TABLE {object_name} (\n"
                for i, col in enumerate(columns_info["data"]):
                    ddl += f"    {col['column_name']} {col['data_type']}"
                    
                    # Add length, precision, scale if applicable
                    if col['data_type'] in ('varchar', 'nvarchar', 'char', 'nchar'):
                        ddl += f"({col['max_length'] if col['max_length'] != -1 else 'MAX'})"
                    elif col['data_type'] in ('decimal', 'numeric'):
                        ddl += f"({col['precision']}, {col['scale']})"
                    
                    # Add NULL constraint
                    ddl += " NOT NULL" if not col['is_nullable'] else " NULL"
                    
                    # Add identity property
                    if col['is_identity']:
                        ddl += " IDENTITY(1,1)"
                    
                    # Add comma if not the last column
                    if i < len(columns_info["data"]) - 1:
                        ddl += ",\n"
                    
                ddl += "\n);"
                
                # Get primary key
                pk_query = f"""
                SELECT 
                    i.name AS index_name,
                    c.name AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE i.is_primary_key = 1 AND t.name = '{object_name}'
                ORDER BY ic.key_ordinal
                """
                
                pk_info = self.execute_query(pk_query)
                if pk_info["data"]:
                    pk_columns = ", ".join([row["column_name"] for row in pk_info["data"]])
                    pk_name = pk_info["data"][0]["index_name"]
                    ddl += f"\n\nALTER TABLE {object_name} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});"
                
                # Get foreign keys
                fk_query = f"""
                SELECT 
                    fk.name AS fk_name,
                    OBJECT_NAME(fk.parent_object_id) AS parent_table,
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS parent_column,
                    OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                WHERE OBJECT_NAME(fk.parent_object_id) = '{object_name}'
                ORDER BY fk.name, fkc.constraint_column_id
                """
                
                fk_info = self.execute_query(fk_query)
                if fk_info["data"]:
                    # Group by FK name
                    fk_dict = {}
                    for row in fk_info["data"]:
                        fk_name = row["fk_name"]
                        if fk_name not in fk_dict:
                            fk_dict[fk_name] = {
                                "parent_table": row["parent_table"],
                                "referenced_table": row["referenced_table"],
                                "parent_columns": [],
                                "referenced_columns": []
                            }
                        fk_dict[fk_name]["parent_columns"].append(row["parent_column"])
                        fk_dict[fk_name]["referenced_columns"].append(row["referenced_column"])
                    
                    # Create ALTER TABLE statements for each FK
                    for fk_name, fk_data in fk_dict.items():
                        parent_cols = ", ".join(fk_data["parent_columns"])
                        ref_cols = ", ".join(fk_data["referenced_columns"])
                        ddl += f"\n\nALTER TABLE {fk_data['parent_table']} ADD CONSTRAINT {fk_name} "
                        ddl += f"FOREIGN KEY ({parent_cols}) REFERENCES {fk_data['referenced_table']} ({ref_cols});"
                
                return ddl
            
        elif object_type == "view":
            # Get view definition
            query = f"""
            SELECT definition 
            FROM sys.sql_modules m
            JOIN sys.views v ON m.object_id = v.object_id
            WHERE v.name = '{object_name}'
            """
            
            view_info = self.execute_query(query)
            if view_info["data"]:
                return f"CREATE VIEW {object_name} AS\n{view_info['data'][0]['definition']}"
            return f"-- View {object_name} definition not found"
            
        elif object_type in ["procedure", "stored_procedure"]:
            # Get stored procedure definition
            query = f"""
            SELECT definition 
            FROM sys.sql_modules m
            JOIN sys.procedures p ON m.object_id = p.object_id
            WHERE p.name = '{object_name}'
            """
            
            proc_info = self.execute_query(query)
            if proc_info["data"]:
                return proc_info["data"][0]["definition"]
            return f"-- Stored procedure {object_name} definition not found"
            
        elif object_type == "function":
            # Get function definition
            query = f"""
            SELECT definition 
            FROM sys.sql_modules m
            JOIN sys.objects o ON m.object_id = o.object_id
            WHERE o.type_desc LIKE '%FUNCTION%' AND o.name = '{object_name}'
            """
            
            func_info = self.execute_query(query)
            if func_info["data"]:
                return func_info["data"][0]["definition"]
            return f"-- Function {object_name} definition not found"
            
        else:
            return f"-- DDL generation for {object_type} is not supported yet"