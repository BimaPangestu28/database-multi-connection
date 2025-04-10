"""
PostgreSQL connector implementation
"""

import psycopg2
import psycopg2.extras
from app.db.base_connector import BaseConnector

class PostgresConnector(BaseConnector):
    """
    PostgreSQL connector for PostgreSQL databases
    """
    
    def _parse_connection_string(self, connection_string):
        """
        Parse PostgreSQL connection string into keyword parameters
        
        Args:
            connection_string (str): PostgreSQL connection string
            
        Returns:
            dict: Connection parameters
        """
        params = {}
        
        # Handle connection string formats
        if "=" in connection_string:
            # Key-value pairs format: "host=localhost port=5432 dbname=mydb user=postgres password=secret"
            parts = connection_string.split()
            for part in parts:
                if "=" in part:
                    key, value = part.split("=", 1)
                    params[key] = value
        elif "://" in connection_string:
            # URI format: "postgresql://user:password@localhost:5432/dbname"
            # Extract components from URI format
            import re
            
            # Parse URI
            match = re.match(
                r"postgresql://(?:([^:@]+)(?::([^@]*))?@)?([^:/]+)(?::(\d+))?/([^?]+)", 
                connection_string
            )
            
            if match:
                user, password, host, port, dbname = match.groups()
                if user:
                    params["user"] = user
                if password:
                    params["password"] = password
                if host:
                    params["host"] = host
                if port:
                    params["port"] = port
                if dbname:
                    params["dbname"] = dbname
            else:
                raise ValueError(f"Invalid PostgreSQL connection string format: {connection_string}")
        else:
            raise ValueError(f"Unsupported PostgreSQL connection string format: {connection_string}")
            
        return params
    
    def connect(self):
        """
        Connect to the PostgreSQL database
        
        Returns:
            bool: True if connection successful
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            # Parse connection string to psycopg2 parameters
            params = self._parse_connection_string(self.connection_string)
            
            # Connect to PostgreSQL
            self.connection = psycopg2.connect(**params)
            self.connection.autocommit = True  # For DDL operations
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL database: {str(e)}")

    def execute_query(self, query):
        """
        Execute a query on the PostgreSQL database
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            dict: Query results with columns, data, and row count
        """
        if not self.connection:
            self.connect()
        
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute(query)
            
            # Check if the query returns results
            if cursor.description:
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all results
                results = []
                for row in cursor.fetchall():
                    # Convert DictRow to a regular dict
                    results.append(dict(row))
                
                return {
                    "columns": columns,
                    "data": results,
                    "row_count": len(results),
                    "affected_rows": 0
                }
            else:
                # For queries that don't return data (INSERT, UPDATE, etc.)
                return {
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "affected_rows": cursor.rowcount
                }
        finally:
            cursor.close()

    def get_ddl(self, object_name, object_type):
        """
        Get DDL for PostgreSQL objects
        
        Args:
            object_name (str): Name of the database object or '*' for all
            object_type (str): Type of the database object (table, view, procedure, etc.)
            
        Returns:
            str: DDL statement for the object(s)
        """
        object_type = object_type.lower()

        print(f"Getting DDL for object: {object_name}, type: {object_type}")
        
        if object_type == "table":
            # First, ensure the pg_get_tabledef function exists
            self._create_tabledef_function()
            
            if object_name == '*':
                # Get all tables
                tables_query = """
                SELECT tablename AS table_name
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
                """

                print(f"Executing query to get all tables: {tables_query}")
                
                tables_result = self.execute_query(tables_query)
                
                if not tables_result["data"]:
                    return "-- No tables found in the public schema"
                
                # Build DDL for each table
                all_ddl = ""
                for table in tables_result["data"]:
                    table_name = table["table_name"]
                    table_ddl = self.get_ddl(table_name, "table")
                    all_ddl += f"\n\n{table_ddl}"
                
                return all_ddl.strip()
            else:
                try:
                    # First try: See if the table exists exactly as provided
                    query = f"""
                    SELECT to_regclass('{object_name}') IS NOT NULL as exists;
                    """
                    
                    exists_result = self.execute_query(query)
                    
                    if not exists_result["data"] or not exists_result["data"][0].get("exists", False):
                        # Try with lowercase
                        lowercase_name = object_name.lower()
                        query = f"""
                        SELECT to_regclass('{lowercase_name}') IS NOT NULL as exists;
                        """
                        exists_result = self.execute_query(query)
                        
                        if exists_result["data"] and exists_result["data"][0].get("exists", False):
                            # Use lowercase name
                            object_name = lowercase_name
                        else:
                            # Try with public schema prefix
                            query = f"""
                            SELECT to_regclass('public.{object_name}') IS NOT NULL as exists;
                            """
                            exists_result = self.execute_query(query)
                            
                            if exists_result["data"] and exists_result["data"][0].get("exists", False):
                                # Use with public schema
                                object_name = f"public.{object_name}"
                            else:
                                # Try with public schema and lowercase
                                query = f"""
                                SELECT to_regclass('public.{lowercase_name}') IS NOT NULL as exists;
                                """
                                exists_result = self.execute_query(query)
                                
                                if exists_result["data"] and exists_result["data"][0].get("exists", False):
                                    # Use with public schema and lowercase
                                    object_name = f"public.{lowercase_name}"
                    
                    # Query to get table DDL
                    query = f"""
                    SELECT pg_get_tabledef('{object_name}'::regclass::oid) as ddl;
                    """
                    
                    # Execute query
                    table_ddl = self.execute_query(query)
                    if table_ddl["data"]:
                        return table_ddl["data"][0]["ddl"]
                    
                    return f"-- Table {object_name} definition not found"
                
                except Exception as e:
                    # If we get an error, try to find the actual table name regardless of case
                    try:
                        # Get case-insensitive match
                        search_name = object_name.lower()
                        if "." in search_name:
                            schema, name = search_name.split(".", 1)
                            case_query = f"""
                            SELECT schemaname || '.' || tablename AS full_name
                            FROM pg_tables 
                            WHERE LOWER(schemaname) = '{schema.lower()}' 
                            AND LOWER(tablename) = '{name.lower()}'
                            """
                        else:
                            case_query = f"""
                            SELECT schemaname || '.' || tablename AS full_name
                            FROM pg_tables 
                            WHERE LOWER(tablename) = '{search_name}' 
                            """
                        
                        case_result = self.execute_query(case_query)
                        
                        if case_result["data"]:
                            # Use the correctly cased table name
                            actual_table_name = case_result["data"][0]["full_name"]
                            
                            # Now get the DDL with the correct case
                            query = f"""
                            SELECT pg_get_tabledef('{actual_table_name}'::regclass::oid) as ddl;
                            """
                            
                            table_ddl = self.execute_query(query)
                            if table_ddl["data"]:
                                return table_ddl["data"][0]["ddl"]
                        
                        return f"-- Table '{object_name}' not found. Error: {str(e)}"
                    except Exception as e2:
                        return f"-- Error getting table definition: {str(e2)}"
            
        elif object_type == "view":
            # Get view definition
            query = f"""
            SELECT 
                'CREATE OR REPLACE VIEW ' || schemaname || '.' || viewname || ' AS\n' ||
                definition as ddl
            FROM pg_views
            WHERE viewname = '{object_name}'
            """
            
            view_info = self.execute_query(query)
            if view_info["data"]:
                return view_info["data"][0]["ddl"]
            
            # Try with schema.name format
            if "." in object_name:
                schema, name = object_name.split(".", 1)
                query = f"""
                SELECT 
                    'CREATE OR REPLACE VIEW ' || schemaname || '.' || viewname || ' AS\n' ||
                    definition as ddl
                FROM pg_views
                WHERE viewname = '{name}' AND schemaname = '{schema}'
                """
                view_info = self.execute_query(query)
                if view_info["data"]:
                    return view_info["data"][0]["ddl"]
                    
            return f"-- View {object_name} definition not found"
            
        elif object_type in ["function", "procedure"]:
            # Get function definition
            query = f"""
            SELECT pg_get_functiondef(oid) as ddl
            FROM pg_proc
            WHERE proname = '{object_name}'
            """
            
            func_info = self.execute_query(query)
            if func_info["data"]:
                return func_info["data"][0]["ddl"]
            
            # Try with schema.name format
            if "." in object_name:
                schema, name = object_name.split(".", 1)
                query = f"""
                SELECT pg_get_functiondef(p.oid) as ddl
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE p.proname = '{name}' AND n.nspname = '{schema}'
                """
                func_info = self.execute_query(query)
                if func_info["data"]:
                    return func_info["data"][0]["ddl"]
                
            return f"-- Function/Procedure {object_name} definition not found"
            
        elif object_type == "trigger":
            # Get trigger definition
            query = f"""
            SELECT 
                pg_get_triggerdef(t.oid) as ddl
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            WHERE t.tgname = '{object_name}'
            """
            
            trigger_info = self.execute_query(query)
            if trigger_info["data"]:
                ddl = ""
                for trigger in trigger_info["data"]:
                    ddl += trigger["ddl"] + ";\n\n"
                return ddl
            return f"-- Trigger {object_name} definition not found"
            
        elif object_type == "sequence":
            # Get sequence definition
            query = f"""
            SELECT 
                'CREATE SEQUENCE ' || sequence_schema || '.' || sequence_name || 
                ' INCREMENT BY ' || increment || 
                ' MINVALUE ' || minimum_value || 
                ' MAXVALUE ' || maximum_value || 
                ' START WITH ' || start_value ||
                CASE WHEN cycle_option = 'YES' THEN ' CYCLE' ELSE ' NO CYCLE' END ||
                ';' as ddl
            FROM information_schema.sequences
            WHERE sequence_name = '{object_name}'
            """
            
            seq_info = self.execute_query(query)
            if seq_info["data"]:
                return seq_info["data"][0]["ddl"]
            return f"-- Sequence {object_name} definition not found"
            
        else:
            return f"-- DDL generation for {object_type} is not supported yet"
            
    def _create_tabledef_function(self):
        """
        Create the pg_get_tabledef function in the database if it doesn't exist
        """
        # Function to get table definition
        create_function_query = """
        CREATE OR REPLACE FUNCTION pg_get_tabledef(p_table_oid oid)
        RETURNS text AS
        $BODY$
        DECLARE
            v_table_name text;
            v_schema_name text;
            v_ret text;
            v_column_record record;
            v_constraint_record record;
            v_index_record record;
            v_comment_record record;
        BEGIN
            -- Get schema and table name
            SELECT n.nspname, c.relname INTO v_schema_name, v_table_name
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = p_table_oid;
            
            -- Start the table definition
            v_ret := 'CREATE TABLE ' || v_schema_name || '.' || v_table_name || ' (' || E'\n';
            
            -- Add columns
            FOR v_column_record IN 
                SELECT 
                    a.attname as column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
                    CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END as nullable,
                    CASE WHEN a.atthasdef THEN pg_get_expr(d.adbin, d.adrelid) ELSE '' END as default_value
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
                WHERE a.attrelid = p_table_oid
                AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY a.attnum
            LOOP
                v_ret := v_ret || '    ' || v_column_record.column_name || ' ' || v_column_record.data_type;
                IF v_column_record.default_value != '' THEN
                    v_ret := v_ret || ' DEFAULT ' || v_column_record.default_value;
                END IF;
                v_ret := v_ret || ' ' || v_column_record.nullable || ',' || E'\n';
            END LOOP;
            
            -- Remove the last comma
            v_ret := substring(v_ret, 1, length(v_ret) - 2) || E'\n);' || E'\n';
            
            -- Add primary key
            FOR v_constraint_record IN
                SELECT con.conname, pg_get_constraintdef(con.oid) as condef
                FROM pg_constraint con
                WHERE con.conrelid = p_table_oid AND con.contype = 'p'
            LOOP
                v_ret := v_ret || E'\n' || 'ALTER TABLE ' || v_schema_name || '.' || v_table_name || 
                         ' ADD CONSTRAINT ' || v_constraint_record.conname || 
                         ' ' || v_constraint_record.condef || ';';
            END LOOP;
            
            -- Add other constraints (foreign keys, unique, etc.)
            FOR v_constraint_record IN
                SELECT con.conname, pg_get_constraintdef(con.oid) as condef
                FROM pg_constraint con
                WHERE con.conrelid = p_table_oid AND con.contype != 'p'
            LOOP
                v_ret := v_ret || E'\n' || 'ALTER TABLE ' || v_schema_name || '.' || v_table_name || 
                         ' ADD CONSTRAINT ' || v_constraint_record.conname || 
                         ' ' || v_constraint_record.condef || ';';
            END LOOP;
            
            -- Add indexes
            FOR v_index_record IN
                SELECT indexrelid::regclass as index_name, pg_get_indexdef(indexrelid) as indexdef
                FROM pg_index
                WHERE indrelid = p_table_oid AND indisprimary = false
            LOOP
                v_ret := v_ret || E'\n' || v_index_record.indexdef || ';';
            END LOOP;
            
            -- Add comments
            FOR v_comment_record IN
                SELECT col.attname as column_name, 
                       pg_description.description
                FROM pg_description
                JOIN pg_attribute col ON pg_description.objoid = p_table_oid
                                    AND col.attrelid = p_table_oid
                                    AND pg_description.objsubid = col.attnum
                WHERE NOT col.attisdropped
            LOOP
                IF v_comment_record.description IS NOT NULL THEN
                    v_ret := v_ret || E'\n' || 'COMMENT ON COLUMN ' || v_schema_name || '.' || v_table_name || '.' || 
                             v_comment_record.column_name || ' IS ''' || 
                             replace(v_comment_record.description, '''', '''''') || ''';';
                END IF;
            END LOOP;
            
            RETURN v_ret;
        END;
        $BODY$
        LANGUAGE plpgsql;
        """
        
        try:
            # Execute the function creation query
            self.execute_query(create_function_query)
        except Exception as e:
            # Function may already exist or there might be permissions issues
            # We'll continue anyway and let the calling function handle any errors
            pass