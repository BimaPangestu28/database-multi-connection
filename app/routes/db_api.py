"""
API routes for database operations
"""

import time
from flask import Blueprint, request, jsonify
from app.services.db_service import execute_query_with_cache, get_ddl_with_cache

# Try to use the simple hash verifier instead of the cryptography-based one
try:
    from app.utils.hash_verifier_simple import verify_hash
    print("Using simplified hash verification")
except ImportError:
    from app.utils.hash_verifier import verify_hash
    print("Using cryptography-based hash verification")

# Create Blueprint
bp = Blueprint('db_api', __name__, url_prefix='/api')

@bp.route('/query', methods=['POST'])
def execute_query_api():
    """
    API endpoint to execute queries
    
    Request body:
    {
        "connection_hash": "hashed_connection_string",
        "db_type": "postgres|fabric",
        "query": "SELECT * FROM users",
        "cache_enabled": true|false,
        "cache_ttl": 3600
    }
    
    Returns:
        JSON: Query results
    """
    try:
        # Get request data
        request_data = request.get_json()
        
        # Validate request data
        if not request_data:
            return jsonify({"error": "No request data provided"}), 400
        
        # Extract parameters
        connection_hash = request_data.get('connection_hash')
        db_type = request_data.get('db_type')
        query = request_data.get('query')
        
        # Cache settings
        cache_enabled = request_data.get('cache_enabled', False)
        cache_ttl = request_data.get('cache_ttl', 3600)  # Default: 1 hour
        
        # Validate required parameters
        if not connection_hash:
            return jsonify({"error": "connection_hash is required"}), 400
        if not db_type:
            return jsonify({"error": "db_type is required"}), 400
        if not query:
            return jsonify({"error": "query is required"}), 400
        
        # Verify and get connection string
        connection_string = verify_hash(connection_hash)
        if not connection_string:
            return jsonify({"error": "Invalid connection hash"}), 400
        
        # Execute query with caching
        start_time = time.time()
        result = execute_query_with_cache(connection_string, db_type, query, 
                                         cache_enabled=cache_enabled, 
                                         cache_ttl=cache_ttl)
        execution_time = time.time() - start_time
        
        # Add execution time to result
        result["execution_time"] = execution_time
        
        return jsonify(result)
    
    except ConnectionError as e:
        return jsonify({"error": f"Connection error: {str(e)}"}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@bp.route('/ddl', methods=['POST'])
def get_ddl_api():
    """
    API endpoint to get DDL for database objects
    
    Request body:
    {
        "connection_hash": "hashed_connection_string",
        "db_type": "postgres|fabric",
        "object_name": "table_name" or "*" for all tables,
        "object_type": "table|view|procedure|function|trigger|sequence",
        "cache_enabled": true|false,
        "cache_ttl": 3600
    }
    
    Returns:
        JSON: DDL definition
    """
    try:
        # Get request data
        request_data = request.get_json()
        
        # Validate request data
        if not request_data:
            return jsonify({"error": "No request data provided"}), 400
        
        print(f"Received request data: {request_data}")
        
        # Extract parameters
        connection_hash = request_data.get('connection_hash')
        db_type = request_data.get('db_type')
        object_name = request_data.get('object_name', '*')  # Default to all tables
        object_type = request_data.get('object_type')
        
        # Cache settings
        cache_enabled = request_data.get('cache_enabled', False)
        cache_ttl = request_data.get('cache_ttl', 3600)  # Default: 1 hour
        
        # Validate required parameters
        if not connection_hash:
            return jsonify({"error": "connection_hash is required"}), 400
        if not db_type:
            return jsonify({"error": "db_type is required"}), 400
        if not object_type:
            return jsonify({"error": "object_type is required"}), 400
        
        # Verify and get connection string
        try:
            connection_string = verify_hash(connection_hash)
            if not connection_string:
                return jsonify({"error": "Invalid connection hash"}), 400
        except Exception as e:
            return jsonify({"error": f"Hash verification error: {str(e)}"}), 400
        
        print(f"Verified connection string: {connection_string[:30]}...")
        
        # Get DDL with caching
        start_time = time.time()
        result = get_ddl_with_cache(connection_string, db_type, object_name, object_type,
                                   cache_enabled=cache_enabled,
                                   cache_ttl=cache_ttl)
        execution_time = time.time() - start_time
        
        # Add execution time to result
        result["execution_time"] = execution_time
        
        return jsonify(result)
    
    except ConnectionError as e:
        return jsonify({"error": f"Connection error: {str(e)}"}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500