"""
API routes for connection string hash operations
"""

import os
from flask import Blueprint, request, jsonify

# Try to use the simple hash verifier instead of the cryptography-based one
try:
    from app.utils.hash_verifier_simple import encrypt_connection_string, verify_hash
    print("Using simplified hash encryption")
except ImportError:
    from app.utils.hash_verifier import encrypt_connection_string, verify_hash
    print("Using cryptography-based hash encryption")

# Create Blueprint
bp = Blueprint('hash_api', __name__, url_prefix='/api/hash')

@bp.route('/encrypt', methods=['POST'])
def encrypt_connection():
    """
    API endpoint to encrypt a connection string
    
    Request body:
    {
        "connection_string": "host=localhost port=5432 dbname=testdb user=postgres password=postgres"
    }
    
    Returns:
        JSON: Encrypted hash
    """
    try:
        # Get request data
        request_data = request.get_json()
        
        # Validate request data
        if not request_data:
            return jsonify({"error": "No request data provided"}), 400
        
        # Extract connection string
        connection_string = request_data.get('connection_string')
        
        # Validate required parameters
        if not connection_string:
            return jsonify({"error": "connection_string is required"}), 400
        
        # Encrypt connection string
        try:
            encrypted_hash = encrypt_connection_string(connection_string)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({"error": f"Encryption failed: {str(e)}"}), 500
        
        if not encrypted_hash:
            return jsonify({"error": "Failed to encrypt connection string"}), 500
        
        # Verify the generated hash can be decrypted
        verification_result = None
        try:
            verification_result = verify_hash(encrypted_hash)
        except Exception as e:
            print(f"Verification check failed: {str(e)}")
        
        return jsonify({
            "status": "success",
            "hash": encrypted_hash,
            "test_verification": verification_result is not None,
            "hash_length": len(encrypted_hash)
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@bp.route('/verify', methods=['POST'])
def verify_connection_hash():
    """
    API endpoint to verify a connection string hash
    
    Request body:
    {
        "hash": "encrypted_hash_string"
    }
    
    Returns:
        JSON: Decrypted connection string
    """
    try:
        # Get request data
        request_data = request.get_json()
        
        # Validate request data
        if not request_data:
            return jsonify({"error": "No request data provided"}), 400
        
        # Extract hash
        connection_hash = request_data.get('hash')
        
        # Validate required parameters
        if not connection_hash:
            return jsonify({"error": "hash is required"}), 400
        
        print(f"Received hash for verification: {connection_hash[:30]}...")
        
        # Verify and decrypt hash
        try:
            connection_string = verify_hash(connection_hash)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({"error": f"Verification processing error: {str(e)}"}), 500
        
        if not connection_string:
            return jsonify({"error": "Invalid hash or decryption failed"}), 400
        
        return jsonify({
            "status": "success",
            "connection_string": connection_string,
            "hash_length": len(connection_hash)
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@bp.route('/test', methods=['GET'])
def test_hash_functionality():
    """
    API endpoint to test the hash encryption/decryption functionality
    
    Returns:
        JSON: Test results
    """
    try:
        # Test connection string
        test_string = "host=localhost port=5432 dbname=testdb user=postgres password=test123"
        
        # Encrypt the test string
        encrypted = encrypt_connection_string(test_string)
        if not encrypted:
            return jsonify({
                "status": "error",
                "message": "Encryption failed"
            }), 500
        
        # Verify and decrypt the hash
        decrypted = verify_hash(encrypted)
        
        # Show hash system in use
        hash_system = "Simple Base64" if 'app.utils.hash_verifier_simple' in globals() else "AES Cryptography"
        
        return jsonify({
            "status": "success" if decrypted == test_string else "error",
            "hash_system": hash_system,
            "original": test_string,
            "encrypted_hash": encrypted,
            "decrypted": decrypted,
            "match": decrypted == test_string,
            "secret_key_length": len(os.environ.get('HASH_SECRET', ''))
        })
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500