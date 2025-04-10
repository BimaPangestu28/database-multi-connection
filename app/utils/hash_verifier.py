"""
Simplified hash verification utility for connection strings
Uses simple encoding instead of cryptography libraries to avoid system dependencies
"""

import os
import base64
import hashlib

# Configuration
HASH_SECRET = os.environ.get('HASH_SECRET', 'default_secret_key_change_me')

def simple_encrypt(connection_string):
    """
    Simple encryption for connection strings that doesn't rely on cryptography libraries
    
    Args:
        connection_string (str): Database connection string to encrypt
        
    Returns:
        str: Base64-encoded "encrypted" string
    """
    try:
        # Create a signature with the connection string and secret
        signature = hashlib.sha256((connection_string + HASH_SECRET).encode()).digest()
        
        # Encode the connection string
        encoded_conn = base64.b64encode(connection_string.encode()).decode()
        
        # Combine signature and encoded connection string
        result = base64.b64encode(signature + encoded_conn.encode()).decode()
        
        return result
    except Exception as e:
        print(f"Simple encryption error: {str(e)}")
        return None

def simple_decrypt(hash_value):
    """
    Simple decryption for connection strings
    
    Args:
        hash_value (str): Base64-encoded hash string
        
    Returns:
        str: Decrypted connection string
    """
    try:
        # Decode the outer base64
        decoded = base64.b64decode(hash_value)
        
        # Extract signature (first 32 bytes) and encoded connection string
        signature = decoded[:32]
        encoded_conn = decoded[32:].decode()
        
        # Decode the connection string
        connection_string = base64.b64decode(encoded_conn).decode()
        
        # Verify the signature
        expected_signature = hashlib.sha256((connection_string + HASH_SECRET).encode()).digest()
        
        # Simple signature comparison (not timing-safe but simpler)
        if signature != expected_signature:
            print("Invalid signature")
            return None
            
        return connection_string
    except Exception as e:
        print(f"Simple decryption error: {str(e)}")
        return None

# For compatibility with the existing code
encrypt_connection_string = simple_encrypt
verify_hash = simple_decrypt