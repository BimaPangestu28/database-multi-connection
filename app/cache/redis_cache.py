"""
Redis cache implementation
"""

import os
import json
import hashlib
import redis
from functools import wraps

# Redis Configuration
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

def get_redis_client():
    """
    Get a Redis client instance
    
    Returns:
        redis.Redis: Redis client instance
    """
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True
    )

def redis_cache(func):
    """
    Decorator to cache function results in Redis
    
    Args:
        func (callable): Function to cache
        
    Returns:
        callable: Wrapped function with caching
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get cache settings from kwargs
        cache_enabled = kwargs.pop('cache_enabled', False)
        cache_ttl = kwargs.pop('cache_ttl', 3600)  # Default: 1 hour
        
        if not cache_enabled:
            # Cache disabled, directly execute function
            return func(*args, **kwargs)
        
        # Generate cache key
        # Create a stable string representation of args and kwargs
        args_str = str(args)
        kwargs_str = str(sorted(kwargs.items()))
        
        # Include function name in the cache key for uniqueness
        key_data = f"{func.__name__}:{args_str}:{kwargs_str}"
        
        # Generate MD5 hash for the cache key
        cache_key = f"db_api:{func.__name__}:{hashlib.md5(key_data.encode()).hexdigest()}"
        
        # Try to get from cache
        try:
            redis_client = get_redis_client()
            cached_result = redis_client.get(cache_key)
            
            if cached_result:
                try:
                    # Return cached result
                    result = json.loads(cached_result)
                    # Mark as coming from cache
                    result['cached'] = True
                    return result
                except Exception:
                    # If JSON parsing fails, ignore cache
                    pass
        except Exception as e:
            # Log Redis connection error but continue with original function
            print(f"Redis connection error: {str(e)}")
            # Continue with original function execution
        
        # Execute function
        result = func(*args, **kwargs)
        
        # Cache result
        try:
            if cache_enabled and result:
                # Copy result to avoid modifying the original
                cache_result = result.copy()
                # Set cached flag to False in the cached data
                cache_result['cached'] = False 
                
                # Cache the result
                redis_client.setex(
                    cache_key, 
                    cache_ttl, 
                    json.dumps(cache_result, default=str)
                )
        except Exception as e:
            # Log cache error but continue
            print(f"Redis cache error: {str(e)}")
        
        return result
    
    return wrapper