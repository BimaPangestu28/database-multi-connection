"""
Database API for retrieving DDL and executing queries
Supports multiple databases (ODBC Microsoft Fabric and PostgreSQL)
With Redis caching functionality
"""

from flask import Flask

app = Flask(__name__)

# Import routes after app is created to avoid circular imports
from app.routes import db_api, hash_api, direct_connect

# Register blueprints
app.register_blueprint(db_api.bp)
app.register_blueprint(hash_api.bp)
app.register_blueprint(direct_connect.bp)