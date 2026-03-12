#!/usr/bin/env python3
"""
Direct fetch and store 
"""

from proofsafe_trees_postgis_schema import ProofSafeGeoDB
from datetime import datetime

# Generate timestamp for table name
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Initialize the API client
# It will automatically load credentials from .creds.json
api = ProofSafeGeoDB(schema='sde')

# Fetch tree data and store in database 
api.fetch_and_store(
    table_name=f"trees_{timestamp}",    
    if_exists='replace',               # 'replace'-clean replacement!,  'append'-duplicates! or 'fail'
    debug=True
)
