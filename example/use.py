#!/usr/bin/env python3
"""
Example usage of ProofSafe Tree Data Extractor
"""

from proofsafe_trees_postgis import ProofSafeGeoDB
from datetime import datetime
import os

# Ensure backup directory exists
BACKUP_DIR = "D:/ProofsafeCSV"
os.makedirs(BACKUP_DIR, exist_ok=True)

# Generate timestamp for filenames
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Initialize the API client
# It will automatically load credentials from .creds.json
api = ProofSafeGeoDB()

# ============================================
# OPTION 1: Just save to CSV
# ============================================
csv_file = f"{BACKUP_DIR}/trees_{timestamp}.csv"

api.get_trees(
    output_path=csv_file,
    # Optional filters:
    # project_ids=[123, 456],            # Specific projects
    # health="good",                     # Filter by health
    # start_date="2024-01-01",           # Date range
    # end_date="2024-12-31",
    debug=True
)

# ============================================
# OPTION 2: Store in database only
# ============================================
records_fetched, records_stored = api.fetch_and_store(
    # Optional filters:
    # project_ids=[123, 456],
    # health="good",
    # start_date="2024-01-01",
    # end_date="2024-12-31",
    
    # Table options:
    table_name=f"trees_{timestamp}",     # Table name in database
    if_exists='replace',                 # 'append'-duplicates!, 'replace'-clean replacement!, or 'fail'
    
    # Import tracking:
    import_notes="Initial data load",    # Notes for import history
    
    debug=True
)

# ============================================
# OPTION 3: Both CSV and database
# ============================================
csv_file_both = f"{BACKUP_DIR}/trees_both_{timestamp}.csv"

records_fetched, records_stored = api.fetch_and_store(
    csv_backup=csv_file_both,             # Save CSV backup
    import_notes="Full backup with CSV",
    debug=True
)

# ============================================
# OPTION 4: With filters and sample size
# ============================================
csv_file_filtered = f"{BACKUP_DIR}/trees_filtered_{timestamp}.csv"

records_fetched, records_stored = api.fetch_and_store(
    # Filters
    project_ids=[123],                     # Only project ID 123
    health=["good", "fair"],               # Multiple health statuses
    status=0,                              # No action required
    start_date="2024-01-01",
    end_date="2024-12-31",
    
    # Sample (for testing)
    sample_size=100,                       # Get only 100 records
    
    # Output
    csv_backup=csv_file_filtered,
    import_notes="Filtered test data",
    debug=True
)

# ============================================
# CHECK RESULTS
# ============================================
# Check database statistics
stats = api.get_tree_count()
if stats:
    print(f"\nDatabase statistics:")
    print(f"  Total trees: {stats.get('total_trees', 0)}")
    print(f"  With coordinates: {stats.get('with_coordinates', 0)}")
    print(f"  Unique projects: {stats.get('unique_projects', 0)}")
    print(f"  Number of imports: {stats.get('import_count', 0)}")

# View import history
print(f"\nRecent imports:")
history = api.get_import_history(limit=5)
if not history.empty:
    print(history[['import_id', 'records_stored', 'status', 'import_date']].to_string(index=False))

# Query some trees
print(f"\nSample trees from database:")
trees = api.query_trees(limit=5)
if not trees.empty:
    print(trees[['asset_id', 'asset_name', 'species', 'health']].to_string(index=False))
