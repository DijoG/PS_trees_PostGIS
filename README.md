# PS Trees PostGIS

A Python tool to extract tree data from ProofSafe API (database) and store it in a PostGIS database with full schema support.

## Features
- Extract all trees from ProofSafe API with pagination 
- Store data directly in PostGIS 
- **Full PostgreSQL schema support** - Organize tables in any schema (public, sde, gisdata, etc.)
- Track import history and data lineage
- Automatic geometry column creation from latitude/longitude
- Support for filters (project, health, date range, status)
- Simple data viewer scripts

## Installation

### 1. Clone the repository
```bash
git clone https://github.com/DijoG/PS_trees_PostGIS.git
cd PS_trees_PostGIS
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure credentials
Create `.creds.json`:
```bash
cp .creds_template.json .creds.json
```
Edit `creds.json` with your credentials:
```json
{
    "proofsafe": {
        "base_url": "yourPSbaseurl",
        "username": "yourPSusername",
        "password": "yourPSpassword"
    },
    "database": {
        "host": "yourDBhost",
        "port": 5432,
        "database": "yourDBname",
        "user": "yourDBusername",
        "password": "yourpassword"
    }
}
```
## The Schema Parameter

The `schema` parameter tells the script which PostgreSQL schema to use for table creation and queries. This is a standard PostgreSQL feature for organizing tables into logical groups.
```python
# Use the default 'public' schema
api = ProofSafeGeoDB()  # schema defaults to 'public'

# Use a custom schema (e.g., 'sde', 'gisdata', 'trees', etc.)
api = ProofSafeGeoDB(schema='sde')

# Any valid PostgreSQL schema name works
api = ProofSafeGeoDB(schema='tree_survey')
```
## Quick Start

### Basic Import
```python
from proofsafe_trees_postgis_schema import ProofSafeGeoDB
from datetime import datetime

# Initialize (auto-loads credentials)
api = ProofSafeGeoDB(schema='TreeSurvey')

# Store in database with timestamped table
records_fetched, records_stored = api.fetch_and_store(
    table_name=f"trees_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    if_exists='replace',  # 'replace', 'append', or 'fail'
    debug=True
)

print(f"✅ Stored {records_stored} trees")
```
#### For Green Riyadh TMO
Run in CLI:
```bash
python example/fetch_and_store.py
```
## Usage Examples

### 1. Extract Data to Different Schemas
```python
from proofsafe_trees_postgis_schema import ProofSafeGeoDB
from datetime import datetime

# Example 1: Store in default 'public' schema
api_public = ProofSafeGeoDB()  # schema='public' by default
api_public.fetch_and_store(
    table_name="trees_public",
    if_exists='replace',
    debug=True
)

# Example 2: Store in 'sde' schema
api_sde = ProofSafeGeoDB(schema='sde')
api_sde.fetch_and_store(
    table_name="trees_sde",
    if_exists='replace',
    debug=True
)

# Example 3: Store in project-specific schema
api_project = ProofSafeGeoDB(schema='project_x')
api_project.fetch_and_store(
    table_name="tree_survey_2024",
    if_exists='replace',
    debug=True
)
```
### 2. Import with Filters
```python
from proofsafe_trees_postgis_schema import ProofSafeGeoDB

api = ProofSafeGeoDB(schema='sde')

# Import only healthy trees from specific projects
records_fetched, records_stored = api.fetch_and_store(
    table_name="healthy_trees_2024",
    project_ids=[123, 456],        # Specific projects
    health="good",                  # Only healthy trees
    start_date="2024-01-01",        # Date range
    end_date="2024-12-31",
    include_events=True,             # Include comments/events
    if_exists='replace',
    debug=True
)
```
### 3. Simple Data Viewer
```python
from proofsafe_trees_postgis_schema import ProofSafeGeoDB
import pandas as pd

# CHANGE THIS LINE to use different schemas:
# schema = 'public'      # default
# schema = 'sde'         # for sde schema
# schema = 'TreeSurvey'  # for Treesurvey schema
# schema = 'Project_x'   # for project-specific schema

schema = 'sde'  # <-- Change this to your desired schema

# Connect to database with sde schema
api = ProofSafeGeoDB(schema='sde')

# 1. Show recent imports
print("\n📋 RECENT IMPORTS")
print("-" * 40)
history = api.get_import_history(limit=3)
print(history[['import_id', 'records_stored', 'status']].to_string(index=False))

# 2. Pick the latest successful import
latest_import = history[history['status'] == 'SUCCESS'].iloc[0]['import_id']
print(f"\n📊 LATEST DATA: {latest_import}")

# 3. Show sample of that data
query = f"SELECT asset_id, asset_name, lo2item_id_name as health, lo3item_id_name as species, latitude, longitude FROM sde.trees_{latest_import} LIMIT 5"
df = pd.read_sql(query, api.engine)

print("\n🌳 SAMPLE TREES")
print("-" * 40)
print(df.to_string(index=False))

# 4. Quick stats
count = pd.read_sql(f"SELECT COUNT(*) FROM sde.trees_{latest_import}", api.engine).iloc[0,0]
print(f"\n📊 Total trees: {count:,}")
```
### 3. Command Line Usage with Schema
```bash
# Store in different schemas
python proofsafe_trees_postgis_schema.py --store --db-schema public --table trees_data
python proofsafe_trees_postgis_schema.py --store --db-schema sde --table trees_data
python proofsafe_trees_postgis_schema.py --store --db-schema gisdata --table trees_data

# With filters
python proofsafe_trees_postgis_schema.py --store --db-schema sde --table trees_data --project 123

# View statistics for different schemas
python proofsafe_trees_postgis_schema.py --db-schema sde --stats
python proofsafe_trees_postgis_schema.py --db-schema public --stats

# View import history for different schemas
python proofsafe_trees_postgis_schema.py --db-schema sde --import-history 
```

## Requirements

- Python 3.7+
- PostgreSQL 12+ with PostGIS
- See `requirements.txt` for Python packages
