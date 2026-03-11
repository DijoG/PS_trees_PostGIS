# PS Trees PostGIS

A Python tool to extract tree data from ProofSafe API (database) and store it in PostGIS database with spatial capabilities.

## Features
- Extract all trees from ProofSafe API with pagination
- Store directly in PostGIS with geometry columns
- Import tracking with metadata
- CSV backup option
- Support for filters (project, health, date range, status)
- Timestamped table versions

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
Copy and create `.creds.json`:
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

## Quick Start

### Python Script

Create a file `import_trees.py` or use example/`fetch_and_store.py`:
```python
from proofsafe_trees_postgis import ProofSafeGeoDB
from datetime import datetime

# Initialize (auto-loads credentials)
api = ProofSafeGeoDB()

# Store in database with timestamped table
records_fetched, records_stored = api.fetch_and_store(
    table_name=f"trees_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    if_exists='replace',
    debug=True
)
print(f"Stored {records_stored} trees")
```
Run it in CLI:
```bash
python import_trees.py
# or:
python example/fetch_and_store.py
```

### Command Line Interface
```bash
# Show help
python proofsafe_trees_postgis.py --help

# List available projects
python proofsafe_trees_postgis.py --list-projects

# Import all trees to database
python proofsafe_trees_postgis.py --store

# Import with filters
python proofsafe_trees_postgis.py --store --health good --start 2024-01-01 --end 2024-12-31

# Import with CSV backup
python proofsafe_trees_postgis.py --store --csv-backup "D:/backups/trees.csv"

# Show import history
python proofsafe_trees_postgis.py --import-history

# Show database statistics
python proofsafe_trees_postgis.py --stats
```

## Usage Examples

### Example 1: Basic Import
```python
from proofsafe_trees_postgis import ProofSafeGeoDB
from datetime import datetime

api = ProofSafeGeoDB()

api.fetch_and_store(
    table_name=f"trees_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    if_exists='replace',
    debug=True
)
```
### Example 2: Import with Filters
```python
api.fetch_and_store(
    table_name="trees_healthy_2024",
    health="good",
    start_date="2024-01-01",
    end_date="2024-12-31",
    csv_backup="D:/backups/healthy_trees_2024.csv",
    import_notes="Q1 2024 healthy trees",
    debug=True
)
```
### Example 3: Check Results
```python
# Get database statistics
stats = api.get_tree_count()
print(f"Total trees: {stats['total_trees']}")

# View import history
history = api.get_import_history(limit=5)
print(history)

# Query trees
trees = api.query_trees("SELECT * FROM trees_20260310_155536 LIMIT 10")
print(trees)
```
## Query Examples

After import, you can query your data:
```sql
-- Connect to your database
psql -h localhost -U postgres -d trees_db

-- List all trees tables
\dt trees_*

-- Count trees in latest import
SELECT COUNT(*) FROM trees_20260310_155536;

-- Find trees by species
SELECT asset_id, species, health 
FROM trees_20260310_155536 
WHERE species LIKE '%Oak%';

-- Spatial query: trees within 1km of a point
SELECT asset_id, species, ST_AsText(geom)
FROM trees_20260310_155536
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(46.63, 24.69), 4326)::geography,
    1000
);
```

