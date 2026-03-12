#!/usr/bin/env python3
"""
ProofSafe Tree Data Extractor, Transformer and PostgIS geodatabase Storer
"""

import os
import json
import time
import ast
import re
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Union, List, Dict, Any, Generator, Tuple
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from geoalchemy2 import Geometry
import warnings
from pathlib import Path
warnings.filterwarnings('ignore', category=UserWarning, module='geoalchemy2')

# Default config paths
DEFAULT_CREDS_PATHS = [
    Path('.creds.json'),
    Path.home() / '.proofsafe' / 'credentials.json',
    Path('/etc/proofsafe/credentials.json')
]

def load_credentials(creds_path: Optional[str] = None) -> Dict:
    """
    Load credentials from JSON file
    
    Args:
        creds_path: Path to credentials JSON file
        
    Returns:
        Dictionary with credentials
        
    Raises:
        FileNotFoundError: If no credentials file found
        json.JSONDecodeError: If JSON is invalid
    """
    
    # If specific path provided, use it
    if creds_path:
        paths_to_try = [Path(creds_path)]
    else:
        paths_to_try = DEFAULT_CREDS_PATHS
    
    # Try each path
    for path in paths_to_try:
        if path.exists():
            try:
                with open(path, 'r') as f:
                    creds = json.load(f)
                
                # Validate required keys
                if 'proofsafe' not in creds:
                    raise ValueError("Missing 'proofsafe' section in credentials")
                if 'database' not in creds:
                    raise ValueError("Missing 'database' section in credentials")
                
                # Validate ProofSafe credentials
                proofsafe = creds['proofsafe']
                if not all(k in proofsafe for k in ['username', 'password']):
                    raise ValueError("ProofSafe credentials must include 'username' and 'password'")
                
                # Validate database credentials
                db = creds['database']
                required_db = ['host', 'port', 'database', 'user', 'password']
                if not all(k in db for k in required_db):
                    raise ValueError(f"Database credentials must include: {', '.join(required_db)}")
                
                print(f"✅ Loaded credentials from {path}")
                return creds
                
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON in {path}: {e}")
                raise
            except Exception as e:
                print(f"❌ Error loading {path}: {e}")
                raise
    
    # No credentials found
    error_msg = (
        "No credentials file found. Please create one at:\n"
        f"  {DEFAULT_CREDS_PATHS[0]}\n"
        "With content:\n"
        """{
    "proofsafe": {
        "base_url": "https://your-url.com",
        "username": "your-username@example.com",
        "password": "your-password"
    },
    "database": {
        "host": "localhost",
        "port": 5432,
        "database": "trees_db",
        "user": "postgres",
        "password": "your-db-password"
    }
}"""
    )
    raise FileNotFoundError(error_msg)

# Load credentials
try:
    CREDS = load_credentials()
except Exception as e:
    print(f"❌ Failed to load credentials: {e}")
    print("\n💡 Tip: You can specify a custom path with --creds /path/to/creds.json")
    exit(1)

# Database configuration from credentials
DB_CONFIG = CREDS['database']

# From Proofsave to dir CSV storage
class ProofSafeTreeAPI:
    """Official ProofSafe API client for tree data with full pagination"""
    
    def __init__(self, 
                 base_url: Optional[str] = None, 
                 username: Optional[str] = None, 
                 password: Optional[str] = None):
        """
        Initialize the ProofSafe API client
        
        Args:
            base_url: API base URL (optional, defaults to credentials or standard URL)
            username: Your API username (optional, defaults to credentials)
            password: Your API password (optional, defaults to credentials)
        """
        proofsafe_creds = CREDS['proofsafe']
        
        self.base_url = (base_url or 
                        proofsafe_creds.get('base_url') or 
                        "https://proofsafe-portalapi.tmo-gr.com").rstrip('/')
        
        self.username = username or proofsafe_creds['username']
        self.password = password or proofsafe_creds['password']
        
        # Don't print credentials!
        print(f"🔐 Authenticating to {self.base_url} as {self.username}")
        
        self.auth = HTTPBasicAuth(self.username, self.password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def get_trees_generator(self,
                           project_ids: Optional[Union[int, List[int]]] = None,
                           health: Optional[Union[str, List[str]]] = None,
                           species: Optional[Union[int, List[int]]] = None,
                           status: Optional[Union[int, List[int]]] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           date_field: str = "Created_At",
                           page_size: int = 1000,                   # Max page size
                           include_events: bool = False,
                           include_images: bool = False,
                           debug: bool = True) -> Generator[List[Dict], None, None]:
        """
        Generator that yields pages of tree data
        
        This allows processing large datasets without loading everything into memory at once.
        """
        
        # Health text to ID mapping (from documentation)
        health_map = {
            "excellent": 40346,
            "good": 40347,
            "fair": 40348,
            "poor": 40349,
            "dead": 40350
        }
        
        # Build XCFG string for additional data
        xcfg_parts = []
        if include_events:
            xcfg_parts.append("LOAD=EVENTS")
        if include_images:
            xcfg_parts.append("LOAD=IMAGES")
        xcfg = f"[{','.join(xcfg_parts)}]" if xcfg_parts else ""
        
        # Build select filters
        select_filters = []
        
        # Project filter
        if project_ids:
            if isinstance(project_ids, int):
                project_ids = [project_ids]
            project_str = f"({','.join(str(pid) for pid in project_ids)})"
            select_filters.append({
                "selections": project_str,
                "field_Name": "projects",
                "filterType": "Select"
            })
            if debug:
                print(f"📋 Project filter: {project_str}")
        
        # Health filter (LO2)
        if health:
            health_ids = []
            if isinstance(health, str):
                # Convert text to ID
                if health.lower() in health_map:
                    health_ids = [health_map[health.lower()]]
                else:
                    # Assume it's already an ID
                    health_ids = [int(health)]
            elif isinstance(health, list):
                for h in health:
                    if isinstance(h, str) and h.lower() in health_map:
                        health_ids.append(health_map[h.lower()])
                    else:
                        health_ids.append(int(h))
            else:
                health_ids = [int(health)]
            
            health_str = f"({','.join(str(hid) for hid in health_ids)})"
            select_filters.append({
                "selections": health_str,
                "field_Name": "lo2s",
                "filterType": "Select"
            })
            if debug:
                print(f"❤️ Health filter: {health_str}")
        
        # Species filter (LO3)
        if species:
            if isinstance(species, int):
                species = [species]
            species_str = f"({','.join(str(sid) for sid in species)})"
            select_filters.append({
                "selections": species_str,
                "field_Name": "lo3s",
                "filterType": "Select"
            })
            if debug:
                print(f"🌳 Species filter: {species_str}")
        
        # Status filter
        if status is not None:
            if isinstance(status, int):
                status = [status]
            status_str = f"({','.join(str(s) for s in status)})"
            select_filters.append({
                "selections": status_str,
                "field_Name": "statuses",
                "filterType": "Select"
            })
            if debug:
                print(f"📊 Status filter: {status_str}")
        
        # Build date filters
        date_filters = []
        if start_date and end_date:
            # Convert dates to timestamps (milliseconds since epoch)
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Add time to make it full day range
            start_ts = int(start_dt.timestamp() * 1000)
            end_ts = int(end_dt.timestamp() * 1000) + (24 * 60 * 60 * 1000 - 1)
            
            date_filters.append({
                "field_Name": date_field,
                "from": start_ts,
                "to": end_ts,
                "filterType": "Date"
            })
            if debug:
                print(f"📅 Date filter: {date_field} from {start_date} to {end_date}")
        
        endpoint = f"{self.base_url}/api/DataViewer/DV_AssetXMR_VList"
        page = 1
        total_records = 0
        
        while True:
            # Build the complete request body for this page
            body = {
                "NumberFilters": [],
                "SearchFilters": [],
                "SelectFilters": select_filters,
                "DateFilters": date_filters,
                "category_Id": 39,  # Tree category
                "XCFG": xcfg,
                "p_no": str(page),
                "p_size": str(page_size),
                "sortName": "",
                "sortDirection": "None"
            }
            
            if debug:
                print(f"\n📄 Fetching page {page}...")
                if page == 1:
                    print(f"🔍 Endpoint: {endpoint}")
            
            try:
                response = self.session.post(endpoint, json=body, timeout=60)
                
                if response.status_code != 200:
                    print(f"❌ Error on page {page}: {response.status_code}")
                    print(f"   {response.text[:500]}")
                    break
                
                data = response.json()
                
                # Extract assets from response
                assets = []
                if isinstance(data, dict):
                    if 'Data' in data:
                        assets = data['Data']
                    elif 'data' in data:
                        assets = data['data']
                    elif 'Assets' in data:
                        assets = data['Assets']
                    elif 'assets' in data:
                        assets = data['assets']
                    elif 'rows' in data:
                        assets = data['rows']
                    elif 'items' in data:
                        assets = data['items']
                    else:
                        # Try to find any list in the response
                        for key, value in data.items():
                            if isinstance(value, list):
                                assets = value
                                if debug and page == 1:
                                    print(f"📋 Found data in '{key}'")
                                break
                elif isinstance(data, list):
                    assets = data
                
                record_count = len(assets)
                total_records += record_count
                
                if debug:
                    print(f"   Retrieved {record_count} records (Total: {total_records})")
                
                if record_count == 0:
                    if debug:
                        print(f"\n✅ No more records. Total: {total_records}")
                    break
                
                # Yield this page of assets
                yield assets
                
                # If we got fewer records than page size, we're done
                if record_count < page_size:
                    if debug:
                        print(f"\n✅ Last page reached. Total: {total_records}")
                    break
                
                page += 1
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error on page {page}: {e}")
                break
    
    def get_trees(self,
                  output_path: str = "proofsafe_trees.csv",
                  project_ids: Optional[Union[int, List[int]]] = None,
                  health: Optional[Union[str, List[str]]] = None,
                  species: Optional[Union[int, List[int]]] = None,
                  status: Optional[Union[int, List[int]]] = None,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  date_field: str = "Created_At",
                  page_size: int = 1000,
                  include_events: bool = False,
                  include_images: bool = False,
                  debug: bool = True,
                  sample_size: Optional[int] = None) -> pd.DataFrame:
        """
        Get ALL trees from ProofSafe with automatic pagination
        
        Args:
            output_path: Where to save the CSV file
            project_ids: Single project ID or list of project IDs
            health: Health status(es) - "good", "fair", "poor", "dead" or IDs
            species: Species ID(s)
            status: Status ID(s) - 0=No Action, 1=Action Required, 2=Removed
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            date_field: Which date field to filter on
            page_size: Number of records per page (max 1000)
            include_events: Load associated events/comments
            include_images: Load associated images
            debug: Print debug information
            sample_size: If provided, only fetch this many records (for testing)
        
        Returns:
            DataFrame with ALL tree data
        """
        
        all_assets = []
        total_pages = 0
        max_pages = None
        
        if sample_size:
            max_pages = (sample_size + page_size - 1) // page_size
            if debug:
                print(f"🔍 Sample mode: fetching up to {sample_size} records ({max_pages} pages)")
        
        # Use the generator to fetch all pages
        for page_num, assets in enumerate(self.get_trees_generator(
            project_ids=project_ids,
            health=health,
            species=species,
            status=status,
            start_date=start_date,
            end_date=end_date,
            date_field=date_field,
            page_size=page_size,
            include_events=include_events,
            include_images=include_images,
            debug=debug
        ), 1):
            
            all_assets.extend(assets)
            total_pages = page_num
            
            if max_pages and page_num >= max_pages:
                if debug:
                    print(f"\n⏸️ Sample limit reached ({sample_size} records)")
                break
        
        if debug:
            print(f"\n📊 Total records fetched: {len(all_assets)} from {total_pages} pages")
        
        if not all_assets:
            print("⚠️ No trees found matching criteria")
            # Create empty DataFrame with expected columns
            df = pd.DataFrame(columns=[
                "Asset_Id", "Asset_Name", "Asset_Code", "Project_Id", "Project_Id_Name",
                "L01Item_Id_Name", "L02Item_Id_Name", "L03Item_Id_Name", "L04Item_Id_Name",
                "L05Item_Id_Name", "L06Item_Id_Name", "L07Item_Id_Name", "L08Item_Id_Name",
                "L09Item_Id_Name", "Status_Id_Name", "Height", "Spread", "DBH_CM",
                "GEO", "Created_At", "Modified_At", "Created_By_Name", "Modified_By_Name"
            ])
        else:
            # Convert to DataFrame
            df = pd.DataFrame(all_assets)
            
            if debug:
                print(f"\n📋 DataFrame shape: {df.shape}")
                print(f"\n📋 Columns ({len(df.columns)}):")
                for i, col in enumerate(sorted(df.columns), 1):
                    print(f"  {i}. {col}")
                
                # Show sample
                print(f"\n📊 First 3 records:")
                print(df.head(3).to_string())
            
            # Convert timestamp columns to datetime
            timestamp_cols = ['Created_At', 'Modified_At', 'DOB', 'Check_Out_Date']
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], unit='ms')
            
            # Add human-readable columns
            column_mapping = {
                'L01Item_Id_Name': 'Structure',
                'L02Item_Id_Name': 'Health',
                'L03Item_Id_Name': 'Species',
                'L04Item_Id_Name': 'Density',
                'L05Item_Id_Name': 'Maturity',
                'L06Item_Id_Name': 'Exceptional',
                'L07Item_Id_Name': 'Irrigated',
                'L08Item_Id_Name': 'Tree_Source',
                'L09Item_Id_Name': 'Condition',
                'Project_Id_Name': 'Project',
                'Status_Id_Name': 'Status',
                'Created_By_Name': 'Created_By',
                'Modified_By_Name': 'Modified_By'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df[new_col] = df[old_col]
            
            if debug and len(all_assets) > 0:
                print(f"\n📈 Health distribution:")
                if 'Health' in df.columns:
                    print(df['Health'].value_counts())
                print(f"\n📈 Project distribution:")
                if 'Project' in df.columns:
                    print(df['Project'].value_counts())
        
        # Save to CSV
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
        df.to_csv(output_path, index=False)
        
        if debug:
            print(f"\n💾 Saved {len(df)} trees to {os.path.abspath(output_path)}")
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # Size in MB
            print(f"   File size: {file_size:.2f} MB")
        
        return df
    
    def get_projects(self, debug: bool = True) -> pd.DataFrame:
        """Get list of available projects"""
        df = self.get_trees(page_size=10, debug=debug)
        
        if not df.empty and 'Project_Id' in df.columns and 'Project_Id_Name' in df.columns:
            projects = df[['Project_Id', 'Project_Id_Name']].drop_duplicates()
            projects = projects.rename(columns={'Project_Id': 'id', 'Project_Id_Name': 'name'})
            projects = projects.sort_values('name').reset_index(drop=True)
            return projects
        else:
            return pd.DataFrame(columns=['id', 'name'])
    
    def get_health_options(self) -> pd.DataFrame:
        """Get available health status options"""
        return pd.DataFrame([
            {"id": 40346, "name": "excellent"},
            {"id": 40347, "name": "good"},
            {"id": 40348, "name": "fair"},
            {"id": 40349, "name": "poor"},
            {"id": 40350, "name": "dead"}
        ])
    
    def get_status_options(self) -> pd.DataFrame:
        """Get available status options"""
        return pd.DataFrame([
            {"id": 0, "name": "No Action Required"},
            {"id": 1, "name": "Action Required"},
            {"id": 2, "name": "Removed"}
        ])


# From ProofSafe to PostGIS (optinal backup: CSV) storage
class ProofSafeGeoDB(ProofSafeTreeAPI):
    """
    Extended ProofSafe API client with PostGIS database storage capabilities
    and full import tracking
    """
    
    def __init__(self, 
                 base_url: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 db_config: Optional[Dict] = None,
                 create_db: bool = True,
                 schema: str = 'public'):
        """
        Initialize the ProofSafe GeoDB client
        
        Args:
            base_url: API base URL (optional, defaults to credentials)
            username: API username (optional, defaults to credentials)
            password: API password (optional, defaults to credentials)
            db_config: Database configuration dictionary (optional, defaults to credentials)
            create_db: If True, create database if it doesn't exist
            schema: Database schema name (default: 'public')
        """
        # Pass credentials to parent class - they will be loaded from .creds.json
        # if not provided explicitly
        super().__init__(base_url, username, password)
        
        # Use database config from credentials if none provided
        if db_config is None:
            # DB_CONFIG is already loaded from .creds.json at module level
            self.db_config = DB_CONFIG.copy()
        else:
            self.db_config = db_config.copy()
            
        self.create_db = create_db
        self.schema = schema
        
        # Initialize database connection
        self.engine = None
        
        print(f"📁 Database target: {self.db_config['database']}.{self.schema}")
    
    def create_database_if_not_exists(self) -> bool:
        """
        Create the database if it doesn't exist
        Also ensures PostGIS extension is enabled
        
        Returns:
            True if database exists or was created successfully
        """
        try:
            # Connect to default 'postgres' database to create new database
            default_conn_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/postgres"
            default_engine = create_engine(default_conn_string)
            
            with default_engine.connect() as conn:
                conn.execute(text("COMMIT"))  # Need to commit to create database
                
                # Check if database exists
                result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_config['database']}'"))
                exists = result.scalar() is not None
                
                if not exists:
                    print(f"📁 Creating database: {self.db_config['database']}")
                    conn.execute(text(f"CREATE DATABASE {self.db_config['database']}"))
                    conn.commit()
                    print(f"✅ Database created")
                else:
                    print(f"ℹ️ Database '{self.db_config['database']}' already exists")
            
            # Now connect to the new database and enable PostGIS
            self.connect_db(create_if_missing=False)  # Skip creation check this time
            
            with self.engine.connect() as conn:
                # Create schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
                
                # Check if PostGIS is enabled
                result = conn.execute(text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')"))
                postgis_exists = result.scalar()
                
                if not postgis_exists:
                    print("🔧 Enabling PostGIS extension...")
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                    conn.commit()
                    print("✅ PostGIS enabled")
                else:
                    conn.commit()
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create database: {e}")
            return False
    
    def connect_db(self, create_if_missing: bool = True) -> bool:
        """
        Establish connection to PostgreSQL database
        
        Args:
            create_if_missing: If True, create database if it doesn't exist
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
                # Create schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
                conn.commit()
            
            print(f"✅ Connected to database: {self.db_config['database']} on {self.db_config['host']}")
            print(f"✅ Using schema: {self.schema}")
            return True
            
        except Exception as e:
            if create_if_missing and "does not exist" in str(e):
                print(f"📁 Database '{self.db_config['database']}' does not exist. Creating...")
                return self.create_database_if_not_exists()
            else:
                print(f"❌ Database connection failed: {e}")
                return False
    
    def check_postgis(self) -> bool:
        """
        Check if PostGIS extension is available
        
        Returns:
            True if PostGIS is available, False otherwise
        """
        if not self.engine:
            if not self.connect_db():
                return False
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT PostGIS_Version()"))
                version = result.scalar()
                print(f"✅ PostGIS version: {version}")
                return True
        except Exception as e:
            print(f"❌ PostGIS not available: {e}")
            print("   Please enable PostGIS extension: CREATE EXTENSION postgis;")
            return False
    
    @staticmethod
    def parse_geopoint(geopoint_field) -> Tuple[Optional[float], Optional[float]]:
        """
        Parse GEOPoint field which can be a dict or string like:
        "{'Latitude': 24.6901406601921, 'Longitude': 46.6312370449305}"
        
        Returns tuple of (latitude, longitude) or (None, None) if parsing fails
        """
        if pd.isna(geopoint_field) or geopoint_field == '' or geopoint_field == '{}':
            return None, None
        
        # If it's already a dict, just extract values
        if isinstance(geopoint_field, dict):
            lat = geopoint_field.get('Latitude')
            lon = geopoint_field.get('Longitude')
            if lat is not None and lon is not None:
                return float(lat), float(lon)
            return None, None
        
        # If it's a string, try to parse it
        if isinstance(geopoint_field, str):
            try:
                # Method 1: Using ast.literal_eval (safe evaluation of Python literals)
                geopoint_str = geopoint_field.strip()
                
                # Handle case where it might be a string representation of a dict
                if geopoint_str.startswith('{') and geopoint_str.endswith('}'):
                    # Replace single quotes with double quotes for JSON parsing
                    geopoint_str = geopoint_str.replace("'", '"')
                    
                    # Parse as JSON
                    geopoint_dict = json.loads(geopoint_str)
                    lat = geopoint_dict.get('Latitude')
                    lon = geopoint_dict.get('Longitude')
                    
                    if lat is not None and lon is not None:
                        return float(lat), float(lon)
                
                # Method 2: Using regex as a fallback
                pattern = r"'Latitude':\s*([\d\.-]+),\s*'Longitude':\s*([\d\.-]+)"
                match = re.search(pattern, str(geopoint_field))
                if match:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    return lat, lon
                
                # Method 3: Try ast.literal_eval as another fallback
                try:
                    # Convert string to a format that ast.literal_eval can handle
                    clean_str = str(geopoint_field).strip()
                    if clean_str.startswith('{') and clean_str.endswith('}'):
                        # ast.literal_eval works with Python literals including dicts
                        geopoint_dict = ast.literal_eval(clean_str)
                        lat = geopoint_dict.get('Latitude')
                        lon = geopoint_dict.get('Longitude')
                        if lat is not None and lon is not None:
                            return float(lat), float(lon)
                except:
                    pass
                    
            except Exception as e:
                print(f"Warning: Could not parse GEOPoint: {geopoint_field[:50]}... - Error: {e}")
        
        return None, None
    
    def create_metadata_table(self) -> bool:
        """
        Create a table to track data imports
        
        Returns:
            True if successful, False otherwise
        """
        if not self.engine:
            if not self.connect_db():
                return False
        
        try:
            with self.engine.connect() as conn:
                create_sql = """
                CREATE TABLE IF NOT EXISTS data_imports (
                    id SERIAL PRIMARY KEY,
                    import_id VARCHAR(50) UNIQUE NOT NULL,
                    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    table_name VARCHAR(100) NOT NULL,
                    records_fetched INTEGER,
                    records_stored INTEGER,
                    date_range_start DATE,
                    date_range_end DATE,
                    filters_applied TEXT,
                    status VARCHAR(50),
                    error_message TEXT,
                    created_by VARCHAR(255),
                    notes TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_imports_date ON data_imports (import_date);
                CREATE INDEX IF NOT EXISTS idx_imports_id ON data_imports (import_id);
                """
                conn.execute(text(create_sql))
                conn.commit()
            
            print("✅ Created data_imports metadata table")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create metadata table: {e}")
            return False
    
    def log_import(self, 
                  import_id: str,
                  records_fetched: int,
                  records_stored: int,
                  table_name: str,                    
                  filters: Dict = None,
                  status: str = "SUCCESS",
                  error: str = None,
                  notes: str = None) -> bool:
        """
        Log an import operation to the metadata table
        
        Args:
            import_id: Import identifier
            records_fetched: Number of records fetched from API
            records_stored: Number of records stored in database
            table_name: Name of the table where data was stored (can include schema)
            filters: Filters applied to the import
            status: Import status
            error: Error message if any
            notes: Additional notes
            
        Returns:
            True if successful, False otherwise
        """
        if not self.engine:
            return False
        
        try:
            filters_json = json.dumps(filters, default=str) if filters else None
            
            # Extract date range if present
            date_range_start = None
            date_range_end = None
            if filters and 'start_date' in filters:
                date_range_start = filters['start_date']
            if filters and 'end_date' in filters:
                date_range_end = filters['end_date']
            
            with self.engine.connect() as conn:
                # Check if import_id already exists
                result = conn.execute(
                    text("SELECT 1 FROM data_imports WHERE import_id = :import_id"),
                    {"import_id": import_id}
                )
                exists = result.scalar() is not None
                
                if exists:
                    # Update existing record
                    update_sql = """
                    UPDATE data_imports 
                    SET records_fetched = :fetched,
                        records_stored = :stored,
                        table_name = :table_name,              
                        status = :status,
                        error_message = :error,
                        notes = :notes
                    WHERE import_id = :import_id
                    """
                    conn.execute(
                        text(update_sql),
                        {
                            "import_id": import_id,
                            "fetched": records_fetched,
                            "stored": records_stored,
                            "table_name": table_name,           
                            "status": status,
                            "error": error,
                            "notes": notes
                        }
                    )
                else:
                    # Insert new record
                    insert_sql = """
                    INSERT INTO data_imports 
                        (import_id, records_fetched, records_stored, table_name,
                         date_range_start, date_range_end, filters_applied, status, 
                         error_message, notes, created_by)
                    VALUES 
                        (:import_id, :fetched, :stored, :table_name,
                         :start, :end, :filters, :status, :error, :notes, :created_by)
                    """
                    
                    conn.execute(
                        text(insert_sql),
                        {
                            "import_id": import_id,
                            "fetched": records_fetched,
                            "stored": records_stored,
                            "table_name": table_name,           
                            "start": date_range_start,
                            "end": date_range_end,
                            "filters": filters_json,
                            "status": status,
                            "error": error,
                            "notes": notes,
                            "created_by": "ProofSafe API"
                        }
                    )
                
                conn.commit()
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to log import: {e}")
            return False
    
    def create_trees_table(self, table_name, drop_existing: bool = False) -> bool:
        """
        Create the trees table with PostGIS geometry column and metadata
        
        Args:
            drop_existing: If True, drop the table if it exists
            
        Returns:
            True if successful, False otherwise
        """
        if not self.engine:
            if not self.connect_db():
                return False
        
        if not self.check_postgis():
            return False
        
        try:
            with self.engine.connect() as conn:
                # Drop table if requested
                if drop_existing:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{table_name} CASCADE"))
                    conn.commit()
                    print(f"🗑️ Dropped existing table '{self.schema}.{table_name}'")
                
                # Check if table exists
                inspector = inspect(self.engine)
                if table_name in inspector.get_table_names(schema=self.schema):
                    print(f"ℹ️ Table '{self.schema}.{table_name}' already exists.")
                    return True
              
                # Create table with schema-qualified name
                create_table_sql = f"""
                CREATE TABLE {self.schema}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    asset_id INTEGER,
                    asset_name VARCHAR(255),
                    asset_code VARCHAR(50),
                    asset_sn VARCHAR(100),
                    asset_details TEXT,
                    asset_score FLOAT,
                    linked_account_id INTEGER,
                    linked_account_id_name VARCHAR(255),
                    manager_id INTEGER,
                    manager_id_name VARCHAR(255),
                    business_id INTEGER,
                    business_id_name VARCHAR(255),
                    office_id INTEGER,
                    office_id_name VARCHAR(255),
                    project_id INTEGER,
                    project_id_name VARCHAR(255),
                    job_id INTEGER,
                    job_id_name VARCHAR(255),
                    dob DATE,
                    geo VARCHAR(255),
                    geo_z VARCHAR(100),
                    geopoint TEXT,
                    type_id INTEGER,
                    type_id_name VARCHAR(255),
                    category_id INTEGER,
                    category_id_name VARCHAR(255),
                    sys_type_id INTEGER,
                    sys_type_id_name VARCHAR(255),
                    status_id INTEGER,
                    status_id_name VARCHAR(255),
                    state_id INTEGER,
                    state_id_name VARCHAR(255),
                    stage_id INTEGER,
                    stage_id_name VARCHAR(255),
                    group_id INTEGER,
                    group_id_name VARCHAR(255),
                    lo1item_id INTEGER,
                    lo1item_id_name VARCHAR(255),
                    lo2item_id INTEGER,
                    lo2item_id_name VARCHAR(255),
                    lo3item_id INTEGER,
                    lo3item_id_name VARCHAR(255),
                    lo4item_id_name VARCHAR(255),
                    lo5item_id_name VARCHAR(255),
                    lo6item_id_name VARCHAR(255),
                    lo7item_id_name VARCHAR(255),
                    lo8item_id INTEGER,
                    lo8item_id_name VARCHAR(255),
                    lo9item_id_name VARCHAR(255),
                    zone_id INTEGER,
                    zone_id_name VARCHAR(255),
                    team_id INTEGER,
                    team_id_name VARCHAR(255),
                    weight FLOAT,
                    height FLOAT,
                    length FLOAT,
                    width FLOAT,
                    spread FLOAT,
                    dbh_cm FLOAT,
                    created_at TIMESTAMP,
                    modified_at TIMESTAMP,
                    created_by VARCHAR(255),
                    modified_by VARCHAR(255),
                    updated_at BIGINT,
                    created_by_name VARCHAR(255),
                    modified_by_name VARCHAR(255),
                    tags VARCHAR(255),
                    access_level VARCHAR(50),
                    create_children BOOLEAN,
                    ubx INTEGER,
                    uby INTEGER,
                    settings_bag TEXT,
                    details_private TEXT,
                    check_out_date TIMESTAMP,
                    lo4item_id INTEGER,
                    lo5item_id INTEGER,
                    lo6item_id INTEGER,
                    check_in_date TIMESTAMP,
                    lo7item_id INTEGER,
                    lo9item_id INTEGER,
                    project VARCHAR(255),
                    status VARCHAR(255),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    geom geometry(Point, 4326),
                    
                    -- METADATA COLUMNS
                    db_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    db_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_date DATE,
                    import_id VARCHAR(50)
                );
                
                CREATE INDEX idx_{table_name}_geom ON {self.schema}.{table_name} USING GIST (geom);
                CREATE INDEX idx_{table_name}_lat_lon ON {self.schema}.{table_name} (latitude, longitude);
                CREATE INDEX idx_{table_name}_project ON {self.schema}.{table_name} (project_id);
                CREATE INDEX idx_{table_name}_species ON {self.schema}.{table_name} (lo3item_id);
                CREATE INDEX idx_{table_name}_health ON {self.schema}.{table_name} (lo2item_id);
                CREATE INDEX idx_{table_name}_status ON {self.schema}.{table_name} (status_id);
                CREATE INDEX idx_{table_name}_import_id ON {self.schema}.{table_name} (import_id);
                CREATE INDEX idx_{table_name}_data_date ON {self.schema}.{table_name} (data_date);
                """
                
                conn.execute(text(create_table_sql))
                conn.commit()
                
            print(f"✅ Created table '{self.schema}.{table_name}' with PostGIS geometry column")
            return True
          
        except Exception as e:
            print(f"❌ Failed to create table '{self.schema}.{table_name}': {e}")
            return False
    
    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame to extract coordinates and prepare for database insertion
        """
        # Make a copy to avoid modifying original
        processed_df = df.copy()
        
        # Ensure column names are lowercase
        processed_df.columns = [col.lower() for col in processed_df.columns]
        
        # Add columns for extracted coordinates
        processed_df['latitude'] = np.nan
        processed_df['longitude'] = np.nan
        
        # Convert GEOPoint dictionary to JSON string
        if 'geopoint' in processed_df.columns:
            processed_df['geopoint'] = processed_df['geopoint'].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
        
        # Extract coordinates from geopoint field
        valid_coords = 0
        
        for idx, row in processed_df.iterrows():
            if pd.notna(row.get('geopoint')):
                geopoint_value = row['geopoint']
                if isinstance(geopoint_value, str):
                    try:
                        geopoint_dict = json.loads(geopoint_value)
                    except:
                        geopoint_dict = self.parse_geopoint(geopoint_value)
                elif isinstance(geopoint_value, dict):
                    geopoint_dict = geopoint_value
                else:
                    geopoint_dict = None
                
                if geopoint_dict and isinstance(geopoint_dict, dict):
                    lat = geopoint_dict.get('Latitude')
                    lon = geopoint_dict.get('Longitude')
                    
                    if lat is not None and lon is not None:
                        processed_df.at[idx, 'latitude'] = float(lat)
                        processed_df.at[idx, 'longitude'] = float(lon)
                        valid_coords += 1
        
        print(f"📍 Extracted coordinates: {valid_coords} of {len(processed_df)} records")
        
        # Convert timestamp columns properly
        if 'dob' in processed_df.columns:
            processed_df['dob'] = pd.to_datetime(processed_df['dob'], unit='ms', errors='coerce').dt.date
        
        if 'created_at' in processed_df.columns:
            processed_df['created_at'] = pd.to_datetime(processed_df['created_at'], unit='ms', errors='coerce')
        
        if 'modified_at' in processed_df.columns:
            processed_df['modified_at'] = pd.to_datetime(processed_df['modified_at'], unit='ms', errors='coerce')
        
        if 'check_out_date' in processed_df.columns:
            processed_df['check_out_date'] = pd.to_datetime(processed_df['check_out_date'], unit='ms', errors='coerce')
        
        if 'check_in_date' in processed_df.columns:
            processed_df['check_in_date'] = pd.to_datetime(processed_df['check_in_date'], unit='ms', errors='coerce')
        
        if 'updated_at' in processed_df.columns:
            processed_df['updated_at'] = pd.to_numeric(processed_df['updated_at'], errors='coerce')
        
        return processed_df
    
    def store_trees(self, 
                   df: pd.DataFrame,
                   table_name: str = 'trees',
                   if_exists: str = 'append',
                   chunk_size: int = 1000,
                   debug: bool = True,
                   import_id: Optional[str] = None,
                   import_notes: Optional[str] = None) -> Tuple[int, bool]:
        """
        Store trees DataFrame in PostGIS database
        
        Args:
            df: DataFrame with tree data
            table_name: Target table name
            if_exists: 'append', 'replace', or 'fail'
            chunk_size: Number of records to insert per chunk
            debug: Print debug information
            import_id: Optional import identifier for tracking updates
            import_notes: Optional notes for this import
            
        Returns:
            Tuple of (records_stored, success_flag)
        """
        if not self.engine:
            if not self.connect_db():
                return 0, False
        
        # Generate import ID if not provided
        if import_id is None:
            import_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        data_date = datetime.now().date()
        
        # Check if table exists
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names(schema=self.schema):
            print(f"⚠️ Table '{self.schema}.{table_name}' doesn't exist. Creating it...")
            if not self.create_trees_table(table_name):
                return 0, False
        
        # Process DataFrame
        processed_df = self.process_dataframe(df)
        
        # Add metadata columns
        processed_df['db_created_at'] = datetime.now()
        processed_df['db_updated_at'] = datetime.now()
        processed_df['data_date'] = data_date
        processed_df['import_id'] = import_id
        
        if debug:
            print(f"\n📊 Processing {len(processed_df)} records for database storage")
            print(f"   Records with coordinates: {processed_df['latitude'].notna().sum()}")
            print(f"   Import ID: {import_id}")
            print(f"   Data date: {data_date}")
            print(f"   Target: {self.schema}.{table_name}")
        
        try:
            # Create a temporary table for insertion
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    CREATE TEMP TABLE trees_temp (LIKE {self.schema}.{table_name} INCLUDING DEFAULTS) 
                    ON COMMIT DROP
                """))
            
            total_inserted = 0
            
            # Insert in chunks
            for i in range(0, len(processed_df), chunk_size):
                chunk = processed_df.iloc[i:i+chunk_size].copy()
                
                # Insert chunk into temporary table
                chunk.to_sql('trees_temp', self.engine, if_exists='append', index=False)
                
                # Update geometry and insert into main table
                with self.engine.connect() as conn:
                    update_sql = f"""
                    INSERT INTO {self.schema}.{table_name}
                    SELECT 
                        *,
                        CASE 
                            WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
                            THEN ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                            ELSE NULL
                        END as geom
                    FROM trees_temp;
                    
                    DELETE FROM trees_temp;
                    """
                    conn.execute(text(update_sql))
                    conn.commit()
                
                total_inserted += len(chunk)
                if debug:
                    print(f"   Inserted {total_inserted} of {len(processed_df)} records")
            
            if debug:
                print(f"✅ Successfully stored {total_inserted} trees in {self.schema}.{table_name} (Import ID: {import_id})")
            
            return total_inserted, True
            
        except Exception as e:
            print(f"❌ Failed to store data: {e}")
            import traceback
            traceback.print_exc()
            return 0, False
    
    def fetch_and_store(self,
                       table_name: str = 'trees',
                       schema: str = None,
                       if_exists: str = 'append',
                       project_ids: Optional[Union[int, List[int]]] = None,
                       health: Optional[Union[str, List[str]]] = None,
                       species: Optional[Union[int, List[int]]] = None,
                       status: Optional[Union[int, List[int]]] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       date_field: str = "Created_At",
                       page_size: int = 1000,
                       include_events: bool = False,
                       include_images: bool = False,
                       debug: bool = True,
                       sample_size: Optional[int] = None,
                       csv_backup: Optional[str] = None,
                       import_notes: Optional[str] = None) -> Tuple[int, int]:
        """
        Fetch trees from API and store directly in database with import tracking
        
        Args:
            table_name: Target table name
            schema: Database schema name (overrides instance schema if provided)
            if_exists: 'append', 'replace', or 'fail'
            project_ids: Single project ID or list of project IDs
            health: Health status(es) - "good", "fair", "poor", "dead" or IDs
            species: Species ID(s)
            status: Status ID(s) - 0=No Action, 1=Action Required, 2=Removed
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            date_field: Which date field to filter on
            page_size: Number of records per page (max 1000)
            include_events: Load associated events/comments
            include_images: Load associated images
            debug: Print debug information
            sample_size: If provided, only fetch this many records (for testing)
            csv_backup: Optional path to save CSV backup
            import_notes: Optional notes for this import
            
        Returns:
            Tuple of (records_fetched, records_stored)
        """
        # Use provided schema or fall back to instance schema
        if schema:
            self.schema = schema
        
        if not self.engine:
            if not self.connect_db():
                return 0, 0
        
        # Generate import ID
        import_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Prepare filters for logging
        filters = {
            "project_ids": project_ids,
            "health": health,
            "species": species,
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
            "date_field": date_field,
            "include_events": include_events,
            "include_images": include_images
        }
        
        # Ensure table exists
        inspector = inspect(self.engine)
        if table_name not in inspector.get_table_names(schema=self.schema):
            print(f"📋 Creating table '{self.schema}.{table_name}'...")
            if not self.create_trees_table(table_name=table_name):
                self.log_import(
                    import_id=import_id,
                    records_fetched=0,
                    records_stored=0,
                    table_name=f"{self.schema}.{table_name}",
                    filters=filters,
                    status="FAILED",
                    error="Failed to create table",
                    notes=import_notes
                )
                return 0, 0
        elif if_exists == 'replace':
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {self.schema}.{table_name}"))
                conn.commit()
            print(f"🗑️ Truncated table '{self.schema}.{table_name}'")
        
        # Create metadata table if it doesn't exist
        self.create_metadata_table()
        
        records_fetched = 0
        records_stored = 0
        total_pages = 0
        max_pages = None
        all_assets = [] if csv_backup else None
        
        if sample_size:
            max_pages = (sample_size + page_size - 1) // page_size
            if debug:
                print(f"🔍 Sample mode: fetching up to {sample_size} records ({max_pages} pages)")
        
        print(f"\n🌳 Fetching trees from ProofSafe API... (Import ID: {import_id})")
        print(f"📁 Target: {self.schema}.{table_name}")
        
        # Type definitions for data conversion
        integer_columns = [
            'Asset_Id', 'Linked_Account_Id', 'Manager_Id', 'Business_Id', 'Office_Id',
            'Project_Id', 'Job_Id', 'Type_Id', 'Category_Id', 'Sys_Type_Id',
            'Status_Id', 'State_Id', 'Stage_Id', 'Group_Id', 'LO1Item_Id',
            'LO2Item_Id', 'LO3Item_Id', 'LO8Item_Id', 'Zone_Id', 'Team_Id',
            'LO4Item_Id', 'LO5Item_Id', 'LO6Item_Id', 'LO7Item_Id', 'LO9Item_Id',
            'UBX', 'UBY', 'Created_By', 'Modified_By', 'Updated_At'
        ]
        
        float_columns = [
            'Asset_Score', 'Weight', 'Height', 'Length', 'Width', 'Spread', 'DBH_CM'
        ]
        
        boolean_columns = ['Create_Children']
        
        date_columns = ['DOB']
        
        timestamp_columns = ['Created_At', 'Modified_At', 'Check_Out_Date', 'Check_In_Date']
        
        try:
            # Use the generator to fetch all pages
            for page_num, assets in enumerate(self.get_trees_generator(
                project_ids=project_ids,
                health=health,
                species=species,
                status=status,
                start_date=start_date,
                end_date=end_date,
                date_field=date_field,
                page_size=page_size,
                include_events=include_events,
                include_images=include_images,
                debug=debug
            ), 1):
                
                # Convert to DataFrame
                df_page = pd.DataFrame(assets)
                
                # Lowercase all column names to match PostgreSQL
                df_page.columns = [col.lower() for col in df_page.columns]
                
                records_fetched += len(df_page)
                
                if csv_backup:
                    all_assets.extend(assets)
                
                # Process and store this page
                processed_df = self.process_dataframe(df_page)
                
                # Add metadata columns
                processed_df['db_created_at'] = datetime.now()
                processed_df['db_updated_at'] = datetime.now()
                processed_df['data_date'] = datetime.now().date()
                processed_df['import_id'] = import_id
                
                # Convert integer columns
                for col in integer_columns:
                    col_lower = col.lower()
                    if col_lower in processed_df.columns:
                        processed_df[col_lower] = pd.to_numeric(processed_df[col_lower], errors='coerce')
                
                # Convert float columns
                for col in float_columns:
                    col_lower = col.lower()
                    if col_lower in processed_df.columns:
                        processed_df[col_lower] = pd.to_numeric(processed_df[col_lower], errors='coerce').astype(float)
                
                # Convert boolean columns
                for col in boolean_columns:
                    col_lower = col.lower()
                    if col_lower in processed_df.columns:
                        processed_df[col_lower] = processed_df[col_lower].fillna(False).astype(bool)
                
                # Convert date columns
                for col in date_columns:
                    col_lower = col.lower()
                    if col_lower in processed_df.columns:
                        processed_df[col_lower] = pd.to_datetime(processed_df[col_lower], errors='coerce')
                
                # Convert timestamp columns
                for col in timestamp_columns:
                    col_lower = col.lower()
                    if col_lower in processed_df.columns:
                        processed_df[col_lower] = pd.to_datetime(processed_df[col_lower], errors='coerce')
                
                # Store in database
                try:
                    from sqlalchemy import Integer, Float, Boolean, Date, DateTime, String, BigInteger
                    
                    dtype_dict = {}
                    
                    # Integer columns
                    for col in integer_columns:
                        col_lower = col.lower()
                        if col_lower in processed_df.columns:
                            dtype_dict[col_lower] = Integer()
                    
                    # Float columns
                    for col in float_columns:
                        col_lower = col.lower()
                        if col_lower in processed_df.columns:
                            dtype_dict[col_lower] = Float()
                    
                    # Boolean columns
                    for col in boolean_columns:
                        col_lower = col.lower()
                        if col_lower in processed_df.columns:
                            dtype_dict[col_lower] = Boolean()
                    
                    # Date columns
                    for col in date_columns:
                        col_lower = col.lower()
                        if col_lower in processed_df.columns:
                            dtype_dict[col_lower] = Date()
                    
                    # Timestamp columns
                    for col in timestamp_columns:
                        col_lower = col.lower()
                        if col_lower in processed_df.columns:
                            dtype_dict[col_lower] = DateTime()
                    
                    # Special handling for updated_at (bigint)
                    if 'updated_at' in processed_df.columns:
                        dtype_dict['updated_at'] = BigInteger()
                    
                    # Text columns (all remaining columns)
                    for col in processed_df.columns:
                        if col not in dtype_dict and col not in ['latitude', 'longitude', 'geom']:
                            dtype_dict[col] = String()
                    
                    # Add our custom columns
                    dtype_dict['latitude'] = Float()
                    dtype_dict['longitude'] = Float()
                    dtype_dict['db_created_at'] = DateTime()
                    dtype_dict['db_updated_at'] = DateTime()
                    dtype_dict['data_date'] = Date()
                    dtype_dict['import_id'] = String()
                    
                    # Calculate geometry placeholder
                    processed_df['geom'] = None
                    
                    # Insert directly into the main table with schema qualification
                    processed_df.to_sql(
                        table_name, 
                        self.engine, 
                        schema=self.schema,
                        if_exists='append', 
                        index=False,
                        dtype=dtype_dict
                    )
                    
                    # Now update the geometry column
                    with self.engine.connect() as conn:
                        update_geom_sql = f"""
                        UPDATE {self.schema}.{table_name} 
                        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                        WHERE import_id = '{import_id}' 
                        AND latitude IS NOT NULL 
                        AND longitude IS NOT NULL;
                        """
                        conn.execute(text(update_geom_sql))
                        conn.commit()
                    
                    records_stored += len(processed_df)
                    
                    if debug:
                        print(f"   ✅ Stored page {page_num} ({len(processed_df)} records)")
                    
                except Exception as e:
                    print(f"❌ Error storing page {page_num}: {e}")
                    import traceback
                    traceback.print_exc()
                    # Log the error but continue with next pages
                    self.log_import(
                        import_id=import_id,
                        records_fetched=records_fetched,
                        records_stored=records_stored,
                        table_name=f"{self.schema}.{table_name}",
                        filters=filters,
                        status="PARTIAL",
                        error=str(e),
                        notes=import_notes
                    )
                
                total_pages = page_num
                
                if max_pages and page_num >= max_pages:
                    if debug:
                        print(f"\n⏸️ Sample limit reached ({records_fetched} records)")
                    break
            
            # Log final import status
            status = "SUCCESS" if records_stored == records_fetched else "PARTIAL"
            self.log_import(
                import_id=import_id,
                records_fetched=records_fetched,
                records_stored=records_stored,
                table_name=f"{self.schema}.{table_name}",
                filters=filters,
                status=status,
                error=None,
                notes=import_notes
            )
            
            print(f"\n📊 Fetch complete: {records_fetched} records fetched, {records_stored} stored")
            print(f"   Import ID: {import_id}")
            print(f"   Target: {self.schema}.{table_name}")
            print(f"   Success rate: {records_stored/records_fetched*100:.1f}%")
            
            # Save CSV backup if requested
            if csv_backup and all_assets:
                df_all = pd.DataFrame(all_assets)
                df_all = self.process_dataframe(df_all)
                os.makedirs(os.path.dirname(csv_backup) if os.path.dirname(csv_backup) else ".", exist_ok=True)
                df_all.to_csv(csv_backup, index=False)
                if debug:
                    print(f"💾 Saved CSV backup to {os.path.abspath(csv_backup)}")
            
            return records_fetched, records_stored
            
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            import traceback
            traceback.print_exc()
            self.log_import(
                import_id=import_id,
                records_fetched=records_fetched,
                records_stored=records_stored,
                table_name=f"{self.schema}.{table_name}",
                filters=filters,
                status="FAILED",
                error=str(e),
                notes=import_notes
            )
            return records_fetched, records_stored
    
    def get_import_history(self, limit: int = 10) -> pd.DataFrame:
        """
        Get history of data imports
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with import history
        """
        if not self.engine:
            if not self.connect_db():
                return pd.DataFrame()
        
        try:
            query = f"""
            SELECT 
                import_id,
                import_date,
                records_fetched,
                records_stored,
                date_range_start,
                date_range_end,
                status,
                error_message,
                notes
            FROM data_imports
            ORDER BY import_date DESC
            LIMIT {limit}
            """
            
            df = pd.read_sql(query, self.engine)
            return df
            
        except Exception as e:
            print(f"❌ Failed to get import history: {e}")
            return pd.DataFrame()
    
    def get_trees_by_import(self, import_id: str, limit: int = 100) -> pd.DataFrame:
        """
        Get trees from a specific import
        
        Args:
            import_id: Import identifier
            limit: Maximum number of records
            
        Returns:
            DataFrame with trees from that import
        """
        if not self.engine:
            if not self.connect_db():
                return pd.DataFrame()
        
        try:
            # Need to find which table contains this import_id
            # First, check data_imports to get the table name
            query = f"""
            SELECT table_name 
            FROM data_imports 
            WHERE import_id = '{import_id}'
            """
            
            df_table = pd.read_sql(query, self.engine)
            
            if df_table.empty:
                print(f"❌ Import ID {import_id} not found")
                return pd.DataFrame()
            
            table_name = df_table.iloc[0]['table_name']
            
            # Now query that table
            query = f"""
            SELECT 
                asset_id, 
                asset_name, 
                lo3item_id_name as species,
                lo2item_id_name as health,
                latitude, 
                longitude,
                import_id,
                data_date
            FROM {table_name}
            WHERE import_id = '{import_id}'
            LIMIT {limit}
            """
            
            df = pd.read_sql(query, self.engine)
            print(f"📊 Found {len(df)} trees from import {import_id}")
            return df
            
        except Exception as e:
            print(f"❌ Failed to get trees by import: {e}")
            return pd.DataFrame()
    
    def query_trees(self, 
                   sql: Optional[str] = None,
                   as_geojson: bool = False,
                   limit: int = 100) -> pd.DataFrame:
        """
        Query trees from the database
        
        Args:
            sql: Optional custom SQL query
            as_geojson: If True, return geometry as GeoJSON
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with query results
        """
        if not self.engine:
            if not self.connect_db():
                return pd.DataFrame()
        
        if sql:
            query = sql
        else:
            if as_geojson:
                query = f"""
                SELECT *, 
                       ST_AsGeoJSON(geom) as geojson,
                       ST_X(geom) as longitude,
                       ST_Y(geom) as latitude,
                       import_id,
                       data_date
                FROM {self.schema}.trees 
                WHERE geom IS NOT NULL
                LIMIT {limit}
                """
            else:
                query = f"""
                SELECT *, import_id, data_date 
                FROM {self.schema}.trees 
                LIMIT {limit}
                """
        
        try:
            df = pd.read_sql(query, self.engine)
            print(f"📊 Query returned {len(df)} records")
            return df
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return pd.DataFrame()
    
    def get_tree_count(self, table_name: Optional[str] = None) -> Dict:
        """
        Get statistics about trees in database
        
        Args:
            table_name: Optional specific table to query. If None, finds the most recent trees_* table
            
        Returns:
            Dictionary with statistics
        """
        if not self.engine:
            if not self.connect_db():
                return {}
        
        try:
            # If no table name provided, find the most recent trees_* table
            if table_name is None:
                # Query to get the most recent trees_* table
                latest_table_query = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{self.schema}' 
                AND table_name LIKE 'trees_%'
                ORDER BY table_name DESC 
                LIMIT 1
                """
                
                with self.engine.connect() as conn:
                    result = conn.execute(text(latest_table_query))
                    latest = result.fetchone()
                    
                if latest:
                    table_name = latest[0]
                    print(f"🔍 Automatically detected latest table: '{self.schema}.{table_name}'")
                else:
                    # Fallback to default 'trees' table
                    table_name = 'trees'
                    print(f"ℹ️ No trees_* tables found, checking default '{self.schema}.{table_name}' table")
            
            # Check if table exists
            inspector = inspect(self.engine)
            if table_name not in inspector.get_table_names(schema=self.schema):
                return {
                    'error': f"Table '{self.schema}.{table_name}' does not exist",
                    'table_name': f"{self.schema}.{table_name}",
                    'total_trees': 0,
                    'with_coordinates': 0,
                    'without_coordinates': 0
                }
            
            with self.engine.connect() as conn:
                # Get count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{table_name}"))
                total = result.scalar()
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{table_name} WHERE geom IS NOT NULL"))
                with_geom = result.scalar()
                
                result = conn.execute(text(f"""
                    SELECT COUNT(DISTINCT project_id_name) FROM {self.schema}.{table_name} 
                    WHERE project_id_name IS NOT NULL
                """))
                projects = result.scalar() or 0
                
                result = conn.execute(text(f"""
                    SELECT lo2item_id_name, COUNT(*) 
                    FROM {self.schema}.{table_name} 
                    WHERE lo2item_id_name IS NOT NULL 
                    GROUP BY lo2item_id_name 
                    ORDER BY COUNT(*) DESC
                """))
                health_stats = {row[0]: row[1] for row in result}
                
                result = conn.execute(text(f"""
                    SELECT lo3item_id_name, COUNT(*) 
                    FROM {self.schema}.{table_name} 
                    WHERE lo3item_id_name IS NOT NULL 
                    GROUP BY lo3item_id_name 
                    ORDER BY COUNT(*) DESC
                    LIMIT 10
                """))
                top_species = {row[0]: row[1] for row in result}
                
                result = conn.execute(text(f"SELECT COUNT(DISTINCT import_id) FROM {self.schema}.{table_name} WHERE import_id IS NOT NULL"))
                import_count = result.scalar() or 0
                
                result = conn.execute(text(f"""
                    SELECT MIN(data_date), MAX(data_date) FROM {self.schema}.{table_name} 
                    WHERE data_date IS NOT NULL
                """))
                date_range = result.fetchone()
                
                return {
                    'table_name': f"{self.schema}.{table_name}",
                    'total_trees': total,
                    'with_coordinates': with_geom,
                    'without_coordinates': total - with_geom,
                    'unique_projects': projects,
                    'health_distribution': health_stats,
                    'top_species': top_species,
                    'import_count': import_count,
                    'earliest_data': date_range[0] if date_range else None,
                    'latest_data': date_range[1] if date_range else None,
                    'table_auto_detected': table_name is None
                }
                
        except Exception as e:
            print(f"❌ Failed to get statistics: {e}")
            return {
                'error': str(e),
                'table_name': f"{self.schema}.{table_name}" if table_name else f"{self.schema}.unknown",
                'total_trees': 0,
                'with_coordinates': 0,
                'without_coordinates': 0
            }
            
# Simple wrapper function for easy use
def get_trees(output_path: str = "proofsafe_trees.csv",
              project_ids: Optional[Union[int, List[int]]] = None,
              health: Optional[str] = None,
              start_date: Optional[str] = None,
              end_date: Optional[str] = None,
              include_images: bool = False,
              debug: bool = True,
              sample_size: Optional[int] = None) -> pd.DataFrame:
    """
    Simple function to get ALL trees from ProofSafe
    """
    api = ProofSafeTreeAPI()
    
    return api.get_trees(
        output_path=output_path,
        project_ids=project_ids,
        health=health,
        start_date=start_date,
        end_date=end_date,
        include_images=include_images,
        debug=debug,
        sample_size=sample_size
    )


def store_in_db(db_config: Optional[Dict] = None,
               project_ids: Optional[Union[int, List[int]]] = None,
               health: Optional[str] = None,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               table_name: str = 'trees',
               if_exists: str = 'append',
               debug: bool = True,
               sample_size: Optional[int] = None,
               csv_backup: Optional[str] = None,
               import_notes: Optional[str] = None) -> Tuple[int, int]:
    """
    Convenience function to fetch and store trees in database
    """
    geo_db = ProofSafeGeoDB(db_config=db_config, create_db=True)
    
    return geo_db.fetch_and_store(
        table_name=table_name,
        if_exists=if_exists,
        project_ids=project_ids,
        health=health,
        start_date=start_date,
        end_date=end_date,
        debug=debug,
        sample_size=sample_size,
        csv_backup=csv_backup,
        import_notes=import_notes
    )


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Extract tree data from ProofSafe and store in PostGIS")
    parser.add_argument("--output", "-o", default="proofsafe_trees.csv", 
                       help="Output CSV file path (default: proofsafe_trees.csv)")
    parser.add_argument("--project", "-p", help="Project ID(s) - single ID or comma-separated")
    parser.add_argument("--health", choices=["excellent", "good", "fair", "poor", "dead"],
                       help="Filter by health status")
    parser.add_argument("--status", type=int, choices=[0, 1, 2],
                       help="Filter by status (0=No Action, 1=Action Required, 2=Removed)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--date-field", default="Created_At",
                       choices=["Created_At", "DOB", "Modified_At", "Check_Out_Date"],
                       help="Date field to filter on (default: Created_At)")
    parser.add_argument("--images", action="store_true", help="Include image links")
    parser.add_argument("--events", action="store_true", help="Include events/comments")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress debug output")
    parser.add_argument("--list-projects", action="store_true", help="List available projects")
    parser.add_argument("--sample", type=int, help="Only fetch SAMPLE records (for testing)")
    parser.add_argument("--page-size", type=int, default=1000, help="Records per page (max 1000)")
    
    # Database arguments
    parser.add_argument("--db-host", default="localhost", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="trees_db", help="Database name")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-pass", default="postgres", help="Database password")
    parser.add_argument("--store", action="store_true", help="Store in database instead of CSV")
    parser.add_argument("--table", default="trees", help="Database table name (default: trees)")
    parser.add_argument("--create-table", action="store_true", help="Create database table")
    parser.add_argument("--drop-table", action="store_true", help="Drop and recreate table")
    parser.add_argument("--csv-backup", help="Save CSV backup when storing in database")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    
    # Import tracking arguments (NEW)
    parser.add_argument("--import-notes", help="Notes for this import (will be stored in metadata)")
    parser.add_argument("--import-history", action="store_true", help="Show import history")
    parser.add_argument("--show-import", help="Show trees from a specific import ID")
    
    args = parser.parse_args()
    
    debug = not args.quiet
    
    # Database configuration
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_pass
    }
    
    # Initialize API client
    if args.store or args.create_table or args.stats or args.import_history or args.show_import:
        api = ProofSafeGeoDB(db_config=db_config, create_db=True)
    else:
        api = ProofSafeTreeAPI()
    
    if args.list_projects:
        print("\n📋 Fetching projects...")
        projects = api.get_projects(debug=debug)
        if not projects.empty:
            print("\nAvailable Projects:")
            for _, row in projects.iterrows():
                print(f"  {row['id']}: {row['name']}")
        else:
            print("❌ No projects found")
        sys.exit(0)
    
    # Show import history (NEW)
    if args.import_history and isinstance(api, ProofSafeGeoDB):
        print("\n📋 Import History:")
        history = api.get_import_history(limit=20)
        if not history.empty:
            # Format for display
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(history.to_string(index=False))
        else:
            print("No import history found")
        sys.exit(0)
    
    # Show trees from specific import (NEW)
    if args.show_import and isinstance(api, ProofSafeGeoDB):
        print(f"\n🌳 Trees from import {args.show_import}:")
        trees = api.get_trees_by_import(args.show_import, limit=50)
        if not trees.empty:
            pd.set_option('display.max_columns', None)
            print(trees.to_string(index=False))
            print(f"\nTotal: {len(trees)} trees")
        else:
            print(f"No trees found for import ID: {args.show_import}")
        sys.exit(0)
    
    # Create table if requested
    if args.create_table or args.drop_table:
        if isinstance(api, ProofSafeGeoDB):
            if api.create_trees_table(drop_existing=args.drop_table):
                print("✅ Table ready")
            else:
                print("❌ Failed to create table")
        else:
            print("❌ Database operations require --store flag")
        if not args.store:
            sys.exit(0)
    
    # Show statistics if requested
    if args.stats:
        if isinstance(api, ProofSafeGeoDB):
            stats = api.get_tree_count()
            if stats:
                print("\n📊 Database Statistics:")
                print(f"   Total trees: {stats.get('total_trees', 0):,}")
                print(f"   With coordinates: {stats.get('with_coordinates', 0):,}")
                print(f"   Without coordinates: {stats.get('without_coordinates', 0):,}")
                print(f"   Unique projects: {stats.get('unique_projects', 0)}")
                print(f"   Number of imports: {stats.get('import_count', 0)}")
                print(f"   Earliest data: {stats.get('earliest_data')}")
                print(f"   Latest data: {stats.get('latest_data')}")
                
                if stats.get('health_distribution'):
                    print("\n   Health Distribution:")
                    for health, count in stats['health_distribution'].items():
                        print(f"     {health}: {count}")
                
                if stats.get('top_species'):
                    print("\n   Top Species:")
                    for species, count in list(stats['top_species'].items())[:5]:
                        print(f"     {species}: {count}")
        else:
            print("❌ Statistics require database connection (--store flag)")
        sys.exit(0)
    
    # Parse project IDs
    project_ids = None
    if args.project:
        if ',' in args.project:
            project_ids = [int(x.strip()) for x in args.project.split(',')]
        else:
            project_ids = int(args.project)
    
    print("="*60)
    print("🌳 PROOFSAFE TREE DATA EXTRACTOR WITH POSTGIS AND IMPORT TRACKING")
    print("="*60)
    
    if args.store:
        # Store in database
        print(f"📁 Storing in database: {args.db_name} on {args.db_host}")
        
        records_fetched, records_stored = api.fetch_and_store(
            table_name=args.table,
            if_exists='replace' if args.drop_table else 'append',
            project_ids=project_ids,
            health=args.health,
            status=args.status,
            start_date=args.start,
            end_date=args.end,
            date_field=args.date_field,
            page_size=args.page_size,
            include_events=args.events,
            include_images=args.images,
            debug=debug,
            sample_size=args.sample,
            csv_backup=args.csv_backup,
            import_notes=args.import_notes
        )
        
        if records_fetched > 0:
            print(f"\n{'='*60}")
            print(f"✅ SUCCESS! Fetched {records_fetched:,} trees, stored {records_stored:,}")
            if records_stored < records_fetched:
                print(f"⚠️  {records_fetched - records_stored} records could not be stored")
            
            # Show final stats
            stats = api.get_tree_count()
            if stats:
                print(f"\n📊 Database now has {stats.get('total_trees', 0):,} total trees")
                print(f"   Latest import ID: {datetime.now().strftime('%Y%m%d_%H%M%S')}")
        else:
            print("\n❌ No trees found")
    
    else:
        # Save to CSV only
        df = api.get_trees(
            output_path=args.output,
            project_ids=project_ids,
            health=args.health,
            status=args.status,
            start_date=args.start,
            end_date=args.end,
            date_field=args.date_field,
            page_size=args.page_size,
            include_events=args.events,
            include_images=args.images,
            debug=debug,
            sample_size=args.sample
        )
        
        if not df.empty:
            print(f"\n{'='*60}")
            print(f"✅ SUCCESS! Found {len(df):,} trees")
            print(f"📁 Saved to: {os.path.abspath(args.output)}")
