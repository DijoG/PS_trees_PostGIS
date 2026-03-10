#!/usr/bin/env python3
"""
ProofSafe Tree Data Extractor
Official API implementation with FULL pagination support
"""

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json
import os
import time
from datetime import datetime
from typing import Optional, Union, List, Dict, Any, Generator

class ProofSafeTreeAPI:
    """Official ProofSafe API client for tree data with full pagination"""
    
    def __init__(self, base_url: str = "https://proofsafe-portalapi.tmo-gr.com", 
                 username: str = "ksa2@proofsafe.com.au", 
                 password: str = "Test#1234"):
        """
        Initialize the ProofSafe API client
        
        Args:
            base_url: API base URL (defaults to production)
            username: Your API username
            password: Your API password
        """
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
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
    
    Args:
        output_path: Where to save the CSV file
        project_ids: Project ID(s) to filter by
        health: Health status ("good", "fair", "poor", "dead")
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        include_images: Include image links
        debug: Print debug info
        sample_size: If provided, only fetch this many records (for testing)
    
    Returns:
        DataFrame with ALL tree data
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


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Extract ALL tree data from ProofSafe")
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
    
    args = parser.parse_args()
    
    api = ProofSafeTreeAPI()
    debug = not args.quiet
    
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
    
    # Parse project IDs
    project_ids = None
    if args.project:
        if ',' in args.project:
            project_ids = [int(x.strip()) for x in args.project.split(',')]
        else:
            project_ids = int(args.project)
    
    print("="*60)
    print("🌳 PROOFSAFE TREE DATA EXTRACTOR")
    print("="*60)
    
    # Get ALL trees with automatic pagination
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
        
        if debug and 'Health' in df.columns:
            print(f"\n📊 Health Summary ({len(df['Health'].value_counts())} categories):")
            print(df['Health'].value_counts().head(10))
            if len(df['Health'].value_counts()) > 10:
                print(f"   ... and {len(df['Health'].value_counts()) - 10} more")
        
        if debug and 'Project' in df.columns:
            print(f"\n📊 Project Summary ({len(df['Project'].value_counts())} projects):")
            print(df['Project'].value_counts().head(10))
            if len(df['Project'].value_counts()) > 10:
                print(f"   ... and {len(df['Project'].value_counts()) - 10} more")
    else:
        print("\n❌ No trees found")
