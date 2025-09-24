"""USDA NASS Quick Stats API connector for US agricultural data."""

import time
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class USDANASSConnector:
    """Connector for USDA NASS Quick Stats API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize USDA NASS connector.
        
        Args:
            api_key: USDA NASS API key (required for data access)
        """
        self.base_url = "https://quickstats.nass.usda.gov/api"
        self.api_key = api_key
        self.session = self._create_session()
        self.rate_limit_delay = 2.0  # 2 seconds between requests (conservative)
        self.last_request_time = 0
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic."""
        session = requests.Session()
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "NewsDashboard/0.1 (https://github.com/stone-ericm/news_dashboard)"
        })
        return session
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Make API request with error handling."""
        if not self.api_key:
            print("Error: USDA NASS API key is required")
            return None
        
        try:
            self._rate_limit()
            
            # Add API key to parameters
            params['key'] = self.api_key
            params['format'] = 'JSON'
            
            url = f"{self.base_url}/{endpoint}"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error making USDA NASS request: {e}")
            return None
    
    def get_param_values(self, param: str, **filters) -> Optional[List[str]]:
        """
        Get possible values for a parameter.
        
        Args:
            param: Parameter name (e.g., 'commodity_desc', 'state_name')
            **filters: Additional filters to narrow down values
        
        Returns:
            List of possible values or None if error
        """
        params = {'param': param}
        params.update(filters)
        
        data = self._make_request('get_param_values', params)
        if data and param in data:
            return data[param]
        return None
    
    def get_counts(self, **filters) -> Optional[int]:
        """
        Get count of records matching filters.
        
        Args:
            **filters: Query filters
        
        Returns:
            Number of matching records or None if error
        """
        data = self._make_request('get_counts', filters)
        if data and 'count' in data:
            return int(data['count'])
        return None
    
    def fetch_data(self, **filters) -> Optional[pd.DataFrame]:
        """
        Fetch data from USDA NASS.
        
        Args:
            **filters: Query filters (commodity_desc, state_name, year, etc.)
        
        Returns:
            DataFrame with USDA NASS data or None if error
        """
        data = self._make_request('api_GET', filters)
        
        if data and 'data' in data and data['data']:
            df = pd.DataFrame(data['data'])
            
            # Add metadata
            df['fetch_time'] = datetime.utcnow()
            
            # Convert numeric columns
            if 'Value' in df.columns:
                # Remove commas and convert to numeric
                df['Value'] = df['Value'].astype(str).str.replace(',', '')
                df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
            
            if 'year' in df.columns:
                df['year'] = pd.to_numeric(df['year'], errors='coerce')
            
            return df
        
        return None
    
    def fetch_crop_production(
        self,
        commodities: List[str] = None,
        states: List[str] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch crop production data.
        
        Args:
            commodities: List of commodity names (default: major crops)
            states: List of state names (default: top producing states)
            years: List of years (default: last 5 years)
        
        Returns:
            DataFrame with crop production data
        """
        # Default commodities
        if commodities is None:
            commodities = ['CORN', 'SOYBEANS', 'WHEAT', 'COTTON', 'RICE']
        
        # Default states (top agricultural producers)
        if states is None:
            states = ['IOWA', 'ILLINOIS', 'NEBRASKA', 'MINNESOTA', 'INDIANA']
        
        # Default years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        filters = {
            'source_desc': 'SURVEY',
            'sector_desc': 'CROPS',
            'group_desc': 'FIELD CROPS',
            'statisticcat_desc': 'PRODUCTION',
            'unit_desc': 'BU',  # Bushels
            'domain_desc': 'TOTAL',
            'agg_level_desc': 'STATE',
            'commodity_desc': ','.join(commodities),
            'state_name': ','.join(states),
            'year': ','.join(map(str, years)),
        }
        
        return self.fetch_data(**filters)
    
    def fetch_crop_yields(
        self,
        commodities: List[str] = None,
        states: List[str] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch crop yield data.
        
        Args:
            commodities: List of commodity names
            states: List of state names
            years: List of years
        
        Returns:
            DataFrame with crop yield data
        """
        # Default commodities
        if commodities is None:
            commodities = ['CORN', 'SOYBEANS', 'WHEAT', 'COTTON', 'RICE']
        
        # Default states
        if states is None:
            states = ['IOWA', 'ILLINOIS', 'NEBRASKA', 'MINNESOTA', 'INDIANA']
        
        # Default years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        filters = {
            'source_desc': 'SURVEY',
            'sector_desc': 'CROPS',
            'group_desc': 'FIELD CROPS',
            'statisticcat_desc': 'YIELD',
            'unit_desc': 'BU / ACRE',  # Bushels per acre
            'domain_desc': 'TOTAL',
            'agg_level_desc': 'STATE',
            'commodity_desc': ','.join(commodities),
            'state_name': ','.join(states),
            'year': ','.join(map(str, years)),
        }
        
        return self.fetch_data(**filters)
    
    def fetch_livestock_inventory(
        self,
        commodities: List[str] = None,
        states: List[str] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch livestock inventory data.
        
        Args:
            commodities: List of livestock types
            states: List of state names
            years: List of years
        
        Returns:
            DataFrame with livestock inventory data
        """
        # Default livestock
        if commodities is None:
            commodities = ['CATTLE', 'HOGS', 'SHEEP', 'CHICKENS']
        
        # Default states
        if states is None:
            states = ['TEXAS', 'NEBRASKA', 'KANSAS', 'CALIFORNIA', 'IOWA']
        
        # Default years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        filters = {
            'source_desc': 'SURVEY',
            'sector_desc': 'ANIMALS & PRODUCTS',
            'statisticcat_desc': 'INVENTORY',
            'unit_desc': 'HEAD',
            'domain_desc': 'TOTAL',
            'agg_level_desc': 'STATE',
            'commodity_desc': ','.join(commodities),
            'state_name': ','.join(states),
            'year': ','.join(map(str, years)),
        }
        
        return self.fetch_data(**filters)
    
    def fetch_agricultural_prices(
        self,
        commodities: List[str] = None,
        states: List[str] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch agricultural price data.
        
        Args:
            commodities: List of commodity names
            states: List of state names
            years: List of years
        
        Returns:
            DataFrame with price data
        """
        # Default commodities
        if commodities is None:
            commodities = ['CORN', 'SOYBEANS', 'WHEAT', 'COTTON']
        
        # Default to US total
        if states is None:
            states = ['US TOTAL']
        
        # Default years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        filters = {
            'source_desc': 'SURVEY',
            'sector_desc': 'CROPS',
            'group_desc': 'FIELD CROPS',
            'statisticcat_desc': 'PRICE RECEIVED',
            'domain_desc': 'TOTAL',
            'agg_level_desc': 'NATIONAL',
            'commodity_desc': ','.join(commodities),
            'state_name': ','.join(states),
            'year': ','.join(map(str, years)),
        }
        
        return self.fetch_data(**filters)
    
    def get_available_commodities(self, sector: str = 'CROPS') -> Optional[List[str]]:
        """Get list of available commodities for a sector."""
        return self.get_param_values('commodity_desc', sector_desc=sector)
    
    def get_available_states(self) -> Optional[List[str]]:
        """Get list of available states."""
        return self.get_param_values('state_name')
    
    def get_available_years(self) -> Optional[List[str]]:
        """Get list of available years."""
        return self.get_param_values('year')
