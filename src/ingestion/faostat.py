"""FAOSTAT data connector for agricultural statistics."""

import time
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class FAOSTATConnector:
    """Connector for FAOSTAT agricultural data."""
    
    def __init__(self):
        """Initialize FAOSTAT connector."""
        self.base_url = "https://fenixservices.fao.org/faostat/api/v1/en"
        self.session = self._create_session()
        self.rate_limit_delay = 1.0  # 1 second between requests
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
    
    def get_dimensions(self) -> Optional[Dict]:
        """Get available dimensions (domains, elements, items, areas)."""
        try:
            self._rate_limit()
            url = f"{self.base_url}/dimensions"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching FAOSTAT dimensions: {e}")
            return None
    
    def get_codes(self, dimension: str) -> Optional[List[Dict]]:
        """
        Get codes for a specific dimension.
        
        Args:
            dimension: Dimension name (e.g., 'areas', 'elements', 'items')
        
        Returns:
            List of code dictionaries or None if error
        """
        try:
            self._rate_limit()
            url = f"{self.base_url}/codes/{dimension}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data.get('data', [])
        except requests.exceptions.RequestException as e:
            print(f"Error fetching FAOSTAT codes for {dimension}: {e}")
            return None
    
    def fetch_data(
        self,
        domain_code: str,
        area_codes: List[Union[str, int]] = None,
        element_codes: List[Union[str, int]] = None,
        item_codes: List[Union[str, int]] = None,
        year_codes: List[Union[str, int]] = None,
        output_type: str = "objects",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch data from FAOSTAT.
        
        Args:
            domain_code: Domain code (e.g., 'QCL' for crops and livestock)
            area_codes: List of area codes (countries/regions)
            element_codes: List of element codes (what to measure)
            item_codes: List of item codes (what items)
            year_codes: List of year codes
            output_type: Output format ('objects' or 'csv')
        
        Returns:
            DataFrame with FAOSTAT data or None if error
        """
        try:
            self._rate_limit()
            
            # Build query parameters
            params = {
                'domain_code': domain_code,
                'output_type': output_type,
            }
            
            if area_codes:
                params['area_codes'] = ','.join(map(str, area_codes))
            if element_codes:
                params['element_codes'] = ','.join(map(str, element_codes))
            if item_codes:
                params['item_codes'] = ','.join(map(str, item_codes))
            if year_codes:
                params['year_codes'] = ','.join(map(str, year_codes))
            
            url = f"{self.base_url}/data"
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                
                # Add metadata
                df['fetch_time'] = datetime.utcnow()
                df['domain_code'] = domain_code
                
                # Convert numeric columns
                numeric_cols = ['Value', 'Year']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching FAOSTAT data: {e}")
            return None
    
    def fetch_crop_production(
        self,
        area_codes: List[Union[str, int]] = None,
        item_codes: List[Union[str, int]] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch crop production data.
        
        Args:
            area_codes: Country/region codes (default: major producers)
            item_codes: Crop codes (default: major crops)
            years: Years to fetch (default: last 5 years)
        
        Returns:
            DataFrame with crop production data
        """
        # Default to major crop producing countries
        if area_codes is None:
            area_codes = [231, 156, 840, 76, 356]  # USA, China, India, Brazil, Argentina
        
        # Default to major crops
        if item_codes is None:
            item_codes = [15, 27, 56, 71, 83]  # Wheat, Rice, Maize, Barley, Soybeans
        
        # Default to last 5 years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        # Element code 5312 = Production (tonnes)
        return self.fetch_data(
            domain_code='QCL',  # Crops and livestock products
            area_codes=area_codes,
            element_codes=[5312],  # Production
            item_codes=item_codes,
            year_codes=years,
        )
    
    def fetch_crop_yields(
        self,
        area_codes: List[Union[str, int]] = None,
        item_codes: List[Union[str, int]] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch crop yield data.
        
        Args:
            area_codes: Country/region codes
            item_codes: Crop codes
            years: Years to fetch
        
        Returns:
            DataFrame with crop yield data
        """
        # Default to major crop producing countries
        if area_codes is None:
            area_codes = [231, 156, 840, 76, 356]  # USA, China, India, Brazil, Argentina
        
        # Default to major crops
        if item_codes is None:
            item_codes = [15, 27, 56, 71, 83]  # Wheat, Rice, Maize, Barley, Soybeans
        
        # Default to last 5 years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        # Element code 5419 = Yield (hg/ha)
        return self.fetch_data(
            domain_code='QCL',  # Crops and livestock products
            area_codes=area_codes,
            element_codes=[5419],  # Yield
            item_codes=item_codes,
            year_codes=years,
        )
    
    def fetch_food_security_indicators(
        self,
        area_codes: List[Union[str, int]] = None,
        years: List[int] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch food security indicators.
        
        Args:
            area_codes: Country/region codes
            years: Years to fetch
        
        Returns:
            DataFrame with food security data
        """
        # Default to all countries
        if area_codes is None:
            area_codes = [5000]  # World + (Total)
        
        # Default to last 5 years
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        # Food security domain
        return self.fetch_data(
            domain_code='FS',  # Food Security
            area_codes=area_codes,
            year_codes=years,
        )
    
    def get_country_codes(self) -> Optional[Dict[str, int]]:
        """Get mapping of country names to codes."""
        areas = self.get_codes('areas')
        if areas:
            return {area['Area']: area['Area Code'] for area in areas}
        return None
    
    def get_crop_codes(self) -> Optional[Dict[str, int]]:
        """Get mapping of crop names to codes."""
        items = self.get_codes('items')
        if items:
            # Filter for crops (exclude livestock items)
            crop_keywords = ['wheat', 'rice', 'maize', 'corn', 'barley', 'oats', 
                           'soybean', 'cotton', 'sugar', 'potato', 'tomato']
            crops = {}
            for item in items:
                item_name = item.get('Item', '').lower()
                if any(keyword in item_name for keyword in crop_keywords):
                    crops[item['Item']] = item['Item Code']
            return crops
        return None
