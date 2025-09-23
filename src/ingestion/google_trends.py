"""Google Trends data connector."""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from pytrends.request import TrendReq


class GoogleTrendsConnector:
    """Connector for Google Trends data."""
    
    def __init__(self, hl: str = "en-US", tz: int = 360):
        """
        Initialize Google Trends connector.
        
        Args:
            hl: Host language (default: en-US)
            tz: Timezone offset in minutes (default: 360 for CST)
        """
        self.pytrends = TrendReq(hl=hl, tz=tz, timeout=(10, 25), retries=2, backoff_factor=0.1)
        self.rate_limit_delay = 1.0  # Seconds between requests
        self.last_request_time = 0
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch_interest_over_time(
        self,
        keywords: List[str],
        timeframe: str = "today 3-m",
        geo: str = "",
        cat: int = 0,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch interest over time for keywords.
        
        Args:
            keywords: List of search terms (max 5)
            timeframe: Time range (e.g., "today 3-m", "2022-01-01 2022-12-31")
            geo: Geographic location (e.g., "US", "GB")
            cat: Category code (0 = all categories)
        
        Returns:
            DataFrame with interest scores or None if error
        """
        if not keywords or len(keywords) > 5:
            raise ValueError("Keywords must be 1-5 terms")
        
        try:
            self._rate_limit()
            
            # Build payload
            self.pytrends.build_payload(
                keywords,
                timeframe=timeframe,
                geo=geo,
                cat=cat,
            )
            
            # Get interest over time
            data = self.pytrends.interest_over_time()
            
            if data is not None and not data.empty:
                # Add metadata
                data["fetch_time"] = datetime.utcnow()
                data["geo"] = geo
                data["timeframe"] = timeframe
                
                # Reset index to make date a column
                data = data.reset_index()
                
                # Remove 'isPartial' column if present
                if "isPartial" in data.columns:
                    data = data.drop(columns=["isPartial"])
                
                return data
            
            return None
            
        except Exception as e:
            print(f"Error fetching Google Trends data: {e}")
            return None
    
    def fetch_interest_by_region(
        self,
        keywords: List[str],
        timeframe: str = "today 3-m",
        geo: str = "US",
        inc_low_vol: bool = True,
        resolution: str = "COUNTRY",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch interest by region for keywords.
        
        Args:
            keywords: List of search terms (max 5)
            timeframe: Time range
            geo: Geographic location
            inc_low_vol: Include low search volume regions
            resolution: Geographic resolution (COUNTRY, REGION, CITY, DMA)
        
        Returns:
            DataFrame with regional interest or None if error
        """
        try:
            self._rate_limit()
            
            # Build payload
            self.pytrends.build_payload(
                keywords,
                timeframe=timeframe,
                geo=geo,
            )
            
            # Get interest by region
            data = self.pytrends.interest_by_region(
                resolution=resolution,
                inc_low_vol=inc_low_vol,
            )
            
            if data is not None and not data.empty:
                data["fetch_time"] = datetime.utcnow()
                data["geo"] = geo
                data["timeframe"] = timeframe
                data = data.reset_index()
                return data
            
            return None
            
        except Exception as e:
            print(f"Error fetching regional data: {e}")
            return None
    
    def fetch_related_queries(
        self,
        keywords: List[str],
        timeframe: str = "today 3-m",
        geo: str = "",
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Fetch related queries for keywords.
        
        Args:
            keywords: List of search terms
            timeframe: Time range
            geo: Geographic location
        
        Returns:
            Dictionary of DataFrames with related queries
        """
        try:
            self._rate_limit()
            
            # Build payload
            self.pytrends.build_payload(
                keywords,
                timeframe=timeframe,
                geo=geo,
            )
            
            # Get related queries
            data = self.pytrends.related_queries()
            
            if data:
                # Add metadata to each DataFrame
                for keyword in data:
                    for query_type in ["top", "rising"]:
                        if data[keyword][query_type] is not None:
                            df = data[keyword][query_type]
                            df["fetch_time"] = datetime.utcnow()
                            df["keyword"] = keyword
                            df["query_type"] = query_type
            
            return data
            
        except Exception as e:
            print(f"Error fetching related queries: {e}")
            return None
    
    def fetch_trending_searches(self, geo: str = "US") -> Optional[pd.DataFrame]:
        """
        Fetch current trending searches.
        
        Args:
            geo: Geographic location (e.g., "US")
        
        Returns:
            DataFrame with trending searches or None if error
        """
        try:
            self._rate_limit()
            
            # Get trending searches
            data = self.pytrends.trending_searches(pn=geo.lower())
            
            if data is not None and not data.empty:
                data["fetch_time"] = datetime.utcnow()
                data["geo"] = geo
                return data
            
            return None
            
        except Exception as e:
            print(f"Error fetching trending searches: {e}")
            return None
