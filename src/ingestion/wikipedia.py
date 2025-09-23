"""Wikipedia pageviews data connector."""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class WikipediaPageviewsConnector:
    """Connector for Wikipedia pageviews data."""
    
    def __init__(self):
        """Initialize Wikipedia pageviews connector."""
        self.base_url = "https://wikimedia.org/api/rest_v1"
        self.session = self._create_session()
        self.rate_limit_delay = 0.1  # 100ms between requests
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
    
    def fetch_article_pageviews(
        self,
        article: str,
        start: str,
        end: str,
        project: str = "en.wikipedia",
        access: str = "all-access",
        agent: str = "user",
        granularity: str = "daily",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch pageview data for a specific article.
        
        Args:
            article: Article title (URL-encoded)
            start: Start date (YYYYMMDD)
            end: End date (YYYYMMDD)
            project: Wiki project (e.g., en.wikipedia, de.wikipedia)
            access: Access type (all-access, desktop, mobile-web, mobile-app)
            agent: Agent type (all-agents, user, spider, bot)
            granularity: Data granularity (daily, monthly)
        
        Returns:
            DataFrame with pageview data or None if error
        """
        try:
            self._rate_limit()
            
            # Build URL
            url = (
                f"{self.base_url}/metrics/pageviews/per-article/"
                f"{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"
            )
            
            # Make request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "items" in data and data["items"]:
                # Convert to DataFrame
                df = pd.DataFrame(data["items"])
                
                # Parse timestamp
                df["date"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H")
                
                # Add metadata
                df["fetch_time"] = datetime.utcnow()
                df["article_title"] = article
                df["wiki_project"] = project
                
                # Select and rename columns
                df = df[["date", "views", "article_title", "wiki_project", "fetch_time"]]
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Wikipedia pageviews: {e}")
            return None
    
    def fetch_top_articles(
        self,
        year: int,
        month: int,
        day: int,
        project: str = "en.wikipedia",
        access: str = "all-access",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch top articles for a specific day.
        
        Args:
            year: Year
            month: Month
            day: Day
            project: Wiki project
            access: Access type
        
        Returns:
            DataFrame with top articles or None if error
        """
        try:
            self._rate_limit()
            
            # Build URL
            url = (
                f"{self.base_url}/metrics/pageviews/top/"
                f"{project}/{access}/{year}/{month:02d}/{day:02d}"
            )
            
            # Make request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "items" in data and data["items"]:
                articles = data["items"][0].get("articles", [])
                if articles:
                    df = pd.DataFrame(articles)
                    
                    # Add metadata
                    df["date"] = pd.Timestamp(year=year, month=month, day=day)
                    df["fetch_time"] = datetime.utcnow()
                    df["wiki_project"] = project
                    
                    return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching top articles: {e}")
            return None
    
    def fetch_aggregate_pageviews(
        self,
        start: str,
        end: str,
        project: str = "en.wikipedia",
        access: str = "all-access",
        agent: str = "user",
        granularity: str = "daily",
    ) -> Optional[pd.DataFrame]:
        """
        Fetch aggregate pageview data for a project.
        
        Args:
            start: Start date (YYYYMMDD)
            end: End date (YYYYMMDD)
            project: Wiki project
            access: Access type
            agent: Agent type
            granularity: Data granularity
        
        Returns:
            DataFrame with aggregate pageviews or None if error
        """
        try:
            self._rate_limit()
            
            # Build URL
            url = (
                f"{self.base_url}/metrics/pageviews/aggregate/"
                f"{project}/{access}/{agent}/{granularity}/{start}/{end}"
            )
            
            # Make request
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if "items" in data and data["items"]:
                df = pd.DataFrame(data["items"])
                
                # Parse timestamp
                df["date"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H")
                
                # Add metadata
                df["fetch_time"] = datetime.utcnow()
                df["wiki_project"] = project
                
                # Select columns
                df = df[["date", "views", "wiki_project", "fetch_time"]]
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching aggregate pageviews: {e}")
            return None
    
    def fetch_multiple_articles(
        self,
        articles: List[str],
        start: str,
        end: str,
        project: str = "en.wikipedia",
    ) -> pd.DataFrame:
        """
        Fetch pageviews for multiple articles.
        
        Args:
            articles: List of article titles
            start: Start date (YYYYMMDD)
            end: End date (YYYYMMDD)
            project: Wiki project
        
        Returns:
            Combined DataFrame with all article pageviews
        """
        all_data = []
        
        for article in articles:
            # URL-encode article title
            article_encoded = article.replace(" ", "_")
            
            data = self.fetch_article_pageviews(
                article=article_encoded,
                start=start,
                end=end,
                project=project,
            )
            
            if data is not None and not data.empty:
                all_data.append(data)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
