"""OpenSky Network API connector for aviation data."""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class OpenSkyConnector:
    """Connector for OpenSky Network aviation data."""
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        """
        Initialize OpenSky Network connector.
        
        Args:
            username: OpenSky username (optional, for higher rate limits)
            password: OpenSky password (optional, for higher rate limits)
        """
        self.base_url = "https://opensky-network.org/api"
        self.username = username
        self.password = password
        self.session = self._create_session()
        
        # Rate limits: anonymous users get 10 requests per minute
        # Registered users get 400 requests per minute
        self.rate_limit_delay = 0.15 if username else 6.0  # Conservative limits
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
        
        # Add authentication if provided
        if self.username and self.password:
            session.auth = (self.username, self.password)
        
        return session
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch_states(
        self,
        icao24: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        extended: bool = False,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch current aircraft states.
        
        Args:
            icao24: Specific aircraft ICAO24 address
            bbox: Bounding box (min_lat, max_lat, min_lon, max_lon)
            extended: Include additional state vectors
        
        Returns:
            DataFrame with aircraft states or None if error
        """
        try:
            self._rate_limit()
            
            params = {}
            if icao24:
                params['icao24'] = icao24
            if bbox:
                params['lamin'], params['lamax'], params['lomin'], params['lomax'] = bbox
            if extended:
                params['extended'] = '1'
            
            url = f"{self.base_url}/states/all"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'states' in data and data['states']:
                # Column names for state vectors
                columns = [
                    'icao24', 'callsign', 'origin_country', 'time_position',
                    'last_contact', 'longitude', 'latitude', 'baro_altitude',
                    'on_ground', 'velocity', 'true_track', 'vertical_rate',
                    'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source'
                ]
                
                df = pd.DataFrame(data['states'], columns=columns)
                
                # Add metadata
                df['fetch_time'] = datetime.utcnow()
                df['data_time'] = pd.to_datetime(data.get('time', 0), unit='s')
                
                # Clean up data types
                numeric_cols = [
                    'time_position', 'last_contact', 'longitude', 'latitude',
                    'baro_altitude', 'velocity', 'true_track', 'vertical_rate',
                    'geo_altitude'
                ]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Convert timestamps
                df['time_position'] = pd.to_datetime(df['time_position'], unit='s', errors='coerce')
                df['last_contact'] = pd.to_datetime(df['last_contact'], unit='s', errors='coerce')
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenSky states: {e}")
            return None
    
    def fetch_flights(
        self,
        begin: datetime,
        end: datetime,
        icao24: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch flight data for a time interval.
        
        Args:
            begin: Start time
            end: End time (max 7 days from begin)
            icao24: Specific aircraft ICAO24 address
        
        Returns:
            DataFrame with flight data or None if error
        """
        try:
            self._rate_limit()
            
            # Convert to Unix timestamps
            begin_ts = int(begin.timestamp())
            end_ts = int(end.timestamp())
            
            params = {
                'begin': begin_ts,
                'end': end_ts,
            }
            if icao24:
                params['icao24'] = icao24
            
            url = f"{self.base_url}/flights/all"
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                
                # Add metadata
                df['fetch_time'] = datetime.utcnow()
                
                # Convert timestamps
                timestamp_cols = ['firstSeen', 'lastSeen', 'departureTime', 'arrivalTime']
                for col in timestamp_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenSky flights: {e}")
            return None
    
    def fetch_arrivals(
        self,
        airport: str,
        begin: datetime,
        end: datetime,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch arrivals for an airport.
        
        Args:
            airport: ICAO airport code (e.g., 'KJFK')
            begin: Start time
            end: End time (max 7 days from begin)
        
        Returns:
            DataFrame with arrival data or None if error
        """
        try:
            self._rate_limit()
            
            begin_ts = int(begin.timestamp())
            end_ts = int(end.timestamp())
            
            params = {
                'airport': airport,
                'begin': begin_ts,
                'end': end_ts,
            }
            
            url = f"{self.base_url}/flights/arrival"
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                df['fetch_time'] = datetime.utcnow()
                df['airport'] = airport
                df['flight_type'] = 'arrival'
                
                # Convert timestamps
                timestamp_cols = ['firstSeen', 'lastSeen', 'departureTime', 'arrivalTime']
                for col in timestamp_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenSky arrivals for {airport}: {e}")
            return None
    
    def fetch_departures(
        self,
        airport: str,
        begin: datetime,
        end: datetime,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch departures for an airport.
        
        Args:
            airport: ICAO airport code (e.g., 'KJFK')
            begin: Start time
            end: End time (max 7 days from begin)
        
        Returns:
            DataFrame with departure data or None if error
        """
        try:
            self._rate_limit()
            
            begin_ts = int(begin.timestamp())
            end_ts = int(end.timestamp())
            
            params = {
                'airport': airport,
                'begin': begin_ts,
                'end': end_ts,
            }
            
            url = f"{self.base_url}/flights/departure"
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data:
                df = pd.DataFrame(data)
                df['fetch_time'] = datetime.utcnow()
                df['airport'] = airport
                df['flight_type'] = 'departure'
                
                # Convert timestamps
                timestamp_cols = ['firstSeen', 'lastSeen', 'departureTime', 'arrivalTime']
                for col in timestamp_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], unit='s', errors='coerce')
                
                return df
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching OpenSky departures for {airport}: {e}")
            return None
    
    def fetch_airport_activity(
        self,
        airports: List[str],
        days_back: int = 7,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch activity data for multiple airports.
        
        Args:
            airports: List of ICAO airport codes
            days_back: Number of days to look back
        
        Returns:
            Combined DataFrame with airport activity data
        """
        end_time = datetime.utcnow()
        begin_time = end_time - timedelta(days=days_back)
        
        all_data = []
        
        for airport in airports:
            # Fetch arrivals
            arrivals = self.fetch_arrivals(airport, begin_time, end_time)
            if arrivals is not None and not arrivals.empty:
                all_data.append(arrivals)
            
            # Fetch departures
            departures = self.fetch_departures(airport, begin_time, end_time)
            if departures is not None and not departures.empty:
                all_data.append(departures)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_flight_counts_by_region(
        self,
        bbox: Tuple[float, float, float, float],
    ) -> Optional[Dict]:
        """
        Get current flight counts in a bounding box.
        
        Args:
            bbox: Bounding box (min_lat, max_lat, min_lon, max_lon)
        
        Returns:
            Dictionary with flight statistics
        """
        states = self.fetch_states(bbox=bbox)
        
        if states is not None and not states.empty:
            stats = {
                'total_flights': len(states),
                'on_ground': len(states[states['on_ground'] == True]),
                'in_air': len(states[states['on_ground'] == False]),
                'countries': states['origin_country'].nunique(),
                'avg_altitude': states['baro_altitude'].mean(),
                'avg_velocity': states['velocity'].mean(),
                'timestamp': datetime.utcnow(),
            }
            return stats
        
        return None
    
    def get_major_airports(self) -> List[str]:
        """Get list of major airport ICAO codes."""
        return [
            'KJFK',  # JFK New York
            'KLAX',  # LAX Los Angeles
            'KORD',  # ORD Chicago
            'KATL',  # ATL Atlanta
            'KDFW',  # DFW Dallas
            'KDEN',  # DEN Denver
            'KSFO',  # SFO San Francisco
            'KLAS',  # LAS Las Vegas
            'KMIA',  # MIA Miami
            'KBOS',  # BOS Boston
            'EGLL',  # LHR London Heathrow
            'LFPG',  # CDG Paris Charles de Gaulle
            'EDDF',  # FRA Frankfurt
            'EHAM',  # AMS Amsterdam
            'LIRF',  # FCO Rome Fiumicino
        ]
