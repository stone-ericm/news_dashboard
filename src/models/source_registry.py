"""Source registry schema and management."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class DataStandard(str, Enum):
    """Data access standards."""
    
    SDMX = "sdmx"
    REST_JSON = "rest_json"
    REST_XML = "rest_xml"
    CSV_BULK = "csv_bulk"
    GRAPHQL = "graphql"
    STREAMING = "streaming"
    WEBSOCKET = "websocket"
    RSS = "rss"
    HTML_SCRAPE = "html_scrape"


class AuthMethod(str, Enum):
    """Authentication methods."""
    
    NONE = "none"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    BEARER = "bearer"
    CUSTOM = "custom"


class UpdateCadence(str, Enum):
    """Expected update frequency."""
    
    REALTIME = "realtime"  # < 1 minute
    MINUTES_5 = "5_minutes"
    MINUTES_15 = "15_minutes"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"
    IRREGULAR = "irregular"


class GeographicScope(str, Enum):
    """Geographic coverage level."""
    
    GLOBAL = "global"
    CONTINENTAL = "continental"
    REGIONAL = "regional"
    NATIONAL = "national"
    SUBNATIONAL = "subnational"
    LOCAL = "local"
    POINT = "point"


class HealthStatus(str, Enum):
    """Source health status."""
    
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    DOWN = "down"
    MAINTENANCE = "maintenance"
    UNKNOWN = "unknown"


class DataSource(BaseModel):
    """Registry entry for a data source."""
    
    # Identification
    source_id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Human-readable name")
    description: str = Field(..., description="What this source provides")
    category: str = Field(..., description="Category (e.g., social, economic, environmental)")
    
    # Access configuration
    standard: DataStandard = Field(..., description="Data access standard")
    base_url: HttpUrl = Field(..., description="Base API/data URL")
    dataset_ids: List[str] = Field(default_factory=list, description="Specific dataset IDs")
    endpoint_paths: Dict[str, str] = Field(default_factory=dict, description="Named endpoints")
    
    # Authentication
    auth_method: AuthMethod = Field(AuthMethod.NONE, description="Authentication method")
    auth_notes: Optional[str] = Field(None, description="Auth setup instructions")
    registration_required: bool = Field(False, description="Registration needed?")
    
    # Licensing
    license_name: Optional[str] = Field(None, description="License type")
    license_url: Optional[HttpUrl] = Field(None, description="License details URL")
    redistribution_permitted: bool = Field(True, description="Can redistribute?")
    tos_url: Optional[HttpUrl] = Field(None, description="Terms of service URL")
    
    # Cadence and lag
    cadence_expected: UpdateCadence = Field(..., description="Update frequency")
    typical_lag_days: float = Field(0, description="Typical publication delay in days")
    revision_policy: Optional[str] = Field(None, description="How revisions are handled")
    
    # Coverage
    geographic_scope: GeographicScope = Field(..., description="Geographic coverage")
    coverage_notes: Optional[str] = Field(None, description="Coverage details/limitations")
    units_primary: Optional[str] = Field(None, description="Primary measurement units")
    unit_variants: List[str] = Field(default_factory=list, description="Alternative units")
    
    # Rate limiting
    rate_limit_per_minute: Optional[int] = Field(None, description="Requests per minute")
    burst_limit: Optional[int] = Field(None, description="Burst request limit")
    backoff_policy: Optional[str] = Field(None, description="Backoff strategy")
    
    # Quality and reliability
    data_quality_flags: bool = Field(False, description="Quality indicators available?")
    historical_depth_years: Optional[float] = Field(None, description="Years of history")
    
    # Operational status
    health_status: HealthStatus = Field(HealthStatus.UNKNOWN, description="Current status")
    last_success_ts: Optional[datetime] = Field(None, description="Last successful fetch")
    last_failure_ts: Optional[datetime] = Field(None, description="Last failed fetch")
    failure_count: int = Field(0, description="Consecutive failure count")
    
    # Metadata
    provenance_notes: Optional[str] = Field(None, description="Data origin/methodology")
    contact_email: Optional[str] = Field(None, description="Support contact")
    contact_url: Optional[HttpUrl] = Field(None, description="Support URL")
    documentation_url: Optional[HttpUrl] = Field(None, description="API/data docs")
    
    # Internal
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = Field(True, description="Include in pipelines?")
    priority: int = Field(100, description="Processing priority (lower = higher)")
    tags: List[str] = Field(default_factory=list, description="Searchable tags")
    
    class Config:
        """Pydantic config."""
        
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            HttpUrl: str,
        }


class SourceRegistry:
    """Manages the source registry."""
    
    def __init__(self, storage_path: str = "data/source_registry.json"):
        """Initialize registry with storage path."""
        self.storage_path = storage_path
        self.sources: Dict[str, DataSource] = {}
        self.load()
    
    def add_source(self, source: DataSource) -> None:
        """Add or update a source."""
        source.updated_at = datetime.utcnow()
        self.sources[source.source_id] = source
        self.save()
    
    def get_source(self, source_id: str) -> Optional[DataSource]:
        """Get source by ID."""
        return self.sources.get(source_id)
    
    def list_sources(
        self,
        category: Optional[str] = None,
        is_active: bool = True,
        cadence: Optional[UpdateCadence] = None,
    ) -> List[DataSource]:
        """List sources with optional filters."""
        results = []
        for source in self.sources.values():
            if not is_active or source.is_active:
                if category and source.category != category:
                    continue
                if cadence and source.cadence_expected != cadence:
                    continue
                results.append(source)
        return sorted(results, key=lambda s: s.priority)
    
    def update_health(
        self,
        source_id: str,
        status: HealthStatus,
        is_success: bool = True,
    ) -> None:
        """Update source health status."""
        source = self.get_source(source_id)
        if source:
            source.health_status = status
            if is_success:
                source.last_success_ts = datetime.utcnow()
                source.failure_count = 0
            else:
                source.last_failure_ts = datetime.utcnow()
                source.failure_count += 1
            source.updated_at = datetime.utcnow()
            self.save()
    
    def save(self) -> None:
        """Save registry to disk."""
        import json
        from pathlib import Path
        
        Path(self.storage_path).parent.mkdir(parents=True, exist_ok=True)
        data = {
            source_id: source.model_dump(mode="json")
            for source_id, source in self.sources.items()
        }
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def load(self) -> None:
        """Load registry from disk."""
        import json
        from pathlib import Path
        
        if Path(self.storage_path).exists():
            with open(self.storage_path) as f:
                data = json.load(f)
                self.sources = {
                    source_id: DataSource(**source_data)
                    for source_id, source_data in data.items()
                }


# Predefined source configurations
PREDEFINED_SOURCES = [
    DataSource(
        source_id="google_trends",
        name="Google Trends",
        description="Search interest over time for topics and queries",
        category="attention",
        standard=DataStandard.REST_JSON,
        base_url="https://trends.google.com",
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.HOURLY,
        typical_lag_days=0,
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="Available by country/region; sampling varies by query volume",
        rate_limit_per_minute=10,
        backoff_policy="exponential",
        data_quality_flags=False,
        historical_depth_years=20,
        provenance_notes="Normalized search interest (0-100 scale)",
        documentation_url="https://support.google.com/trends",
        tags=["social", "attention", "search"],
    ),
    DataSource(
        source_id="wikipedia_pageviews",
        name="Wikipedia Pageviews",
        description="Article pageview counts across Wikipedia projects",
        category="attention",
        standard=DataStandard.REST_JSON,
        base_url="https://wikimedia.org/api/rest_v1",
        endpoint_paths={
            "pageviews": "/metrics/pageviews/per-article",
            "top": "/metrics/pageviews/top",
        },
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.HOURLY,
        typical_lag_days=0.04,  # ~1 hour
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="All Wikipedia languages and projects",
        rate_limit_per_minute=200,
        data_quality_flags=False,
        historical_depth_years=8,
        provenance_notes="Raw pageview counts, bot traffic filtered",
        documentation_url="https://wikitech.wikimedia.org/wiki/Analytics/AQS/Pageviews",
        tags=["attention", "encyclopedia", "multilingual"],
    ),
    DataSource(
        source_id="openaq",
        name="OpenAQ",
        description="Global air quality measurements",
        category="environmental",
        standard=DataStandard.REST_JSON,
        base_url="https://api.openaq.org/v2",
        endpoint_paths={
            "measurements": "/measurements",
            "locations": "/locations",
            "latest": "/latest",
        },
        auth_method=AuthMethod.API_KEY,
        auth_notes="Free API key via registration",
        registration_required=True,
        cadence_expected=UpdateCadence.HOURLY,
        typical_lag_days=0.04,
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="Coverage varies by region; best in urban areas",
        units_primary="µg/m³",
        unit_variants=["ppm", "ppb"],
        rate_limit_per_minute=100,
        data_quality_flags=True,
        historical_depth_years=10,
        provenance_notes="Aggregated from government and research stations",
        documentation_url="https://docs.openaq.org",
        tags=["air-quality", "pollution", "health"],
    ),
    DataSource(
        source_id="eurostat",
        name="Eurostat",
        description="European Union statistics",
        category="economic",
        standard=DataStandard.SDMX,
        base_url="https://ec.europa.eu/eurostat/api/dissemination",
        dataset_ids=["nama_10_gdp", "prc_hicp_midx", "une_rt_m"],
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.MONTHLY,
        typical_lag_days=45,
        revision_policy="Revisions marked; previous values preserved",
        geographic_scope=GeographicScope.REGIONAL,
        coverage_notes="EU member states + candidates",
        rate_limit_per_minute=30,
        data_quality_flags=True,
        historical_depth_years=30,
        license_name="CC BY 4.0",
        license_url="https://ec.europa.eu/eurostat/web/main/help/copyright-notice",
        redistribution_permitted=True,
        provenance_notes="Official EU statistics",
        documentation_url="https://wikis.ec.europa.eu/display/EUROSTATHELP/API",
        tags=["official", "europe", "sdmx"],
    ),
    # Agriculture Data Sources
    DataSource(
        source_id="faostat",
        name="FAOSTAT",
        description="UN Food and Agriculture Organization statistics",
        category="agriculture",
        standard=DataStandard.REST_JSON,
        base_url="https://fenixservices.fao.org/faostat/api/v1",
        endpoint_paths={
            "data": "/en/data",
            "dimensions": "/en/dimensions",
            "codes": "/en/codes",
        },
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.ANNUAL,
        typical_lag_days=180,  # 6 months lag for agricultural statistics
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="Global coverage, country-level data",
        units_primary="tonnes",
        unit_variants=["hectares", "tonnes/hectare", "USD"],
        rate_limit_per_minute=60,
        data_quality_flags=True,
        historical_depth_years=60,
        license_name="CC BY-NC-SA 3.0 IGO",
        license_url="https://www.fao.org/contact-us/terms/en/",
        redistribution_permitted=True,
        provenance_notes="Official UN agricultural statistics",
        documentation_url="https://fenixservices.fao.org/faostat/static/documents/api/",
        tags=["agriculture", "food", "crops", "livestock", "official"],
    ),
    DataSource(
        source_id="usda_nass",
        name="USDA NASS Quick Stats",
        description="US agricultural statistics and crop data",
        category="agriculture",
        standard=DataStandard.REST_JSON,
        base_url="https://quickstats.nass.usda.gov/api",
        endpoint_paths={
            "data": "/api_GET",
            "params": "/get_param_values",
            "counts": "/get_counts",
        },
        auth_method=AuthMethod.API_KEY,
        auth_notes="Free API key via USDA NASS registration",
        registration_required=True,
        cadence_expected=UpdateCadence.WEEKLY,
        typical_lag_days=7,
        geographic_scope=GeographicScope.NATIONAL,
        coverage_notes="US states and counties",
        units_primary="bushels",
        unit_variants=["acres", "bushels/acre", "cwt", "tons"],
        rate_limit_per_minute=30,
        data_quality_flags=True,
        historical_depth_years=30,
        license_name="Public Domain",
        redistribution_permitted=True,
        provenance_notes="Official USDA agricultural census and survey data",
        documentation_url="https://quickstats.nass.usda.gov/api",
        tags=["agriculture", "crops", "livestock", "usa", "official"],
    ),
    DataSource(
        source_id="world_bank_agriculture",
        name="World Bank Agriculture Indicators",
        description="World Bank agricultural and rural development indicators",
        category="agriculture",
        standard=DataStandard.REST_JSON,
        base_url="https://api.worldbank.org/v2",
        endpoint_paths={
            "indicators": "/indicator",
            "countries": "/country",
        },
        dataset_ids=["AG.PRD.CROP.XD", "AG.YLD.CREL.KG", "AG.LND.ARBL.HA"],
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.ANNUAL,
        typical_lag_days=365,  # 1 year lag
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="World Bank member countries",
        units_primary="metric tons",
        unit_variants=["hectares", "kg/hectare", "USD"],
        rate_limit_per_minute=120,
        data_quality_flags=True,
        historical_depth_years=50,
        license_name="CC BY 4.0",
        license_url="https://datacatalog.worldbank.org/public-licenses",
        redistribution_permitted=True,
        provenance_notes="World Bank development indicators",
        documentation_url="https://datahelpdesk.worldbank.org/knowledgebase/articles/889392",
        tags=["agriculture", "development", "global", "official"],
    ),
    # Transportation Data Sources
    DataSource(
        source_id="opensky_network",
        name="OpenSky Network",
        description="Real-time aviation and flight tracking data",
        category="transportation",
        standard=DataStandard.REST_JSON,
        base_url="https://opensky-network.org/api",
        endpoint_paths={
            "states": "/states/all",
            "flights": "/flights/all",
            "arrivals": "/flights/arrival",
            "departures": "/flights/departure",
        },
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.REALTIME,
        typical_lag_days=0,
        geographic_scope=GeographicScope.GLOBAL,
        coverage_notes="Global aviation coverage, higher density in Europe/North America",
        rate_limit_per_minute=100,  # Higher for registered users
        data_quality_flags=False,
        historical_depth_years=5,
        license_name="CC BY-SA 4.0",
        license_url="https://opensky-network.org/about/license",
        redistribution_permitted=True,
        provenance_notes="Crowdsourced ADS-B data from aircraft transponders",
        documentation_url="https://opensky-network.org/apidoc/",
        tags=["aviation", "flights", "realtime", "transportation"],
    ),
    DataSource(
        source_id="bts_freight",
        name="Bureau of Transportation Statistics",
        description="US freight and transportation statistics",
        category="transportation",
        standard=DataStandard.CSV_BULK,
        base_url="https://www.bts.gov/browse-statistical-products-and-data",
        auth_method=AuthMethod.NONE,
        cadence_expected=UpdateCadence.MONTHLY,
        typical_lag_days=60,
        geographic_scope=GeographicScope.NATIONAL,
        coverage_notes="US transportation modes: rail, truck, water, air, pipeline",
        units_primary="tons",
        unit_variants=["ton-miles", "dollars", "vehicles"],
        rate_limit_per_minute=30,
        data_quality_flags=True,
        historical_depth_years=25,
        license_name="Public Domain",
        redistribution_permitted=True,
        provenance_notes="Official US DOT transportation statistics",
        documentation_url="https://www.bts.gov/statistical-products",
        tags=["freight", "transportation", "logistics", "usa", "official"],
    ),
    DataSource(
        source_id="eurocontrol",
        name="Eurocontrol",
        description="European aviation and air traffic management data",
        category="transportation",
        standard=DataStandard.REST_JSON,
        base_url="https://www.eurocontrol.int/performance/data",
        auth_method=AuthMethod.API_KEY,
        auth_notes="Registration required for API access",
        registration_required=True,
        cadence_expected=UpdateCadence.DAILY,
        typical_lag_days=1,
        geographic_scope=GeographicScope.REGIONAL,
        coverage_notes="European airspace and airports",
        units_primary="flights",
        unit_variants=["minutes", "nautical miles", "tonnes CO2"],
        rate_limit_per_minute=60,
        data_quality_flags=True,
        historical_depth_years=10,
        license_name="Custom",
        tos_url="https://www.eurocontrol.int/terms-use",
        redistribution_permitted=False,
        provenance_notes="Official European air traffic management data",
        documentation_url="https://www.eurocontrol.int/performance/data/download",
        tags=["aviation", "europe", "air-traffic", "official"],
    ),
]
