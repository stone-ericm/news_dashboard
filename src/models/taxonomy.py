"""Topic taxonomy for categorizing signals."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Topic(BaseModel):
    """A topic in the taxonomy."""
    
    topic_id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Human-readable name")
    description: str = Field(..., description="What this topic covers")
    parent_id: Optional[str] = Field(None, description="Parent topic ID")
    
    # Keywords and queries
    keywords: List[str] = Field(default_factory=list, description="Search keywords")
    aliases: List[str] = Field(default_factory=list, description="Alternative names")
    google_trends_queries: List[str] = Field(default_factory=list, description="Google Trends queries")
    wikipedia_articles: List[str] = Field(default_factory=list, description="Wikipedia article titles")
    
    # Metadata
    color: Optional[str] = Field(None, description="Display color (hex)")
    icon: Optional[str] = Field(None, description="Icon identifier")
    priority: int = Field(100, description="Display priority (lower = higher)")
    is_active: bool = Field(True, description="Include in analysis?")
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TaxonomyManager:
    """Manages the topic taxonomy."""
    
    def __init__(self, storage_path: str = "data/taxonomy.json"):
        """Initialize taxonomy manager."""
        self.storage_path = storage_path
        self.topics: Dict[str, Topic] = {}
        self.load()
    
    def add_topic(self, topic: Topic) -> None:
        """Add or update a topic."""
        topic.updated_at = datetime.utcnow()
        self.topics[topic.topic_id] = topic
        self.save()
    
    def get_topic(self, topic_id: str) -> Optional[Topic]:
        """Get topic by ID."""
        return self.topics.get(topic_id)
    
    def get_children(self, parent_id: str) -> List[Topic]:
        """Get child topics."""
        return [
            topic for topic in self.topics.values()
            if topic.parent_id == parent_id
        ]
    
    def get_ancestors(self, topic_id: str) -> List[Topic]:
        """Get ancestor topics (path to root)."""
        ancestors = []
        current = self.get_topic(topic_id)
        
        while current and current.parent_id:
            parent = self.get_topic(current.parent_id)
            if parent:
                ancestors.append(parent)
                current = parent
            else:
                break
        
        return list(reversed(ancestors))
    
    def search_topics(self, query: str) -> List[Topic]:
        """Search topics by name, keywords, or aliases."""
        query_lower = query.lower()
        results = []
        
        for topic in self.topics.values():
            if (
                query_lower in topic.name.lower()
                or query_lower in topic.description.lower()
                or any(query_lower in kw.lower() for kw in topic.keywords)
                or any(query_lower in alias.lower() for alias in topic.aliases)
            ):
                results.append(topic)
        
        return results
    
    def save(self) -> None:
        """Save taxonomy to disk."""
        Path(self.storage_path).parent.mkdir(parents=True, exist_ok=True)
        data = {
            topic_id: topic.model_dump(mode="json")
            for topic_id, topic in self.topics.items()
        }
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def load(self) -> None:
        """Load taxonomy from disk."""
        if Path(self.storage_path).exists():
            with open(self.storage_path) as f:
                data = json.load(f)
                self.topics = {
                    topic_id: Topic(**topic_data)
                    for topic_id, topic_data in data.items()
                }


def get_default_taxonomy() -> TaxonomyManager:
    """Get default taxonomy with initial topics."""
    taxonomy = TaxonomyManager()
    
    # Root categories
    economy = Topic(
        topic_id="economy",
        name="Economy",
        description="Economic indicators and trends",
        keywords=["economy", "economic", "gdp", "growth"],
        color="#2E7D32",
        priority=10,
    )
    taxonomy.add_topic(economy)
    
    health = Topic(
        topic_id="health",
        name="Public Health",
        description="Health outcomes and healthcare",
        keywords=["health", "healthcare", "medical", "disease"],
        color="#1565C0",
        priority=20,
    )
    taxonomy.add_topic(health)
    
    crime = Topic(
        topic_id="crime",
        name="Crime & Safety",
        description="Crime rates and public safety",
        keywords=["crime", "safety", "violence", "police"],
        color="#C62828",
        priority=30,
    )
    taxonomy.add_topic(crime)
    
    climate = Topic(
        topic_id="climate",
        name="Climate & Environment",
        description="Climate change and environmental conditions",
        keywords=["climate", "weather", "environment", "pollution"],
        color="#388E3C",
        priority=40,
    )
    taxonomy.add_topic(climate)
    
    # Economy subcategories
    taxonomy.add_topic(
        Topic(
            topic_id="economy_employment",
            name="Employment",
            description="Jobs, unemployment, labor market",
            parent_id="economy",
            keywords=["employment", "jobs", "unemployment", "labor", "workforce"],
            google_trends_queries=["unemployment rate", "job openings", "layoffs"],
            wikipedia_articles=["Unemployment", "Employment", "Labor_economics"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="economy_inflation",
            name="Inflation",
            description="Price levels and inflation",
            parent_id="economy",
            keywords=["inflation", "prices", "cpi", "cost of living"],
            google_trends_queries=["inflation rate", "price increase", "cost of living"],
            wikipedia_articles=["Inflation", "Consumer_price_index"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="economy_housing",
            name="Housing",
            description="Housing market and affordability",
            parent_id="economy",
            keywords=["housing", "real estate", "rent", "mortgage", "homelessness"],
            google_trends_queries=["housing prices", "rent prices", "mortgage rates"],
            wikipedia_articles=["Housing", "Real_estate", "Homelessness"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="economy_stocks",
            name="Stock Market",
            description="Stock market performance",
            parent_id="economy",
            keywords=["stocks", "stock market", "dow jones", "s&p 500", "nasdaq"],
            google_trends_queries=["stock market", "dow jones", "market crash"],
            wikipedia_articles=["Stock_market", "Dow_Jones_Industrial_Average"],
        )
    )
    
    # Health subcategories
    taxonomy.add_topic(
        Topic(
            topic_id="health_covid",
            name="COVID-19",
            description="COVID-19 pandemic metrics",
            parent_id="health",
            keywords=["covid", "coronavirus", "pandemic", "covid-19"],
            google_trends_queries=["covid cases", "covid deaths", "covid vaccine"],
            wikipedia_articles=["COVID-19_pandemic", "COVID-19"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="health_mental",
            name="Mental Health",
            description="Mental health and wellbeing",
            parent_id="health",
            keywords=["mental health", "depression", "anxiety", "suicide"],
            google_trends_queries=["mental health", "depression", "anxiety"],
            wikipedia_articles=["Mental_health", "Depression_(mood)", "Anxiety"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="health_drugs",
            name="Drugs & Overdoses",
            description="Drug use and overdose deaths",
            parent_id="health",
            keywords=["drugs", "overdose", "opioid", "fentanyl", "addiction"],
            google_trends_queries=["drug overdose", "opioid crisis", "fentanyl"],
            wikipedia_articles=["Opioid_epidemic", "Drug_overdose"],
        )
    )
    
    # Crime subcategories
    taxonomy.add_topic(
        Topic(
            topic_id="crime_violent",
            name="Violent Crime",
            description="Homicides, assaults, and violent crime",
            parent_id="crime",
            keywords=["murder", "homicide", "assault", "violence", "shooting"],
            google_trends_queries=["murder rate", "gun violence", "mass shooting"],
            wikipedia_articles=["Violent_crime", "Homicide", "Gun_violence"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="crime_property",
            name="Property Crime",
            description="Theft, burglary, and property crime",
            parent_id="crime",
            keywords=["theft", "burglary", "robbery", "shoplifting", "car theft"],
            google_trends_queries=["crime rate", "burglary", "car theft"],
            wikipedia_articles=["Property_crime", "Burglary", "Theft"],
        )
    )
    
    # Climate subcategories
    taxonomy.add_topic(
        Topic(
            topic_id="climate_temperature",
            name="Temperature",
            description="Temperature anomalies and heat waves",
            parent_id="climate",
            keywords=["temperature", "heat wave", "global warming", "heat"],
            google_trends_queries=["heat wave", "temperature record", "global warming"],
            wikipedia_articles=["Global_warming", "Heat_wave", "Temperature"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="climate_disasters",
            name="Natural Disasters",
            description="Hurricanes, floods, fires, earthquakes",
            parent_id="climate",
            keywords=["hurricane", "flood", "wildfire", "earthquake", "disaster"],
            google_trends_queries=["hurricane", "wildfire", "flooding", "earthquake"],
            wikipedia_articles=["Natural_disaster", "Hurricane", "Wildfire"],
        )
    )
    
    taxonomy.add_topic(
        Topic(
            topic_id="climate_air",
            name="Air Quality",
            description="Air pollution and quality",
            parent_id="climate",
            keywords=["air quality", "pollution", "smog", "pm2.5", "aqi"],
            google_trends_queries=["air quality", "air pollution", "aqi"],
            wikipedia_articles=["Air_pollution", "Air_quality_index"],
        )
    )
    
    return taxonomy
