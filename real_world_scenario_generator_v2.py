# real_world_scenario_generator_v2.py - Configuration-driven real-world scenario generator
import os
import requests
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging
from sqlalchemy import create_engine, text
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import openai
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class LocationData:
    """Location information for scenario generation"""
    latitude: float
    longitude: float
    city: str
    state: str
    country: str
    timezone: str
    population: Optional[int] = None

@dataclass
class WeatherData:
    """Current weather information"""
    temperature: float
    condition: str
    humidity: float
    wind_speed: float
    precipitation_chance: float
    uv_index: float
    feels_like: float
    visibility: float
    pressure: float
    sunrise: str
    sunset: str

@dataclass
class TrendingData:
    """Social media and search trending data"""
    trending_topics: List[str]
    local_events: List[Dict]
    search_trends: List[Dict]
    social_mentions: List[Dict]
    news_headlines: List[str]

class ConfigurationManager:
    """Manages configuration from multiple sources"""
    
    def __init__(self):
        self.config = self._load_configuration()
    
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration from multiple sources with priority order"""
        config = {}
        
        # 1. Load from config file if exists
        config_file = os.getenv('SCENARIO_CONFIG_FILE', 'scenario_config.yaml')
        if Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    config.update(yaml.safe_load(f) or {})
                logger.info(f"Loaded configuration from {config_file}")
            except Exception as e:
                logger.warning(f"Failed to load config file {config_file}: {e}")
        
        # 2. Override with environment variables
        env_config = self._load_from_environment()
        config.update(env_config)
        
        # 3. Set defaults for missing values
        config = self._set_defaults(config)
        
        return config
    
    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        env_config = {}
        
        # API Keys
        env_config['weather_api_key'] = os.getenv('OPENWEATHER_API_KEY')
        env_config['google_api_key'] = os.getenv('GOOGLE_API_KEY')
        env_config['twitter_bearer_token'] = os.getenv('TWITTER_BEARER_TOKEN')
        env_config['news_api_key'] = os.getenv('NEWS_API_KEY')
        env_config['eventbrite_api_key'] = os.getenv('EVENTBRITE_API_KEY')
        
        # Location settings
        env_config['default_latitude'] = float(os.getenv('DEFAULT_LATITUDE', '0'))
        env_config['default_longitude'] = float(os.getenv('DEFAULT_LONGITUDE', '0'))
        env_config['default_city'] = os.getenv('DEFAULT_CITY', 'Unknown')
        env_config['default_state'] = os.getenv('DEFAULT_STATE', 'Unknown')
        env_config['default_country'] = os.getenv('DEFAULT_COUNTRY', 'Unknown')
        env_config['default_timezone'] = os.getenv('DEFAULT_TIMEZONE', 'UTC')
        env_config['default_population'] = int(os.getenv('DEFAULT_POPULATION', '0'))
        
        # LLM settings
        env_config['llm_model'] = os.getenv('LLM_MODEL', 'gpt-3.5-turbo')
        env_config['llm_temperature'] = float(os.getenv('LLM_TEMPERATURE', '0.7'))
        
        # Weather settings
        env_config['weather_units'] = os.getenv('WEATHER_UNITS', 'imperial')
        env_config['weather_timeout'] = int(os.getenv('WEATHER_TIMEOUT', '10'))
        env_config['hot_weather_threshold'] = float(os.getenv('HOT_WEATHER_THRESHOLD', '85'))
        env_config['cold_weather_threshold'] = float(os.getenv('COLD_WEATHER_THRESHOLD', '32'))
        
        # Trending data settings
        env_config['max_trending_topics'] = int(os.getenv('MAX_TRENDING_TOPICS', '10'))
        env_config['max_local_events'] = int(os.getenv('MAX_LOCAL_EVENTS', '5'))
        env_config['max_search_trends'] = int(os.getenv('MAX_SEARCH_TRENDS', '10'))
        env_config['max_social_mentions'] = int(os.getenv('MAX_SOCIAL_MENTIONS', '10'))
        env_config['max_news_headlines'] = int(os.getenv('MAX_NEWS_HEADLINES', '5'))
        
        # Inventory settings
        env_config['low_stock_threshold'] = int(os.getenv('LOW_STOCK_THRESHOLD', '10'))
        env_config['expiry_days_threshold'] = int(os.getenv('EXPIRY_DAYS_THRESHOLD', '3'))
        
        # Product categories (comma-separated)
        categories_str = os.getenv('PRODUCT_CATEGORIES', 'beverages,dairy,produce,bakery,frozen,meat,pantry,snacks,household,personal_care')
        env_config['product_categories'] = [cat.strip() for cat in categories_str.split(',')]
        
        # Trending topics (comma-separated)
        topics_str = os.getenv('BASE_TRENDING_TOPICS', 'local restaurants,food delivery,grocery shopping,weather,events,traffic,local news')
        env_config['base_trending_topics'] = [topic.strip() for topic in topics_str.split(',')]
        
        # Fallback values
        env_config['fallback_temperature'] = float(os.getenv('FALLBACK_TEMPERATURE', '72.0'))
        env_config['fallback_humidity'] = float(os.getenv('FALLBACK_HUMIDITY', '50.0'))
        env_config['fallback_condition'] = os.getenv('FALLBACK_CONDITION', 'clear')
        
        return env_config
    
    def _set_defaults(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Set default values for missing configuration"""
        defaults = {
            'weather_api_key': 'demo_key',
            'google_api_key': 'demo_key',
            'twitter_bearer_token': 'demo_token',
            'news_api_key': 'demo_key',
            'eventbrite_api_key': 'demo_key',
            'default_latitude': 40.7128,
            'default_longitude': -74.0060,
            'default_city': 'New York',
            'default_state': 'NY',
            'default_country': 'US',
            'default_timezone': 'America/New_York',
            'default_population': 8500000,
            'llm_model': 'gpt-3.5-turbo',
            'llm_temperature': 0.7,
            'weather_units': 'imperial',
            'weather_timeout': 10,
            'hot_weather_threshold': 85,
            'cold_weather_threshold': 32,
            'max_trending_topics': 10,
            'max_local_events': 5,
            'max_search_trends': 10,
            'max_social_mentions': 10,
            'max_news_headlines': 5,
            'low_stock_threshold': 10,
            'expiry_days_threshold': 3,
            'product_categories': ['beverages', 'dairy', 'produce', 'bakery', 'frozen', 'meat', 'pantry', 'snacks', 'household', 'personal_care'],
            'base_trending_topics': ['local restaurants', 'food delivery', 'grocery shopping', 'weather', 'events', 'traffic', 'local news'],
            'fallback_temperature': 72.0,
            'fallback_humidity': 50.0,
            'fallback_condition': 'clear'
        }
        
        for key, default_value in defaults.items():
            if key not in config or config[key] is None:
                config[key] = default_value
        
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration"""
        return self.config.copy()

class RealWorldScenarioGenerator:
    """Generates real-world scenarios from live data sources"""
    
    def __init__(self, openai_api_key: str, db_url: str, config: Optional[ConfigurationManager] = None):
        self.openai_api_key = openai_api_key
        self.db_url = db_url
        self.engine = create_engine(db_url)
        self.config = config or ConfigurationManager()
        
        # LLM for scenario generation
        self.llm = ChatOpenAI(
            model=self.config.get('llm_model'),
            openai_api_key=openai_api_key,
            temperature=self.config.get('llm_temperature')
        )
        
        # Store current weather for trending analysis
        self._current_weather = None

    async def generate_real_world_scenario(self, location_override: Optional[Dict] = None) -> Dict[str, Any]:
        """Generate a complete real-world scenario"""
        
        try:
            # 1. Get location data
            location = await self._get_location_data(location_override)
            
            # 2. Get current weather
            weather = await self._get_weather_data(location)
            self._current_weather = weather
            
            # 3. Get trending data
            trending = await self._get_trending_data(location)
            
            # 4. Get local inventory context
            inventory_context = await self._get_inventory_context(location)
            
            # 5. Generate scenario using LLM
            scenario = await self._generate_scenario_with_llm(
                location, weather, trending, inventory_context
            )
            
            return {
                "scenario_type": "real_world",
                "generated_at": datetime.now().isoformat(),
                "location": location.__dict__,
                "weather": weather.__dict__,
                "trending": trending.__dict__,
                "inventory_context": inventory_context,
                "scenario": scenario,
                "data_sources": ["openweather", "google_trends", "social_media", "local_news"],
                "configuration_used": self.config.get_all()
            }
            
        except Exception as e:
            logger.error(f"Error generating real-world scenario: {e}")
            return self._generate_fallback_scenario()

    async def _get_location_data(self, location_override: Optional[Dict] = None) -> LocationData:
        """Get location information"""
        
        if location_override:
            lat, lon = location_override["latitude"], location_override["longitude"]
        else:
            lat = self.config.get('default_latitude')
            lon = self.config.get('default_longitude')
        
        # Use geopy to get location details
        try:
            geolocator = Nominatim(user_agent="retail_ai_scenario_generator")
            location = geolocator.reverse(f"{lat}, {lon}")
            
            address = location.raw.get('address', {})
            
            return LocationData(
                latitude=lat,
                longitude=lon,
                city=address.get('city', address.get('town', self.config.get('default_city'))),
                state=address.get('state', self.config.get('default_state')),
                country=address.get('country', self.config.get('default_country')),
                timezone=self._get_timezone(lat, lon),
                population=self._get_population_estimate(lat, lon)
            )
        except Exception as e:
            logger.error(f"Error getting location data: {e}")
            return LocationData(
                latitude=lat,
                longitude=lon,
                city=self.config.get('default_city'),
                state=self.config.get('default_state'),
                country=self.config.get('default_country'),
                timezone=self.config.get('default_timezone'),
                population=self.config.get('default_population')
            )

    async def _get_weather_data(self, location: LocationData) -> WeatherData:
        """Get current weather data from OpenWeather API"""
        
        weather_api_key = self.config.get('weather_api_key')
        if weather_api_key == 'demo_key':
            return self._get_demo_weather_data(location)
        
        url = f"http://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": location.latitude,
            "lon": location.longitude,
            "appid": weather_api_key,
            "units": self.config.get('weather_units')
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.config.get('weather_timeout'))
            response.raise_for_status()
            data = response.json()
            
            return WeatherData(
                temperature=data['main']['temp'],
                condition=data['weather'][0]['main'].lower(),
                humidity=data['main']['humidity'],
                wind_speed=data['wind']['speed'],
                precipitation_chance=data.get('pop', 0) * 100,
                uv_index=data.get('uvi', 0),
                feels_like=data['main']['feels_like'],
                visibility=data.get('visibility', 10000) / 1000,  # Convert to km
                pressure=data['main']['pressure'],
                sunrise=datetime.fromtimestamp(data['sys']['sunrise']).strftime('%H:%M'),
                sunset=datetime.fromtimestamp(data['sys']['sunset']).strftime('%H:%M')
            )
            
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            return self._get_demo_weather_data(location)

    def _get_demo_weather_data(self, location: LocationData) -> WeatherData:
        """Get demo weather data when API is not available"""
        return WeatherData(
            temperature=self.config.get('fallback_temperature'),
            condition=self.config.get('fallback_condition'),
            humidity=self.config.get('fallback_humidity'),
            wind_speed=5.0,
            precipitation_chance=10.0,
            uv_index=5.0,
            feels_like=self.config.get('fallback_temperature'),
            visibility=10.0,
            pressure=1013.0,
            sunrise="06:30",
            sunset="19:30"
        )

    async def _get_trending_data(self, location: LocationData) -> TrendingData:
        """Get trending data from multiple sources"""
        
        trending_topics = await self._get_google_trends(location)
        local_events = await self._get_local_events(location)
        search_trends = await self._get_search_trends(location)
        social_mentions = await self._get_social_mentions(location)
        news_headlines = await self._get_local_news(location)
        
        return TrendingData(
            trending_topics=trending_topics,
            local_events=local_events,
            search_trends=search_trends,
            social_mentions=social_mentions,
            news_headlines=news_headlines
        )

    async def _get_google_trends(self, location: LocationData) -> List[str]:
        """Get Google Trends data for the location"""
        
        base_trends = self.config.get('base_trending_topics', [])
        
        # Add weather-specific trends
        if self._current_weather:
            weather = self._current_weather
            hot_threshold = self.config.get('hot_weather_threshold')
            cold_threshold = self.config.get('cold_weather_threshold')
            
            if weather.temperature > hot_threshold:
                base_trends.extend(["ice cream", "cold drinks", "air conditioning"])
            elif weather.temperature < cold_threshold:
                base_trends.extend(["hot chocolate", "soup", "warm food"])
            if weather.condition in ["rain", "storm"]:
                base_trends.extend(["umbrella", "indoor activities", "comfort food"])
        
        return base_trends[:self.config.get('max_trending_topics')]

    async def _get_local_events(self, location: LocationData) -> List[Dict]:
        """Get local events that might affect retail demand"""
        
        # In production, you'd use Eventbrite API or similar
        eventbrite_key = self.config.get('eventbrite_api_key')
        if eventbrite_key == 'demo_key':
            return self._get_demo_local_events()
        
        # TODO: Implement real Eventbrite API call
        return self._get_demo_local_events()

    def _get_demo_local_events(self) -> List[Dict]:
        """Get demo local events"""
        return [
            {
                "name": "Local Food Festival",
                "date": (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d'),
                "type": "food_event",
                "expected_attendance": 5000,
                "impact": "high"
            },
            {
                "name": "Sports Game",
                "date": datetime.now().strftime('%Y-%m-%d'),
                "type": "sports_event",
                "expected_attendance": 20000,
                "impact": "medium"
            }
        ][:self.config.get('max_local_events')]

    async def _get_search_trends(self, location: LocationData) -> List[Dict]:
        """Get search trends related to retail products"""
        
        # In production, you'd use Google Search Console API or similar
        google_key = self.config.get('google_api_key')
        if google_key == 'demo_key':
            return self._get_demo_search_trends()
        
        # TODO: Implement real Google Search Console API call
        return self._get_demo_search_trends()

    def _get_demo_search_trends(self) -> List[Dict]:
        """Get demo search trends"""
        return [
            {
                "term": "organic produce",
                "trend": "rising",
                "volume": "high",
                "category": "produce"
            },
            {
                "term": "plant-based milk",
                "trend": "rising",
                "volume": "medium",
                "category": "dairy"
            },
            {
                "term": "energy drinks",
                "trend": "stable",
                "volume": "high",
                "category": "beverages"
            }
        ][:self.config.get('max_search_trends')]

    async def _get_social_mentions(self, location: LocationData) -> List[Dict]:
        """Get social media mentions related to retail"""
        
        # In production, you'd use Twitter API, Instagram API, etc.
        twitter_token = self.config.get('twitter_bearer_token')
        if twitter_token == 'demo_token':
            return self._get_demo_social_mentions()
        
        # TODO: Implement real Twitter API call
        return self._get_demo_social_mentions()

    def _get_demo_social_mentions(self) -> List[Dict]:
        """Get demo social mentions"""
        return [
            {
                "platform": "twitter",
                "mention": "Local grocery store has amazing organic selection!",
                "sentiment": "positive",
                "category": "produce",
                "engagement": 150
            },
            {
                "platform": "instagram",
                "mention": "Best coffee in town at our local market",
                "sentiment": "positive",
                "category": "beverages",
                "engagement": 89
            }
        ][:self.config.get('max_social_mentions')]

    async def _get_local_news(self, location: LocationData) -> List[str]:
        """Get local news headlines that might affect retail"""
        
        # In production, you'd use News API or similar
        news_key = self.config.get('news_api_key')
        if news_key == 'demo_key':
            return self._get_demo_local_news()
        
        # TODO: Implement real News API call
        return self._get_demo_local_news()

    def _get_demo_local_news(self) -> List[str]:
        """Get demo local news"""
        return [
            "Local farmers market expands to new location",
            "New restaurant district opening downtown",
            "Weather alert: Heat wave expected this weekend",
            "Community food drive this Saturday"
        ][:self.config.get('max_news_headlines')]

    async def _get_inventory_context(self, location: LocationData) -> Dict[str, Any]:
        """Get current inventory context from database"""
        
        try:
            low_stock_threshold = self.config.get('low_stock_threshold')
            expiry_days_threshold = self.config.get('expiry_days_threshold')
            
            # Get current inventory levels
            query = f"""
            SELECT 
                p.category,
                COUNT(*) as product_count,
                SUM(i.quantity) as total_quantity,
                AVG(p.unit_price) as avg_price,
                COUNT(CASE WHEN i.quantity < {low_stock_threshold} THEN 1 END) as low_stock_count
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity > 0
            GROUP BY p.category
            ORDER BY total_quantity DESC
            """
            
            df = pd.read_sql(query, self.engine)
            
            # Get expiring products
            expiry_query = f"""
            SELECT 
                p.category,
                COUNT(*) as expiring_count,
                SUM(i.quantity * p.unit_price) as value_at_risk
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.expiry_date <= CURRENT_DATE + INTERVAL '{expiry_days_threshold} days'
            AND i.quantity > 0
            GROUP BY p.category
            """
            
            expiry_df = pd.read_sql(expiry_query, self.engine)
            
            return {
                "inventory_summary": df.to_dict('records'),
                "expiring_products": expiry_df.to_dict('records'),
                "total_products": len(df),
                "low_stock_categories": df[df['low_stock_count'] > 0]['category'].tolist()
            }
            
        except Exception as e:
            logger.error(f"Error fetching inventory context: {e}")
            return {
                "inventory_summary": [],
                "expiring_products": [],
                "total_products": 0,
                "low_stock_categories": []
            }

    async def _generate_scenario_with_llm(
        self,
        location: LocationData,
        weather: WeatherData,
        trending: TrendingData,
        inventory_context: Dict[str, Any]
    ) -> str:
        """Use LLM to generate a comprehensive scenario"""
        
        prompt = f"""
        Generate a realistic retail scenario based on this real-world data:
        
        LOCATION: {location.city}, {location.state}, {location.country}
        POPULATION: {location.population or 'Unknown'}
        
        CURRENT WEATHER:
        - Temperature: {weather.temperature}°F (feels like {weather.feels_like}°F)
        - Condition: {weather.condition}
        - Humidity: {weather.humidity}%
        - Wind: {weather.wind_speed} mph
        - Precipitation chance: {weather.precipitation_chance}%
        - UV Index: {weather.uv_index}
        - Sunrise: {weather.sunrise}, Sunset: {weather.sunset}
        
        TRENDING TOPICS: {', '.join(trending.trending_topics[:5])}
        LOCAL EVENTS: {len(trending.local_events)} events this week
        SEARCH TRENDS: {len(trending.search_trends)} trending product searches
        NEWS: {len(trending.news_headlines)} relevant local news items
        
        INVENTORY CONTEXT:
        - Total products: {inventory_context.get('total_products', 0)}
        - Low stock categories: {', '.join(inventory_context.get('low_stock_categories', []))}
        - Expiring products: {len(inventory_context.get('expiring_products', []))} categories
        
        Create a detailed scenario that:
        1. Describes the current market conditions
        2. Identifies specific product categories that will be affected
        3. Predicts demand changes based on weather, events, and trends
        4. Suggests immediate actions needed
        5. Includes specific product recommendations
        
        Make it realistic and actionable for a retail AI system.
        """
        
        messages = [
            SystemMessage(content="You are a retail scenario generator. Create realistic, actionable scenarios based on real-world data."),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            return response.content
        except Exception as e:
            logger.error(f"Error generating scenario with LLM: {e}")
            return self._generate_fallback_scenario_text(location, weather)

    def _get_timezone(self, lat: float, lon: float) -> str:
        """Get timezone for location"""
        # In production, use a timezone API
        # For now, use configuration default
        return self.config.get('default_timezone')

    def _get_population_estimate(self, lat: float, lon: float) -> Optional[int]:
        """Get population estimate for location"""
        # In production, use a demographics API
        # For now, use configuration default
        return self.config.get('default_population')

    def _generate_fallback_scenario(self) -> Dict[str, Any]:
        """Generate a fallback scenario when APIs fail"""
        
        default_location = {
            "latitude": self.config.get('default_latitude'),
            "longitude": self.config.get('default_longitude'),
            "city": self.config.get('default_city'),
            "state": self.config.get('default_state'),
            "country": self.config.get('default_country')
        }
        
        return {
            "scenario_type": "real_world_fallback",
            "generated_at": datetime.now().isoformat(),
            "location": default_location,
            "weather": {
                "temperature": self.config.get('fallback_temperature'),
                "condition": self.config.get('fallback_condition'),
                "humidity": self.config.get('fallback_humidity')
            },
            "trending": {
                "trending_topics": self.config.get('base_trending_topics', [])[:3],
                "local_events": [],
                "search_trends": [],
                "social_mentions": [],
                "news_headlines": []
            },
            "inventory_context": {
                "inventory_summary": [],
                "expiring_products": [],
                "total_products": 0,
                "low_stock_categories": []
            },
            "scenario": self._generate_fallback_scenario_text(
                LocationData(**default_location),
                WeatherData(
                    temperature=self.config.get('fallback_temperature'),
                    condition=self.config.get('fallback_condition'),
                    humidity=self.config.get('fallback_humidity'),
                    wind_speed=5.0,
                    precipitation_chance=10.0,
                    uv_index=5.0,
                    feels_like=self.config.get('fallback_temperature'),
                    visibility=10.0,
                    pressure=1013.0,
                    sunrise="06:30",
                    sunset="19:30"
                )
            ),
            "data_sources": ["fallback"],
            "configuration_used": self.config.get_all()
        }

    def _generate_fallback_scenario_text(self, location: LocationData, weather: WeatherData) -> str:
        """Generate fallback scenario text"""
        
        return f"""
        Real-World Scenario: {location.city}, {location.state}
        
        Current Conditions:
        - Weather: {weather.temperature}°F, {weather.condition}
        - Time: {datetime.now().strftime('%A, %B %d at %I:%M %p')}
        
        Market Analysis:
        Based on current conditions, we're experiencing typical retail patterns for this time and location.
        
        Recommended Actions:
        1. Monitor weather-sensitive products (beverages, seasonal items)
        2. Check inventory levels for trending categories
        3. Review pricing strategies for competitive positioning
        4. Prepare for potential demand fluctuations
        
        This scenario provides a baseline for AI agent analysis and decision-making.
        """

# Integration with existing workflow
async def integrate_real_world_scenario(workflow, location_override: Optional[Dict] = None, config: Optional[ConfigurationManager] = None) -> Dict[str, Any]:
    """Integrate real-world scenario generation with existing workflow"""
    
    generator = RealWorldScenarioGenerator(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        db_url=os.getenv('NEON_DATABASE_URL'),
        config=config
    )
    
    # Generate real-world scenario
    scenario_data = await generator.generate_real_world_scenario(location_override)
    
    # Run through workflow
    if workflow:
        final_state = workflow.run_scenario(scenario_data['scenario'])
        scenario_data['workflow_results'] = final_state
    
    return scenario_data 