from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime

# Pydantic models for webhook payloads
class WeatherEvent(BaseModel):
    event_type: str = "weather_update"
    timestamp: datetime = Field(default_factory=datetime.now)
    data: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "weather_update",
                "timestamp": "2024-01-15T14:30:00Z",
                "data": {
                    "temperature": 95,
                    "condition": "heatwave",
                    "humidity": 85,
                    "duration_hours": 72,
                    "location": "New York",
                    "affected_categories": ["beverages", "frozen", "dairy"]
                }
            }
        }

class SalesEvent(BaseModel):
    event_type: str = "sales_update"
    timestamp: datetime = Field(default_factory=datetime.now)
    data: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "sales_update",
                "timestamp": "2024-01-15T14:35:00Z",
                "data": {
                    "category": "beverages",
                    "change_percentage": 40,
                    "trending_products": [
                        {"product_id": 21, "name": "Bottled Water", "increase": 60},
                        {"product_id": 22, "name": "Sports Drinks", "increase": 45}
                    ],
                    "time_window": "last_hour"
                }
            }
        }

class CompetitorEvent(BaseModel):
    event_type: str = "competitor_action"
    timestamp: datetime = Field(default_factory=datetime.now)
    data: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "competitor_action",
                "timestamp": "2024-01-15T14:40:00Z",
                "data": {
                    "competitor": "MegaMart",
                    "action": "price_reduction",
                    "changes": [
                        {"category": "dairy", "discount": 0.4, "duration": "weekend"},
                        {"category": "produce", "discount": 0.25, "duration": "48_hours"}
                    ],
                    "market_impact": "high"
                }
            }
        }

class InventoryAlert(BaseModel):
    event_type: str = "inventory_alert"
    timestamp: datetime = Field(default_factory=datetime.now)
    alert_level: str = Field(..., pattern="^(critical|high|medium|low)$")
    data: Dict[str, Any]
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "inventory_alert",
                "timestamp": "2024-01-15T14:45:00Z",
                "alert_level": "critical",
                "data": {
                    "alert_type": "mass_expiry",
                    "products": [
                        {
                            "product_id": 10,
                            "name": "Organic Milk",
                            "category": "dairy",
                            "expires_in_hours": 24,
                            "quantity": 50,
                            "value_at_risk": 199.50
                        }
                    ],
                    "total_value_at_risk": 450.00,
                    "recommended_action": "immediate_markdown_and_donation"
                }
            }
        }

class DemoControl(BaseModel):
    action: str = Field(..., pattern="^(start|stop|reset|status)$")
    scenario: Optional[str] = Field(None, pattern="^(heatwave|competitor|expiry|mixed|database_based)$")

class RealWorldScenarioRequest(BaseModel):
    location: Optional[Dict[str, float]] = Field(None, description="Override location with lat/lon")
    include_weather: bool = Field(True, description="Include weather data")
    include_trending: bool = Field(True, description="Include trending data")
    include_inventory: bool = Field(True, description="Include inventory context")
    
    class Config:
        json_schema_extra = {
            "example": {
                "location": {"latitude": 40.7128, "longitude": -74.0060},
                "include_weather": True,
                "include_trending": True,
                "include_inventory": True
            }
        }

# NEW: Pydantic model for user simulation requests
class UserSimulationRequest(BaseModel):
    prompt: str = Field(..., example="What if a competitor runs a big sale on dairy this weekend?")

# Worker Management Models
class WorkerRegistration(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: str = Field(..., pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    password: str = Field(..., min_length=6, max_length=50)
    role: str = Field(..., pattern="^(manager|associate|supervisor|specialist)$")
    department: str = Field(..., pattern="^(inventory|pricing|waste_management|sustainability|general)$")
    phone: Optional[str] = Field(None, pattern=r"^\+?1?\d{9,15}$")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "John Smith",
                "email": "john.smith@walmart.com",
                "password": "secure123",
                "role": "manager",
                "department": "inventory",
                "phone": "+1234567890"
            }
        }

class WorkerLogin(BaseModel):
    email: str = Field(..., pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    password: str = Field(..., min_length=6, max_length=50)

class NotificationPreferences(BaseModel):
    email_notifications: bool = True
    critical_alerts: bool = True
    daily_summaries: bool = False
    weekly_reports: bool = True
    inventory_alerts: bool = True
    pricing_alerts: bool = True
    waste_alerts: bool = True

class WasteAssetEvent(BaseModel):
    asset_id: str = Field(..., description="Unique identifier for the waste asset.", alias="assetId")
    facility_id: str = Field(..., description="Identifier for the facility where the asset was created.", alias="facilityId")
    material_type: str = Field(..., description="Type of material (e.g., 'PET', 'HDPE').", alias="materialType")
    weight_kg: float = Field(..., gt=0, description="Weight of the asset in kilograms.", alias="weightKg")
    purity_percentage: Optional[float] = Field(None, ge=0, le=100, description="Estimated purity of the material.", alias="purityPercentage")
    quality_grade: str = Field(..., max_length=1, description="Quality grade of the material (e.g., 'A', 'B', 'C').")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp of the event.")

    class Config:
        json_schema_extra = {
            "example": {
                "assetId": "ASSET_PET_1689362001",
                "facilityId": "FAC_001",
                "materialType": "PET",
                "weightKg": 150.5,
                "purityPercentage": 98.5,
                "quality_grade": "A",
                "timestamp": "2024-07-20T10:00:00Z"
            }
        }
        allow_population_by_field_name = True 