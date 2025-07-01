# webhook_receiver.py - FastAPI backend with webhook endpoints
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import asyncio
import json
import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from retail_workflow import RetailSustainabilityWorkflow
from shared_state import initialize_state, AgentMode
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from real_world_scenario_generator import RealWorldScenarioGenerator, integrate_real_world_scenario

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Retail AI Webhook Receiver", version="1.0.0")

# Configure CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def serialize_for_json(obj):
    """Recursively serialize objects for JSON, handling dates and other non-serializable types"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(item) for item in obj]
    else:
        return obj

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        # Serialize the message to handle dates and other non-JSON types
        serialized_message = serialize_for_json(message)
        
        # Create a copy of the list to avoid modification during iteration
        connections_to_remove = []
        
        for connection in self.active_connections:
            try:
                # Check if connection is still open before sending
                if connection.client_state.value == 1:  # WebSocketState.CONNECTED
                    await connection.send_json(serialized_message)
                else:
                    # Mark for removal
                    connections_to_remove.append(connection)
            except Exception as e:
                logger.error(f"Error broadcasting to websocket: {e}")
                # Mark for removal on error
                connections_to_remove.append(connection)
        
        # Remove closed connections
        for connection in connections_to_remove:
            try:
                self.active_connections.remove(connection)
                logger.info(f"Removed closed WebSocket connection. Total connections: {len(self.active_connections)}")
            except ValueError:
                # Connection already removed
                pass

manager = ConnectionManager()

# Global state for demo
class DemoState:
    def __init__(self):
        self.workflow: Optional[RetailSustainabilityWorkflow] = None
        self.current_scenario: Optional[str] = None
        self.is_running: bool = False
        self.metrics: Dict[str, Any] = {
            "events_received": 0,
            "workflows_completed": 0,
            "total_waste_prevented": 0,
            "total_meals_donated": 0
        }

demo_state = DemoState()

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
    scenario: Optional[str] = Field(None, pattern="^(heatwave|competitor|expiry|mixed)$")

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

# Initialize workflow on startup
@app.on_event("startup")
async def startup_event():
    """Initialize the workflow system"""
    try:
        demo_state.workflow = RetailSustainabilityWorkflow(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            db_url=os.getenv('NEON_DATABASE_URL')
        )
        logger.info("Workflow system initialized successfully")
        
        # Update database with initial webhook configuration
        await update_database_webhook_config()
        
    except Exception as e:
        logger.error(f"Failed to initialize workflow: {e}")

async def update_database_webhook_config():
    """Update database to simulate webhook integration"""
    try:
        engine = create_engine(os.getenv("NEON_DATABASE_URL"))
        with engine.connect() as conn:
            # Insert webhook configuration
            conn.execute(text("""
                INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
                VALUES ('webhook_config', :data, 1.0, CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING
            """), {
                "data": json.dumps({
                    "status": "active",
                    "endpoints": [
                        "/webhook/events/weather",
                        "/webhook/events/sales",
                        "/webhook/events/competitor",
                        "/webhook/events/inventory"
                    ]
                })
            })
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating webhook config: {e}")

# Webhook endpoints
@app.post("/webhook/events/weather")
async def receive_weather_event(event: WeatherEvent, background_tasks: BackgroundTasks):
    """Receive weather update webhooks"""
    logger.info(f"Received weather event: {event.data}")
    demo_state.metrics["events_received"] += 1
    
    # Broadcast to WebSocket clients
    await manager.broadcast({
        "type": "webhook_received",
        "event": "weather",
        "data": event.dict()
    })
    
    # Process event in background
    background_tasks.add_task(process_weather_event, event)
    
    return {"status": "accepted", "event_id": f"weather_{datetime.now().timestamp()}"}

@app.post("/webhook/events/sales")
async def receive_sales_event(event: SalesEvent, background_tasks: BackgroundTasks):
    """Receive sales update webhooks"""
    logger.info(f"Received sales event: {event.data}")
    demo_state.metrics["events_received"] += 1
    
    await manager.broadcast({
        "type": "webhook_received",
        "event": "sales",
        "data": event.dict()
    })
    
    background_tasks.add_task(process_sales_event, event)
    
    return {"status": "accepted", "event_id": f"sales_{datetime.now().timestamp()}"}

@app.post("/webhook/events/competitor")
async def receive_competitor_event(event: CompetitorEvent, background_tasks: BackgroundTasks):
    """Receive competitor action webhooks"""
    logger.info(f"Received competitor event: {event.data}")
    demo_state.metrics["events_received"] += 1
    
    await manager.broadcast({
        "type": "webhook_received",
        "event": "competitor",
        "data": event.dict()
    })
    
    background_tasks.add_task(process_competitor_event, event)
    
    return {"status": "accepted", "event_id": f"competitor_{datetime.now().timestamp()}"}

@app.post("/webhook/events/inventory")
async def receive_inventory_alert(event: InventoryAlert, background_tasks: BackgroundTasks):
    """Receive inventory alert webhooks"""
    logger.info(f"Received inventory alert: {event.data}")
    demo_state.metrics["events_received"] += 1
    
    await manager.broadcast({
        "type": "webhook_received",
        "event": "inventory",
        "data": event.dict(),
        "alert_level": event.alert_level
    })
    
    background_tasks.add_task(process_inventory_alert, event)
    
    return {"status": "accepted", "event_id": f"inventory_{datetime.now().timestamp()}"}

# Event processing functions
async def process_weather_event(event: WeatherEvent):
    """Process weather event through AI workflow"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text("""
            INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
            VALUES ('weather', :data, :impact, :timestamp)
            ON CONFLICT (id) DO NOTHING
        """), {
            "data": json.dumps(event.data),
            "impact": calculate_weather_impact(event.data),
            "timestamp": event.timestamp
        })
        trans.commit()
        scenario = generate_scenario_from_weather(event.data)
        await trigger_workflow(scenario, "weather_triggered")
    except Exception as e:
        if conn is not None:
            try:
                trans.rollback()
            except:
                pass  # Transaction may already be closed
        logger.error(f"Error processing weather event: {e}")
        await manager.broadcast({
            "type": "error",
            "message": f"Failed to process weather event: {str(e)}"
        })
    finally:
        if conn is not None:
            conn.close()

async def process_sales_event(event: SalesEvent):
    """Process sales event through AI workflow"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text("""
            INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
            VALUES ('sales_trend', :data, :impact, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING
        """), {
            "data": json.dumps(event.data),
            "impact": abs(event.data.get("change_percentage", 0)) / 100
        })
        trans.commit()
        scenario = generate_scenario_from_sales(event.data)
        await trigger_workflow(scenario, "sales_triggered")
    except Exception as e:
        if conn is not None:
            try:
                trans.rollback()
            except:
                pass  # Transaction may already be closed
        logger.error(f"Error processing sales event: {e}")
    finally:
        if conn is not None:
            conn.close()

async def process_competitor_event(event: CompetitorEvent):
    """Process competitor event through AI workflow"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text("""
            INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
            VALUES ('competitor', :data, :impact, :timestamp)
            ON CONFLICT (id) DO NOTHING
        """), {
            "data": json.dumps(event.data),
            "impact": 0.8,
            "timestamp": event.timestamp
        })
        trans.commit()
        scenario = generate_scenario_from_competitor(event.data)
        await trigger_workflow(scenario, "competitor_triggered")
    except Exception as e:
        if conn is not None:
            try:
                trans.rollback()
            except:
                pass  # Transaction may already be closed
        logger.error(f"Error processing competitor event: {e}")
    finally:
        if conn is not None:
            conn.close()

async def process_inventory_alert(event: InventoryAlert):
    """Process inventory alert through AI workflow"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text("""
            INSERT INTO agent_decisions (agent_name, decision_type, decision_data, reasoning, mode, created_at)
            VALUES ('inventory_monitor', 'alert_generated', :data, :reason, 'streaming', CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING
        """), {
            "data": json.dumps(event.data),
            "reason": f"System detected {len(event.data.get('products', []))} products at risk"
        })
        trans.commit()
        scenario = generate_scenario_from_inventory(event.data, event.alert_level)
        await trigger_workflow(scenario, "inventory_triggered", priority=event.alert_level)
    except Exception as e:
        if conn is not None:
            try:
                trans.rollback()
            except:
                pass  # Transaction may already be closed
        logger.error(f"Error processing inventory alert: {e}")
    finally:
        if conn is not None:
            conn.close()

# Helper functions
def calculate_weather_impact(weather_data: Dict) -> float:
    """Calculate impact score based on weather conditions"""
    temp = weather_data.get("temperature", 70)
    condition = weather_data.get("condition", "normal")
    
    if condition == "heatwave" and temp > 90:
        return 0.9
    elif condition in ["storm", "heavy_rain"]:
        return 0.7
    elif temp < 32 or temp > 85:
        return 0.6
    else:
        return 0.3

def generate_scenario_from_weather(weather_data: Dict) -> str:
    """Generate scenario description from weather data"""
    temp = weather_data.get("temperature", 70)
    condition = weather_data.get("condition", "normal")
    duration = weather_data.get("duration_hours", 24)
    
    return f"""
    Weather Alert: {condition.replace('_', ' ').title()} conditions detected.
    Temperature: {temp}°F for next {duration} hours.
    Expected impact on {', '.join(weather_data.get('affected_categories', []))} categories.
    Immediate analysis and response required.
    """

def generate_scenario_from_sales(sales_data: Dict) -> str:
    """Generate scenario description from sales data"""
    category = sales_data.get("category", "unknown")
    change = sales_data.get("change_percentage", 0)
    trending = sales_data.get("trending_products", [])
    
    return f"""
    Sales Alert: {category.title()} category showing {change}% {'increase' if change > 0 else 'decrease'}.
    Top trending products: {', '.join([p['name'] for p in trending[:3]])}.
    Time window: {sales_data.get('time_window', 'recent')}.
    Inventory and pricing adjustment needed.
    """

def generate_scenario_from_competitor(competitor_data: Dict) -> str:
    """Generate scenario description from competitor data"""
    competitor = competitor_data.get("competitor", "Unknown")
    action = competitor_data.get("action", "price_change")
    changes = competitor_data.get("changes", [])
    
    scenario = f"""
    Competitor Action: {competitor} has initiated {action.replace('_', ' ')}.
    """
    
    for change in changes:
        scenario += f"\n- {change['category']}: {change['discount']*100}% discount for {change['duration']}"
    
    scenario += f"\nMarket impact: {competitor_data.get('market_impact', 'medium')}. Strategic response required."
    
    return scenario

def generate_scenario_from_inventory(inventory_data: Dict, alert_level: str) -> str:
    """Generate scenario description from inventory alert data"""
    alert_type = inventory_data.get("alert_type", "inventory_alert")
    products = inventory_data.get("products", [])
    total_value = inventory_data.get("total_value_at_risk", 0)
    
    scenario = f"""
    {alert_level.upper()} Inventory Alert: {alert_type.replace('_', ' ').title()}.
    {len(products)} products affected with ${total_value:.2f} at risk.
    """
    
    for product in products[:5]:  # Top 5 products
        quantity = product.get('quantity', 0)
        expires_in_hours = product.get('expires_in_hours', 24)
        scenario += f"\n- {product.get('name', 'Unknown Product')}: {quantity} units expire in {expires_in_hours}h"
    
    scenario += f"\nRecommended action: {inventory_data.get('recommended_action', 'immediate review')}"
    
    return scenario

async def update_sales_data(sales_data: Dict):
    """Update sales data in database using UPSERT"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
            VALUES ('sales_trend', :data, :impact, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
                condition_data = EXCLUDED.condition_data,
                impact_score = EXCLUDED.impact_score,
                recorded_at = EXCLUDED.recorded_at
        """), {
            "data": json.dumps(sales_data),
            "impact": abs(sales_data.get("change_percentage", 0)) / 100
        })
        conn.commit()

async def update_inventory_alerts(inventory_data: Dict):
    """Update inventory alerts in database using UPSERT"""
    engine = create_engine(os.getenv("NEON_DATABASE_URL"))
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO agent_decisions (agent_name, decision_type, decision_data, reasoning, mode, created_at)
            VALUES ('inventory_monitor', 'alert_generated', :data, :reason, 'streaming', CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
                decision_data = EXCLUDED.decision_data,
                reasoning = EXCLUDED.reasoning,
                created_at = EXCLUDED.created_at
        """), {
            "data": json.dumps(inventory_data),
            "reason": f"System detected {len(inventory_data.get('products', []))} products at risk"
        })
        conn.commit()

async def trigger_workflow(scenario: str, trigger_source: str, priority: str = "medium"):
    """Trigger the AI workflow with given scenario"""
    if not demo_state.workflow:
        logger.error("Workflow not initialized")
        return
    
    try:
        # Broadcast workflow start
        await manager.broadcast({
            "type": "workflow_started",
            "scenario": scenario,
            "trigger": trigger_source,
            "priority": priority
        })
        
        # Check if we should use fallback due to API limits
        use_fallback = False
        try:
            # Try a simple LLM call to check if API is available
            test_llm = ChatOpenAI(
                model="gpt-3.5-turbo",
                temperature=0.1
            )
            test_llm.invoke([HumanMessage(content="test")])
        except Exception as e:
            if "quota" in str(e).lower() or "429" in str(e):
                use_fallback = True
                logger.warning("OpenAI API rate limited, using fallback workflow")
        
        if use_fallback:
            # Use fallback workflow without LLM
            state = await run_fallback_workflow(scenario)
        else:
            # Run normal workflow
            state = demo_state.workflow.run_scenario(scenario, AgentMode.STREAMING)
        
        # Broadcast agent messages to frontend
        await broadcast_agent_messages(state)
        
        # Extract metrics
        metrics = state.get("environmental_metrics", {})
        demo_state.metrics["total_waste_prevented"] += metrics.get("waste_prevented_kg", 0)
        demo_state.metrics["total_meals_donated"] += metrics.get("donation_meals", 0)
        demo_state.metrics["workflows_completed"] += 1
        
        # Broadcast results
        await manager.broadcast({
            "type": "workflow_completed",
            "results": {
                "waste_prevented": metrics.get("waste_prevented_kg", 0),
                "meals_donated": metrics.get("donation_meals", 0),
                "carbon_saved": metrics.get("carbon_saved_kg", 0),
                "actions_taken": len(state.get("final_actions", []))
            },
            "executive_summary": state.get("executive_summary", {}),
            "used_fallback": use_fallback
        })
        
    except Exception as e:
        logger.error(f"Error in workflow execution: {e}")
        await manager.broadcast({
            "type": "workflow_error",
            "error": str(e)
        })

async def broadcast_agent_messages(state: Dict):
    """Broadcast detailed agent messages to frontend"""
    try:
        # Broadcast demand agent results
        if state.get('demand_analysis'):
            await manager.broadcast({
                "type": "agent_message",
                "agent": "demand_agent",
                "message": f"Analyzed demand trends: {len(state['demand_analysis'].get('trending_up', []))} products trending up",
                "data": serialize_for_json({
                    "demand_analysis": state['demand_analysis']
                }),
                "timestamp": datetime.now().isoformat()
            })
        
        # Broadcast shelf life agent results
        if state.get('shelf_life_alerts'):
            critical_count = len([a for a in state['shelf_life_alerts'] if a.get('urgency') == 'critical'])
            await manager.broadcast({
                "type": "agent_message",
                "agent": "shelf_life_agent",
                "message": f"Found {len(state['shelf_life_alerts'])} products expiring soon ({critical_count} critical)",
                "data": serialize_for_json({
                    "shelf_life_alerts": state['shelf_life_alerts']
                }),
                "timestamp": datetime.now().isoformat()
            })
        
        # Broadcast inventory agent results
        if state.get('inventory_levels') or state.get('restock_recommendations'):
            await manager.broadcast({
                "type": "agent_message",
                "agent": "inventory_agent",
                "message": f"Inventory analysis complete: {len(state.get('restock_recommendations', []))} restock recommendations",
                "data": serialize_for_json({
                    "inventory_levels": state.get('inventory_levels', {}),
                    "restock_recommendations": state.get('restock_recommendations', [])
                }),
                "timestamp": datetime.now().isoformat()
            })
        
        # Broadcast pricing agent results
        if state.get('pricing_changes'):
            urgent_pricing = len([p for p in state['pricing_changes'] if p.get('effective_immediately')])
            await manager.broadcast({
                "type": "agent_message",
                "agent": "pricing_agent",
                "message": f"Generated {len(state['pricing_changes'])} pricing changes ({urgent_pricing} urgent)",
                "data": serialize_for_json({
                    "pricing_changes": state['pricing_changes']
                }),
                "timestamp": datetime.now().isoformat()
            })
        
        # Broadcast waste diversion agent results
        if state.get('diversion_actions'):
            donation_count = len([a for a in state['diversion_actions'] if a.get('diversion_type') == 'donation'])
            await manager.broadcast({
                "type": "agent_message",
                "agent": "waste_diversion_agent",
                "message": f"Planned {len(state['diversion_actions'])} diversion actions ({donation_count} donations)",
                "data": serialize_for_json({
                    "diversion_actions": state['diversion_actions']
                }),
                "timestamp": datetime.now().isoformat()
            })
        
        # Broadcast sustainability agent results
        if state.get('environmental_metrics'):
            metrics = state['environmental_metrics']
            await manager.broadcast({
                "type": "agent_message",
                "agent": "sustainability_agent",
                "message": f"Sustainability impact: {metrics.get('waste_prevented_kg', 0):.1f}kg waste prevented, {metrics.get('donation_meals', 0)} meals donated",
                "data": serialize_for_json({
                    "environmental_metrics": metrics
                }),
                "timestamp": datetime.now().isoformat()
            })
            
    except Exception as e:
        logger.error(f"Error broadcasting agent messages: {e}")

async def run_fallback_workflow(scenario: str) -> Dict:
    """Run a fallback workflow without LLM when API is rate limited"""
    from shared_state import initialize_state, AgentMode
    
    # Initialize state
    state = initialize_state(scenario, AgentMode.STREAMING)
    
    # Simulate agent outputs based on scenario
    if "heatwave" in scenario.lower():
        state['demand_analysis'] = {
            "trending_up": [
                {"product_id": 21, "name": "Bottled Water", "increase_pct": 0.6, "reason": "Heatwave demand"},
                {"product_id": 22, "name": "Sports Drinks", "increase_pct": 0.45, "reason": "Heatwave demand"},
                {"product_id": 23, "name": "Iced Tea", "increase_pct": 0.38, "reason": "Heatwave demand"}
            ],
            "trending_down": [
                {"product_id": 8, "name": "Frozen Pizza", "decrease_pct": 0.2, "reason": "Too hot for frozen foods"}
            ],
            "seasonal_impact": {"summer_products": "high", "frozen_foods": "low"},
            "external_factors": [
                {"factor": "heatwave", "impact": "beverages +40%", "categories_affected": ["beverages"]},
                {"factor": "temperature", "impact": "frozen foods -20%", "categories_affected": ["frozen"]}
            ],
            "recommendations": ["stock_up_on_beverages", "increase_cooling_products", "reduce_frozen_orders"]
        }
        
        state['shelf_life_alerts'] = [
            {
                "product_id": 21,
                "name": "Bottled Water",
                "urgency": "high",
                "quantity": 50,
                "days_until_expiry": 2,
                "value_at_risk": 99.50,
                "markdown_recommendation": 20
            },
            {
                "product_id": 22,
                "name": "Sports Drinks",
                "urgency": "medium",
                "quantity": 30,
                "days_until_expiry": 5,
                "value_at_risk": 134.70,
                "markdown_recommendation": 15
            }
        ]
        
        state['inventory_levels'] = {
            "critical_stock": [
                {"product_id": 21, "name": "Bottled Water", "current_stock": 50, "days_of_supply": 0.5},
                {"product_id": 22, "name": "Sports Drinks", "current_stock": 30, "days_of_supply": 1.2}
            ],
            "overstock": [
                {"product_id": 8, "name": "Frozen Pizza", "current_stock": 200, "days_of_supply": 25.0}
            ],
            "optimal": []
        }
        
        state['restock_recommendations'] = [
            {
                "product_id": 21,
                "name": "Bottled Water",
                "current_stock": 50,
                "recommended_order": 500,
                "urgency": "high",
                "reasoning": "Critical stock - 0.5 days supply"
            },
            {
                "product_id": 22,
                "name": "Sports Drinks",
                "current_stock": 30,
                "recommended_order": 300,
                "urgency": "medium",
                "reasoning": "Low stock - 1.2 days supply"
            }
        ]
        
        state['pricing_changes'] = [
            {
                "product_id": 21,
                "name": "Bottled Water",
                "current_price": 1.99,
                "new_price": 2.49,
                "discount_percentage": 0,
                "reason": "High demand during heatwave",
                "effective_immediately": True
            },
            {
                "product_id": 8,
                "name": "Frozen Pizza",
                "current_price": 6.99,
                "new_price": 5.59,
                "discount_percentage": 20,
                "reason": "Reduced demand during heatwave",
                "effective_immediately": False
            }
        ]
        
        state['diversion_actions'] = []
        state['environmental_metrics'] = {
            "waste_prevented_kg": 25.5,
            "carbon_saved_kg": 63.8,
            "donation_meals": 0,
            "water_saved_liters": 25500,
            "donation_rate": 0.0
        }
        
    elif "expiry" in scenario.lower():
        state['demand_analysis'] = {
            "trending_up": [],
            "trending_down": [],
            "external_factors": [],
            "recommendations": ["urgent_markdown", "donation_coordination"]
        }
        
        state['shelf_life_alerts'] = [
            {
                "product_id": 5,
                "name": "Greek Yogurt",
                "urgency": "critical",
                "quantity": 30,
                "days_until_expiry": 1,
                "value_at_risk": 134.70,
                "markdown_recommendation": 50
            },
            {
                "product_id": 6,
                "name": "Cheddar Cheese",
                "urgency": "high",
                "quantity": 25,
                "days_until_expiry": 2,
                "value_at_risk": 149.75,
                "markdown_recommendation": 30
            }
        ]
        
        state['inventory_levels'] = {
            "critical_stock": [],
            "overstock": [
                {"product_id": 5, "name": "Greek Yogurt", "current_stock": 30, "days_of_supply": 0.5},
                {"product_id": 6, "name": "Cheddar Cheese", "current_stock": 25, "days_of_supply": 1.0}
            ],
            "optimal": []
        }
        
        state['restock_recommendations'] = []
        
        state['pricing_changes'] = [
            {
                "product_id": 5,
                "name": "Greek Yogurt",
                "current_price": 4.49,
                "new_price": 2.25,
                "discount_percentage": 50,
                "reason": "Expires tomorrow",
                "effective_immediately": True
            },
            {
                "product_id": 6,
                "name": "Cheddar Cheese",
                "current_price": 5.99,
                "new_price": 4.19,
                "discount_percentage": 30,
                "reason": "Expires in 2 days",
                "effective_immediately": True
            }
        ]
        
        state['diversion_actions'] = [
            {
                "product_id": 5,
                "name": "Greek Yogurt",
                "quantity": 15,
                "diversion_type": "donation",
                "partner": "Local Food Bank",
                "retail_value": 67.35,
                "tax_benefit": 20.21
            },
            {
                "product_id": 6,
                "name": "Cheddar Cheese",
                "quantity": 10,
                "diversion_type": "donation",
                "partner": "Community Kitchen",
                "retail_value": 59.90,
                "tax_benefit": 17.97
            }
        ]
        
        state['environmental_metrics'] = {
            "waste_prevented_kg": 15.0,
            "carbon_saved_kg": 37.5,
            "donation_meals": 8,
            "water_saved_liters": 15000,
            "donation_rate": 0.8
        }
    
    else:
        # Generic fallback
        state['demand_analysis'] = {
            "trending_up": [
                {"product_id": 1, "name": "Organic Apples", "increase_pct": 0.15, "reason": "Seasonal demand"}
            ],
            "trending_down": [],
            "external_factors": [],
            "recommendations": ["monitor_trends"]
        }
        state['shelf_life_alerts'] = []
        state['inventory_levels'] = {"critical_stock": [], "overstock": [], "optimal": []}
        state['restock_recommendations'] = []
        state['pricing_changes'] = []
        state['diversion_actions'] = []
        state['environmental_metrics'] = {
            "waste_prevented_kg": 10.0,
            "carbon_saved_kg": 25.0,
            "donation_meals": 3,
            "water_saved_liters": 10000,
            "donation_rate": 0.0
        }
    
    state['final_actions'] = [
        {
            "action": "Execute pricing changes",
            "priority": 1,
            "details": f"{len(state.get('pricing_changes', []))} products to reprice"
        },
        {
            "action": "Process waste diversions", 
            "priority": 2,
            "details": f"{len(state.get('diversion_actions', []))} items for donation/recycling"
        }
    ]
    
    state['executive_summary'] = {
        "impact_summary": {
            "environmental": state['environmental_metrics'],
            "financial": {"revenue_impact": 0, "tax_benefits": 0},
            "social": {"people_fed": state['environmental_metrics'].get('donation_meals', 0) // 3}
        }
    }
    
    return state

# Demo control endpoints
@app.post("/demo/control")
async def control_demo(control: DemoControl):
    """Control demo execution"""
    if control.action == "start":
        # Clear previous demo data to avoid conflicts
        await clear_demo_data()
        
        demo_state.is_running = True
        demo_state.current_scenario = control.scenario
        
        # Automatically trigger webhook events for the selected scenario
        if control.scenario:
            await trigger_demo_events(control.scenario)
        
        return {
            "status": "started",
            "scenario": control.scenario,
            "message": "Demo started. Webhook events are being sent automatically."
        }
    
    elif control.action == "stop":
        demo_state.is_running = False
        return {"status": "stopped", "metrics": demo_state.metrics}
    
    elif control.action == "reset":
        demo_state.metrics = {
            "events_received": 0,
            "workflows_completed": 0,
            "total_waste_prevented": 0,
            "total_meals_donated": 0
        }
        return {"status": "reset", "message": "Demo metrics reset"}
    
    elif control.action == "status":
        return {
            "is_running": demo_state.is_running,
            "current_scenario": demo_state.current_scenario,
            "metrics": demo_state.metrics
        }

@app.post("/scenario/real-world")
async def generate_real_world_scenario(request: RealWorldScenarioRequest, background_tasks: BackgroundTasks):
    """Generate and execute a real-world scenario based on live data"""
    
    try:
        # Initialize scenario generator
        generator = RealWorldScenarioGenerator(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            db_url=os.getenv('NEON_DATABASE_URL')
        )
        
        # Generate real-world scenario
        scenario_data = await generator.generate_real_world_scenario(request.location)
        
        # Broadcast scenario generation
        await manager.broadcast({
            "type": "real_world_scenario_generated",
            "data": {
                "location": scenario_data.get('location', {}),
                "weather": scenario_data.get('weather', {}),
                "trending_topics": scenario_data.get('trending', {}).get('trending_topics', []),
                "generated_at": scenario_data.get('generated_at')
            }
        })
        
        # Process scenario in background
        background_tasks.add_task(process_real_world_scenario, scenario_data)
        
        return {
            "status": "generated",
            "scenario_id": f"real_world_{datetime.now().timestamp()}",
            "location": scenario_data.get('location', {}),
            "weather_summary": {
                "temperature": scenario_data.get('weather', {}).get('temperature'),
                "condition": scenario_data.get('weather', {}).get('condition')
            },
            "trending_topics": scenario_data.get('trending', {}).get('trending_topics', [])[:5],
            "data_sources": scenario_data.get('data_sources', [])
        }
        
    except Exception as e:
        logger.error(f"Error generating real-world scenario: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate real-world scenario: {str(e)}")

async def process_real_world_scenario(scenario_data: Dict[str, Any]):
    """Process real-world scenario through AI workflow"""
    
    try:
        # Extract scenario text
        scenario_text = scenario_data.get('scenario', '')
        
        # Trigger workflow with real-world scenario
        await trigger_workflow(scenario_text, "real_world_triggered", priority="high")
        
        # Update metrics
        demo_state.metrics["events_received"] += 1
        
        # Broadcast completion
        await manager.broadcast({
            "type": "real_world_scenario_completed",
            "scenario_data": scenario_data,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error processing real-world scenario: {e}")
        await manager.broadcast({
            "type": "error",
            "message": f"Failed to process real-world scenario: {str(e)}"
        })

async def clear_demo_data():
    """Clear demo-related data from database to avoid conflicts"""
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        logger.warning("NEON_DATABASE_URL not found, skipping database cleanup")
        return
    
    engine = create_engine(db_url)
    conn = None
    try:
        conn = engine.connect()
        # Clear ALL recent data to avoid conflicts
        conn.execute(text("""
            DELETE FROM market_conditions 
            WHERE recorded_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
        """))
        conn.execute(text("""
            DELETE FROM agent_decisions 
            WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
        """))
        # Reset sequences to avoid ID conflicts
        conn.execute(text("""
            SELECT setval('market_conditions_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM market_conditions))
        """))
        conn.execute(text("""
            SELECT setval('agent_decisions_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM agent_decisions))
        """))
        conn.commit()
        logger.info("Cleared previous demo data and reset sequences")
    except Exception as e:
        if conn is not None:
            conn.rollback()
        logger.error(f"Error clearing demo data: {e}")
        logger.info("Falling back to UPSERT approach for data insertion")
    finally:
        if conn is not None:
            conn.close()

async def trigger_demo_events(scenario: str):
    """Automatically trigger webhook events for demo scenarios"""
    import asyncio
    
    # Define demo events for each scenario
    demo_events = {
        "heatwave": [
            {
                "delay": 2,
                "type": "weather",
                "payload": {
                    "event_type": "weather_update",
                    "data": {
                        "temperature": 95,
                        "condition": "heatwave",
                        "humidity": 85,
                        "duration_hours": 72,
                        "location": "New York",
                        "affected_categories": ["beverages", "frozen", "dairy"]
                    }
                }
            },
            {
                "delay": 7,
                "type": "sales",
                "payload": {
                    "event_type": "sales_update",
                    "data": {
                        "category": "beverages",
                        "change_percentage": 40,
                        "trending_products": [
                            {"product_id": 21, "name": "Bottled Water", "increase": 60},
                            {"product_id": 22, "name": "Sports Drinks", "increase": 45},
                            {"product_id": 23, "name": "Iced Tea", "increase": 38}
                        ],
                        "time_window": "last_hour"
                    }
                }
            },
            {
                "delay": 12,
                "type": "inventory",
                "payload": {
                    "event_type": "inventory_alert",
                    "alert_level": "high",
                    "data": {
                        "alert_type": "low_stock",
                        "products": [
                            {
                                "product_id": 21,
                                "name": "Bottled Water",
                                "category": "beverages",
                                "current_stock": 50,
                                "recommended_reorder": 500
                            }
                        ],
                        "recommended_action": "urgent_reorder"
                    }
                }
            }
        ],
        "competitor": [
            {
                "delay": 2,
                "type": "competitor",
                "payload": {
                    "event_type": "competitor_action",
                    "data": {
                        "competitor": "MegaMart",
                        "action": "flash_sale",
                        "changes": [
                            {"category": "dairy", "discount": 0.4, "duration": "weekend"},
                            {"category": "produce", "discount": 0.25, "duration": "48_hours"}
                        ],
                        "market_impact": "high"
                    }
                }
            },
            {
                "delay": 10,
                "type": "sales",
                "payload": {
                    "event_type": "sales_update",
                    "data": {
                        "category": "dairy",
                        "change_percentage": -25,
                        "trending_products": [
                            {"product_id": 5, "name": "Whole Milk", "increase": -30},
                            {"product_id": 6, "name": "Greek Yogurt", "increase": -20}
                        ],
                        "time_window": "last_2_hours"
                    }
                }
            }
        ],
        "expiry": [
            {
                "delay": 2,
                "type": "inventory",
                "payload": {
                    "event_type": "inventory_alert",
                    "alert_level": "critical",
                    "data": {
                        "alert_type": "mass_expiry",
                        "products": [
                            {
                                "product_id": 5,
                                "name": "Whole Milk",
                                "category": "dairy",
                                "expires_in_hours": 24,
                                "quantity": 50,
                                "value_at_risk": 199.50
                            },
                            {
                                "product_id": 6,
                                "name": "Greek Yogurt",
                                "category": "dairy",
                                "expires_in_hours": 18,
                                "quantity": 30,
                                "value_at_risk": 134.70
                            }
                        ],
                        "total_value_at_risk": 334.20,
                        "recommended_action": "immediate_markdown_and_donation"
                    }
                }
            }
        ]
    }
    
    if scenario not in demo_events:
        logger.warning(f"Unknown scenario: {scenario}")
        return
    
    # Schedule events
    for event in demo_events[scenario]:
        asyncio.create_task(schedule_demo_event(event))
    
    logger.info(f"Scheduled {len(demo_events[scenario])} events for {scenario} scenario")

async def schedule_demo_event(event: dict):
    """Schedule a demo event to be sent after a delay"""
    await asyncio.sleep(event["delay"])
    
    if not demo_state.is_running:
        return  # Demo was stopped
    
    # Send the webhook event
    event_type = event["type"]
    payload = event["payload"]
    
    try:
        # Send to the appropriate endpoint
        if event_type == "weather":
            await process_weather_event(WeatherEvent(**payload))
        elif event_type == "sales":
            await process_sales_event(SalesEvent(**payload))
        elif event_type == "competitor":
            await process_competitor_event(CompetitorEvent(**payload))
        elif event_type == "inventory":
            await process_inventory_alert(InventoryAlert(**payload))
        
        # Update metrics
        demo_state.metrics["events_received"] += 1
        
        # Broadcast event to frontend
        await manager.broadcast({
            "type": "webhook_event",
            "event_type": event_type,
            "payload": serialize_for_json(payload),
            "timestamp": datetime.now().isoformat()
        })
        
        logger.info(f"Sent demo {event_type} event")
        
    except Exception as e:
        logger.error(f"Error sending demo event: {e}")

@app.get("/demo/metrics")
async def get_demo_metrics():
    """Get current demo metrics"""
    return {
        "timestamp": datetime.now().isoformat(),
        "metrics": demo_state.metrics,
        "is_running": demo_state.is_running
    }

# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial connection message
        await websocket.send_json({
            "type": "connected",
            "message": "Connected to Retail AI Webhook System",
            "timestamp": datetime.now().isoformat()
        })
        
        # Keep connection alive and wait for messages
        while True:
            try:
                # Wait for messages from client (with timeout)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=300.0)  # 5 minute timeout
                
                # Echo back or handle commands
                await websocket.send_json({
                    "type": "echo",
                    "data": data,
                    "timestamp": datetime.now().isoformat()
                })
                
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                try:
                    await websocket.send_json({
                        "type": "ping",
                        "timestamp": datetime.now().isoformat()
                    })
                except Exception:
                    # Connection lost, break out of loop
                    break
                    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected normally")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Retail AI Webhook Receiver",
        "workflow_ready": demo_state.workflow is not None,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)