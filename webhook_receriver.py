# webhook_receiver.py - FastAPI backend with webhook endpoints
from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime, date, timedelta
import asyncio
import json
import logging
import re
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from db_utils import execute_query_with_retry
from retail_workflow import RetailSustainabilityWorkflow
from shared_state import initialize_state, AgentMode
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from real_world_scenario_generator import RealWorldScenarioGenerator, integrate_real_world_scenario
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import hashlib
import secrets
from contextlib import asynccontextmanager
import pandas as pd
from veracity_engine import verify_asset_data
from arbitrage_engine import run_arbitrage_analysis
from schemas import (
    WeatherEvent, SalesEvent, CompetitorEvent, InventoryAlert, DemoControl,
    RealWorldScenarioRequest, UserSimulationRequest, WorkerRegistration,
    WorkerLogin, NotificationPreferences, WasteAssetEvent
)

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Debug: Check if .env file is loaded
logger.info(f"Environment variables check:")
logger.info(f"  SMTP_USERNAME: {os.getenv('SMTP_USERNAME', 'NOT_FOUND')}")
logger.info(f"  SMTP_PASSWORD: {'*' * len(os.getenv('SMTP_PASSWORD', '')) if os.getenv('SMTP_PASSWORD') else 'NOT_FOUND'}")
logger.info(f"  .env file path: {os.path.join(os.path.dirname(__file__), '.env')}")
logger.info(f"  .env file exists: {os.path.exists(os.path.join(os.path.dirname(__file__), '.env'))}")

@asynccontextmanager
async def lifespan(app):
    try:
        provider = os.getenv('LLM_PROVIDER', 'claude')
        logger.info(f"LLM_PROVIDER from env: {os.getenv('LLM_PROVIDER')}")
        logger.info(f"Using provider: {provider}")
        if provider == 'claude':
            api_key = os.getenv('ANTHROPIC_API_KEY')
            logger.info(f"ANTHROPIC_API_KEY found: {'Yes' if api_key else 'No'}")
        else:
            api_key = os.getenv('OPENAI_API_KEY')
            logger.info(f"OPENAI_API_KEY found: {'Yes' if api_key else 'No'}")
        demo_state.workflow = RetailSustainabilityWorkflow(
            api_key=api_key,
            db_url=os.getenv('NEON_DATABASE_URL'),
            provider=provider
        )
        logger.info(f"Workflow system initialized successfully with provider: {provider}")
        await ensure_workers_table()
        await update_database_webhook_config()
    except Exception as e:
        logger.error(f"Failed to initialize workflow: {e}")
    yield
    logger.info("Shutting down application...")

app = FastAPI(title="Retail AI Webhook Receiver", version="1.0.0", lifespan=lifespan)

# Configure CORS for Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[ "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
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
        self._lock = asyncio.Lock()  # Add thread safety

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            # Remove any existing connections from the same client
            self.active_connections = [conn for conn in self.active_connections 
                                     if conn.client_state.value == 1]  # Keep only open connections
            self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Safely disconnect websocket"""
        try:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
                logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
        except ValueError:
            # Connection already removed
            logger.debug("WebSocket already removed from connections")
        except Exception as e:
            logger.error(f"Error disconnecting websocket: {e}")

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        serialized_message = serialize_for_json(message)
        
        async with self._lock:
            connections_to_remove = []
            
            for connection in self.active_connections:
                try:
                    if connection.client_state.value == 1:  # WebSocketState.CONNECTED
                        await connection.send_json(serialized_message)
                    else:
                        connections_to_remove.append(connection)
                except Exception as e:
                    logger.error(f"Error broadcasting to websocket: {e}")
                    connections_to_remove.append(connection)
            
            # Remove closed connections
            for connection in connections_to_remove:
                try:
                    self.active_connections.remove(connection)
                except ValueError:
                    pass  # Already removed

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
        # NEW: store last workflow result for late-connecting clients
        self.last_workflow_result: Optional[Dict[str, Any]] = None

demo_state = DemoState()

# Email Service for Worker Notifications
class EmailService:
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.from_email = os.getenv('FROM_EMAIL', 'noreply@walmart-sustainability.com')
        
        # Debug: Log SMTP configuration
        logger.info(f"SMTP Configuration:")
        logger.info(f"  Server: {self.smtp_server}")
        logger.info(f"  Port: {self.smtp_port}")
        logger.info(f"  Username: {self.smtp_username}")
        logger.info(f"  Password: {'*' * len(self.smtp_password) if self.smtp_password else 'None'}")
        logger.info(f"  From Email: {self.from_email}")
        
    async def send_email(self, to_email: str, subject: str, html_content: str, text_content: str = None):
        """Send email using SMTP"""
        try:
            if not self.smtp_username or not self.smtp_password:
                logger.warning("SMTP credentials not configured, skipping email")
                return False
                
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = to_email
            msg['Subject'] = subject
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Add text content if provided
            if text_content:
                text_part = MIMEText(text_content, 'plain')
                msg.attach(text_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
                
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email to {to_email}: {e}")
            return False
    
    def create_critical_alert_email(self, worker_name: str, alert_data: dict) -> tuple:
        """Create critical alert email content"""
        subject = f"🚨 URGENT: {alert_data.get('alert_type', 'Inventory Alert')} - Action Required"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }}
                .container {{ max-width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #e74c3c, #c0392b); color: white; padding: 20px; border-radius: 8px 8px 0 0; margin: -20px -20px 20px -20px; }}
                .alert-box {{ background: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin: 15px 0; }}
                .action-box {{ background: #d4edda; border: 1px solid #c3e6cb; padding: 15px; border-radius: 5px; margin: 15px 0; }}
                .product-list {{ background: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                .button {{ background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0; }}
                .footer {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚨 Critical Alert</h1>
                    <p>Walmart Sustainability AI System</p>
                </div>
                
                <p>Hello <strong>{worker_name}</strong>,</p>
                
                <div class="alert-box">
                    <h3>⚠️ Immediate Action Required</h3>
                    <p><strong>Alert Type:</strong> {alert_data.get('alert_type', 'Inventory Alert')}</p>
                    <p><strong>Priority:</strong> {alert_data.get('priority', 'Critical')}</p>
                    <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="action-box">
                    <h3>🎯 Required Actions</h3>
                    <ul>
                        <li>Review affected products immediately</li>
                        <li>Coordinate with team members</li>
                        <li>Execute recommended actions</li>
                        <li>Update status in dashboard</li>
                    </ul>
                </div>
                
                <h3>📦 Affected Products</h3>
                <div class="product-list">
                    {self._format_products_html(alert_data.get('products', []))}
                </div>
                
                <p><strong>Total Value at Risk:</strong> ${alert_data.get('total_value_at_risk', 0):.2f}</p>
                <p><strong>Recommended Action:</strong> {alert_data.get('recommended_action', 'Immediate review required')}</p>
                
                <a href="http://localhost:3000/dashboard" class="button">View Dashboard</a>
                
                <div class="footer">
                    <p>This is an automated alert from the Walmart Sustainability AI System.</p>
                    <p>If you have questions, contact your supervisor immediately.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_content = f"""
CRITICAL ALERT - Action Required

Hello {worker_name},

URGENT: {alert_data.get('alert_type', 'Inventory Alert')} detected.

Priority: {alert_data.get('priority', 'Critical')}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

REQUIRED ACTIONS:
- Review affected products immediately
- Coordinate with team members  
- Execute recommended actions
- Update status in dashboard

Affected Products: {len(alert_data.get('products', []))} items
Total Value at Risk: ${alert_data.get('total_value_at_risk', 0):.2f}
Recommended Action: {alert_data.get('recommended_action', 'Immediate review required')}

View Dashboard: http://localhost:3000/dashboard

This is an automated alert from the Walmart Sustainability AI System.
        """
        
        return subject, html_content, text_content
    
    def create_daily_summary_email(self, worker_name: str, summary_data: dict) -> tuple:
        """Create daily summary email content"""
        subject = f"📊 Daily Sustainability Summary - {datetime.now().strftime('%Y-%m-%d')}"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }}
                .container {{ max-width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ background: linear-gradient(135deg, #27ae60, #2ecc71); color: white; padding: 20px; border-radius: 8px 8px 0 0; margin: -20px -20px 20px -20px; }}
                .metric-box {{ background: #f8f9fa; border: 1px solid #dee2e6; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .metric {{ display: flex; justify-content: space-between; margin: 5px 0; }}
                .positive {{ color: #28a745; }}
                .negative {{ color: #dc3545; }}
                .button {{ background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0; }}
                .footer {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📊 Daily Summary</h1>
                    <p>Walmart Sustainability AI System - {datetime.now().strftime('%Y-%m-%d')}</p>
                </div>
                
                <p>Hello <strong>{worker_name}</strong>,</p>
                
                <p>Here's your daily sustainability performance summary:</p>
                
                <div class="metric-box">
                    <h3>🌱 Environmental Impact</h3>
                    <div class="metric">
                        <span>Waste Prevented:</span>
                        <span class="positive">{summary_data.get('waste_prevented_kg', 0):.1f} kg</span>
                    </div>
                    <div class="metric">
                        <span>Carbon Saved:</span>
                        <span class="positive">{summary_data.get('carbon_saved_kg', 0):.1f} kg CO₂</span>
                    </div>
                    <div class="metric">
                        <span>Meals Donated:</span>
                        <span class="positive">{summary_data.get('meals_donated', 0)} meals</span>
                    </div>
                    <div class="metric">
                        <span>Water Saved:</span>
                        <span class="positive">{summary_data.get('water_saved_liters', 0):.0f} liters</span>
                    </div>
                </div>
                
                <div class="metric-box">
                    <h3>📈 Operational Metrics</h3>
                    <div class="metric">
                        <span>Alerts Processed:</span>
                        <span>{summary_data.get('alerts_processed', 0)}</span>
                    </div>
                    <div class="metric">
                        <span>Actions Taken:</span>
                        <span>{summary_data.get('actions_taken', 0)}</span>
                    </div>
                    <div class="metric">
                        <span>Products Reviewed:</span>
                        <span>{summary_data.get('products_reviewed', 0)}</span>
                    </div>
                </div>
                
                <a href="http://localhost:3000/dashboard" class="button">View Full Dashboard</a>
                
                <div class="footer">
                    <p>Keep up the great work in building a sustainable future!</p>
                    <p>Walmart Sustainability AI Team</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return subject, html_content, ""
    
    def _format_products_html(self, products: list) -> str:
        """Format products list for HTML email"""
        if not products:
            return "<p>No specific products listed</p>"
        
        html = ""
        for product in products[:5]:  # Show top 5 products
            html += f"""
            <div style="border-bottom: 1px solid #eee; padding: 5px 0;">
                <strong>{product.get('name', 'Unknown Product')}</strong><br>
                Quantity: {product.get('quantity', 0)} | 
                Expires: {product.get('expires_in_hours', 'N/A')}h | 
                Value: ${product.get('value_at_risk', 0):.2f}
            </div>
            """
        
        if len(products) > 5:
            html += f"<p><em>... and {len(products) - 5} more products</em></p>"
        
        return html

# Initialize email service
email_service = EmailService()

# Database setup functions
async def ensure_workers_table():
    """Ensure workers table exists in database"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            logger.warning("Database URL not configured")
            return
            
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Create workers table if it doesn't exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS workers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(50) NOT NULL,
                    department VARCHAR(50) NOT NULL,
                    phone VARCHAR(20),
                    session_token VARCHAR(255),
                    notification_preferences JSONB DEFAULT '{"email_notifications": true, "critical_alerts": true, "daily_summaries": false, "weekly_reports": true, "inventory_alerts": true, "pricing_alerts": true, "waste_alerts": true}',
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create worker_activities table for tracking
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS worker_activities (
                    id SERIAL PRIMARY KEY,
                    worker_id INTEGER REFERENCES workers(id),
                    activity_type VARCHAR(50) NOT NULL,
                    activity_data JSONB,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create daily_impact_summary table for analytics
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_impact_summary (
                    id SERIAL PRIMARY KEY,
                    summary_date DATE NOT NULL UNIQUE,
                    waste_prevented_kg DECIMAL(10, 2) DEFAULT 0.0,
                    meals_donated INTEGER DEFAULT 0,
                    carbon_saved_kg DECIMAL(10, 2) DEFAULT 0.0,
                    revenue_recovered DECIMAL(10, 2) DEFAULT 0.0,
                    total_runs INTEGER DEFAULT 0,
                    revenue_by_category JSONB DEFAULT '{}'::jsonb,
                    diverted_value_by_category JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # Create function to update 'updated_at' timestamp
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                   NEW.updated_at = NOW();
                   RETURN NEW;
                END;
                $$ language 'plpgsql';
            """))

            # Create trigger for daily_impact_summary
            conn.execute(text("""
                DROP TRIGGER IF EXISTS update_daily_impact_summary_updated_at ON daily_impact_summary;
                CREATE TRIGGER update_daily_impact_summary_updated_at
                BEFORE UPDATE ON daily_impact_summary
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """))
            
            # Create function to merge JSONB objects by summing numeric values
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION jsonb_sum_values(a jsonb, b jsonb)
                RETURNS jsonb AS $$
                DECLARE
                    key text;
                    value_a numeric;
                    value_b numeric;
                    result jsonb := COALESCE(a, '{}'::jsonb); -- Use COALESCE for safety
                BEGIN
                    IF b IS NULL THEN
                        RETURN a;
                    END IF;
                    FOR key IN SELECT k FROM jsonb_object_keys(b) k LOOP
                        -- Ensure value is a valid numeric string before casting
                        IF (b->>key) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN
                            value_b := (b->>key)::numeric;
                            IF result ? key AND (result->>key) IS NOT NULL AND (result->>key) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN
                                value_a := (result->>key)::numeric;
                                result := jsonb_set(result, ARRAY[key], to_jsonb(value_a + value_b), true);
                            ELSE
                                result := jsonb_set(result, ARRAY[key], to_jsonb(value_b), true);
                            END IF;
                        END IF;
                    END LOOP;
                    RETURN result;
                END;
                $$ LANGUAGE plpgsql;
            """))

            # materials_ledger table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS materials_ledger (
                    id SERIAL PRIMARY KEY,
                    asset_id VARCHAR(255) NOT NULL,
                    material_type VARCHAR(100),
                    weight_kg DECIMAL(10, 2),
                    purity_percentage DECIMAL(5, 2),
                    quality_grade CHAR(1) NOT NULL,
                    is_verified BOOLEAN DEFAULT false,
                    verification_notes TEXT,
                    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    source_facility_id VARCHAR(100),
                    destination_facility_id VARCHAR(100),
                    status VARCHAR(50) -- e.g., 'available', 'in_transit', 'processed'
                )
            """))

            # pcr_needs table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pcr_needs (
                    id SERIAL PRIMARY KEY,
                    facility_id VARCHAR(100) NOT NULL,
                    material_type VARCHAR(100) NOT NULL,
                    required_kg DECIMAL(10, 2) NOT NULL,
                    required_purity DECIMAL(5, 2),
                    open_market_price_per_kg DECIMAL(10, 2),
                    needed_by_date DATE,
                    status VARCHAR(50) DEFAULT 'open', -- e.g., 'open', 'partially_filled', 'filled'
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """))

            # Add columns to materials_ledger if they don't exist
            conn.execute(text("""
                ALTER TABLE materials_ledger
                ADD COLUMN IF NOT EXISTS quality_grade CHAR(1),
                ADD COLUMN IF NOT EXISTS is_verified BOOLEAN DEFAULT false,
                ADD COLUMN IF NOT EXISTS verification_notes TEXT;
            """))

            # Add column to pcr_needs if it doesn't exist
            conn.execute(text("""
                ALTER TABLE pcr_needs
                ADD COLUMN IF NOT EXISTS open_market_price_per_kg DECIMAL(10, 2);
            """))

            # Create trigger for pcr_needs updated_at
            conn.execute(text("""
                DROP TRIGGER IF EXISTS update_pcr_needs_updated_at ON pcr_needs;
                CREATE TRIGGER update_pcr_needs_updated_at
                BEFORE UPDATE ON pcr_needs
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column();
            """))

            conn.commit()
            logger.info("Workers and analytics tables ensured")
        
    except Exception as e:
        logger.error(f"Error ensuring database tables: {e}")

async def update_database_webhook_config():
    """Update database to simulate webhook integration"""
    try:
        engine = create_engine(
            os.getenv("NEON_DATABASE_URL"),
            connect_args={"sslmode": "require"}
        )
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
                        "/webhook/events/inventory",
                        "/webhook/events/waste-asset-created"
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
        "data": event.model_dump()
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
        "data": event.model_dump()
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
        "data": event.model_dump()
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
                    "data": event.model_dump(),
        "alert_level": event.alert_level
    })
    
    background_tasks.add_task(process_inventory_alert, event)
    
    return {"status": "accepted", "event_id": f"inventory_{datetime.now().timestamp()}"}

@app.post("/webhook/events/waste-asset-created")
async def receive_waste_asset_event(event: WasteAssetEvent, background_tasks: BackgroundTasks):
    """Receives notification when a new recyclable waste asset is created."""
    logger.info(f"Received new waste asset event for assetId: {event.asset_id}")
    
    background_tasks.add_task(process_new_waste_asset, event)
    
    await manager.broadcast({
        "type": "webhook_received",
        "event": "waste_asset_created",
        "data": event.model_dump()
    })
    
    return {"status": "accepted", "assetId": event.asset_id}

@app.post("/synergia/analyze/{need_id}")
async def analyze_synergia_path(need_id: int):
    """Analyzes the cost-effectiveness of the Synergia path for a given need."""
    logger.info(f"Received analysis request for need ID: {need_id}")
    
    analysis_result = await run_arbitrage_analysis(need_id)
    
    await manager.broadcast({
        "type": "ANALYSIS_COMPLETE",
        "data": analysis_result
    })
    
    return analysis_result

# Event processing functions
async def process_weather_event(event: WeatherEvent):
    """Process weather event through AI workflow"""
    engine = None
    conn = None
    trans = None
    try:
        engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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
        print("Received Weather Data:", event.data)
        scenario = generate_scenario_from_weather(event.data)
        await trigger_workflow(scenario, "weather_triggered")
    except Exception as e:
        if trans is not None:
            try:
                trans.rollback()
            except Exception as rollback_error:
                logger.warning(f"Error rolling back transaction: {rollback_error}")
        logger.error(f"Error processing weather event: {e}")
        await manager.broadcast({
            "type": "error",
            "message": f"Failed to process weather event: {str(e)}"
        })
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")
        if engine is not None:
            try:
                engine.dispose()
            except Exception as dispose_error:
                logger.warning(f"Error disposing engine: {dispose_error}")

async def process_sales_event(event: SalesEvent):
    """Process sales event through AI workflow"""
    engine = None
    conn = None
    trans = None
    try:
        engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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
        if trans is not None:
            try:
                trans.rollback()
            except Exception as rollback_error:
                logger.warning(f"Error rolling back transaction: {rollback_error}")
        logger.error(f"Error processing sales event: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")
        if engine is not None:
            try:
                engine.dispose()
            except Exception as dispose_error:
                logger.warning(f"Error disposing engine: {dispose_error}")

async def process_competitor_event(event: CompetitorEvent):
    """Process competitor event through AI workflow"""
    engine = None
    conn = None
    trans = None
    try:
        engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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
        if trans is not None:
            try:
                trans.rollback()
            except Exception as rollback_error:
                logger.warning(f"Error rolling back transaction: {rollback_error}")
        logger.error(f"Error processing competitor event: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")
        if engine is not None:
            try:
                engine.dispose()
            except Exception as dispose_error:
                logger.warning(f"Error disposing engine: {dispose_error}")

async def process_inventory_alert(event: InventoryAlert):
    """Process inventory alert through AI workflow with worker notifications"""
    engine = None
    conn = None
    trans = None
    try:
        engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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
        
        # Send notifications to workers
        alert_data = {
            "alert_type": event.data.get("alert_type", "inventory_alert"),
            "priority": event.alert_level,
            "products": event.data.get("products", []),
            "total_value_at_risk": event.data.get("total_value_at_risk", 0),
            "recommended_action": event.data.get("recommended_action", "immediate review")
        }
        
        # Determine notification type based on alert level
        if event.alert_level in ["critical", "high"]:
            await notify_workers(alert_data, "critical")
        else:
            await notify_workers(alert_data, "inventory")
            
    except Exception as e:
        if trans is not None:
            try:
                trans.rollback()
            except Exception as rollback_error:
                logger.warning(f"Error rolling back transaction: {rollback_error}")
        logger.error(f"Error processing inventory alert: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_error:
                logger.warning(f"Error closing connection: {close_error}")
        if engine is not None:
            try:
                engine.dispose()
            except Exception as dispose_error:
                logger.warning(f"Error disposing engine: {dispose_error}")

async def process_new_waste_asset(event: WasteAssetEvent):
    """Processes a new waste asset by adding it to the materials ledger."""
    logger.info(f"Background task started for asset: {event.asset_id}")
    
    # Part A: Integrate the Veracity Engine
    verification_result = await verify_asset_data(event)
    logger.info(f"Asset {event.asset_id} verification result: {verification_result}")

    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            logger.error("Database URL not configured, cannot process waste asset.")
            return

        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        
        with engine.connect() as conn:
            query = text("""
                INSERT INTO materials_ledger (
                    asset_id, material_type, weight_kg, purity_percentage, quality_grade,
                    timestamp, source_facility_id, status, is_verified, verification_notes
                ) VALUES (
                    :asset_id, :material_type, :weight_kg, :purity_percentage, :quality_grade,
                    :timestamp, :source_facility_id, 'available', :is_verified, :verification_notes
                )
            """)
            conn.execute(query, {
                "asset_id": event.asset_id,
                "material_type": event.material_type,
                "weight_kg": event.weight_kg,
                "purity_percentage": event.purity_percentage,
                "quality_grade": event.quality_grade,
                "timestamp": event.timestamp,
                "source_facility_id": event.facility_id,
                "is_verified": verification_result['is_verified'],
                "verification_notes": verification_result['verification_notes']
            })
            conn.commit()
        logger.info(f"Successfully inserted asset {event.asset_id} into materials_ledger.")

    except Exception as e:
        logger.error(f"Error processing waste asset {event.asset_id}: {e}", exc_info=True)

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
    engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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
    engine = create_engine(os.getenv("NEON_DATABASE_URL"), connect_args={"sslmode": "require"})
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

async def trigger_workflow(scenario: str, trigger_source: str, priority: str = "medium", dry_run: bool = False) -> Dict:
    """Trigger the AI workflow with given scenario"""
    if not demo_state.workflow:
        logger.error("Workflow not initialized")
        return {}
    
    try:
        # Broadcast workflow start
        await manager.broadcast({
            "type": "workflow_started",
            "scenario": scenario,
            "trigger": trigger_source,
            "priority": priority
        })
        
        # Check if we should use fallback due to API limits or timeouts
        use_fallback = False
        provider = os.getenv('LLM_PROVIDER', 'claude')
        
        try:
            # Try a simple LLM call to check if configured API is available with timeout
            if provider == 'claude':
                from langchain_anthropic import ChatAnthropic
                test_llm = ChatAnthropic(
                    model="claude-3-haiku-20240307",
                    anthropic_api_key=os.getenv('ANTHROPIC_API_KEY'),
                    temperature=0.1
                )
            else:
                test_llm = ChatOpenAI(
                    model="gpt-4o-mini",
                    openai_api_key=os.getenv('OPENAI_API_KEY'),
                    temperature=0.1
                )
            
            # Use asyncio.wait_for to add timeout
            import asyncio
            await asyncio.wait_for(
                asyncio.to_thread(test_llm.invoke, [HumanMessage(content="test")]),
                timeout=10.0  # 10 second timeout
            )
        except (asyncio.TimeoutError, Exception) as e:
            if ("quota" in str(e).lower() or "429" in str(e) or "401" in str(e) or 
                "timeout" in str(e).lower() or "dns" in str(e).lower()):
                use_fallback = True
                logger.warning(f"{provider.title()} API issue detected ({str(e)[:100]}), using fallback workflow")
        
        if use_fallback:
            # Use fallback workflow without LLM
            print("Using fallback workflow")
            state = await run_fallback_workflow(scenario)
        else:
            # Run normal workflow with real-time broadcasting
            state = await run_workflow_with_realtime_updates(scenario)
        
        # Extract metrics
        metrics = state.get("environmental_metrics", {})
        demo_state.metrics["total_waste_prevented"] += metrics.get("waste_prevented_kg", 0)
        demo_state.metrics["total_meals_donated"] += metrics.get("donation_meals", 0)
        demo_state.metrics["workflows_completed"] += 1
        
        # NEW: Update the daily analytics summary table (only if not a dry run)
        if not dry_run:
            await update_daily_summary(state)

        # Broadcast results
        completion_message = {
            "type": "workflow_completed",
            "results": {
                "waste_prevented": metrics.get("waste_prevented_kg", 0),
                "meals_donated": metrics.get("donation_meals", 0),
                "carbon_saved": metrics.get("carbon_saved_kg", 0),
                "actions_taken": len(state.get("final_actions", []))
            },
            "executive_summary": state.get("executive_summary", {}),
            "used_fallback": use_fallback
        }

        # Save for future WebSocket clients
        demo_state.last_workflow_result = completion_message

        # Broadcast as usual
        await manager.broadcast(completion_message)
        
        return state
    except Exception as e:
        logger.error(f"Error in workflow execution: {e}")
        await manager.broadcast({
            "type": "workflow_error",
            "error": str(e)
        })
        return {"error": str(e)}

async def update_daily_summary(state: dict):
    """
    UPSERT daily sustainability metrics into the summary table.
    This function is called after each workflow run to aggregate results.
    """
    try:
        logger.info("[update_daily_summary] Attempting to update daily impact summary...")
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            logger.warning("[update_daily_summary] Database not configured, skipping summary update.")
            return

        # 1. Extract core metrics
        metrics = state.get("environmental_metrics", {})
        waste_prevented = metrics.get("waste_prevented_kg", 0)
        meals_donated = metrics.get("donation_meals", 0)
        carbon_saved = metrics.get("carbon_saved_kg", 0)
        logger.info(f"[update_daily_summary] Extracted metrics: waste={waste_prevented}, meals={meals_donated}, carbon={carbon_saved}")


        # 2. Collect product IDs to fetch categories
        product_ids = set()
        for change in state.get('pricing_changes', []):
            if 'product_id' in change:
                product_ids.add(change['product_id'])
        for action in state.get('diversion_actions', []):
            if 'product_id' in action:
                product_ids.add(action['product_id'])
        logger.info(f"[update_daily_summary] Found {len(product_ids)} product IDs for category lookup.")

        product_info = {}
        if product_ids:
            from db_utils import create_robust_engine
            engine = create_robust_engine(db_url)
            with engine.connect() as conn:
                query = text("SELECT id, category, unit_price FROM products WHERE id = ANY(:ids)")
                result = conn.execute(query, {'ids': list(product_ids)})
                for row in result:
                    product_info[row[0]] = {'category': row[1], 'unit_price': float(row[2])}
            logger.info(f"[update_daily_summary] Fetched info for {len(product_info)} products from DB.")


        # 3. Calculate revenue and diverted value by category
        revenue_by_category = {}
        total_revenue_recovered = 0
        for change in state.get('pricing_changes', []):
            p_id = change.get('product_id')
            if p_id and p_id in product_info:
                category = product_info[p_id]['category']
                quantity = change.get('quantity', 1)  # Assume 1 if not present
                recovered = change.get('new_price', 0) * quantity
                total_revenue_recovered += recovered
                revenue_by_category[category] = revenue_by_category.get(category, 0) + recovered

        diverted_by_category = {}
        for action in state.get('diversion_actions', []):
            p_id = action.get('product_id')
            if p_id and p_id in product_info:
                category = product_info[p_id]['category']
                quantity = action.get('quantity', 0)
                # Use retail_value from action if available, else estimate
                diverted_value = action.get('retail_value', product_info[p_id]['unit_price'] * quantity)
                diverted_by_category[category] = diverted_by_category.get(category, 0) + diverted_value
        
        logger.info(f"[update_daily_summary] Calculated revenue by category: {revenue_by_category}")
        logger.info(f"[update_daily_summary] Calculated diverted value by category: {diverted_by_category}")

        # 4. Perform the UPSERT operation - FIXED SQL SYNTAX
        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            query = text("""
                INSERT INTO daily_impact_summary (
                    summary_date, waste_prevented_kg, meals_donated, carbon_saved_kg, 
                    revenue_recovered, total_runs, revenue_by_category, diverted_value_by_category
                )
                VALUES (
                    :summary_date, :waste, :meals, :carbon, :revenue, 1, CAST(:rev_cat AS jsonb), CAST(:div_cat AS jsonb)
                )
                ON CONFLICT (summary_date) DO UPDATE SET
                    waste_prevented_kg = daily_impact_summary.waste_prevented_kg + EXCLUDED.waste_prevented_kg,
                    meals_donated = daily_impact_summary.meals_donated + EXCLUDED.meals_donated,
                    carbon_saved_kg = daily_impact_summary.carbon_saved_kg + EXCLUDED.carbon_saved_kg,
                    revenue_recovered = daily_impact_summary.revenue_recovered + EXCLUDED.revenue_recovered,
                    total_runs = daily_impact_summary.total_runs + 1,
                    revenue_by_category = jsonb_sum_values(daily_impact_summary.revenue_by_category, EXCLUDED.revenue_by_category),
                    diverted_value_by_category = jsonb_sum_values(daily_impact_summary.diverted_value_by_category, EXCLUDED.diverted_value_by_category),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            logger.info("[update_daily_summary] Executing UPSERT query...")
            conn.execute(query, {
                'summary_date': date.today(),
                'waste': waste_prevented,
                'meals': meals_donated,
                'carbon': carbon_saved,
                'revenue': total_revenue_recovered,
                'rev_cat': json.dumps(revenue_by_category),
                'div_cat': json.dumps(diverted_by_category)
            })
            conn.commit()
        logger.info("Successfully updated daily impact summary.")

    except Exception as e:
        logger.error(f"Error updating daily impact summary: {e}", exc_info=True)

async def broadcast_agent_messages(agent_name: str, state: Dict):
    """Broadcast a specific agent's message to the frontend."""
    try:
        # Broadcast demand agent results
        if agent_name == "demand_agent" and state.get('demand_analysis'):
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
        elif agent_name == "shelf_life_agent" and state.get('shelf_life_alerts'):
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
        elif agent_name == "inventory_agent" and (state.get('inventory_levels') or state.get('restock_recommendations')):
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
        elif agent_name == "pricing_agent" and state.get('pricing_changes'):
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
        elif agent_name == "waste_diversion_agent" and state.get('diversion_actions'):
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
        elif agent_name == "sustainability_agent" and state.get('environmental_metrics'):
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
        logger.error(f"Error broadcasting agent messages for {agent_name}: {e}")

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

async def run_workflow_with_realtime_updates(scenario: str) -> dict:
    """Run the workflow and broadcast after each agent step."""
    import asyncio
    from shared_state import AgentMode, initialize_state
    
    workflow = demo_state.workflow
    state = initialize_state(scenario, AgentMode.STREAMING)
    
    # List of agent steps in order
    agent_steps = [
        ("demand_agent", workflow.demand_node),
        ("shelf_life_agent", workflow.shelf_life_node),
        ("inventory_agent", workflow.inventory_node),
        ("pricing_agent", workflow.pricing_node),
        ("waste_diversion_agent", workflow.waste_diversion_node),
        ("sustainability_agent", workflow.sustainability_node),
    ]
    
    for agent_name, agent_func in agent_steps:
        state = agent_func(state)
        # Broadcast the specific message for the agent that just ran.
        await broadcast_agent_messages(agent_name, state)
        await manager.broadcast({
            "type": "agent_step_completed",
            "agent": agent_name,
            "timestamp": datetime.now().isoformat()
        })
        await asyncio.sleep(0.1)  # Small delay for UI
    
    # Final summary (no additional broadcast_agent_messages call to avoid duplication)
    state = workflow.final_summary_node(state)
    return state

# Analytics endpoints for sustainability dashboard
@app.get("/analytics/impact-summary")
async def get_impact_summary(time_range: str = "30d"):
    """Get aggregated sustainability impact metrics for KPI cards."""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            time_filter_clause = f"summary_date >= NOW() - INTERVAL '{time_range.replace('d', '')} days'"

            query = text(f"""
                SELECT
                    SUM(waste_prevented_kg) as waste_prevented_kg,
                    SUM(meals_donated) as meals_donated,
                    SUM(carbon_saved_kg) as carbon_saved_kg,
                    SUM(revenue_recovered) as revenue_recovered
                FROM daily_impact_summary
                WHERE {time_filter_clause}
            """)
            
            result = conn.execute(query).fetchone()

            if result:
                return {
                    "waste_prevented_kg": round(float(result.waste_prevented_kg or 0), 2),
                    "meals_donated": int(result.meals_donated or 0),
                    "carbon_saved_kg": round(float(result.carbon_saved_kg or 0), 2),
                    "revenue_recovered": round(float(result.revenue_recovered or 0), 2)
                }
            else:
                return {
                    "waste_prevented_kg": 0,
                    "meals_donated": 0,
                    "carbon_saved_kg": 0,
                    "revenue_recovered": 0
                }

    except Exception as e:
        logger.error(f"Error getting impact summary: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get impact summary: {str(e)}")


@app.get("/analytics/time-series")
async def get_time_series_data(time_range: str = "30d"):
    """Provides time-series data for waste, meals, and carbon saved."""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url: raise HTTPException(status_code=500, detail="Database not configured")

        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            time_filter_clause = f"summary_date >= NOW() - INTERVAL '{time_range.replace('d', '')} days'"
            
            query = text(f"""
                SELECT
                    summary_date as date,
                    waste_prevented_kg as waste_prevented,
                    meals_donated,
                    carbon_saved_kg as carbon_saved
                FROM daily_impact_summary
                WHERE {time_filter_clause}
                ORDER BY date;
            """)
            
            results = conn.execute(query).fetchall()
            
            # Format for JSON response
            time_series = [
                {
                    "date": row.date.strftime('%Y-%m-%d'),
                    "waste_prevented": float(row.waste_prevented or 0),
                    "meals_donated": int(row.meals_donated or 0),
                    "carbon_saved": float(row.carbon_saved or 0)
                }
                for row in results
            ]
            return time_series

    except Exception as e:
        logger.error(f"Error in time-series endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/revenue-recovery")
async def get_revenue_recovery(time_range: str = "30d"):
    """Provides revenue recovery and diverted value data by product category."""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url: raise HTTPException(status_code=500, detail="Database not configured")

        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            time_filter_clause = f"summary_date >= NOW() - INTERVAL '{time_range.replace('d', '')} days'"

            query = text(f"""
                SELECT 
                    revenue_by_category,
                    diverted_value_by_category
                FROM daily_impact_summary
                WHERE {time_filter_clause};
            """)

            results = conn.execute(query).fetchall()
            
            from collections import defaultdict
            category_data = defaultdict(lambda: {'recovered': 0, 'diverted': 0})

            for row in results:
                rev_by_cat = row.revenue_by_category or {}
                div_by_cat = row.diverted_value_by_category or {}
                
                for cat, val in rev_by_cat.items():
                    category_data[cat]['recovered'] += float(val)
                
                for cat, val in div_by_cat.items():
                    category_data[cat]['diverted'] += float(val)

            # Format for response
            response_data = [
                {"category": cat, "recovered": round(values['recovered'], 2), "diverted": round(values['diverted'], 2)} 
                for cat, values in category_data.items()
            ]
            return response_data

    except Exception as e:
        logger.error(f"Error in revenue-recovery endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Analytics endpoints for sustainability dashboard
@app.get("/analytics/impact-log")
async def get_impact_log(page: int = 1, limit: int = 20):
    """Get a log of impactful agent decisions."""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            offset = (page - 1) * limit
            
            impactful_decision_types = [
                'markdown', 'clearance_pricing', 'donation', 
                'compost', 'recycle', 'calculate_impact_metrics'
            ]

            # Get impact log from agent decisions, filtering for impactful actions
            result = conn.execute(text(f"""
                SELECT 
                    id, agent_name, decision_type, decision_data, reasoning, created_at
                FROM agent_decisions 
                WHERE decision_type IN :decision_types
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """), {
                "decision_types": tuple(impactful_decision_types),
                "limit": limit,
                "offset": offset
            })
            
            impact_log = []
            for row in result:
                data = row[3]
                parsed = json.loads(data) if isinstance(data, str) else (data or {})
                
                # Calculate individual impact based on action type
                impact = {"co2_saved": 0.0, "meals_donated": 0, "waste_diverted": 0.0}
                quantity = parsed.get('quantity', 0)
                
                if row.decision_type == 'calculate_impact_metrics':
                    impact['co2_saved'] = parsed.get("carbon_saved_kg", 0)
                    impact['meals_donated'] = parsed.get("donation_meals", 0)
                    impact['waste_diverted'] = parsed.get("waste_prevented_kg", 0)
                elif row.decision_type in ['markdown', 'clearance_pricing']:
                    waste = quantity * 0.5  # Heuristic from sustainability agent
                    impact['waste_diverted'] = round(waste, 2)
                    impact['co2_saved'] = round(waste * 2.5, 2)
                elif row.decision_type in ['donation', 'compost', 'recycle']:
                    waste = quantity * 0.5  # Heuristic from sustainability agent
                    impact['waste_diverted'] = round(waste, 2)
                    impact['co2_saved'] = round(waste * 2.5, 2)
                    if row.decision_type == 'donation':
                        impact['meals_donated'] = int(quantity * 2.5)
                
                impact_log.append({
                    "id": row[0],
                    "agent": row[1],
                    "action_type": row[2],
                    "details": parsed,
                    "reasoning": row[4],
                    "timestamp": row[5].isoformat() if row[5] else None,
                    "sustainability_impact": impact
                })
            
            # Get total count for pagination with the same filter
            count_result = conn.execute(text("""
                SELECT COUNT(*) FROM agent_decisions WHERE decision_type IN :decision_types
            """), {"decision_types": tuple(impactful_decision_types)})
            total_count = count_result.fetchone()[0]
            
            return {
                "page": page,
                "limit": limit,
                "total_count": total_count,
                "total_pages": (total_count + limit - 1) // limit,
                "impact_log": impact_log,
                "generated_at": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error getting impact log: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get impact log: {str(e)}")

@app.get("/analytics/donation-breakdown")
async def get_donation_breakdown(time_range: str = "30d"):
    """Get donation statistics by partner and category"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Calculate time filter
            if time_range == "7d":
                time_filter = "CURRENT_TIMESTAMP - INTERVAL '7 days'"
            elif time_range == "30d":
                time_filter = "CURRENT_TIMESTAMP - INTERVAL '30 days'"
            elif time_range == "90d":
                time_filter = "CURRENT_TIMESTAMP - INTERVAL '90 days'"
            else:
                time_filter = "CURRENT_TIMESTAMP - INTERVAL '30 days'"
            
            # Get donation breakdown from agent decisions
            result = conn.execute(text(f"""
                SELECT 
                    decision_data,
                    COUNT(*) as donation_count
                FROM agent_decisions 
                WHERE decision_type = 'donation' 
                AND created_at > {time_filter}
                GROUP BY decision_data
                ORDER BY donation_count DESC
            """))
            
            donations = []
            for row in result:
                data = json.loads(row[0]) if row[0] else {}
                partner = data.get("donation_partner", "Unknown Partner")
                category = data.get("product_category", "General")
                quantity = int(data.get("quantity", 0))
                donations.append({
                    "partner": partner,
                    "category": category,
                    "donation_count": row[1],
                    "total_quantity": quantity,
                    "estimated_meals": quantity * 5
                })
            
            # Calculate totals
            total_donations = sum(d["donation_count"] for d in donations)
            total_meals = sum(d["estimated_meals"] for d in donations)
            
            return {
                "time_range": time_range,
                "summary": {
                    "total_donations": total_donations,
                    "total_meals_estimated": total_meals,
                    "unique_partners": len(set(d["partner"] for d in donations)),
                    "categories_covered": len(set(d["category"] for d in donations))
                },
                "breakdown": donations,
                "generated_at": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error getting donation breakdown: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get donation breakdown: {str(e)}")

@app.get("/analytics/store-comparison")
async def get_store_comparison():
    """Compare sustainability performance across stores"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Get store performance metrics (using synthetic data for now)
            # In real implementation, this would query store-specific data
            
            # Generate synthetic store comparison data
            stores = [
                {
                    "store_id": "NYC001",
                    "store_name": "New York Downtown",
                    "co2_saved_kg": 45.2,
                    "meals_donated": 180,
                    "waste_diverted_kg": 125.5,
                    "efficiency_score": 85,
                    "donation_partners": 3
                },
                {
                    "store_id": "LA002", 
                    "store_name": "Los Angeles West",
                    "co2_saved_kg": 38.7,
                    "meals_donated": 142,
                    "waste_diverted_kg": 98.3,
                    "efficiency_score": 78,
                    "donation_partners": 2
                },
                {
                    "store_id": "CHI003",
                    "store_name": "Chicago Central", 
                    "co2_saved_kg": 52.1,
                    "meals_donated": 210,
                    "waste_diverted_kg": 145.8,
                    "efficiency_score": 92,
                    "donation_partners": 4
                }
            ]
            
            # Calculate rankings
            for store in stores:
                store["rank"] = 0
                store["performance_percentile"] = 0
            
            # Rank by efficiency score
            stores.sort(key=lambda x: x["efficiency_score"], reverse=True)
            for i, store in enumerate(stores):
                store["rank"] = i + 1
                store["performance_percentile"] = round(((len(stores) - i) / len(stores)) * 100, 1)
            
            return {
                "comparison_period": "Last 30 days",
                "total_stores": len(stores),
                "stores": stores,
                "generated_at": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error getting store comparison: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get store comparison: {str(e)}")

@app.get("/analytics/impact-log")
async def get_impact_log(page: int = 1, limit: int = 50):
    """Get detailed impact log with pagination"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Calculate offset for pagination
            offset = (page - 1) * limit
            
            # Get impact log from agent decisions
            result = conn.execute(text(f"""
                SELECT 
                    id,
                    agent_name,
                    decision_type,
                    decision_data,
                    reasoning,
                    created_at
                FROM agent_decisions 
                ORDER BY created_at DESC
                LIMIT {limit} OFFSET {offset}
            """))
            
            impact_log = []
            for row in result:
                data = row[3]
                if isinstance(data, dict):
                    parsed = data
                elif isinstance(data, str):
                    try:
                        parsed = json.loads(data)
                    except Exception:
                        parsed = {}
                else:
                    parsed = {}
                impact_log.append({
                    "id": row[0],
                    "agent": row[1],
                    "action_type": row[2],
                    "details": parsed,
                    "reasoning": row[4],
                    "timestamp": row[5].isoformat() if row[5] else None,
                    "sustainability_impact": {
                        "co2_saved": parsed.get("co2_saved_kg", 0),
                        "meals_donated": parsed.get("meals_donated", 0),
                        "waste_diverted": parsed.get("waste_diverted_kg", 0)
                    }
                })
            
            # Get total count for pagination
            count_result = conn.execute(text("SELECT COUNT(*) FROM agent_decisions"))
            total_count = count_result.fetchone()[0]
            
            return {
                "page": page,
                "limit": limit,
                "total_count": total_count,
                "total_pages": (total_count + limit - 1) // limit,
                "impact_log": impact_log,
                "generated_at": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error getting impact log: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get impact log: {str(e)}")

# Enhanced Worker Management Endpoints
@app.post("/workers/register")
async def register_worker(worker_data: WorkerRegistration):
    """Register a new worker with enhanced validation"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        # Hash password
        password_hash = hashlib.sha256(worker_data.password.encode()).hexdigest()
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Check if worker already exists
            result = conn.execute(text("""
                SELECT id FROM workers WHERE email = :email
            """), {"email": worker_data.email})
            
            if result.fetchone():
                raise HTTPException(status_code=400, detail="Worker with this email already exists")
            
            # Insert new worker
            result = conn.execute(text("""
                INSERT INTO workers (name, email, password_hash, role, department, phone, created_at)
                VALUES (:name, :email, :password_hash, :role, :department, :phone, CURRENT_TIMESTAMP)
                RETURNING id
            """), {
                "name": worker_data.name,
                "email": worker_data.email, 
                "password_hash": password_hash,
                "role": worker_data.role,
                "department": worker_data.department,
                "phone": worker_data.phone
            })
            
            worker_id = result.fetchone()[0]
            
            # Log activity
            conn.execute(text("""
                INSERT INTO worker_activities (worker_id, activity_type, activity_data)
                VALUES (:worker_id, 'registration', :data)
            """), {
                "worker_id": worker_id,
                "data": json.dumps({"role": worker_data.role, "department": worker_data.department})
            })
            
            conn.commit()
            
            # Send welcome email
            welcome_subject = f"Welcome to Walmart Sustainability AI System, {worker_data.name}!"
            welcome_html = f"""
            <!DOCTYPE html>
            <html>
            <head><style>body{{font-family:Arial,sans-serif;margin:20px;}}</style></head>
            <body>
                <h2>Welcome to the Team! 🎉</h2>
                <p>Hello <strong>{worker_data.name}</strong>,</p>
                <p>Welcome to the Walmart Sustainability AI System! You've been registered as a <strong>{worker_data.role}</strong> in the <strong>{worker_data.department}</strong> department.</p>
                <p>You'll receive notifications about:</p>
                <ul>
                    <li>Critical inventory alerts</li>
                    <li>Pricing optimization opportunities</li>
                    <li>Waste diversion actions</li>
                    <li>Sustainability impact reports</li>
                </ul>
                <p>Access your dashboard at: <a href="http://localhost:3000/dashboard">Dashboard</a></p>
                <p>Best regards,<br>Walmart Sustainability AI Team</p>
            </body>
            </html>
            """
            
            await email_service.send_email(
                worker_data.email, 
                welcome_subject, 
                welcome_html
            )
            
            return {
                "status": "success",
                "worker_id": worker_id,
                "message": "Worker registered successfully. Welcome email sent.",
                "worker": {
                    "id": worker_id,
                    "name": worker_data.name,
                    "email": worker_data.email,
                    "role": worker_data.role,
                    "department": worker_data.department
                }
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering worker: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to register worker: {str(e)}")

@app.post("/workers/login")
async def login_worker(credentials: WorkerLogin):
    """Authenticate worker and return session token"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        # Hash password for comparison
        password_hash = hashlib.sha256(credentials.password.encode()).hexdigest()
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, name, email, role, department, notification_preferences
                FROM workers 
                WHERE email = :email AND password_hash = :password_hash AND is_active = true
            """), {
                "email": credentials.email,
                "password_hash": password_hash
            })
            
            worker = result.fetchone()
            if not worker:
                raise HTTPException(status_code=401, detail="Invalid credentials or inactive account")
            
            # Debug: log the worker data
            logger.info(f"Worker data: {worker}")
            logger.info(f"Notification preferences type: {type(worker[5])}, value: {worker[5]}")
            
            # Generate session token
            session_token = secrets.token_urlsafe(32)
            
            # Update session and activity
            conn.execute(text("""
                UPDATE workers 
                SET session_token = :token, last_login = CURRENT_TIMESTAMP, last_activity = CURRENT_TIMESTAMP
                WHERE id = :id
            """), {
                "token": session_token,
                "id": worker[0]
            })
            
            # Log login activity
            conn.execute(text("""
                INSERT INTO worker_activities (worker_id, activity_type, activity_data)
                VALUES (:worker_id, 'login', :data)
            """), {
                "worker_id": worker[0],
                "data": json.dumps({"timestamp": datetime.now().isoformat()})
            })
            
            conn.commit()
            
            # Handle notification preferences robustly
            notification_prefs = {}
            if worker[5]:
                if isinstance(worker[5], dict):
                    notification_prefs = worker[5]
                elif isinstance(worker[5], str):
                    try:
                        notification_prefs = json.loads(worker[5])
                    except Exception as e:
                        logger.warning(f"Error parsing notification preferences string: {e}")
                        notification_prefs = {}
                else:
                    logger.warning(f"Unknown type for notification_prefs: {type(worker[5])}")
                    notification_prefs = {}
            
            return {
                "status": "success",
                "session_token": session_token,
                "worker": {
                    "id": worker[0],
                    "name": worker[1],
                    "email": worker[2],
                    "role": worker[3],
                    "department": worker[4],
                    "notification_preferences": notification_prefs
                }
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error logging in worker: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to login: {str(e)}")

@app.get("/workers/me")
async def get_worker_profile(session_token: str):
    """Get current worker profile with enhanced data"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, name, email, role, department, phone, notification_preferences, 
                       created_at, last_login, last_activity
                FROM workers WHERE session_token = :token AND is_active = true
            """), {"token": session_token})
            
            worker = result.fetchone()
            if not worker:
                raise HTTPException(status_code=401, detail="Invalid session token")
            
            # Get recent activities
            activities_result = conn.execute(text("""
                SELECT activity_type, activity_data, timestamp
                FROM worker_activities 
                WHERE worker_id = :worker_id 
                ORDER BY timestamp DESC 
                LIMIT 10
            """), {"worker_id": worker[0]})
            
            activities = []
            for activity in activities_result:
                activity_data = {}
                if activity[1]:
                    if isinstance(activity[1], dict):
                        activity_data = activity[1]
                    elif isinstance(activity[1], str):
                        try:
                            activity_data = json.loads(activity[1])
                        except Exception as e:
                            logger.warning(f"Error parsing activity_data string: {e}")
                            activity_data = {}
                    else:
                        logger.warning(f"Unknown type for activity_data: {type(activity[1])}")
                        activity_data = {}
                activities.append({
                    "type": activity[0],
                    "data": activity_data,
                    "timestamp": activity[2].isoformat() if activity[2] else None
                })
            # Handle notification preferences robustly
            notification_prefs = {}
            if worker[6]:
                if isinstance(worker[6], dict):
                    notification_prefs = worker[6]
                elif isinstance(worker[6], str):
                    try:
                        notification_prefs = json.loads(worker[6])
                    except Exception as e:
                        logger.warning(f"Error parsing notification preferences string: {e}")
                        notification_prefs = {}
                else:
                    logger.warning(f"Unknown type for notification_prefs: {type(worker[6])}")
                    notification_prefs = {}
            return {
                "id": worker[0],
                "name": worker[1],
                "email": worker[2],
                "role": worker[3],
                "department": worker[4],
                "phone": worker[5],
                "notification_preferences": notification_prefs,
                "created_at": worker[7].isoformat() if worker[7] else None,
                "last_login": worker[8].isoformat() if worker[8] else None,
                "last_activity": worker[9].isoformat() if worker[9] else None,
                "recent_activities": activities
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting worker profile: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {str(e)}")

@app.put("/workers/notifications")
async def update_notification_preferences(session_token: str, preferences: NotificationPreferences):
    """Update worker notification preferences"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Verify worker exists
            result = conn.execute(text("""
                SELECT id FROM workers WHERE session_token = :token AND is_active = true
            """), {"token": session_token})
            
            worker = result.fetchone()
            if not worker:
                raise HTTPException(status_code=401, detail="Invalid session token")
            
            # Update preferences
            conn.execute(text("""
                UPDATE workers 
                SET notification_preferences = :preferences, last_activity = CURRENT_TIMESTAMP
                WHERE id = :worker_id
            """), {
                "preferences": json.dumps(preferences.model_dump()),
                "worker_id": worker[0]
            })
            
            conn.commit()
            
            return {
                "status": "success",
                "message": "Notification preferences updated successfully",
                "preferences": preferences.model_dump()
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating notification preferences: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update preferences: {str(e)}")

@app.get("/workers/all")
async def get_all_workers(session_token: str):
    """Get all workers (admin only)"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            raise HTTPException(status_code=500, detail="Database not configured")
        
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Verify worker is admin
            result = conn.execute(text("""
                SELECT role FROM workers WHERE session_token = :token AND is_active = true
            """), {"token": session_token})
            
            worker = result.fetchone()
            if not worker or worker[0] not in ['manager', 'supervisor']:
                raise HTTPException(status_code=403, detail="Admin access required")
            
            # Get all workers
            workers_result = conn.execute(text("""
                SELECT id, name, email, role, department, is_active, created_at, last_login
                FROM workers 
                ORDER BY created_at DESC
            """))
            
            workers = []
            for w in workers_result:
                workers.append({
                    "id": w[0],
                    "name": w[1],
                    "email": w[2],
                    "role": w[3],
                    "department": w[4],
                    "is_active": w[5],
                    "created_at": w[6].isoformat() if w[6] else None,
                    "last_login": w[7].isoformat() if w[7] else None
                })
            
            return {
                "status": "success",
                "workers": workers,
                "total_count": len(workers)
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting all workers: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get workers: {str(e)}")

# Demo control endpoints
@app.get("/demo/database-scenarios")
async def get_database_scenarios():
    """Get available scenarios based on current database state"""
    scenarios = await generate_database_based_scenario()
    return scenarios

@app.post("/demo/control")
async def control_demo(control: DemoControl):
    """Control demo execution"""
    if control.action == "start":
        # We no longer clear previous demo data; keep cumulative results
        # await clear_demo_data()
        
        demo_state.is_running = True
        demo_state.current_scenario = control.scenario
        
        # Check if we should use database-based scenarios
        if control.scenario == "mixed" or not control.scenario:
            # Try to use database-based scenario first
            db_scenarios = await generate_database_based_scenario()
            if db_scenarios.get("available_scenarios") and not db_scenarios.get("error"):
                # Use the first (highest priority) database scenario
                selected_scenario = db_scenarios["available_scenarios"][0]
                scenario_text = selected_scenario["scenario_text"]
                
                # Broadcast demo start with database scenario
                await manager.broadcast({
                    "type": "demo_started",
                    "scenario": selected_scenario["title"],
                    "scenario_type": selected_scenario["type"],
                    "urgency": selected_scenario["urgency"],
                    "description": selected_scenario["description"],
                    "timestamp": datetime.now().isoformat()
                })
                
                # Trigger workflow directly with database scenario
                await trigger_workflow(scenario_text, "database_triggered", priority=selected_scenario["urgency"])
                
                return {
                    "status": "started",
                    "scenario": selected_scenario["title"],
                    "scenario_type": selected_scenario["type"],
                    "urgency": selected_scenario["urgency"],
                    "message": f"Demo started with database scenario: {selected_scenario['title']}",
                    "using_database_data": True
                }
        
        # Fallback to regular demo events for specific scenarios or if no database scenarios
        if control.scenario:
            await trigger_demo_events(control.scenario)
        
        return {
            "status": "started",
            "scenario": control.scenario or "mixed",
            "message": "Demo started. Webhook events are being sent automatically.",
            "using_database_data": False
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
    
    from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
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
    
    # Define demo events for each scenario - focused and controlled
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
        
        # If a workflow has already completed, send its result immediately
        if demo_state.last_workflow_result:
            await websocket.send_json(serialize_for_json(demo_state.last_workflow_result))
        
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

# Worker Notification Functions
async def notify_workers(alert_data: dict, alert_type: str = "critical"):
    """Send notifications to relevant workers based on alert type"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            logger.warning("Database not configured for notifications")
            return
            
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Determine which workers to notify based on alert type
            if alert_type == "critical":
                # Notify all active workers
                result = conn.execute(text("""
                    SELECT name, email, notification_preferences, role, department
                    FROM workers 
                    WHERE is_active = true AND notification_preferences->>'critical_alerts' = 'true'
                """))
            elif alert_type == "inventory":
                # Notify inventory and general workers
                result = conn.execute(text("""
                    SELECT name, email, notification_preferences, role, department
                    FROM workers 
                    WHERE is_active = true 
                    AND notification_preferences->>'inventory_alerts' = 'true'
                    AND department IN ('inventory', 'general')
                """))
            elif alert_type == "pricing":
                # Notify pricing and general workers
                result = conn.execute(text("""
                    SELECT name, email, notification_preferences, role, department
                    FROM workers 
                    WHERE is_active = true 
                    AND notification_preferences->>'pricing_alerts' = 'true'
                    AND department IN ('pricing', 'general')
                """))
            elif alert_type == "waste":
                # Notify waste management and general workers
                result = conn.execute(text("""
                    SELECT name, email, notification_preferences, role, department
                    FROM workers 
                    WHERE is_active = true 
                    AND notification_preferences->>'waste_alerts' = 'true'
                    AND department IN ('waste_management', 'general')
                """))
            else:
                # Default: notify general workers
                result = conn.execute(text("""
                    SELECT name, email, notification_preferences, role, department
                    FROM workers 
                    WHERE is_active = true 
                    AND department = 'general'
                """))
            
            workers = result.fetchall()
            
            # Send notifications
            for worker in workers:
                try:
                    if alert_type == "critical":
                        subject, html_content, text_content = email_service.create_critical_alert_email(
                            worker[0], alert_data
                        )
                    else:
                        # Create generic alert email
                        subject = f"📢 {alert_type.title()} Alert - Action Required"
                        html_content = f"""
                        <!DOCTYPE html>
                        <html>
                        <head><style>body{{font-family:Arial,sans-serif;margin:20px;}}</style></head>
                        <body>
                            <h2>Alert Notification</h2>
                            <p>Hello {worker[0]},</p>
                            <p>A {alert_type} alert has been triggered in the Walmart Sustainability AI System.</p>
                            <p><strong>Alert Details:</strong></p>
                            <ul>
                                <li>Type: {alert_data.get('alert_type', 'Unknown')}</li>
                                <li>Priority: {alert_data.get('priority', 'Medium')}</li>
                                <li>Products Affected: {len(alert_data.get('products', []))}</li>
                            </ul>
                            <p>Please review the dashboard for more details.</p>
                            <p>Best regards,<br>Walmart Sustainability AI Team</p>
                        </body>
                        </html>
                        """
                        text_content = ""
                    
                    await email_service.send_email(worker[1], subject, html_content, text_content)
                    
                    # Log notification sent
                    conn.execute(text("""
                        INSERT INTO worker_activities (worker_id, activity_type, activity_data)
                        VALUES ((SELECT id FROM workers WHERE email = :email), 'notification_sent', :data)
                    """), {
                        "email": worker[1],
                        "data": json.dumps({
                            "alert_type": alert_type,
                            "notification_type": "email",
                            "timestamp": datetime.now().isoformat()
                        })
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to send notification to {worker[1]}: {e}")
            
            conn.commit()
            logger.info(f"Sent {alert_type} notifications to {len(workers)} workers")
            
    except Exception as e:
        logger.error(f"Error sending worker notifications: {e}")

async def send_daily_summaries():
    """Send daily summary emails to workers who have opted in"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            return
            
        from db_utils import create_robust_engine; engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            # Get workers who want daily summaries
            result = conn.execute(text("""
                SELECT name, email, notification_preferences
                FROM workers 
                WHERE is_active = true AND notification_preferences->>'daily_summaries' = 'true'
            """))
            
            workers = result.fetchall()
            
            # Get today's metrics
            today_metrics = {
                "waste_prevented_kg": demo_state.metrics["total_waste_prevented"],
                "carbon_saved_kg": demo_state.metrics["total_waste_prevented"] * 2.5,  # Estimate
                "meals_donated": demo_state.metrics["total_meals_donated"],
                "water_saved_liters": demo_state.metrics["total_waste_prevented"] * 100,  # Estimate
                "alerts_processed": demo_state.metrics["events_received"],
                "actions_taken": demo_state.metrics["workflows_completed"],
                "products_reviewed": demo_state.metrics["events_received"] * 5  # Estimate
            }
            
            # Send summaries
            for worker in workers:
                try:
                    subject, html_content, text_content = email_service.create_daily_summary_email(
                        worker[0], today_metrics
                    )
                    await email_service.send_email(worker[1], subject, html_content, text_content)
                except Exception as e:
                    logger.error(f"Failed to send daily summary to {worker[1]}: {e}")
                    
    except Exception as e:
        logger.error(f"Error sending daily summaries: {e}")

# Demo endpoint to trigger daily summaries
@app.post("/demo/send-daily-summaries")
async def trigger_daily_summaries():
    """Trigger daily summary emails (for demo purposes)"""
    await send_daily_summaries()
    return {
        "status": "success",
        "message": "Daily summaries sent to eligible workers"
    }

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Retail AI Webhook Receiver",
        "workflow_ready": demo_state.workflow is not None,
        "timestamp": datetime.now().isoformat()
    }

async def generate_database_based_scenario() -> Dict[str, Any]:
    """Generate realistic scenarios based on current database state"""
    try:
        db_url = os.getenv("NEON_DATABASE_URL")
        if not db_url:
            return {"error": "Database not configured"}
        
        from db_utils import create_robust_engine
        engine = create_robust_engine(db_url)
        
        with engine.connect() as conn:
            # Get expiring products
            expiring_query = """
            SELECT p.name, p.category, i.quantity, i.expiry_date,
                   (i.quantity * p.unit_price) as value_at_risk,
                   (i.expiry_date - CURRENT_DATE) as days_until_expiry
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.expiry_date <= CURRENT_DATE + INTERVAL '5 days'
            AND i.quantity > 0
            ORDER BY i.expiry_date, value_at_risk DESC
            LIMIT 10
            """
            expiring_result = conn.execute(text(expiring_query))
            expiring_products = [dict(row._mapping) for row in expiring_result.fetchall()]
            
            # Get low stock items
            low_stock_query = """
            SELECT p.name, p.category, SUM(i.quantity) as total_stock
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity > 0
            GROUP BY p.id, p.name, p.category
            HAVING SUM(i.quantity) < 100
            ORDER BY total_stock
            LIMIT 5
            """
            low_stock_result = conn.execute(text(low_stock_query))
            low_stock_products = [dict(row._mapping) for row in low_stock_result.fetchall()]
            
            # Get category distribution
            category_query = """
            SELECT p.category, COUNT(*) as product_count, SUM(i.quantity) as total_inventory
            FROM inventory i
            JOIN products p ON i.product_id = p.id
            WHERE i.quantity > 0
            GROUP BY p.category
            ORDER BY total_inventory DESC
            """
            category_result = conn.execute(text(category_query))
            categories = [dict(row._mapping) for row in category_result.fetchall()]
            
            # Generate scenario based on current state
            scenarios = []
            
            if expiring_products:
                total_expiring_value = sum(float(p.get('value_at_risk', 0) or 0) for p in expiring_products)
                critical_expiring = [p for p in expiring_products if (p.get('days_until_expiry') or 0) <= 2]
                
                scenarios.append({
                    "type": "expiry_management",
                    "title": f"Critical Expiry Alert: ${total_expiring_value:.2f} at Risk",
                    "description": f"{len(expiring_products)} products expiring soon ({len(critical_expiring)} critical)",
                    "urgency": "high" if critical_expiring else "medium",
                    "affected_products": len(expiring_products),
                    "value_at_risk": total_expiring_value,
                    "scenario_text": f"""
                    URGENT: Food Waste Prevention Required
                    
                    {len(expiring_products)} products are approaching expiration within 5 days, with a total value of ${total_expiring_value:.2f} at risk.
                    
                    Critical items ({len(critical_expiring)} expiring in ≤2 days):
                    {chr(10).join([f"- {p['name']} ({p['category']}): {p['quantity']} units, expires in {p.get('days_until_expiry', 0)} days, ${float(p.get('value_at_risk', 0) or 0):.2f} value" for p in critical_expiring[:3]])}
                    
                    Recommended actions: Immediate markdown pricing, donation coordination, and waste diversion planning.
                    """.strip()
                })
            
            if low_stock_products:
                scenarios.append({
                    "type": "inventory_shortage",
                    "title": f"Low Stock Alert: {len(low_stock_products)} Items Need Restocking",
                    "description": f"Multiple products running low on inventory",
                    "urgency": "medium",
                    "affected_products": len(low_stock_products),
                    "scenario_text": f"""
                    INVENTORY SHORTAGE DETECTED
                    
                    {len(low_stock_products)} products are running low on stock:
                    {chr(10).join([f"- {p['name']} ({p['category']}): {p['total_stock']} units remaining" for p in low_stock_products])}
                    
                    Risk of stockouts may lead to lost sales and customer dissatisfaction.
                    Immediate restock recommendations needed.
                    """.strip()
                })
            
            # Weather-based scenario using highest inventory category
            if categories:
                top_category = categories[0]
                weather_scenarios = {
                    'beverages': "Heatwave Expected: 98°F temperatures forecast for next 3 days",
                    'dairy': "Temperature fluctuations affecting cold storage efficiency",
                    'produce': "Seasonal demand spike for fresh fruits and vegetables",
                    'bakery': "Weekend rush expected for fresh baked goods",
                    'frozen': "Supply chain delays affecting frozen food deliveries"
                }
                
                weather_text = weather_scenarios.get(top_category['category'], "Seasonal demand changes expected")
                scenarios.append({
                    "type": "market_conditions",
                    "title": f"Market Conditions: {top_category['category'].title()} Category Impact",
                    "description": f"External factors affecting {top_category['category']} demand",
                    "urgency": "medium",
                    "affected_category": top_category['category'],
                    "scenario_text": f"""
                    MARKET CONDITIONS ALERT
                    
                    {weather_text}
                    
                    Your store has {top_category['total_inventory']} units of {top_category['category']} products in stock.
                    This represents your largest inventory category ({top_category['product_count']} different products).
                    
                    Demand forecasting and pricing optimization needed to maximize sales and minimize waste.
                    """.strip()
                })
            
            return {
                "available_scenarios": scenarios,
                "database_summary": {
                    "total_categories": len(categories),
                    "expiring_products": len(expiring_products),
                    "low_stock_items": len(low_stock_products),
                    "top_category": categories[0] if categories else None
                },
                "generated_at": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error generating database scenarios: {e}")
        return {"error": f"Failed to generate scenarios: {str(e)}"}

# NEW: User-facing simulation endpoint
@app.post("/demo/simulate")
async def simulate_scenario(request: UserSimulationRequest):
    """
    Accepts a natural language prompt from a user, parses it into a structured
    event, and runs a full "what-if" simulation without saving to the database.
    """
    logger.info(f"Received simulation request with prompt: '{request.prompt}'")
    
    if not demo_state.workflow:
        raise HTTPException(status_code=503, detail="Workflow engine not ready.")
        
    try:
        # 1. Create and invoke the parser agent
        parser_agent = demo_state.workflow.create_parser_agent()
        agent_response = await asyncio.to_thread(
            parser_agent.invoke, {"input": request.prompt}
        )
        
        # 2. Extract the output from the agent response
        logger.info(f"Parser agent output: {agent_response}")
        
        # The agent response is a dict with 'output' key
        output = agent_response.get("output", "")
        if isinstance(output, list):
            # If the list contains dicts, extract the 'text' content
            if output and isinstance(output[0], dict) and 'text' in output[0]:
                output_text = output[0]['text']
            else:
                # Otherwise, join whatever is in the list
                output_text = " ".join(map(str, output))
        elif isinstance(output, dict):
            output_text = json.dumps(output)
        else:
            output_text = str(output) # Ensure it's a string
        
        # 3. Extract JSON from the output text
        # The agent typically returns JSON in a markdown code block
        json_match = re.search(r'```json\s*(.*?)\s*```', output_text, re.DOTALL)
        
        if not json_match:
            # Try to find any JSON-like structure
            json_match = re.search(r'\{[^{}]*"event_type"[^{}]*\}', output_text, re.DOTALL)
        
        if not json_match:
            logger.warning(f"No JSON found in agent output: {output_text}")
            # Fallback: try to infer from the text
            parsed_event = {
                "event_type": "weather" if "weather" in output_text.lower() or "heatwave" in output_text.lower() else "generic",
                "event_data": {
                    "description": request.prompt,
                    "raw_output": output_text
                }
            }
        else:
            json_string = json_match.group(1) if '```' in json_match.group(0) else json_match.group(0)
            logger.info(f"Extracted JSON string: {json_string}")
            
            try:
                # Parse the JSON
                parsed_event = json.loads(json_string)
                logger.info(f"Successfully parsed event: {parsed_event}")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON: {e}")
                # Try to clean up common issues
                json_string = json_string.strip()
                # Remove trailing commas
                json_string = re.sub(r',\s*}', '}', json_string)
                json_string = re.sub(r',\s*]', ']', json_string)
                
                try:
                    parsed_event = json.loads(json_string)
                except:
                    logger.error(f"Could not parse cleaned JSON: {json_string}")
                    parsed_event = {
                        "event_type": "generic",
                        "event_data": {"description": request.prompt}
                    }
        
        # 4. Validate and extract event details
        event_type = parsed_event.get("event_type", "generic")
        event_data = parsed_event.get("event_data", {})
        
        logger.info(f"Event type: {event_type}, Event data: {event_data}")

        # 5. Generate a scenario string from the structured event data
        scenario_string = f"Simulating {event_type} event\n"
        
        if event_type == "weather":
            scenario_string = generate_scenario_from_weather(event_data)
        elif event_type == "sales" or event_type == "sales_update":
            scenario_string = generate_scenario_from_sales(event_data)
        elif event_type == "competitor":
            scenario_string = generate_scenario_from_competitor(event_data)
        elif event_type == "inventory":
            # The alert level is not in the what-if, so we default it
            scenario_string = generate_scenario_from_inventory(event_data, "medium")
        else:
            # Generic fallback for other event types
            scenario_string += f"Event details: {json.dumps(event_data, indent=2)}"

        logger.info(f"Generated scenario: {scenario_string}")

        # 6. Trigger the workflow in "dry run" mode
        logger.info("Triggering workflow in dry_run mode...")
        final_state = await trigger_workflow(
            scenario=scenario_string,
            trigger_source="simulation",
            priority="high",
            dry_run=True
        )
        
        # 7. Extract key results for response
        metrics = final_state.get("environmental_metrics", {})
        summary = final_state.get("executive_summary", {})
        
        simulation_result = {
            "status": "success",
            "simulation_id": f"sim_{datetime.now().timestamp()}",
            "original_prompt": request.prompt,
            "parsed_event": parsed_event,
            "scenario_description": scenario_string,
            "results": {
                "environmental_impact": {
                    "waste_prevented_kg": metrics.get("waste_prevented_kg", 0),
                    "carbon_saved_kg": metrics.get("carbon_saved_kg", 0),
                    "meals_donated": metrics.get("donation_meals", 0),
                    "water_saved_liters": metrics.get("water_saved_liters", 0)
                },
                "actions_taken": {
                    "pricing_changes": len(final_state.get("pricing_changes", [])),
                    "diversion_actions": len(final_state.get("diversion_actions", [])),
                    "restock_recommendations": len(final_state.get("restock_recommendations", []))
                },
                "financial_impact": summary.get("impact_summary", {}).get("financial", {}),
                "key_recommendations": final_state.get("final_actions", [])[:3]  # Top 3 recommendations
            },
            "full_analysis": {
                "demand_analysis": final_state.get("demand_analysis", {}),
                "shelf_life_alerts": final_state.get("shelf_life_alerts", [])[:5],  # Top 5 alerts
                "pricing_changes": final_state.get("pricing_changes", [])[:5],  # Top 5 changes
                "executive_summary": summary
            }
        }
        
        return simulation_result

    except Exception as e:
        logger.error(f"Error during simulation: {e}", exc_info=True)
        
        # Return a user-friendly error response
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "original_prompt": request.prompt,
            "suggestion": "Try rephrasing your scenario or being more specific about the event type (weather, competitor action, inventory alert, etc.)"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)