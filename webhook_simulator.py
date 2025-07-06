# webhook_simulator.py - Simulates external services sending webhooks
import asyncio
import aiohttp
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from colorama import init, Fore, Style
import os

# Initialize colorama for colored output
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WebhookSimulator:
    """Simulates external services sending webhooks to your system"""
    
    def __init__(self, target_base_url: str = "http://localhost:8000"):
        self.target_base_url = target_base_url
        self.is_running = False
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Define webhook endpoints
        self.endpoints = {
            "weather": f"{target_base_url}/webhook/events/weather",
            "sales": f"{target_base_url}/webhook/events/sales",
            "competitor": f"{target_base_url}/webhook/events/competitor",
            "inventory": f"{target_base_url}/webhook/events/inventory",
            "waste_asset_created": f"{target_base_url}/webhook/events/waste-asset-created"
        }
        
        # Predefined scenarios with timed events
        self.scenarios = {
            "heatwave": [
                {
                    "delay": 0,
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
                    "delay": 5,
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
                    "delay": 10,
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
                    "delay": 0,
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
                    "delay": 8,
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
                    "delay": 0,
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
                                },
                                {
                                    "product_id": 11,
                                    "name": "White Bread",
                                    "category": "bakery",
                                    "expires_in_hours": 12,
                                    "quantity": 25,
                                    "value_at_risk": 74.75
                                }
                            ],
                            "total_value_at_risk": 408.95,
                            "recommended_action": "immediate_markdown_and_donation"
                        }
                    }
                },
                {
                    "delay": 3,
                    "type": "weather",
                    "payload": {
                        "event_type": "weather_update",
                        "data": {
                            "temperature": 72,
                            "condition": "rainy",
                            "humidity": 90,
                            "duration_hours": 24,
                            "location": "New York",
                            "affected_categories": ["bakery", "produce"]
                        }
                    }
                }
            ],
            
            "recycling_stream": [
                {
                    "delay": 0,
                    "type": "waste_asset_created",
                    "payload": {
                        "assetId": "ASSET_PET_1689362001",
                        "facilityId": "FAC_001",
                        "materialType": "PET",
                        "weightKg": 150.5,
                        "purityPercentage": 98.5,
                        "quality_grade": "A"
                    }
                },
                {
                    "delay": 5,
                    "type": "waste_asset_created",
                    "payload": {
                        "assetId": "ASSET_HDPE_1689362006",
                        "facilityId": "FAC_001",
                        "materialType": "HDPE",
                        "weightKg": 220.0,
                        "purityPercentage": 95.0,
                        "quality_grade": "B"
                    }
                }
            ],
            
            "mixed": [
                # This scenario combines multiple events for a complex demo
                {
                    "delay": 0,
                    "type": "weather",
                    "payload": {
                        "event_type": "weather_update",
                        "data": {
                            "temperature": 92,
                            "condition": "hot_humid",
                            "humidity": 88,
                            "duration_hours": 48,
                            "location": "New York",
                            "affected_categories": ["beverages", "frozen", "produce"]
                        }
                    }
                },
                {
                    "delay": 3,
                    "type": "competitor",
                    "payload": {
                        "event_type": "competitor_action",
                        "data": {
                            "competitor": "FreshCo",
                            "action": "new_store_opening",
                            "changes": [
                                {"category": "produce", "discount": 0.3, "duration": "grand_opening_week"}
                            ],
                            "market_impact": "medium"
                        }
                    }
                },
                {
                    "delay": 6,
                    "type": "inventory",
                    "payload": {
                        "event_type": "inventory_alert",
                        "alert_level": "high",
                        "data": {
                            "alert_type": "multiple_expiries",
                            "products": [
                                {
                                    "product_id": 1,
                                    "name": "Organic Apples",
                                    "category": "produce",
                                    "expires_in_hours": 36,
                                    "quantity": 40,
                                    "value_at_risk": 159.60
                                }
                            ],
                            "total_value_at_risk": 159.60,
                            "recommended_action": "markdown_promotion"
                        }
                    }
                },
                {
                    "delay": 10,
                    "type": "sales",
                    "payload": {
                        "event_type": "sales_update",
                        "data": {
                            "category": "beverages",
                            "change_percentage": 55,
                            "trending_products": [
                                {"product_id": 24, "name": "Energy Drinks", "increase": 70},
                                {"product_id": 21, "name": "Bottled Water", "increase": 50}
                            ],
                            "time_window": "last_2_hours"
                        }
                    }
                }
            ]
        }
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    def print_banner(self, text: str, color=Fore.CYAN):
        """Print colored banner"""
        print(f"\n{color}{'='*60}")
        print(f"{color}{text.center(60)}")
        print(f"{color}{'='*60}{Style.RESET_ALL}\n")
        
    def print_event(self, event_type: str, endpoint: str, status: str):
        """Print event with color coding"""
        colors = {
            "weather": Fore.BLUE,
            "sales": Fore.GREEN,
            "competitor": Fore.YELLOW,
            "inventory": Fore.RED
        }
        
        color = colors.get(event_type, Fore.WHITE)
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        print(f"{Fore.LIGHTBLACK_EX}[{timestamp}] {color}[{event_type.upper()}]{Style.RESET_ALL} → {endpoint} {status}")
        
    async def send_webhook(self, event_type: str, payload: Dict) -> bool:
        """Send a single webhook to the target endpoint"""
        endpoint = self.endpoints[event_type]
        
        # Add timestamp if not present
        if "timestamp" not in payload:
            payload["timestamp"] = datetime.now().isoformat()
            
        try:
            async with self.session.post(
                endpoint,
                json=payload,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    self.print_event(
                        event_type, 
                        endpoint, 
                        f"{Fore.GREEN}✓ Success{Style.RESET_ALL} (ID: {result.get('event_id', 'N/A')})"
                    )
                    return True
                else:
                    self.print_event(
                        event_type,
                        endpoint,
                        f"{Fore.RED}✗ Failed{Style.RESET_ALL} (Status: {response.status})"
                    )
                    return False
                    
        except aiohttp.ClientError as e:
            self.print_event(
                event_type,
                endpoint,
                f"{Fore.RED}✗ Error{Style.RESET_ALL} ({str(e)})"
            )
            logger.error(f"Failed to send webhook to {endpoint}: {e}")
            return False
            
    async def run_scenario(self, scenario_name: str):
        """Run a predefined scenario"""
        if scenario_name not in self.scenarios:
            logger.error(f"Unknown scenario: {scenario_name}")
            return
            
        scenario = self.scenarios[scenario_name]
        self.print_banner(f"Running Scenario: {scenario_name.upper()}")
        
        print(f"Scenario contains {len(scenario)} events\n")
        
        # Execute events with delays
        for i, event in enumerate(scenario):
            if event["delay"] > 0:
                print(f"{Fore.LIGHTBLACK_EX}Waiting {event['delay']} seconds...{Style.RESET_ALL}")
                await asyncio.sleep(event["delay"])
                
            print(f"\n{Fore.CYAN}Event {i+1}/{len(scenario)}:{Style.RESET_ALL}")
            await self.send_webhook(event["type"], event["payload"])
            
        print(f"\n{Fore.GREEN}Scenario '{scenario_name}' completed!{Style.RESET_ALL}")
        
    async def run_continuous(self, interval: int = 30):
        """Run continuous random events"""
        self.print_banner("CONTINUOUS MODE", Fore.MAGENTA)
        print(f"Sending random events every {interval} seconds")
        print(f"{Fore.YELLOW}Press Ctrl+C to stop{Style.RESET_ALL}\n")
        
        event_count = 0
        self.is_running = True
        
        try:
            while self.is_running:
                # Generate random event
                event_type = random.choice(["weather", "sales", "competitor", "inventory", "waste_asset_created"])
                payload = self.generate_random_event(event_type)
                
                event_count += 1
                print(f"\n{Fore.MAGENTA}Random Event #{event_count}:{Style.RESET_ALL}")
                await self.send_webhook(event_type, payload)
                
                # Wait for next event
                print(f"{Fore.LIGHTBLACK_EX}Next event in {interval} seconds...{Style.RESET_ALL}")
                await asyncio.sleep(interval)
                
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Continuous mode stopped. Total events sent: {event_count}{Style.RESET_ALL}")
            
    def generate_random_event(self, event_type: str) -> Dict:
        """Generate a random event payload for a given event type"""
        if event_type == "weather":
            conditions = ["clear", "rainy", "stormy", "heatwave", "cold_snap", "cloudy"]
            condition = random.choice(conditions)
            temp = random.randint(30, 100)
            
            return {
                "event_type": "weather_update",
                "data": {
                    "temperature": temp,
                    "condition": condition,
                    "humidity": random.randint(30, 95),
                    "duration_hours": random.choice([24, 48, 72]),
                    "location": "New York",
                    "affected_categories": random.sample(
                        ["beverages", "frozen", "dairy", "produce", "bakery"], 
                        k=random.randint(2, 4)
                    )
                }
            }
            
        elif event_type == "sales":
            categories = ["beverages", "dairy", "produce", "bakery", "frozen"]
            category = random.choice(categories)
            change = random.randint(-50, 100)
            
            return {
                "event_type": "sales_update",
                "data": {
                    "category": category,
                    "change_percentage": change,
                    "trending_products": [
                        {
                            "product_id": random.randint(1, 50),
                            "name": f"Product {i}",
                            "increase": change + random.randint(-10, 10)
                        }
                        for i in range(random.randint(2, 4))
                    ],
                    "time_window": random.choice(["last_hour", "last_2_hours", "last_4_hours"])
                }
            }
            
        elif event_type == "competitor":
            competitors = ["MegaMart", "FreshCo", "ValueStore", "OrganicPlace"]
            actions = ["price_reduction", "flash_sale", "new_promotion", "clearance"]
            
            return {
                "event_type": "competitor_action",
                "data": {
                    "competitor": random.choice(competitors),
                    "action": random.choice(actions),
                    "changes": [
                        {
                            "category": cat,
                            "discount": round(random.uniform(0.1, 0.5), 2),
                            "duration": random.choice(["24_hours", "48_hours", "weekend", "week"])
                        }
                        for cat in random.sample(["dairy", "produce", "bakery"], k=random.randint(1, 3))
                    ],
                    "market_impact": random.choice(["low", "medium", "high"])
                }
            }
            
        elif event_type == "inventory":
            alert_types = ["low_stock", "mass_expiry", "overstock", "quality_issue"]
            alert_type = random.choice(alert_types)
            alert_level = random.choice(["low", "medium", "high", "critical"])
            
            return {
                "event_type": "inventory_alert",
                "alert_level": alert_level,
                "data": {
                    "alert_type": alert_type,
                    "products": [
                        {
                            "product_id": random.randint(1, 50),
                            "name": f"Product {i}",
                            "category": random.choice(["dairy", "produce", "bakery"]),
                            "expires_in_hours": random.randint(6, 72) if alert_type == "mass_expiry" else None,
                            "quantity": random.randint(10, 100),
                            "value_at_risk": round(random.uniform(50, 500), 2)
                        }
                        for i in range(random.randint(1, 5))
                    ],
                    "total_value_at_risk": round(random.uniform(100, 2000), 2),
                    "recommended_action": random.choice([
                        "immediate_markdown",
                        "donation_coordination",
                        "urgent_reorder",
                        "quality_check"
                    ])
                }
            }
            
        elif event_type == "waste_asset_created":
            material_type = random.choice(["PET", "HDPE", "LDPE", "PP", "PS"])
            asset_id = f"ASSET_{material_type}_{int(datetime.now().timestamp())}"
            facility_id = f"FAC_{random.randint(1, 5):03d}"
            return {
                "assetId": asset_id,
                "facilityId": facility_id,
                "materialType": material_type,
                "weightKg": round(random.uniform(50.0, 500.0), 2),
                "purityPercentage": round(random.uniform(85.0, 99.5), 2),
                "quality_grade": random.choice(["A", "B", "C"])
            }
            
        return {}
            
    async def check_backend_health(self) -> bool:
        """Check if the backend is running and healthy"""
        try:
            async with self.session.get(f"{self.target_base_url}/health") as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"{Fore.GREEN}✓ Backend is healthy{Style.RESET_ALL}")
                    print(f"  Service: {data.get('service')}")
                    print(f"  Workflow Ready: {data.get('workflow_ready')}")
                    return True
                else:
                    print(f"{Fore.RED}✗ Backend returned status {response.status}{Style.RESET_ALL}")
                    return False
        except Exception as e:
            print(f"{Fore.RED}✗ Cannot connect to backend: {e}{Style.RESET_ALL}")
            return False
            
    async def interactive_menu(self):
        """Interactive menu for the simulator"""
        self.print_banner("WEBHOOK SIMULATOR", Fore.CYAN)
        
        # Check backend health
        print("Checking backend connection...")
        if not await self.check_backend_health():
            print(f"\n{Fore.YELLOW}Warning: Backend may not be running.{Style.RESET_ALL}")
            print(f"Make sure to start the FastAPI backend on {self.target_base_url}")
            
        while True:
            print("\n" + "="*40)
            print("Select an option:")
            print("1. Run Heatwave Scenario")
            print("2. Run Competitor Flash Sale Scenario")
            print("3. Run Mass Expiry Scenario")
            print("4. Run Mixed Events Scenario")
            print("5. Run Recycling Stream Scenario")
            print("6. Send Custom Event")
            print("7. Run Continuous Random Simulation")
            print("0. Exit")
            print("="*40)
            
            choice = input("\nEnter your choice (0-7): ").strip()
            
            if choice == "0":
                print(f"\n{Fore.YELLOW}Exiting webhook simulator...{Style.RESET_ALL}")
                break
                
            elif choice == "1":
                await self.run_scenario("heatwave")
                
            elif choice == "2":
                await self.run_scenario("competitor")
                
            elif choice == "3":
                await self.run_scenario("expiry")
                
            elif choice == "4":
                await self.run_scenario("mixed")
                
            elif choice == "5":
                await self.run_scenario("recycling_stream")
                
            elif choice.lower() == "c":
                await self.custom_event_menu()
                
            elif choice.lower() == "r":
                interval = input("Enter interval between events (seconds) [30]: ").strip()
                interval = int(interval) if interval else 30
                await self.run_continuous(interval)
                
            else:
                print(f"{Fore.RED}Invalid choice. Please try again.{Style.RESET_ALL}")
                
    async def custom_event_menu(self):
        """Menu for sending custom events"""
        print("\n" + "-"*40)
        print("Custom Event Type:")
        print("1. Weather Event")
        print("2. Sales Event")
        print("3. Competitor Event")
        print("4. Inventory Alert")
        print("5. Waste Asset Created")
        print("0. Back to Main Menu")
        
        event_choice = input("\nSelect event type (0-5): ").strip()
        
        if event_choice == "0":
            return
            
        event_types = {
            "1": "weather",
            "2": "sales",
            "3": "competitor",
            "4": "inventory",
            "5": "waste_asset_created"
        }
        
        if event_choice in event_types:
            event_type = event_types[event_choice]
            payload = self.generate_random_event(event_type)
            
            print(f"\n{Fore.CYAN}Generated {event_type} event:{Style.RESET_ALL}")
            print(json.dumps(payload, indent=2))
            
            confirm = input("\nSend this event? (y/n): ").strip().lower()
            if confirm == 'y':
                await self.send_webhook(event_type, payload)
            else:
                print("Event cancelled.")

async def main():
    """Main function to run the simulator"""
    print("Waiting for backend to start...")
    await asyncio.sleep(5)  # Wait 5 seconds for the server to start
    async with WebhookSimulator() as simulator:
        if await simulator.check_backend_health():
            # await simulator.interactive_menu()
            print("Running the 'recycling_stream' scenario directly for testing.")
            await simulator.run_scenario("recycling_stream")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Simulator stopped by user{Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"Error in simulator: {e}")
        print(f"{Fore.RED}Error: {e}{Style.RESET_ALL}")