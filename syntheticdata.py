import random
import numpy as np
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Any
import json
from dataclasses import dataclass
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Product:
    name: str
    category: str
    sku: str
    unit_price: float
    supplier_id: int
    perishable: bool
    shelf_life_days: int

class RetailDataGenerator:
    def __init__(self, db_url: str = None):
        """Initialize the synthetic data generator with database connection"""
        self.db_url = db_url or os.getenv('NEON_DATABASE_URL')
        self.engine = create_engine(self.db_url) if self.db_url else None
        
        # Product catalog definitions - expanded for retail store
        self.product_catalog = {
            'produce': {
                'items': [
                    ('Organic Apples', 3.99, 5, True),
                    ('Bananas', 0.99, 7, True),
                    ('Roma Tomatoes', 2.49, 5, True),
                    ('Lettuce', 1.99, 7, True),
                    ('Strawberries', 4.99, 3, True)
                ],
                'demand_pattern': 'high_weekend',
                'price_volatility': 0.3,
                'spoilage_rate': 0.15
            },
            'dairy': {
                'items': [
                    ('Whole Milk', 3.99, 14, True),
                    ('Greek Yogurt', 4.49, 21, True),
                    ('Cheddar Cheese', 5.99, 30, True),
                    ('Butter', 4.29, 30, True),
                    ('Eggs (Dozen)', 3.99, 28, True)
                ],
                'demand_pattern': 'steady',
                'price_volatility': 0.1,
                'spoilage_rate': 0.08
            },
            'bakery': {
                'items': [
                    ('White Bread', 2.99, 3, True),
                    ('Croissants', 5.99, 2, True),
                    ('Bagels', 4.49, 5, True),
                    ('Donuts', 8.99, 2, True),
                    ('Birthday Cake', 19.99, 3, True)
                ],
                'demand_pattern': 'morning_peak',
                'price_volatility': 0.2,
                'spoilage_rate': 0.25
            },
            'beverages': {
                'items': [
                    ('Orange Juice', 4.99, 14, True),
                    ('Coca-Cola 12pk', 5.99, 365, False),
                    ('Bottled Water 24pk', 3.99, 730, False),
                    ('Coffee', 12.99, 180, False),
                    ('Energy Drinks', 7.99, 365, False)
                ],
                'demand_pattern': 'weather_dependent',
                'price_volatility': 0.15,
                'spoilage_rate': 0.05
            },
            'frozen': {
                'items': [
                    ('Frozen Pizza', 6.99, 365, False),
                    ('Ice Cream', 4.99, 180, False),
                    ('Frozen Vegetables', 2.99, 365, False),
                    ('Frozen Chicken', 8.99, 180, False),
                    ('French Fries', 2.99, 365, False)
                ],
                'demand_pattern': 'steady',
                'price_volatility': 0.05,
                'spoilage_rate': 0.02
            },
            'clothing': {
                'items': [
                    ('T-Shirts', 14.99, 9999, False),
                    ('Jeans', 39.99, 9999, False),
                    ('Socks (6-pack)', 9.99, 9999, False),
                    ('Winter Jacket', 89.99, 9999, False),
                    ('Running Shoes', 59.99, 9999, False)
                ],
                'demand_pattern': 'seasonal',
                'price_volatility': 0.4,
                'spoilage_rate': 0.0
            },
            'electronics': {
                'items': [
                    ('Phone Charger', 19.99, 9999, False),
                    ('Bluetooth Speaker', 49.99, 9999, False),
                    ('Headphones', 79.99, 9999, False),
                    ('Power Bank', 29.99, 9999, False),
                    ('Smart Watch', 199.99, 9999, False)
                ],
                'demand_pattern': 'steady',
                'price_volatility': 0.3,
                'spoilage_rate': 0.0
            },
            'stationery': {
                'items': [
                    ('Notebooks (5-pack)', 9.99, 9999, False),
                    ('Pens (12-pack)', 4.99, 9999, False),
                    ('Printer Paper', 12.99, 9999, False),
                    ('Markers Set', 14.99, 9999, False),
                    ('Calculator', 19.99, 9999, False)
                ],
                'demand_pattern': 'back_to_school',
                'price_volatility': 0.2,
                'spoilage_rate': 0.0
            },
            'personal_care': {
                'items': [
                    ('Shampoo', 8.99, 730, False),
                    ('Toothpaste', 4.99, 730, False),
                    ('Body Wash', 6.99, 730, False),
                    ('Deodorant', 5.99, 730, False),
                    ('Face Cream', 12.99, 365, False)
                ],
                'demand_pattern': 'steady',
                'price_volatility': 0.1,
                'spoilage_rate': 0.0
            },
            'household': {
                'items': [
                    ('Laundry Detergent', 15.99, 9999, False),
                    ('Paper Towels (6-pack)', 12.99, 9999, False),
                    ('Dish Soap', 3.99, 9999, False),
                    ('Trash Bags', 9.99, 9999, False),
                    ('All-Purpose Cleaner', 4.99, 9999, False)
                ],
                'demand_pattern': 'steady',
                'price_volatility': 0.1,
                'spoilage_rate': 0.0
            }
        }
        
        # Customer segments for sales patterns
        self.customer_segments = [
            'budget_conscious', 'premium_shopper', 'family', 
            'health_conscious', 'convenience_seeker'
        ]
        
        # Store locations
        self.store_locations = ['Aisle-1', 'Aisle-2', 'Aisle-3', 'Back-Storage', 'Front-Display']
        
        # Suppliers - updated for new categories
        self.suppliers = [
            'Fresh Farms Co', 'Dairy Direct', 'Artisan Bakehouse',
            'Beverage Distributors', 'Frozen Foods Inc', 'Fashion Hub',
            'Tech Suppliers', 'Office Depot', 'Beauty Distributors', 
            'Home Essentials'
        ]

    def generate_sku(self, product_name: str, index: int) -> str:
        """Generate unique SKU for products"""
        prefix = ''.join([word[0].upper() for word in product_name.split()[:3]])
        return f"{prefix}-{index:04d}"

    def generate_products(self) -> List[Dict]:
        """Generate complete product catalog"""
        products = []
        product_id = 1
        
        for category, details in self.product_catalog.items():
            supplier_id = self.suppliers.index(
                {'produce': 'Fresh Farms Co', 
                 'dairy': 'Dairy Direct',
                 'bakery': 'Artisan Bakehouse', 
                 'beverages': 'Beverage Distributors',
                 'frozen': 'Frozen Foods Inc',
                 'clothing': 'Fashion Hub',
                 'electronics': 'Tech Suppliers',
                 'stationery': 'Office Depot',
                 'personal_care': 'Beauty Distributors',
                 'household': 'Home Essentials'}[category]
            ) + 1
            
            for item in details['items']:
                name, price, shelf_life, perishable = item
                products.append({
                    'id': product_id,
                    'name': name,
                    'category': category,
                    'sku': self.generate_sku(name, product_id),
                    'unit_price': price,
                    'supplier_id': supplier_id,
                    'perishable': perishable,
                    'shelf_life_days': shelf_life
                })
                product_id += 1
                
        return products

    def generate_inventory_data(self, products: List[Dict], days_back: int = 7) -> List[Dict]:
        """Generate realistic inventory data with multiple batches - limited to 50 entries"""
        inventory = []
        inventory_id = 1
        
        # Limit to 50 inventory entries total
        max_entries = 50
        entries_per_product = max_entries // len(products)
        
        for product in products:
            # Limit batches per product to fit within 50 total entries
            if product['perishable']:
                num_batches = min(2, entries_per_product)
            else:
                num_batches = min(1, entries_per_product)
                
            category_details = self.product_catalog[product['category']]
            
            for batch in range(num_batches):
                # Quantity based on product type and demand
                base_quantity = random.randint(50, 200)
                if category_details['demand_pattern'] == 'high_weekend':
                    base_quantity = int(base_quantity * 1.3)
                elif product['category'] in ['electronics', 'clothing']:
                    base_quantity = random.randint(20, 80)  # Lower stock for high-value items
                
                # Expiry date calculation
                if product['perishable']:
                    days_until_expiry = random.randint(1, product['shelf_life_days'])
                    expiry_date = date.today() + timedelta(days=days_until_expiry)
                else:
                    # Non-perishables get far future date
                    expiry_date = date.today() + timedelta(days=product['shelf_life_days'])
                
                # Random location assignment
                location = random.choice(self.store_locations)
                
                inventory.append({
                    'id': inventory_id,
                    'product_id': product['id'],
                    'quantity': base_quantity,
                    'batch_number': f"BATCH-{product['sku']}-{datetime.now().strftime('%Y%m%d')}-{batch}",
                    'expiry_date': expiry_date.isoformat(),
                    'location': location,
                    'last_updated': datetime.now().isoformat()
                })
                inventory_id += 1
                
                # Stop if we reach 50 entries
                if len(inventory) >= max_entries:
                    break
                    
            if len(inventory) >= max_entries:
                break
                
        return inventory

    def generate_sales_history(self, products: List[Dict], days_back: int = 7) -> List[Dict]:
        """Generate realistic sales transaction history - limited to 50 entries"""
        sales = []
        sale_id = 1
        
        # Limit to 50 sales entries total
        max_entries = 50
        entries_per_day = max_entries // days_back
        
        # Generate sales for each day
        for day_offset in range(days_back):
            current_date = datetime.now() - timedelta(days=day_offset)
            
            # Limit transactions per day to fit within 50 total entries
            num_transactions = min(entries_per_day, random.randint(5, 10))
            
            for _ in range(num_transactions):
                product = random.choice(products)
                category_details = self.product_catalog[product['category']]
                
                # Time of day affects product selection
                hour = self._generate_transaction_hour(category_details['demand_pattern'])
                transaction_time = current_date.replace(
                    hour=hour, 
                    minute=random.randint(0, 59),
                    second=random.randint(0, 59)
                )
                
                # Quantity based on customer segment
                customer_segment = random.choice(self.customer_segments)
                quantity = self._get_purchase_quantity(customer_segment, product['category'])
                
                # Price with possible discount
                discount = self._calculate_discount(product, transaction_time)
                sale_price = product['unit_price'] * (1 - discount)
                
                sales.append({
                    'id': sale_id,
                    'product_id': product['id'],
                    'quantity': quantity,
                    'sale_price': round(sale_price, 2),
                    'transaction_time': transaction_time.isoformat(),
                    'customer_segment': customer_segment
                })
                sale_id += 1
                
                # Stop if we reach 50 entries
                if len(sales) >= max_entries:
                    break
                    
            if len(sales) >= max_entries:
                break
                
        return sales

    def generate_demand_forecasts(self, products: List[Dict], days_ahead: int = 7) -> List[Dict]:
        """Generate demand forecasts for next N days - limited to 50 entries"""
        forecasts = []
        forecast_id = 1
        
        # Limit to 50 forecast entries total
        max_entries = 50
        entries_per_product = max_entries // len(products)
        
        for product in products:
            # Limit forecast days per product to fit within 50 total entries
            forecast_days = min(days_ahead, entries_per_product)
            
            for day_offset in range(forecast_days):
                forecast_date = date.today() + timedelta(days=day_offset)
                
                # Base demand calculation
                base_demand = self._calculate_base_demand(product, forecast_date)
                
                # Add uncertainty
                confidence = random.uniform(0.7, 0.95)
                predicted_demand = int(base_demand * random.uniform(0.9, 1.1))
                
                # Factors affecting demand
                factors = {
                    'day_of_week': forecast_date.strftime('%A'),
                    'weather_impact': random.choice(['positive', 'neutral', 'negative']),
                    'promotion_active': random.choice([True, False]),
                    'competitor_activity': random.choice(['none', 'minor', 'major']),
                    'seasonal_factor': self._get_seasonal_factor(product['category'], forecast_date)
                }
                
                forecasts.append({
                    'id': forecast_id,
                    'product_id': product['id'],
                    'forecast_date': forecast_date.isoformat(),
                    'predicted_demand': predicted_demand,
                    'confidence_level': round(confidence, 2),
                    'factors': json.dumps(factors)
                })
                forecast_id += 1
                
                # Stop if we reach 50 entries
                if len(forecasts) >= max_entries:
                    break
                    
            if len(forecasts) >= max_entries:
                break
                
        return forecasts

    def generate_pricing_history(self, products: List[Dict], days_back: int = 7) -> List[Dict]:
        """Generate pricing change history - limited to 50 entries"""
        pricing_history = []
        price_id = 1
        
        # Limit to 50 pricing entries total
        max_entries = 50
        entries_per_product = max_entries // len(products)
        
        for product in products:
            # Limit price changes per product to fit within 50 total entries
            num_changes = min(2, entries_per_product)
            
            for i in range(num_changes):
                days_ago = random.randint(0, days_back)
                change_time = datetime.now() - timedelta(days=days_ago)
                
                # Price change reasons
                reasons = [
                    'Competitor price match',
                    'Expiry date approaching',
                    'Seasonal adjustment',
                    'Promotion',
                    'Supply cost change',
                    'Demand spike',
                    'Inventory clearance',
                    'Weekend special'
                ]
                
                # Calculate new price
                base_price = product['unit_price']
                if 'clearance' in reasons[0] or 'Expiry' in reasons[0]:
                    discount = random.uniform(0.2, 0.5)
                else:
                    discount = random.uniform(0, 0.3)
                
                new_price = base_price * (1 - discount)
                
                pricing_history.append({
                    'id': price_id,
                    'product_id': product['id'],
                    'price': round(new_price, 2),
                    'discount_percentage': round(discount * 100, 1),
                    'reason': random.choice(reasons),
                    'effective_time': change_time.isoformat()
                })
                price_id += 1
                
                # Stop if we reach 50 entries
                if len(pricing_history) >= max_entries:
                    break
                    
            if len(pricing_history) >= max_entries:
                break
                
        return pricing_history

    def generate_market_conditions(self, days_back: int = 7) -> List[Dict]:
        """Generate various market condition events - limited to 50 entries"""
        conditions = []
        condition_id = 1
        
        # Limit to 50 market condition entries total
        max_entries = 50
        
        # Weather conditions
        weather_patterns = [
            {'type': 'heatwave', 'temp': 95, 'impact': 'increased_beverage_demand'},
            {'type': 'cold_snap', 'temp': 30, 'impact': 'increased_hot_food_demand'},
            {'type': 'rain', 'temp': 65, 'impact': 'reduced_foot_traffic'},
            {'type': 'sunny', 'temp': 75, 'impact': 'normal_demand'},
            {'type': 'storm', 'temp': 60, 'impact': 'panic_buying'}
        ]
        
        # Competitor actions
        competitor_actions = [
            {'competitor': 'MegaMart', 'action': 'price_cut', 'category': 'dairy', 'discount': 0.25},
            {'competitor': 'FreshCo', 'action': 'new_store', 'distance': '2_miles', 'impact': 0.15},
            {'competitor': 'ValueStore', 'action': 'promotion', 'category': 'produce', 'discount': 0.30},
            {'competitor': 'OrganicPlace', 'action': 'closure', 'impact': 'positive', 'magnitude': 0.20}
        ]
        
        # Local events
        local_events = [
            {'event': 'Music Festival', 'duration': 3, 'crowd_size': 5000, 'impact': 'high_demand'},
            {'event': 'Sports Game', 'duration': 1, 'crowd_size': 2000, 'impact': 'beverage_spike'},
            {'event': 'Farmers Market', 'duration': 1, 'crowd_size': 500, 'impact': 'reduced_produce'},
            {'event': 'Holiday Weekend', 'duration': 3, 'crowd_size': 0, 'impact': 'family_shopping'}
        ]
        
        # Generate weather conditions (daily) - limit to 7 days
        for day_offset in range(min(days_back, 7)):
            condition_date = datetime.now() - timedelta(days=day_offset)
            weather = random.choice(weather_patterns)
            
            conditions.append({
                'id': condition_id,
                'condition_type': 'weather',
                'condition_data': json.dumps(weather),
                'impact_score': round(random.uniform(0.5, 1.0), 2),
                'recorded_at': condition_date.isoformat()
            })
            condition_id += 1
            
            if len(conditions) >= max_entries:
                break
        
        # Generate competitor actions (weekly) - limit to 5 entries
        if len(conditions) < max_entries:
            for week in range(min(5, max_entries - len(conditions))):
                action_date = datetime.now() - timedelta(weeks=week)
                competitor_action = random.choice(competitor_actions)
                
                conditions.append({
                    'id': condition_id,
                    'condition_type': 'competitor',
                    'condition_data': json.dumps(competitor_action),
                    'impact_score': round(random.uniform(0.3, 0.8), 2),
                    'recorded_at': action_date.isoformat()
                })
                condition_id += 1
                
                if len(conditions) >= max_entries:
                    break
        
        # Generate local events (random) - fill remaining slots
        if len(conditions) < max_entries:
            remaining_slots = max_entries - len(conditions)
            for _ in range(remaining_slots):
                days_ago = random.randint(0, days_back)
                event_date = datetime.now() - timedelta(days=days_ago)
                event = random.choice(local_events)
                
                conditions.append({
                    'id': condition_id,
                    'condition_type': 'event',
                    'condition_data': json.dumps(event),
                    'impact_score': round(random.uniform(0.4, 0.9), 2),
                    'recorded_at': event_date.isoformat()
                })
                condition_id += 1
                
                if len(conditions) >= max_entries:
                    break
            
        return conditions

    def generate_agent_decisions(self, days_back: int = 7) -> List[Dict]:
        """Generate sample agent decision history - limited to 50 entries"""
        decisions = []
        decision_id = 1
        
        # Limit to 50 agent decision entries total
        max_entries = 50
        entries_per_day = max_entries // days_back
        
        agents = ['inventory_agent', 'demand_agent', 'pricing_agent', 'action_agent']
        decision_types = {
            'inventory_agent': ['reorder', 'transfer', 'markdown', 'dispose'],
            'demand_agent': ['forecast_update', 'trend_alert', 'seasonality_detected'],
            'pricing_agent': ['price_optimization', 'competitor_response', 'clearance_pricing'],
            'action_agent': ['execute_markdown', 'place_order', 'update_display', 'alert_staff']
        }
        
        for day_offset in range(days_back):
            # Limit decisions per day to fit within 50 total entries
            num_decisions = min(entries_per_day, random.randint(3, 8))
            
            for _ in range(num_decisions):
                agent = random.choice(agents)
                decision_type = random.choice(decision_types[agent])
                decision_time = datetime.now() - timedelta(days=day_offset, hours=random.randint(0, 23))
                
                # Generate decision data based on type
                if decision_type == 'reorder':
                    decision_data = {
                        'product_id': random.randint(1, 50),
                        'quantity': random.randint(100, 500),
                        'urgency': random.choice(['low', 'medium', 'high'])
                    }
                elif decision_type == 'price_optimization':
                    decision_data = {
                        'product_id': random.randint(1, 50),
                        'old_price': round(random.uniform(1, 20), 2),
                        'new_price': round(random.uniform(1, 20), 2),
                        'expected_impact': round(random.uniform(0.1, 0.5), 2)
                    }
                else:
                    decision_data = {'action': decision_type, 'confidence': round(random.uniform(0.7, 0.95), 2)}
                
                decisions.append({
                    'id': decision_id,
                    'agent_name': agent,
                    'decision_type': decision_type,
                    'decision_data': json.dumps(decision_data),
                    'reasoning': f"Based on analysis of current market conditions and inventory levels",
                    'mode': random.choice(['manual', 'command', 'streaming']),
                    'created_at': decision_time.isoformat()
                })
                decision_id += 1
                
                # Stop if we reach 50 entries
                if len(decisions) >= max_entries:
                    break
                    
            if len(decisions) >= max_entries:
                break
                
        return decisions

    def generate_system_metrics(self, days_back: int = 7) -> List[Dict]:
        """Generate system performance metrics - limited to 50 entries"""
        metrics = []
        metric_id = 1
        
        # Limit to 50 system metrics entries total
        max_entries = 50
        entries_per_day = max_entries // days_back
        
        metric_types = [
            ('waste_reduction', 10, 30),  # baseline 10-30%
            ('revenue_optimization', 5, 15),  # 5-15% improvement
            ('inventory_turnover', 8, 12),  # 8-12 times per month
            ('stockout_rate', 2, 8),  # 2-8% stockout
            ('forecast_accuracy', 70, 90),  # 70-90% accuracy
            ('response_time', 100, 500)  # 100-500ms
        ]
        
        for day_offset in range(days_back):
            metric_time = datetime.now() - timedelta(days=day_offset)
            
            # Limit readings per day to fit within 50 total entries
            num_readings = min(entries_per_day // len(metric_types), 2)
            
            for metric_type, min_val, max_val in metric_types:
                for hour_offset in range(0, 24, 24 // max(1, num_readings)):
                    reading_time = metric_time.replace(hour=hour_offset)
                    
                    # Add some realistic variation
                    value = random.uniform(min_val, max_val)
                    if metric_type == 'waste_reduction' and reading_time.weekday() in [5, 6]:
                        value *= 0.8  # Better on weekends
                    
                    metrics.append({
                        'id': metric_id,
                        'metric_type': metric_type,
                        'metric_value': round(value, 2),
                        'metadata': json.dumps({
                            'period': 'hourly',
                            'confidence': round(random.uniform(0.8, 0.95), 2)
                        }),
                        'recorded_at': reading_time.isoformat()
                    })
                    metric_id += 1
                    
                    # Stop if we reach 50 entries
                    if len(metrics) >= max_entries:
                        break
                        
                if len(metrics) >= max_entries:
                    break
                    
            if len(metrics) >= max_entries:
                break
                    
        return metrics

    # Helper methods
    def _generate_transaction_hour(self, demand_pattern: str) -> int:
        """Generate realistic transaction hours based on demand pattern"""
        if demand_pattern == 'morning_peak':
            # Peak 7-10 AM
            hours = list(range(7, 11)) * 3 + list(range(11, 22))
        elif demand_pattern == 'high_weekend':
            # Spread throughout day, slight afternoon peak
            hours = list(range(9, 20)) + list(range(14, 18)) * 2
        elif demand_pattern == 'weather_dependent':
            # Afternoon peak for beverages
            hours = list(range(11, 16)) * 2 + list(range(8, 22))
        elif demand_pattern == 'seasonal':
            # Clothing - afternoon and evening shopping
            hours = list(range(12, 20)) + list(range(16, 19)) * 2
        elif demand_pattern == 'back_to_school':
            # Stationery - peaks before school seasons
            hours = list(range(10, 18))
        else:  # steady
            hours = list(range(8, 22))
            
        return random.choice(hours)

    def _get_purchase_quantity(self, segment: str, category: str) -> int:
        """Get purchase quantity based on customer segment"""
        base_quantities = {
            'budget_conscious': {
                'produce': 3, 'dairy': 2, 'bakery': 1, 'beverages': 2, 'frozen': 3,
                'clothing': 1, 'electronics': 1, 'stationery': 2, 'personal_care': 2, 'household': 3
            },
            'premium_shopper': {
                'produce': 2, 'dairy': 1, 'bakery': 2, 'beverages': 1, 'frozen': 1,
                'clothing': 2, 'electronics': 2, 'stationery': 1, 'personal_care': 3, 'household': 2
            },
            'family': {
                'produce': 5, 'dairy': 4, 'bakery': 3, 'beverages': 6, 'frozen': 4,
                'clothing': 3, 'electronics': 1, 'stationery': 4, 'personal_care': 3, 'household': 5
            },
            'health_conscious': {
                'produce': 4, 'dairy': 2, 'bakery': 1, 'beverages': 2, 'frozen': 1,
                'clothing': 1, 'electronics': 1, 'stationery': 1, 'personal_care': 3, 'household': 2
            },
            'convenience_seeker': {
                'produce': 1, 'dairy': 2, 'bakery': 2, 'beverages': 3, 'frozen': 4,
                'clothing': 1, 'electronics': 2, 'stationery': 1, 'personal_care': 2, 'household': 2
            }
        }
        
        base = base_quantities.get(segment, {}).get(category, 2)
        return random.randint(max(1, base - 1), base + 2)

    def _calculate_discount(self, product: Dict, transaction_time: datetime) -> float:
        """Calculate discount based on various factors"""
        discount = 0.0
        
        # Weekend discounts
        if transaction_time.weekday() in [5, 6]:
            discount += 0.1
            
        # Evening discounts for perishables
        if product['perishable'] and transaction_time.hour >= 18:
            discount += 0.15
            
        # Random promotions
        if random.random() < 0.2:  # 20% chance
            discount += random.uniform(0.1, 0.3)
            
        return min(discount, 0.5)  # Max 50% discount

    def _calculate_base_demand(self, product: Dict, forecast_date: date) -> int:
        """Calculate base demand for forecasting"""
        # Base demand varies by category
        category_base = {
            'produce': 50,
            'dairy': 40,
            'bakery': 30,
            'beverages': 45,
            'frozen': 25,
            'clothing': 20,
            'electronics': 15,
            'stationery': 25,
            'personal_care': 30,
            'household': 35
        }
        
        base = category_base.get(product['category'], 30)
        
        # Day of week factor
        dow_factors = [0.8, 0.85, 0.9, 0.95, 1.0, 1.2, 1.1]  # Mon-Sun
        base *= dow_factors[forecast_date.weekday()]
        
        # Seasonal factor
        base *= self._get_seasonal_factor(product['category'], forecast_date)
        
        return int(base)

    def _get_seasonal_factor(self, category: str, date: date) -> float:
        """Get seasonal demand factor"""
        month = date.month
        
        seasonal_factors = {
            'produce': [0.8, 0.8, 0.9, 1.0, 1.2, 1.3, 1.3, 1.3, 1.1, 1.0, 0.9, 0.8],
            'dairy': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            'bakery': [1.1, 1.0, 1.0, 1.0, 0.9, 0.8, 0.8, 0.8, 0.9, 1.0, 1.2, 1.3],
            'beverages': [0.7, 0.7, 0.8, 0.9, 1.1, 1.3, 1.4, 1.4, 1.2, 1.0, 0.8, 0.7],
            'frozen': [1.2, 1.1, 1.0, 0.9, 0.8, 0.7, 0.7, 0.7, 0.8, 0.9, 1.1, 1.2],
            'clothing': [0.8, 0.8, 1.0, 1.1, 1.0, 0.9, 0.8, 1.2, 1.3, 1.2, 1.1, 1.3],
            'electronics': [0.9, 0.8, 0.9, 1.0, 1.0, 1.1, 1.0, 1.1, 1.2, 1.1, 1.3, 1.4],
            'stationery': [0.8, 0.8, 0.9, 0.9, 1.0, 1.0, 1.2, 1.5, 1.4, 1.0, 0.9, 0.8],
            'personal_care': [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            'household': [1.1, 1.0, 1.2, 1.1, 1.0, 0.9, 0.9, 0.9, 1.0, 1.0, 1.1, 1.1]
        }
        
        return seasonal_factors.get(category, [1.0] * 12)[month - 1]

    def seed_database(self, clear_existing: bool = True):
        """Seed all synthetic data into Neon database"""
        if not self.engine:
            raise ValueError("Database connection not configured")
        print("Starting database seeding...")
        # Clear existing data if requested
        if clear_existing:
            conn = None
            try:
                conn = self.engine.connect()
                conn.execute(text("TRUNCATE TABLE products, inventory, sales_transactions, demand_forecasts, pricing_history, market_conditions, agent_decisions, system_metrics CASCADE"))
                conn.commit()
                print("Cleared existing data")
            except Exception as e:
                if conn is not None:
                    conn.rollback()
                print(f"Error clearing data: {e}")
            finally:
                if conn is not None:
                    conn.close()
        # Generate all data
        print("Generating products...")
        products = self.generate_products()
        print("Generating inventory...")
        inventory = self.generate_inventory_data(products)
        print("Generating sales history...")
        sales = self.generate_sales_history(products)
        print("Generating demand forecasts...")
        forecasts = self.generate_demand_forecasts(products)
        print("Generating pricing history...")
        pricing = self.generate_pricing_history(products)
        print("Generating market conditions...")
        conditions = self.generate_market_conditions()
        print("Generating agent decisions...")
        decisions = self.generate_agent_decisions()
        print("Generating system metrics...")
        metrics = self.generate_system_metrics()
        # Insert data into database
        conn = None
        try:
            conn = self.engine.connect()
            # Products
            for product in products:
                conn.execute(text("""
                    INSERT INTO products (id, name, category, sku, unit_price, supplier_id, perishable, shelf_life_days)
                    VALUES (:id, :name, :category, :sku, :unit_price, :supplier_id, :perishable, :shelf_life_days)
                    ON CONFLICT (id) DO NOTHING
                """), product)
            print(f"Inserted {len(products)} products")
            # Inventory
            for item in inventory:
                conn.execute(text("""
                    INSERT INTO inventory (id, product_id, quantity, batch_number, expiry_date, location, last_updated)
                    VALUES (:id, :product_id, :quantity, :batch_number, :expiry_date, :location, :last_updated)
                    ON CONFLICT (id) DO NOTHING
                """), item)
            print(f"Inserted {len(inventory)} inventory items")
            # Sales
            for sale in sales:
                conn.execute(text("""
                    INSERT INTO sales_transactions (id, product_id, quantity, sale_price, transaction_time, customer_segment)
                    VALUES (:id, :product_id, :quantity, :sale_price, :transaction_time, :customer_segment)
                    ON CONFLICT (id) DO NOTHING
                """), sale)
            print(f"Inserted {len(sales)} sales transactions")
            # Demand forecasts
            for forecast in forecasts:
                conn.execute(text("""
                    INSERT INTO demand_forecasts (id, product_id, forecast_date, predicted_demand, confidence_level, factors)
                    VALUES (:id, :product_id, :forecast_date, :predicted_demand, :confidence_level, :factors)
                    ON CONFLICT (id) DO NOTHING
                """), forecast)
            print(f"Inserted {len(forecasts)} demand forecasts")
            # Pricing history
            for price in pricing:
                conn.execute(text("""
                    INSERT INTO pricing_history (id, product_id, price, discount_percentage, reason, effective_time)
                    VALUES (:id, :product_id, :price, :discount_percentage, :reason, :effective_time)
                    ON CONFLICT (id) DO NOTHING
                """), price)
            print(f"Inserted {len(pricing)} pricing records")
            # Market conditions
            for condition in conditions:
                conn.execute(text("""
                    INSERT INTO market_conditions (id, condition_type, condition_data, impact_score, recorded_at)
                    VALUES (:id, :condition_type, :condition_data, :impact_score, :recorded_at)
                    ON CONFLICT (id) DO NOTHING
                """), condition)
            print(f"Inserted {len(conditions)} market conditions")
            # Agent decisions
            for decision in decisions:
                conn.execute(text("""
                    INSERT INTO agent_decisions (id, agent_name, decision_type, decision_data, reasoning, mode, created_at)
                    VALUES (:id, :agent_name, :decision_type, :decision_data, :reasoning, :mode, :created_at)
                    ON CONFLICT (id) DO NOTHING
                """), decision)
            print(f"Inserted {len(decisions)} agent decisions")
            # System metrics
            for metric in metrics:
                conn.execute(text("""
                    INSERT INTO system_metrics (id, metric_type, metric_value, metadata, recorded_at)
                    VALUES (:id, :metric_type, :metric_value, :metadata, :recorded_at)
                    ON CONFLICT (id) DO NOTHING
                """), metric)
            print(f"Inserted {len(metrics)} system metrics")
            conn.commit()
        except Exception as e:
            if conn is not None:
                conn.rollback()
            print(f"Error inserting data: {e}")
        finally:
            if conn is not None:
                conn.close()
        print("Database seeding completed successfully!")
        # Print summary statistics
        self.print_summary_statistics()

    def print_summary_statistics(self):
        """Print summary of generated data"""
        with self.engine.connect() as conn:
            # Get counts
            counts = {}
            tables = ['products', 'inventory', 'sales_transactions', 'demand_forecasts', 
                     'pricing_history', 'market_conditions', 'agent_decisions', 'system_metrics']
            
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()
            
            print("\n=== Database Summary ===")
            for table, count in counts.items():
                print(f"{table}: {count:,} records")
            
            # Get some interesting stats
            result = conn.execute(text("""
                SELECT category, COUNT(*) as count, AVG(unit_price) as avg_price
                FROM products
                GROUP BY category
            """))
            
            print("\n=== Products by Category ===")
            for row in result:
                print(f"{row.category}: {row.count} products, avg price ${row.avg_price:.2f}")
            
            # Inventory status
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_batches,
                    SUM(quantity) as total_units,
                    COUNT(CASE WHEN expiry_date <= CURRENT_DATE + INTERVAL '3 days' THEN 1 END) as expiring_soon
                FROM inventory
            """))
            row = result.fetchone()
            print(f"\n=== Inventory Status ===")
            print(f"Total batches: {row.total_batches}")
            print(f"Total units: {row.total_units:,}")
            print(f"Batches expiring soon: {row.expiring_soon}")

    def generate_demo_scenarios(self):
        """Generate specific scenarios for hackathon demo - limited entries"""
        scenarios = []
        print("\n=== Generating Demo Scenarios ===")
        conn = None
        try:
            conn = self.engine.connect()
            # Insert heatwave condition (only if we have space)
            result = conn.execute(text("SELECT COUNT(*) FROM market_conditions"))
            current_count = result.scalar()
            if current_count < 50:
                conn.execute(text("""
                    INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
                    VALUES ('weather', :data, 0.9, :time)
                    ON CONFLICT DO NOTHING
                """), {
                    'data': json.dumps({
                        'type': 'heatwave',
                        'temp': 98,
                        'duration_days': 3,
                        'impact': 'beverage_demand_spike'
                    }),
                    'time': datetime.now().isoformat()
                })
            # Update beverage inventory to show low stock (limit to 5 products)
            conn.execute(text("""
                UPDATE inventory
                SET quantity = GREATEST(quantity * 0.3, 10)
                WHERE product_id IN (
                    SELECT id FROM products WHERE category = 'beverages' LIMIT 5
                )
                AND location = 'Aisle-2'
            """))
            # Scenario 2: Competitor price war (only if we have space)
            if current_count < 49:
                conn.execute(text("""
                    INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
                    VALUES ('competitor', :data, 0.8, :time)
                    ON CONFLICT DO NOTHING
                """), {
                    'data': json.dumps({
                        'competitor': 'MegaMart',
                        'action': 'aggressive_pricing',
                        'categories': ['dairy', 'produce'],
                        'discount': 0.35,
                        'duration': 'weekend'
                    }),
                    'time': (datetime.now() - timedelta(hours=2)).isoformat()
                })
            # Scenario 3: Mass expiry event (limit to 3 products)
            conn.execute(text("""
                UPDATE inventory
                SET expiry_date = CURRENT_DATE + INTERVAL '1 day'
                WHERE product_id IN (
                    SELECT id FROM products 
                    WHERE category = 'dairy' 
                    LIMIT 3
                )
                AND quantity > 50
            """))
            # Scenario 4: Local festival (only if we have space)
            if current_count < 48:
                conn.execute(text("""
                    INSERT INTO market_conditions (condition_type, condition_data, impact_score, recorded_at)
                    VALUES ('event', :data, 0.85, :time)
                    ON CONFLICT DO NOTHING
                """), {
                    'data': json.dumps({
                        'event': 'Summer Music Festival',
                        'expected_attendance': 10000,
                        'duration_days': 2,
                        'categories_affected': ['beverages', 'frozen', 'bakery'],
                        'expected_demand_multiplier': 2.5
                    }),
                    'time': (datetime.now() + timedelta(days=2)).isoformat()
                })
            conn.commit()
        except Exception as e:
            if conn is not None:
                conn.rollback()
            print(f"Error generating demo scenarios: {e}")
        finally:
            if conn is not None:
                conn.close()
        print("Demo scenarios created successfully!")
        print("\nScenarios available:")
        print("1. Current heatwave affecting beverage demand")
        print("2. Competitor price war on dairy and produce")
        print("3. Multiple dairy products expiring tomorrow")
        print("4. Music festival in 2 days expecting 10k attendees")

# Usage functions
def create_tables(db_url: str):
    """Create all required tables in Neon DB"""
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # Create tables SQL
        create_sql = """
        -- Create tables
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            category VARCHAR(100),
            sku VARCHAR(50) UNIQUE,
            unit_price DECIMAL(10,2),
            supplier_id INTEGER,
            perishable BOOLEAN,
            shelf_life_days INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS inventory (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER,
            batch_number VARCHAR(50),
            expiry_date DATE,
            location VARCHAR(50),
            last_updated TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sales_transactions (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER,
            sale_price DECIMAL(10,2),
            transaction_time TIMESTAMP DEFAULT NOW(),
            customer_segment VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS demand_forecasts (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            forecast_date DATE,
            predicted_demand INTEGER,
            confidence_level DECIMAL(3,2),
            factors JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS pricing_history (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES products(id),
            price DECIMAL(10,2),
            discount_percentage DECIMAL(5,2),
            reason VARCHAR(255),
            effective_time TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS market_conditions (
            id SERIAL PRIMARY KEY,
            condition_type VARCHAR(50),
            condition_data JSONB,
            impact_score DECIMAL(3,2),
            recorded_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS agent_decisions (
            id SERIAL PRIMARY KEY,
            agent_name VARCHAR(50),
            decision_type VARCHAR(100),
            decision_data JSONB,
            reasoning TEXT,
            mode VARCHAR(20),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS system_metrics (
            id SERIAL PRIMARY KEY,
            metric_type VARCHAR(50),
            metric_value DECIMAL(10,2),
            metadata JSONB,
            recorded_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_inventory_expiry ON inventory(expiry_date);
        CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id);
        CREATE INDEX IF NOT EXISTS idx_sales_time ON sales_transactions(transaction_time);
        CREATE INDEX IF NOT EXISTS idx_sales_product ON sales_transactions(product_id);
        CREATE INDEX IF NOT EXISTS idx_conditions_type ON market_conditions(condition_type);
        CREATE INDEX IF NOT EXISTS idx_decisions_agent ON agent_decisions(agent_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_type ON system_metrics(metric_type);
        """
        
        conn.execute(text(create_sql))
        conn.commit()
        
    print("Tables created successfully!")

def main():
    """Main function to run the data generator"""
    # Use the database URL from .env file instead of hardcoded one
    # os.environ['NEON_DATABASE_URL'] = 'postgresql://neondb_owner:npg_bRV0YOz6hCBa@ep-aged-sunset-a8hryukr-pooler.eastus2.azure.neon.tech/neondb?sslmode=require'
    
    db_url = os.getenv('NEON_DATABASE_URL')
    
    if not db_url:
        print("Please set NEON_DATABASE_URL environment variable")
        print("Format: postgresql://user:password@host/database?sslmode=require")
        return
    
    # Create tables first
    print("Creating database tables...")
    create_tables(db_url)
    
    # Initialize generator
    generator = RetailDataGenerator(db_url)
    
    # Seed database
    print("\nSeeding database with synthetic data...")
    generator.seed_database(clear_existing=True)
    
    # Generate demo scenarios
    generator.generate_demo_scenarios()
    
    print("\n✅ Data generation complete!")
    print("Your Neon database is now populated with realistic retail data.")
    print("\nYou can now connect your LangGraph agents to this data!")

if __name__ == "__main__":
    main()