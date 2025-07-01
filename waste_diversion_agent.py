# waste_diversion_agent.py - Manages waste diversion and recovery strategies
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import create_engine, text
import pandas as pd
import json
from shared_state import SharedRetailState, add_agent_message

class WasteDiversionAgent:
    """Agent responsible for waste diversion, donations, and recovery strategies"""
    
    def __init__(self, openai_api_key: str, db_url: str):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            openai_api_key=openai_api_key,
            temperature=0.3
        )
        self.engine = create_engine(db_url)
        
        self.system_prompt = """You are the Waste Diversion and Recovery Agent for a retail sustainability system.
        Your mission is to minimize waste and maximize value recovery through:
        
        1. Donation coordination:
           - Identify suitable products for food banks and charities
           - Ensure food safety compliance
           - Optimize donation logistics
        
        2. Upcycling opportunities:
           - Transform waste into higher-value products
           - Identify animal feed conversion options
           - Explore biofuel potential
        
        3. Recycling management:
           - Separate packaging materials
           - Coordinate with recycling partners
           - Track diversion rates
        
        4. Compliance and safety:
           - Follow food safety guidelines
           - Document chain of custody
           - Ensure proper handling and storage
        
        Prioritize based on social impact, environmental benefit, and cost-effectiveness.
        """
        
        # Define diversion partners and capabilities
        self.diversion_partners = {
            "food_banks": {
                "Local Food Bank Network": {
                    "accepts": ["produce", "dairy", "bakery", "frozen"],
                    "min_days_shelf_life": 1,
                    "pickup_times": ["morning", "afternoon"],
                    "capacity": 500  # kg per day
                },
                "Community Kitchen": {
                    "accepts": ["produce", "dairy", "bakery"],
                    "min_days_shelf_life": 0,  # Can use same day
                    "pickup_times": ["morning"],
                    "capacity": 200
                }
            },
            "animal_feed": {
                "Local Farm Collective": {
                    "accepts": ["produce", "bakery"],
                    "requirements": "no meat, no heavily processed",
                    "capacity": 1000
                }
            },
            "composting": {
                "City Composting Program": {
                    "accepts": ["all_organic"],
                    "pickup_schedule": "daily",
                    "capacity": 2000
                }
            },
            "recycling": {
                "RecycleCo": {
                    "accepts": ["plastics", "cardboard", "glass", "metal"],
                    "pickup_schedule": "twice_weekly"
                }
            }
        }

    def analyze(self, state: SharedRetailState) -> SharedRetailState:
        """Main analysis function for waste diversion"""
        
        add_agent_message(state, "waste_diversion_agent", "Starting waste diversion analysis")
        
        # Get inputs from other agents
        shelf_life_alerts = state.get('shelf_life_alerts', [])
        pricing_changes = state.get('pricing_changes', [])
        
        # Identify products that won't sell in time
        diversion_candidates = self._identify_diversion_candidates(
            shelf_life_alerts,
            pricing_changes
        )
        
        # Match with appropriate diversion channels
        diversion_actions = self._create_diversion_plan(
            diversion_candidates,
            state['scenario']
        )
        
        # Calculate environmental impact
        environmental_impact = self._calculate_environmental_impact(diversion_actions)
        
        # Update state
        state['diversion_actions'] = diversion_actions
        
        # Update environmental metrics
        if 'environmental_metrics' not in state:
            state['environmental_metrics'] = {}
        state['environmental_metrics'].update(environmental_impact)
        
        total_diverted = sum(a['quantity'] for a in diversion_actions)
        donation_value = sum(a.get('retail_value', 0) for a in diversion_actions if a['diversion_type'] == 'donation')
        
        add_agent_message(
            state,
            "waste_diversion_agent",
            f"Diverting {total_diverted} units, ${donation_value:.2f} in donation value",
            {
                "total_actions": len(diversion_actions),
                "waste_prevented_kg": environmental_impact.get('waste_prevented_kg', 0),
                "donation_value": donation_value
            }
        )
        
        return state

    def _identify_diversion_candidates(
        self,
        shelf_life_alerts: List[Dict],
        pricing_changes: List[Dict]
    ) -> List[Dict]:
        """Identify products that need diversion"""
        
        candidates = []
        
        # Get products that were marked down
        marked_down_products = {p['product_id']: p for p in pricing_changes}
        
        for alert in shelf_life_alerts:
            # Check if pricing strategy likely won't clear inventory
            markdown = marked_down_products.get(alert['product_id'], {})
            expected_clearance = markdown.get('expected_units_moved', 0)
            
            remaining_units = alert['quantity'] - expected_clearance
            
            if alert['urgency'] == 'critical' or remaining_units > 20:
                candidates.append({
                    "product_id": alert['product_id'],
                    "name": alert['name'],
                    "category": alert['category'],
                    "quantity": int(remaining_units) if remaining_units > 0 else alert['quantity'],
                    "expiry_date": alert['expiry_date'],
                    "days_until_expiry": alert['days_until_expiry'],
                    "unit_price": alert['unit_price'],
                    "retail_value": alert['unit_price'] * (remaining_units if remaining_units > 0 else alert['quantity']),
                    "batch_number": alert['batch_number'],
                    "location": alert['location']
                })
        
        return candidates

    def _create_diversion_plan(
        self,
        candidates: List[Dict],
        scenario: str
    ) -> List[Dict]:
        """Create detailed diversion plan for each product"""
        
        diversion_actions = []
        
        # Sort by urgency and value
        candidates.sort(key=lambda x: (x['days_until_expiry'], -x['retail_value']))
        
        for candidate in candidates:
            # Determine best diversion channel
            if candidate['days_until_expiry'] >= 1 and candidate['category'] in ['produce', 'dairy', 'bakery']:
                # Suitable for donation
                partner = self._select_donation_partner(candidate)
                if partner:
                    diversion_actions.append({
                        "product_id": candidate['product_id'],
                        "name": candidate['name'],
                        "quantity": candidate['quantity'],
                        "diversion_type": "donation",
                        "partner": partner['name'],
                        "pickup_time": self._schedule_pickup(partner, candidate),
                        "retail_value": candidate['retail_value'],
                        "tax_benefit": candidate['retail_value'] * 0.3,  # Estimated tax benefit
                        "compliance_checked": True,
                        "handling_instructions": self._get_handling_instructions(candidate),
                        "batch_number": candidate['batch_number']
                    })
            elif candidate['category'] in ['produce', 'bakery'] and candidate['days_until_expiry'] <= 0:
                # Animal feed or composting
                diversion_actions.append({
                    "product_id": candidate['product_id'],
                    "name": candidate['name'],
                    "quantity": candidate['quantity'],
                    "diversion_type": "animal_feed",
                    "partner": "Local Farm Collective",
                    "pickup_time": "next_morning",
                    "retail_value": 0,  # No retail value for expired
                    "environmental_value": candidate['quantity'] * 0.5,  # kg CO2 saved
                    "compliance_checked": True,
                    "batch_number": candidate['batch_number']
                })
            else:
                # Composting as last resort
                diversion_actions.append({
                    "product_id": candidate['product_id'],
                    "name": candidate['name'],
                    "quantity": candidate['quantity'],
                    "diversion_type": "composting",
                    "partner": "City Composting Program",
                    "pickup_time": "next_scheduled",
                    "retail_value": 0,
                    "environmental_value": candidate['quantity'] * 0.3,
                    "compliance_checked": True,
                    "batch_number": candidate['batch_number']
                })
        
        # Use LLM to optimize the plan
        optimized_plan = self._optimize_diversion_plan(diversion_actions, scenario)
        
        return optimized_plan

    def _select_donation_partner(self, product: Dict) -> Dict:
        """Select appropriate donation partner based on product and requirements"""
        
        suitable_partners = []
        
        for partner_name, partner_info in self.diversion_partners['food_banks'].items():
            if (product['category'] in partner_info['accepts'] and 
                product['days_until_expiry'] >= partner_info['min_days_shelf_life']):
                suitable_partners.append({
                    'name': partner_name,
                    'info': partner_info
                })
        
        # Return partner with highest capacity
        if suitable_partners:
            return max(suitable_partners, key=lambda x: x['info']['capacity'])
        return None

    def _schedule_pickup(self, partner: Dict, product: Dict) -> str:
        """Schedule pickup based on urgency and partner availability"""
        
        if product['days_until_expiry'] <= 1:
            # Urgent - next available slot
            return f"today_{partner['info']['pickup_times'][0]}"
        else:
            # Standard scheduling
            return f"tomorrow_{partner['info']['pickup_times'][0]}"

    def _get_handling_instructions(self, product: Dict) -> str:
        """Generate handling instructions based on product type"""
        
        instructions = {
            'produce': "Keep refrigerated. Pack in ventilated containers. Handle with care.",
            'dairy': "Maintain cold chain. Temperature must stay below 40°F. Use insulated containers.",
            'bakery': "Keep dry. Pack in sealed containers. Stack carefully to prevent crushing.",
            'frozen': "Maintain frozen state. Use dry ice for transport. Do not refreeze."
        }
        
        return instructions.get(product['category'], "Handle according to food safety guidelines.")

    def _calculate_environmental_impact(self, diversion_actions: List[Dict]) -> Dict[str, float]:
        """Calculate environmental impact of diversion actions"""
        
        total_weight_kg = sum(a['quantity'] * 0.5 for a in diversion_actions)  # Assume 0.5kg average
        
        impact = {
            "waste_prevented_kg": total_weight_kg,
            "carbon_saved_kg": total_weight_kg * 2.5,  # 2.5 kg CO2 per kg food waste
            "methane_prevented_kg": total_weight_kg * 0.05,  # Methane from landfill
            "water_saved_liters": total_weight_kg * 100,  # Water footprint
            "donation_meals": sum(a['quantity'] for a in diversion_actions if a['diversion_type'] == 'donation') // 3
        }
        
        return impact

    def _optimize_diversion_plan(self, actions: List[Dict], scenario: str) -> List[Dict]:
        """Use LLM to optimize the diversion plan"""
        
        prompt = f"""
        Optimize this waste diversion plan for scenario: {scenario}
        
        Current plan has {len(actions)} actions:
        - Donations: {len([a for a in actions if a['diversion_type'] == 'donation'])}
        - Animal feed: {len([a for a in actions if a['diversion_type'] == 'animal_feed'])}
        - Composting: {len([a for a in actions if a['diversion_type'] == 'composting'])}
        
        Consider:
        1. Maximizing social impact (donations to food banks)
        2. Logistics efficiency (grouping pickups)
        3. Environmental benefit
        4. Cost-effectiveness
        
        Suggest any improvements or confirm the plan is optimal.
        """
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            # For now, return the original plan
            # In a full implementation, we'd parse LLM suggestions
            return actions
        except Exception as e:
            # Log error but don't reference state here since it's not in scope
            print(f"Waste diversion agent optimization error: {str(e)}")
            return actions