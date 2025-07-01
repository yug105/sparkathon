# shelf_life_agent.py - Monitors product expiration and shelf life
from typing import Dict, List, Any
from datetime import datetime, timedelta, date
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import create_engine, text
import pandas as pd
import json
from shared_state import SharedRetailState, add_agent_message, Priority

class ShelfLifeAgent:
    """Agent responsible for monitoring product expiration dates and shelf life"""
    
    def __init__(self, openai_api_key: str, db_url: str):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            openai_api_key=openai_api_key,
            temperature=0.2  # Lower temperature for more consistent analysis
        )
        self.engine = create_engine(db_url)
        
        self.system_prompt = """You are the Shelf Life Monitoring Agent for a retail system.
        Your responsibilities:
        1. Identify products approaching expiration
        2. Categorize urgency levels (critical: <24hrs, high: 1-3 days, medium: 4-7 days)
        3. Calculate potential losses if products expire
        4. Prioritize based on quantity and value at risk
        5. Consider product categories and their typical handling
        
        Focus on preventing waste and maximizing value recovery.
        """

    def analyze(self, state: SharedRetailState) -> SharedRetailState:
        """Main analysis function"""
        
        add_agent_message(state, "shelf_life_agent", "Starting shelf life monitoring")
        
        # Fetch expiring products
        expiring_products = self._fetch_expiring_products()
        
        # Categorize by urgency
        categorized = self._categorize_by_urgency(expiring_products)
        
        # Analyze with LLM for recommendations
        alerts = self._create_shelf_life_alerts(
            state['scenario'],
            categorized,
            state.get('demand_analysis', {})
        )
        
        # Update state
        state['shelf_life_alerts'] = alerts
        
        # Update priority if critical items found
        critical_count = len([a for a in alerts if a.get('urgency') == 'critical'])
        if critical_count > 0:
            state['priority_level'] = Priority.CRITICAL.value
            add_agent_message(
                state,
                "shelf_life_agent",
                f"CRITICAL: {critical_count} products expire within 24 hours!",
                {"critical_items": critical_count}
            )
        else:
            add_agent_message(
                state,
                "shelf_life_agent",
                f"Monitoring {len(alerts)} products approaching expiration",
                {"total_alerts": len(alerts)}
            )
        
        return state

    def _fetch_expiring_products(self) -> pd.DataFrame:
        """Fetch products approaching expiration"""
        query = """
        SELECT 
            i.id as inventory_id,
            i.product_id,
            p.name,
            p.category,
            p.unit_price,
            i.quantity,
            i.batch_number,
            i.expiry_date,
            i.location,
            (i.expiry_date - CURRENT_DATE) as days_until_expiry,
            (i.quantity * p.unit_price) as value_at_risk,
            p.perishable,
            p.shelf_life_days
        FROM inventory i
        JOIN products p ON i.product_id = p.id
        WHERE i.expiry_date <= CURRENT_DATE + INTERVAL '7 days'
        AND i.quantity > 0
        ORDER BY i.expiry_date, value_at_risk DESC
        """
        
        return pd.read_sql(query, self.engine)

    def _categorize_by_urgency(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Categorize products by expiration urgency"""
        categorized = {
            'critical': df[df['days_until_expiry'] <= 1],
            'high': df[(df['days_until_expiry'] > 1) & (df['days_until_expiry'] <= 3)],
            'medium': df[(df['days_until_expiry'] > 3) & (df['days_until_expiry'] <= 7)],
            'low': df[df['days_until_expiry'] > 7]
        }
        
        return categorized

    def _create_shelf_life_alerts(
        self,
        scenario: str,
        categorized: Dict[str, pd.DataFrame],
        demand_analysis: Dict[str, Any]
    ) -> List[Dict]:
        """Create detailed shelf life alerts using LLM"""
        
        # Prepare summary for LLM
        summary = {
            'critical': len(categorized['critical']),
            'high': len(categorized['high']),
            'medium': len(categorized['medium']),
            'total_value_at_risk': sum(cat['value_at_risk'].sum() for cat in categorized.values())
        }
        
        # Get top items by value at risk
        all_expiring = pd.concat(categorized.values()).sort_values('value_at_risk', ascending=False)
        top_items = all_expiring.head(20)
        
        prompt = f"""
        Analyze expiring inventory based on scenario: {scenario}
        
        Summary:
        - Critical (≤24 hrs): {summary['critical']} items
        - High (1-3 days): {summary['high']} items  
        - Medium (4-7 days): {summary['medium']} items
        - Total value at risk: ${summary['total_value_at_risk']:.2f}
        
        Top items by value at risk:
        {top_items[['name', 'category', 'quantity', 'days_until_expiry', 'value_at_risk']].to_string()}
        
        Demand trends available: {bool(demand_analysis)}
        
        For each critical and high priority item, recommend:
        1. Optimal markdown percentage
        2. Alternative channels (staff sales, donations)
        3. Expected recovery rate
        
        Respond with actionable alerts in JSON format:
        {{
            "alerts": [
                {{
                    "product_id": 1,
                    "name": "...",
                    "urgency": "critical/high/medium",
                    "markdown_recommendation": 30,
                    "alternative_channel": "donation",
                    "expected_recovery": 0.7
                }}
            ]
        }}
        """
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            
            # Parse JSON response
            import re
            json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
            if json_match:
                llm_analysis = json.loads(json_match.group())
                alerts = llm_analysis.get('alerts', [])
            else:
                alerts = []
                
        except Exception as e:
            # Log error but don't reference state here since it's not in scope
            print(f"Shelf life agent LLM analysis error: {str(e)}")
            alerts = []
        
        # Create comprehensive alerts combining data and LLM recommendations
        final_alerts = []
        
        for _, row in all_expiring.iterrows():
            # Find LLM recommendation for this product
            llm_rec = next((a for a in alerts if a.get('product_id') == row['product_id']), {})
            
            alert = {
                "product_id": int(row['product_id']),
                "inventory_id": int(row['inventory_id']),
                "name": row['name'],
                "category": row['category'],
                "batch_number": row['batch_number'],
                "expiry_date": row['expiry_date'].isoformat(),
                "days_until_expiry": int(row['days_until_expiry']),
                "quantity": int(row['quantity']),
                "unit_price": float(row['unit_price']),
                "value_at_risk": float(row['value_at_risk']),
                "location": row['location'],
                "urgency": self._determine_urgency(row['days_until_expiry']),
                "markdown_recommendation": llm_rec.get('markdown_recommendation', 
                    self._calculate_default_markdown(row['days_until_expiry'])),
                "alternative_channel": llm_rec.get('alternative_channel', 
                    self._suggest_alternative_channel(row)),
                "expected_recovery": llm_rec.get('expected_recovery', 0.5)
            }
            
            final_alerts.append(alert)
        
        return final_alerts

    def _determine_urgency(self, days_until_expiry: int) -> str:
        """Determine urgency level based on days until expiry"""
        if days_until_expiry <= 1:
            return "critical"
        elif days_until_expiry <= 3:
            return "high"
        elif days_until_expiry <= 7:
            return "medium"
        else:
            return "low"

    def _calculate_default_markdown(self, days_until_expiry: int) -> int:
        """Calculate default markdown percentage based on urgency"""
        if days_until_expiry <= 1:
            return 50  # 50% off for critical
        elif days_until_expiry <= 3:
            return 30  # 30% off for high
        elif days_until_expiry <= 7:
            return 20  # 20% off for medium
        else:
            return 10  # 10% off for low

    def _suggest_alternative_channel(self, row: pd.Series) -> str:
        """Suggest alternative channel based on product type"""
        if row['category'] in ['produce', 'dairy', 'bakery']:
            if row['days_until_expiry'] <= 1:
                return "donation"
            else:
                return "flash_sale"
        elif row['category'] in ['frozen', 'beverages']:
            return "staff_sale"
        else:
            return "regular_markdown"