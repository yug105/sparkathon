# shelf_life_agent.py - Monitors product expiration and shelf life with structured output
from typing import Dict, List, Any
from datetime import datetime, timedelta, date
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
import pandas as pd
import json
from shared_state import SharedRetailState, add_agent_message, Priority

# Pydantic models for structured output
class ShelfLifeAlert(BaseModel):
    """Individual shelf life alert with recommendations"""
    product_id: int = Field(description="Product ID from inventory")
    name: str = Field(description="Product name")
    category: str = Field(description="Product category")
    urgency: str = Field(description="Urgency level: critical, high, medium, or low")
    quantity: int = Field(description="Quantity at risk")
    days_until_expiry: int = Field(description="Days until product expires")
    value_at_risk: float = Field(description="Dollar value at risk if expired")
    markdown_recommendation: int = Field(description="Recommended markdown percentage (0-80)")
    alternative_channel: str = Field(description="Alternative channel: staff_sale, donation, compost, or clearance")
    expected_recovery: float = Field(description="Expected recovery rate (0.0-1.0)")

class ShelfLifeAnalysis(BaseModel):
    """Complete shelf life analysis with structured alerts"""
    alerts: List[ShelfLifeAlert] = Field(description="List of shelf life alerts")
    total_value_at_risk: float = Field(description="Total value at risk across all products")
    critical_count: int = Field(description="Number of critical items (expires within 24 hours)")
    high_priority_count: int = Field(description="Number of high priority items (expires in 1-3 days)")

class ShelfLifeAgent:
    """Agent responsible for monitoring product expiration dates and shelf life"""
    
    def __init__(self, api_key: str, db_url: str, provider: str = "openai"):
        if provider == "claude":
            self.llm = ChatAnthropic(
                model="claude-3-haiku-20240307",
                anthropic_api_key=api_key,
                temperature=0.2
            )
        else:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                openai_api_key=api_key,
                temperature=0.3
            )
        from db_utils import create_robust_engine
        self.engine = create_robust_engine(db_url)
        
        # Set up structured output parser
        self.parser = JsonOutputParser(pydantic_object=ShelfLifeAnalysis)
        
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
        
        from db_utils import execute_query_with_retry
        return execute_query_with_retry(self.engine, query)

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
        """Create detailed shelf life alerts using structured LLM output"""
        
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
        
        # Create prompt template with structured output instructions
        prompt_template = PromptTemplate(
            template="""Analyze expiring inventory based on scenario: {scenario}
        
Summary:
- Critical (≤24 hrs): {critical} items
- High (1-3 days): {high} items  
- Medium (4-7 days): {medium} items
- Total value at risk: ${total_value_at_risk:.2f}

Top items by value at risk:
{top_items_data}

Demand trends available: {has_demand_data}

For each critical and high priority item, analyze and provide recommendations for:
1. Optimal markdown percentage (0-80%)
2. Alternative channels (staff_sale, donation, compost, clearance)
3. Expected recovery rate (0.0-1.0)

Consider product category, perishability, and current scenario conditions.

{format_instructions}""",
            input_variables=["scenario", "critical", "high", "medium", "total_value_at_risk", 
                           "top_items_data", "has_demand_data"],
            partial_variables={"format_instructions": self.parser.get_format_instructions()}
        )
        
        # Format the prompt
        formatted_prompt = prompt_template.format(
            scenario=scenario,
            critical=summary['critical'],
            high=summary['high'],
            medium=summary['medium'],
            total_value_at_risk=summary['total_value_at_risk'],
            top_items_data=top_items[['name', 'category', 'quantity', 'days_until_expiry', 'value_at_risk']].to_string(),
            has_demand_data=bool(demand_analysis)
        )
        
        # Create chain with structured output
        chain = self.llm | self.parser
        
        try:
            # Get structured analysis from LLM
            llm_analysis = chain.invoke([
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=formatted_prompt)
            ])
            
            # Handle both dict and Pydantic object responses
            if isinstance(llm_analysis, dict):
                alerts = llm_analysis.get('alerts', [])
            else:
                alerts = [alert.dict() for alert in llm_analysis.alerts]
            
        except Exception as e:
            print(f"Shelf life agent structured parsing error: {str(e)}")
            # Fallback to empty analysis
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
                "urgency": llm_rec.get('urgency', self._determine_urgency(row['days_until_expiry'])),
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