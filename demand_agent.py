# demand_agent.py - Analyzes demand patterns and trends
from typing import Dict, List, Any
from datetime import datetime, timedelta
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import create_engine, text
import pandas as pd
import json
from shared_state import SharedRetailState, add_agent_message

class DemandAgent:
    """Agent responsible for analyzing demand patterns and market conditions"""
    
    def __init__(self, openai_api_key: str, db_url: str):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            openai_api_key=openai_api_key,
            temperature=0.3
        )
        self.engine = create_engine(db_url)
        
        self.system_prompt = """You are the Demand Analysis Agent for a retail system.
        Your role is to:
        1. Analyze sales trends and patterns
        2. Identify products with increasing or decreasing demand
        3. Consider external factors (weather, events, competitor actions)
        4. Predict demand changes based on the scenario
        
        Focus on actionable insights that will help with inventory and pricing decisions.
        Always provide specific product recommendations with quantified impacts.
        """

    def analyze(self, state: SharedRetailState) -> SharedRetailState:
        """Main analysis function"""
        
        add_agent_message(state, "demand_agent", "Starting demand analysis")
        
        # Fetch relevant data
        sales_data = self._fetch_sales_trends()
        market_conditions = self._fetch_market_conditions()
        forecast_data = self._fetch_demand_forecasts()
        
        # Analyze with LLM
        analysis = self._analyze_demand_patterns(
            state['scenario'],
            sales_data,
            market_conditions,
            forecast_data
        )
        
        # Update state
        state['demand_analysis'] = analysis
        
        add_agent_message(
            state, 
            "demand_agent", 
            f"Identified {len(analysis.get('trending_up', []))} products with increasing demand",
            analysis
        )
        
        return state

    def _fetch_sales_trends(self) -> pd.DataFrame:
        """Fetch recent sales trends from database"""
        query = """
        WITH daily_sales AS (
            SELECT 
                p.id as product_id,
                p.name,
                p.category,
                DATE(s.transaction_time) as sale_date,
                SUM(s.quantity) as daily_quantity,
                AVG(s.sale_price) as avg_price
            FROM sales_transactions s
            JOIN products p ON s.product_id = p.id
            WHERE s.transaction_time >= CURRENT_DATE - INTERVAL '14 days'
            GROUP BY p.id, p.name, p.category, DATE(s.transaction_time)
        ),
        trends AS (
            SELECT 
                product_id,
                name,
                category,
                AVG(CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '7 days' 
                    THEN daily_quantity END) as recent_avg,
                AVG(CASE WHEN sale_date < CURRENT_DATE - INTERVAL '7 days' 
                    THEN daily_quantity END) as previous_avg
            FROM daily_sales
            GROUP BY product_id, name, category
        )
        SELECT 
            *,
            CASE 
                WHEN previous_avg > 0 THEN 
                    ((recent_avg - previous_avg) / previous_avg * 100)
                ELSE 0 
            END as demand_change_pct
        FROM trends
        WHERE recent_avg IS NOT NULL AND previous_avg IS NOT NULL
        ORDER BY demand_change_pct DESC
        """
        
        return pd.read_sql(query, self.engine)

    def _fetch_market_conditions(self) -> pd.DataFrame:
        """Fetch recent market conditions"""
        query = """
        SELECT 
            condition_type,
            condition_data,
            impact_score,
            recorded_at
        FROM market_conditions
        WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
        ORDER BY recorded_at DESC
        """
        
        return pd.read_sql(query, self.engine)

    def _fetch_demand_forecasts(self) -> pd.DataFrame:
        """Fetch demand forecasts"""
        query = """
        SELECT 
            p.name,
            p.category,
            df.forecast_date,
            df.predicted_demand,
            df.confidence_level,
            df.factors
        FROM demand_forecasts df
        JOIN products p ON df.product_id = p.id
        WHERE df.forecast_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
        ORDER BY df.forecast_date, p.category
        """
        
        return pd.read_sql(query, self.engine)

    def _analyze_demand_patterns(
        self, 
        scenario: str,
        sales_data: pd.DataFrame,
        market_conditions: pd.DataFrame,
        forecast_data: pd.DataFrame
    ) -> Dict[str, Any]:
        """Use LLM to analyze demand patterns"""
        
        # Prepare data summary for LLM
        trending_up = sales_data[sales_data['demand_change_pct'] > 20].head(10)
        trending_down = sales_data[sales_data['demand_change_pct'] < -20].head(10)
        
        # Extract recent market conditions
        recent_conditions = []
        for _, condition in market_conditions.head(5).iterrows():
            condition_data = condition['condition_data']
            # Check if condition_data is already a dict or needs JSON parsing
            if isinstance(condition_data, str):
                try:
                    parsed_data = json.loads(condition_data)
                except (json.JSONDecodeError, TypeError):
                    parsed_data = condition_data
            else:
                parsed_data = condition_data
                
            recent_conditions.append({
                'type': condition['condition_type'],
                'data': parsed_data,
                'impact': condition['impact_score']
            })
     
        prompt = f"""
        Analyze demand patterns based on this scenario: {scenario}
        
        Current trends:
        Products trending UP (>20% increase):
        {trending_up[['name', 'category', 'demand_change_pct']].to_string()}
        
        Products trending DOWN (>20% decrease):
        {trending_down[['name', 'category', 'demand_change_pct']].to_string()}
        
        Recent market conditions:
        {json.dumps(recent_conditions, indent=2)}
        
        Based on the scenario and data:
        1. Which products will see increased demand?
        2. Which products will see decreased demand?
        3. What external factors are most relevant?
        4. Provide specific quantity predictions
        
        Respond with a JSON object:
        {{
            "trending_up": [
                {{"product_id": 1, "name": "...", "increase_pct": 0.3, "reason": "..."}}
            ],
            "trending_down": [
                {{"product_id": 2, "name": "...", "decrease_pct": 0.2, "reason": "..."}}
            ],
            "seasonal_impact": {{"summer_products": "high", "winter_products": "low"}},
            "external_factors": [
                {{"factor": "heatwave", "impact": "beverages +40%", "categories_affected": ["beverages"]}}
            ],
            "recommendations": ["stock_up_on_beverages", "reduce_frozen_orders"]
        }}
        """
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            
            # Parse JSON from response
            import re
            json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
            if json_match:
                analysis = json.loads(json_match.group())
            else:
                # Fallback analysis
                analysis = self._create_fallback_analysis(sales_data)
                
        except Exception as e:
            # Log error but don't reference state here since it's not in scope
            print(f"Demand agent LLM analysis error: {str(e)}")
            analysis = self._create_fallback_analysis(sales_data)
        
        return analysis

    def _create_fallback_analysis(self, sales_data: pd.DataFrame) -> Dict[str, Any]:
        """Create a basic analysis if LLM fails"""
        trending_up = sales_data[sales_data['demand_change_pct'] > 20].head(5)
        trending_down = sales_data[sales_data['demand_change_pct'] < -20].head(5)
        
        return {
            "trending_up": [
                {
                    "product_id": int(row['product_id']),
                    "name": row['name'],
                    "increase_pct": float(row['demand_change_pct']) / 100,
                    "reason": "Historical trend analysis"
                }
                for _, row in trending_up.iterrows()
            ],
            "trending_down": [
                {
                    "product_id": int(row['product_id']),
                    "name": row['name'],
                    "decrease_pct": abs(float(row['demand_change_pct'])) / 100,
                    "reason": "Historical trend analysis"
                }
                for _, row in trending_down.iterrows()
            ],
            "seasonal_impact": {"general": "moderate"},
            "external_factors": [],
            "recommendations": ["Monitor trending products closely"]
        }