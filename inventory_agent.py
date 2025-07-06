# inventory_agent.py - Manages inventory levels and reordering
from typing import Dict, List, Any
from datetime import datetime, timedelta
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import create_engine, text
import pandas as pd
import json
from shared_state import SharedRetailState, add_agent_message

class InventoryAgent:
    """Agent responsible for inventory management and restock recommendations"""
    
    def __init__(self, api_key: str, db_url: str, provider: str = "openai"):
        if provider == "claude":
            self.llm = ChatAnthropic(
                model="claude-3-haiku-20240307",
                anthropic_api_key=api_key,
                temperature=0.2,
                max_tokens=2048,
            )
        else:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",
                openai_api_key=api_key,
                temperature=0.3
            )
        from db_utils import create_robust_engine
        self.engine = create_robust_engine(db_url)
        
        self.system_prompt = """You are the Inventory Management Agent for a retail system.
        Your responsibilities:
        1. Analyze current stock levels against demand
        2. Consider shelf life constraints from the shelf life agent
        3. Account for demand trends from the demand agent
        4. Recommend optimal reorder quantities
        5. Identify overstock and understock situations
        6. Balance freshness with availability
        
        Always consider sustainability - avoid overordering perishables.
        """

    def analyze(self, state: SharedRetailState) -> SharedRetailState:
        """Main analysis function"""
        
        add_agent_message(state, "inventory_agent", "Starting inventory analysis")
        
        # Get inputs from other agents
        demand_analysis = state.get('demand_analysis', {})
        shelf_life_alerts = state.get('shelf_life_alerts', [])
        
        # Fetch current inventory status
        inventory_status = self._fetch_inventory_status()
        
        # Analyze inventory health
        inventory_analysis = self._analyze_inventory_health(
            inventory_status,
            demand_analysis,
            shelf_life_alerts,
            state['scenario']
        )
        
        # Generate restock recommendations
        restock_recs = self._generate_restock_recommendations(
            inventory_analysis,
            demand_analysis,
            shelf_life_alerts
        )
        
        # Update state
        state['inventory_levels'] = inventory_analysis
        state['restock_recommendations'] = restock_recs
        
        add_agent_message(
            state,
            "inventory_agent",
            f"Analyzed {len(inventory_status)} products, {len(restock_recs)} need restocking",
            {
                "critical_stock": len(inventory_analysis.get('critical_stock', [])),
                "overstock": len(inventory_analysis.get('overstock', [])),
                "optimal": len(inventory_analysis.get('optimal', []))
            }
        )
        
        return state

    def _fetch_inventory_status(self) -> pd.DataFrame:
        """Fetch comprehensive inventory status"""
        query = """
        WITH inventory_summary AS (
            SELECT 
                p.id as product_id,
                p.name,
                p.category,
                p.unit_price,
                p.perishable,
                p.shelf_life_days,
                COALESCE(SUM(i.quantity), 0) as current_stock,
                COUNT(DISTINCT i.batch_number) as num_batches,
                MIN(i.expiry_date) as earliest_expiry,
                AVG(i.expiry_date - CURRENT_DATE) as avg_days_to_expiry
            FROM products p
            LEFT JOIN inventory i ON p.id = i.product_id AND i.quantity > 0
            GROUP BY p.id, p.name, p.category, p.unit_price, p.perishable, p.shelf_life_days
        ),
        recent_sales AS (
            SELECT 
                product_id,
                AVG(quantity) as avg_daily_sales,
                STDDEV(quantity) as sales_stddev
            FROM (
                SELECT 
                    product_id,
                    DATE(transaction_time) as sale_date,
                    SUM(quantity) as quantity
                FROM sales_transactions
                WHERE transaction_time >= CURRENT_DATE - INTERVAL '14 days'
                GROUP BY product_id, DATE(transaction_time)
            ) daily_sales
            GROUP BY product_id
        ),
        forecast_summary AS (
            SELECT 
                product_id,
                AVG(predicted_demand) as avg_forecast_demand
            FROM demand_forecasts
            WHERE forecast_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
            GROUP BY product_id
        )
        SELECT 
            inv.*,
            COALESCE(rs.avg_daily_sales, 0) as avg_daily_sales,
            COALESCE(rs.sales_stddev, 0) as sales_volatility,
            COALESCE(fs.avg_forecast_demand, rs.avg_daily_sales, 0) as forecast_demand,
            CASE 
                WHEN COALESCE(rs.avg_daily_sales, 0) > 0 THEN
                    inv.current_stock / rs.avg_daily_sales
                ELSE 999
            END as days_of_supply
        FROM inventory_summary inv
        LEFT JOIN recent_sales rs ON inv.product_id = rs.product_id
        LEFT JOIN forecast_summary fs ON inv.product_id = fs.product_id
        ORDER BY days_of_supply, inv.category
        """
        
        conn = None
        try:
            conn = self.engine.connect()
            df = pd.read_sql(query, conn)
            # Ensure all required columns exist
            required_columns = ['product_id', 'name', 'category', 'current_stock', 'days_of_supply', 
                              'avg_daily_sales', 'forecast_demand', 'perishable', 'avg_days_to_expiry']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0 if col in ['current_stock', 'days_of_supply', 'avg_daily_sales', 'forecast_demand'] else 'unknown'
            return df
        except Exception as e:
            print(f"Error fetching inventory status: {str(e)}")
            # Return empty DataFrame with required columns on error
            return pd.DataFrame(columns=['product_id', 'name', 'category', 'current_stock', 'days_of_supply', 
                                       'avg_daily_sales', 'forecast_demand', 'perishable', 'avg_days_to_expiry'])
        finally:
            if conn is not None:
                conn.close()

    def _analyze_inventory_health(
        self,
        inventory_df: pd.DataFrame,
        demand_analysis: Dict[str, Any],
        shelf_life_alerts: List[Dict],
        scenario: str
    ) -> Dict[str, Any]:
        """Analyze overall inventory health"""
        
        # Handle empty dataframe
        if inventory_df.empty:
            return {
                "critical_stock": [],
                "overstock": [],
                "optimal": [],
                "insights": "No inventory data available",
                "trending_impact": {"up": [], "down": []}
            }
        
        # Ensure required columns exist with default values
        if 'days_of_supply' not in inventory_df.columns:
            inventory_df['days_of_supply'] = 999.0
        if 'avg_daily_sales' not in inventory_df.columns:
            inventory_df['avg_daily_sales'] = 0.0
        if 'avg_days_to_expiry' not in inventory_df.columns:
            inventory_df['avg_days_to_expiry'] = 999.0
        if 'perishable' not in inventory_df.columns:
            inventory_df['perishable'] = False
        
        # Categorize inventory status
        critical_stock = inventory_df[
            (inventory_df['days_of_supply'] < 2) & 
            (inventory_df['avg_daily_sales'] > 0)
        ]
        
        overstock = inventory_df[
            (inventory_df['days_of_supply'] > 14) | 
            ((inventory_df['perishable'] == True) & 
             (inventory_df['days_of_supply'] > inventory_df['avg_days_to_expiry']))
        ]
        
        optimal = inventory_df[
            (inventory_df['days_of_supply'] >= 2) & 
            (inventory_df['days_of_supply'] <= 14) &
            ~inventory_df['product_id'].isin(overstock['product_id'])
        ]
        
        # Consider demand trends
        trending_up_ids = [p['product_id'] for p in demand_analysis.get('trending_up', [])]
        trending_down_ids = [p['product_id'] for p in demand_analysis.get('trending_down', [])]
        
        # Use LLM to analyze complex patterns
        prompt = f"""
        Analyze inventory health based on:
        Scenario: {scenario}
        
        Critical stock (< 2 days supply): {len(critical_stock)} products
        Overstock: {len(overstock)} products
        Products trending up: {len(trending_up_ids)}
        Products trending down: {len(trending_down_ids)}
        Products with shelf life issues: {len(shelf_life_alerts)}
        
        Top critical items:
        {critical_stock.head(10)[['name', 'category', 'current_stock', 'days_of_supply']].to_string()}
        
        Provide strategic inventory insights and categorize products by action needed.
        """
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        return {
            "critical_stock": critical_stock.to_dict('records'),
            "overstock": overstock.to_dict('records'),
            "optimal": optimal.to_dict('records'),
            "insights": response.content,
            "trending_impact": {
                "up": trending_up_ids,
                "down": trending_down_ids
            }
        }

    def _generate_restock_recommendations(
        self,
        inventory_analysis: Dict[str, Any],
        demand_analysis: Dict[str, Any],
        shelf_life_alerts: List[Dict]
    ) -> List[Dict]:
        """Generate specific restock recommendations"""
        
        recommendations = []
        
        # Products with critical stock
        for item in inventory_analysis['critical_stock']:
            # Adjust for demand trends
            trend_multiplier = 1.0
            if item['product_id'] in inventory_analysis['trending_impact']['up']:
                trend_multiplier = 1.3  # Order 30% more for trending up
            elif item['product_id'] in inventory_analysis['trending_impact']['down']:
                trend_multiplier = 0.7  # Order 30% less for trending down
            
            # Calculate reorder quantity
            if item['perishable']:
                # For perishables, order based on shelf life
                target_days = min(7, item['shelf_life_days'] * 0.5)
            else:
                target_days = 14  # Two weeks for non-perishables
            
            reorder_qty = max(
                10,  # Minimum order
                int(item['forecast_demand'] * target_days * trend_multiplier)
            )
            
            recommendations.append({
                "product_id": int(item['product_id']),
                "name": item['name'],
                "category": item['category'],
                "current_stock": int(item['current_stock']),
                "avg_daily_sales": float(item['avg_daily_sales']),
                "days_of_supply": float(item['days_of_supply']),
                "recommended_order": reorder_qty,
                "urgency": "high" if item['days_of_supply'] < 1 else "medium",
                "reasoning": f"Low stock - {item['days_of_supply']:.1f} days supply"
            })
        
        # Also check products trending up that aren't critical yet
        for trend_item in demand_analysis.get('trending_up', [])[:5]:
            if not any(r['product_id'] == trend_item['product_id'] for r in recommendations):
                # Look up current stock
                product_data = inventory_analysis['optimal'] + inventory_analysis['overstock']
                product_info = next(
                    (p for p in product_data if p['product_id'] == trend_item['product_id']), 
                    None
                )
                
                if product_info and product_info['days_of_supply'] < 7:
                    recommendations.append({
                        "product_id": trend_item['product_id'],
                        "name": trend_item['name'],
                        "category": product_info.get('category', 'unknown'),
                        "current_stock": int(product_info.get('current_stock', 0)),
                        "avg_daily_sales": float(product_info.get('avg_daily_sales', 0)),
                        "days_of_supply": float(product_info.get('days_of_supply', 0)),
                        "recommended_order": int(
                            product_info.get('forecast_demand', 10) * 14 * 1.3
                        ),
                        "urgency": "medium",
                        "reasoning": f"Trending up {trend_item['increase_pct']*100:.0f}%"
                    })
        
        # Sort by urgency and value
        recommendations.sort(
            key=lambda x: (
                0 if x['urgency'] == 'high' else 1,
                -x.get('avg_daily_sales', 0)
            )
        )
        
        return recommendations[:20]  # Top 20 recommendations