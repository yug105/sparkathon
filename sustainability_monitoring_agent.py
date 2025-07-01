# sustainability_monitoring_agent.py - Tracks and reports environmental performance
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import create_engine, text
import pandas as pd
import json
import numpy as np
from shared_state import SharedRetailState, add_agent_message

def convert_numpy_types(obj):
    """Recursively convert numpy types to native Python types"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    else:
        return obj

class SustainabilityMonitoringAgent:
    """Agent responsible for tracking environmental metrics and compliance reporting"""
    
    def __init__(self, openai_api_key: str, db_url: str):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            openai_api_key=openai_api_key,
            temperature=0.3
        )
        self.engine = create_engine(db_url)
        
        self.system_prompt = """You are the Sustainability Monitoring and Reporting Agent.
        Your responsibilities include:
        
        1. Environmental metrics tracking:
           - Energy usage and efficiency
           - Waste generation and diversion rates
           - Carbon footprint calculation
           - Water consumption
           - Plastic reduction progress
        
        2. Compliance monitoring:
           - ESG (Environmental, Social, Governance) requirements
           - CSRD (Corporate Sustainability Reporting Directive)
           - Local environmental regulations
           - Food safety standards
        
        3. Optimization opportunities:
           - Identify areas for improvement
           - Suggest carbon reduction strategies
           - Recommend resource optimization
        
        4. Reporting:
           - Generate automated sustainability reports
           - Track progress against targets
           - Provide actionable insights
        
        Always quantify environmental impact and provide specific recommendations.
        """
        
        # Define sustainability targets
        self.sustainability_targets = {
            "waste_reduction": 0.5,  # 50% reduction target
            "carbon_footprint": 0.3,  # 30% reduction
            "plastic_reduction": 0.4,  # 40% reduction
            "energy_efficiency": 0.2,  # 20% improvement
            "donation_rate": 0.8  # 80% of expiring food donated
        }

    def analyze(self, state: SharedRetailState) -> SharedRetailState:
        """Main sustainability analysis function"""
        
        add_agent_message(state, "sustainability_agent", "Starting sustainability monitoring")
        
        # Collect metrics from all previous agent actions
        current_metrics = self._collect_current_metrics(state)
        
        # Fetch historical metrics for comparison
        historical_metrics = self._fetch_historical_metrics()
        
        # Calculate comprehensive environmental impact
        impact_analysis = self._analyze_environmental_impact(
            current_metrics,
            historical_metrics,
            state
        )
        
        # Check compliance status
        compliance_status = self._check_compliance_status(current_metrics)
        
        # Generate optimization recommendations
        recommendations = self._generate_sustainability_recommendations(
            impact_analysis,
            compliance_status,
            state['scenario']
        )
        
        # Update state
        state['environmental_metrics'].update(convert_numpy_types(impact_analysis['metrics']))
        state['compliance_status'] = convert_numpy_types(compliance_status)
        
        # Add sustainability insights to final actions
        if 'final_actions' not in state:
            state['final_actions'] = []
            
        state['final_actions'].extend(convert_numpy_types(recommendations))
        
        add_agent_message(
            state,
            "sustainability_agent",
            f"Waste reduction: {impact_analysis['metrics']['waste_reduction_rate']:.1%}, " +
            f"Carbon saved: {impact_analysis['metrics']['carbon_saved_kg']:.1f}kg",
            convert_numpy_types(impact_analysis)
        )
        
        return state

    def _collect_current_metrics(self, state: SharedRetailState) -> Dict[str, Any]:
        """Collect metrics from current workflow execution"""
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "workflow_id": str(state.get('timestamp', '')),
            "mode": str(state.get('mode', '')),
            
            # From shelf life agent
            "products_expiring": int(len(state.get('shelf_life_alerts', []))),
            "value_at_risk": float(sum(a.get('value_at_risk', 0) for a in state.get('shelf_life_alerts', []))),
            
            # From pricing agent
            "products_marked_down": int(len(state.get('pricing_changes', []))),
            "avg_discount": float(sum(p.get('discount_percentage', 0) for p in state.get('pricing_changes', [])) / max(len(state.get('pricing_changes', [])), 1)),
            
            # From waste diversion agent
            "items_diverted": int(sum(a.get('quantity', 0) for a in state.get('diversion_actions', []))),
            "items_donated": int(sum(a.get('quantity', 0) for a in state.get('diversion_actions', []) if a.get('diversion_type') == 'donation')),
            "donation_value": float(sum(a.get('retail_value', 0) for a in state.get('diversion_actions', []) if a.get('diversion_type') == 'donation')),
            
            # From environmental metrics
            "waste_prevented_kg": float(state.get('environmental_metrics', {}).get('waste_prevented_kg', 0)),
            "carbon_saved_kg": float(state.get('environmental_metrics', {}).get('carbon_saved_kg', 0)),
            "donation_meals": int(state.get('environmental_metrics', {}).get('donation_meals', 0))
        }
        
        return metrics

    def _fetch_historical_metrics(self) -> pd.DataFrame:
        """Fetch historical sustainability metrics"""
        query = """
        SELECT 
            metric_type,
            metric_value,
            metadata,
            recorded_at
        FROM system_metrics
        WHERE metric_type IN ('waste_reduction', 'carbon_saved', 'donation_rate', 'energy_usage')
        AND recorded_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
        ORDER BY recorded_at DESC
        """
        
        return pd.read_sql(query, self.engine)

    def _analyze_environmental_impact(
        self,
        current_metrics: Dict[str, Any],
        historical_metrics: pd.DataFrame,
        state: SharedRetailState
    ) -> Dict[str, Any]:
        """Analyze environmental impact comprehensively"""
        
        # Calculate waste reduction rate
        total_expiring = current_metrics['products_expiring']
        total_diverted = current_metrics['items_diverted']
        waste_reduction_rate = total_diverted / max(total_expiring, 1)
        
        # Calculate donation effectiveness
        donation_rate = current_metrics['items_donated'] / max(total_diverted, 1)
        
        # Estimate energy usage (simplified)
        energy_usage_kwh = 100  # Base usage
        if state.get('mode') == 'streaming':
            energy_usage_kwh *= 1.2  # 20% more for real-time processing
        
        # Calculate week-over-week improvement
        if not historical_metrics.empty:
            last_week_waste = historical_metrics[
                historical_metrics['metric_type'] == 'waste_reduction'
            ]['metric_value'].mean()
            
            improvement = (waste_reduction_rate - last_week_waste) / max(last_week_waste, 0.1)
        else:
            improvement = 0
        
        # Water saved from prevented waste (1000L per kg of food waste)
        water_saved = current_metrics['waste_prevented_kg'] * 1000
        
        # Plastic reduction (estimated from packaging)
        plastic_reduced_kg = current_metrics['items_diverted'] * 0.02  # 20g per item average
        
        analysis = {
            "metrics": {
                "waste_reduction_rate": float(waste_reduction_rate),
                "donation_rate": float(donation_rate),
                "carbon_saved_kg": float(current_metrics['carbon_saved_kg']),
                "water_saved_liters": float(water_saved),
                "plastic_reduced_kg": float(plastic_reduced_kg),
                "energy_usage_kwh": float(energy_usage_kwh),
                "donation_meals": int(current_metrics['donation_meals']),
                "economic_value_recovered": float(current_metrics['donation_value']) * 0.3  # Tax benefit
            },
            "trends": {
                "week_over_week_improvement": improvement,
                "on_track_for_targets": waste_reduction_rate >= self.sustainability_targets['waste_reduction']
            },
            "social_impact": {
                "meals_provided": current_metrics['donation_meals'],
                "community_partners_served": len(set(a.get('partner') for a in state.get('diversion_actions', []) if a.get('diversion_type') == 'donation')),
                "estimated_people_fed": current_metrics['donation_meals'] // 3
            }
        }
        
        return analysis

    def _check_compliance_status(self, metrics: Dict[str, Any]) -> Dict[str, bool]:
        """Check compliance with various sustainability frameworks"""
        
        compliance = {
            "food_safety": True,  # Assume true if following proper procedures
            "esg_reporting": metrics.get('waste_prevented_kg', 0) > 0,  # Basic ESG compliance
            "csrd_compliant": metrics.get('donation_rate', 0) > 0.5,  # Simplified CSRD check
            "local_regulations": True,  # Assume compliant
            "iso_14001": metrics.get('waste_reduction_rate', 0) > 0.3,  # Environmental management
            "zero_waste_certified": metrics.get('waste_reduction_rate', 0) > 0.9
        }
        
        return compliance

    def _generate_sustainability_recommendations(
        self,
        impact_analysis: Dict[str, Any],
        compliance_status: Dict[str, bool],
        scenario: str
    ) -> List[Dict]:
        """Generate actionable sustainability recommendations"""
        
        recommendations = []
        
        # Check against targets
        metrics = impact_analysis['metrics']
        
        if metrics['waste_reduction_rate'] < self.sustainability_targets['waste_reduction']:
            recommendations.append({
                "action": "Improve waste reduction",
                "priority": 1,
                "details": f"Current rate {metrics['waste_reduction_rate']:.1%} below target {self.sustainability_targets['waste_reduction']:.1%}",
                "suggestion": "Increase markdown timing and donation partnerships"
            })
        
        if metrics['donation_rate'] < self.sustainability_targets['donation_rate']:
            recommendations.append({
                "action": "Increase donation rate",
                "priority": 2,
                "details": f"Only {metrics['donation_rate']:.1%} of diverted items donated",
                "suggestion": "Expand food bank partnerships and improve logistics"
            })
        
        # Use LLM for strategic recommendations
        prompt = f"""
        Based on this sustainability analysis for scenario: {scenario}
        
        Current performance:
        - Waste reduction: {metrics['waste_reduction_rate']:.1%}
        - Carbon saved: {metrics['carbon_saved_kg']:.1f}kg
        - Donation meals: {metrics['donation_meals']}
        - Water saved: {metrics['water_saved_liters']:.0f}L
        
        Compliance status: {sum(compliance_status.values())}/{len(compliance_status)} requirements met
        
        Provide 3 specific recommendations to improve sustainability performance.
        Focus on actionable steps that can be implemented immediately.
        """
        
        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=prompt)
        ]
        
        try:
            response = self.llm.invoke(messages)
            
            # Add LLM recommendations
            recommendations.append({
                "action": "Sustainability optimization",
                "priority": 3,
                "details": "AI-generated strategic recommendations",
                "suggestion": response.content[:200]  # First 200 chars
            })
            
        except Exception as e:
            # Log error but don't reference state here since it's not in scope
            print(f"Sustainability agent LLM recommendation error: {str(e)}")
        
        # Save metrics to database
        self._save_metrics_to_db(metrics)
        
        return recommendations

    def _save_metrics_to_db(self, metrics: Dict[str, Any]):
        """Save sustainability metrics to database"""
        conn = None
        try:
            conn = self.engine.connect()
            # Save key metrics
            for metric_type, value in [
                ('waste_reduction', metrics['waste_reduction_rate']),
                ('carbon_saved', metrics['carbon_saved_kg']),
                ('donation_rate', metrics['donation_rate']),
                ('energy_usage', metrics['energy_usage_kwh'])
            ]:
                conn.execute(text("""
                    INSERT INTO system_metrics (metric_type, metric_value, metadata, recorded_at)
                    VALUES (:type, :value, :metadata, :timestamp)
                    ON CONFLICT DO NOTHING
                """), {
                    'type': metric_type,
                    'value': value,
                    'metadata': json.dumps({"source": "sustainability_agent"}),
                    'timestamp': datetime.now()
                })
            conn.commit()
        except Exception as e:
            if conn is not None:
                conn.rollback()
            print(f"Error saving metrics: {str(e)}")
        finally:
            if conn is not None:
                conn.close()

    def generate_sustainability_report(self, state: SharedRetailState) -> Dict[str, Any]:
        """Generate a comprehensive sustainability report"""
        
        report = {
            "executive_summary": {
                "date": datetime.now().isoformat(),
                "scenario": state['scenario'],
                "overall_performance": "Meeting targets" if state['compliance_status'].get('esg_reporting') else "Needs improvement"
            },
            "key_metrics": state['environmental_metrics'],
            "compliance_status": state['compliance_status'],
            "social_impact": {
                "meals_donated": state['environmental_metrics'].get('donation_meals', 0),
                "community_value": f"${state['environmental_metrics'].get('donation_value', 0):.2f}"
            },
            "recommendations": [
                r['suggestion'] for r in state.get('final_actions', []) 
                if 'sustainability' in r.get('details', '').lower()
            ],
            "next_steps": "Continue monitoring and optimize based on real-time data"
        }
        
        return report