# retail_workflow.py - Main LangGraph workflow orchestration
from typing import TypedDict, Annotated, Sequence, Literal, List, Dict
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
import operator

# Import all agents
from supervisor_agent import SupervisorAgent
from demand_agent import DemandAgent
from shelf_life_agent import ShelfLifeAgent
from inventory_agent import InventoryAgent
from pricing_discount_agent import PricingDiscountAgent
from waste_diversion_agent import WasteDiversionAgent
from sustainability_monitoring_agent import SustainabilityMonitoringAgent
from shared_state import SharedRetailState, initialize_state, AgentMode, add_agent_message

class RetailSustainabilityWorkflow:
    """Main workflow orchestrating all retail sustainability agents"""
    
    def __init__(self, openai_api_key: str, db_url: str):
        self.openai_api_key = openai_api_key
        self.db_url = db_url
        
        # Initialize all agents
        self.supervisor = SupervisorAgent(openai_api_key)
        self.demand_agent = DemandAgent(openai_api_key, db_url)
        self.shelf_life_agent = ShelfLifeAgent(openai_api_key, db_url)
        self.inventory_agent = InventoryAgent(openai_api_key, db_url)
        self.pricing_agent = PricingDiscountAgent(openai_api_key, db_url)
        self.waste_agent = WasteDiversionAgent(openai_api_key, db_url)
        self.sustainability_agent = SustainabilityMonitoringAgent(openai_api_key, db_url)
        
        # Build the workflow
        self.workflow = self._build_workflow()
        
        # Add memory for conversation history
        self.memory = MemorySaver()
        self.app = self.workflow.compile(checkpointer=self.memory)

    def _build_workflow(self) -> StateGraph:
        """Build the enhanced LangGraph workflow with proper integration"""
        
        # Create the graph
        workflow = StateGraph(SharedRetailState)
        
        # Add all nodes (agents)
        workflow.add_node("supervisor", self.supervisor_node)
        workflow.add_node("demand", self.demand_node)
        workflow.add_node("shelf_life", self.shelf_life_node)
        workflow.add_node("inventory", self.inventory_node)
        workflow.add_node("pricing", self.pricing_node)
        workflow.add_node("waste_diversion", self.waste_diversion_node)
        workflow.add_node("sustainability", self.sustainability_node)
        workflow.add_node("final_summary", self.final_summary_node)
        
        # Define the entry point
        workflow.set_entry_point("supervisor")
        
        # Add conditional edges based on supervisor routing
        workflow.add_conditional_edges(
            "supervisor",
            self.route_supervisor,
            {
                "demand": "demand",
                "shelf_life": "shelf_life",
                "parallel_demand": "demand",  # Handle parallel execution differently
                "emergency": "shelf_life"
            }
        )
        
        # Define the complete flow
        # Demand path
        workflow.add_edge("demand", "inventory")
        
        # Shelf life path  
        workflow.add_edge("shelf_life", "inventory")
        
        # Inventory leads to pricing
        workflow.add_edge("inventory", "pricing")
        
        # Pricing leads to waste diversion
        workflow.add_edge("pricing", "waste_diversion")
        
        # Waste diversion leads to sustainability
        workflow.add_edge("waste_diversion", "sustainability")
        
        # Sustainability leads to final summary
        workflow.add_edge("sustainability", "final_summary")
        
        # Final summary to END
        workflow.add_edge("final_summary", END)
        
        return workflow

    # Node functions for each agent
    def supervisor_node(self, state: SharedRetailState) -> SharedRetailState:
        """Supervisor agent node"""
        return self.supervisor.analyze_scenario(state)
    
    def demand_node(self, state: SharedRetailState) -> SharedRetailState:
        """Demand analysis agent node"""
        return self.demand_agent.analyze(state)
    
    def shelf_life_node(self, state: SharedRetailState) -> SharedRetailState:
        """Shelf life monitoring agent node"""
        return self.shelf_life_agent.analyze(state)
    
    def inventory_node(self, state: SharedRetailState) -> SharedRetailState:
        """Inventory management agent node"""
        return self.inventory_agent.analyze(state)
    
    def pricing_node(self, state: SharedRetailState) -> SharedRetailState:
        """Pricing and discount agent node"""
        return self.pricing_agent.analyze(state)
    
    def waste_diversion_node(self, state: SharedRetailState) -> SharedRetailState:
        """Waste diversion agent node"""
        return self.waste_agent.analyze(state)
    
    def sustainability_node(self, state: SharedRetailState) -> SharedRetailState:
        """Sustainability monitoring agent node"""
        return self.sustainability_agent.analyze(state)
    
    def route_supervisor(self, state: SharedRetailState) -> str:
        """Enhanced routing based on supervisor's decision"""
        routing = state.get('routing_decision', 'full_pipeline')
        priority = state.get('priority_level', 'medium')
        
        # For critical situations, check shelf life first
        if priority == 'critical' or routing == 'emergency_waste':
            return "emergency"
        
        # For parallel execution - start with demand, then shelf life will be handled separately
        if routing == 'parallel_analysis':
            return "parallel_demand"
        
        # Check what's been completed
        has_demand = bool(state.get('demand_analysis'))
        has_shelf_life = bool(state.get('shelf_life_alerts') is not None)
        
        # Standard routing
        if not has_demand and routing in ['demand_first', 'full_pipeline']:
            return "demand"
        elif not has_shelf_life:
            return "shelf_life"
        else:
            return "demand"  # Default
    
    def final_summary_node(self, state: SharedRetailState) -> SharedRetailState:
        """Final summary node that consolidates all agent outputs"""
        
        # Calculate total impact
        total_waste_prevented = state.get('environmental_metrics', {}).get('waste_prevented_kg', 0)
        total_meals_donated = state.get('environmental_metrics', {}).get('donation_meals', 0)
        total_carbon_saved = state.get('environmental_metrics', {}).get('carbon_saved_kg', 0)
        
        # Financial impact
        revenue_protected = sum(p.get('expected_revenue_delta', 0) for p in state.get('pricing_changes', []))
        donation_tax_benefit = sum(d.get('tax_benefit', 0) for d in state.get('diversion_actions', []))
        
        # Create executive summary
        summary = {
            "impact_summary": {
                "environmental": {
                    "waste_prevented_kg": total_waste_prevented,
                    "carbon_saved_kg": total_carbon_saved,
                    "meals_donated": total_meals_donated,
                    "water_saved_liters": state.get('environmental_metrics', {}).get('water_saved_liters', 0)
                },
                "financial": {
                    "revenue_impact": revenue_protected,
                    "tax_benefits": donation_tax_benefit,
                    "total_value_recovered": revenue_protected + donation_tax_benefit
                },
                "social": {
                    "people_fed": total_meals_donated // 3,
                    "partner_organizations": len(set(d.get('partner') for d in state.get('diversion_actions', [])))
                }
            },
            "key_actions": {
                "immediate": [a for a in state.get('final_actions', []) if a.get('priority') == 1],
                "scheduled": [a for a in state.get('final_actions', []) if a.get('priority') > 1]
            },
            "compliance_achieved": all(state.get('compliance_status', {}).values())
        }
        
        state['executive_summary'] = summary
        
        add_agent_message(
            state,
            "final_summary",
            f"Workflow complete: {total_waste_prevented:.1f}kg waste prevented, "
            f"{total_meals_donated} meals donated, ${revenue_protected + donation_tax_benefit:.2f} value recovered",
            summary
        )
        
        return state
    
    def run_scenario(self, scenario: str, mode: AgentMode = AgentMode.COMMAND) -> SharedRetailState:
        """Run a complete scenario through the workflow"""
        
        # Initialize state
        initial_state = initialize_state(scenario, mode)
        
        # Run the workflow
        config = {"configurable": {"thread_id": f"scenario_{hash(scenario)}"}}
        final_state = self.app.invoke(initial_state, config)
        
        # Generate final summary
        final_state = self.supervisor.summarize_execution(final_state)
        
        return final_state
    
    def run_manual_step(self, state: SharedRetailState, step: str) -> SharedRetailState:
        """Run a single step in manual mode"""
        
        config = {"configurable": {"thread_id": f"manual_{state['timestamp']}"}}
        
        # Map step to node
        step_map = {
            "analyze_demand": self.demand_node,
            "check_shelf_life": self.shelf_life_node,
            "review_inventory": self.inventory_node,
            "optimize_pricing": self.pricing_node,
            "plan_waste_diversion": self.waste_diversion_node,
            "monitor_sustainability": self.sustainability_node
        }
        
        if step in step_map:
            # Execute the specific step
            updated_state = step_map[step](state)
            return updated_state
        else:
            raise ValueError(f"Unknown step: {step}")
    
    def get_agent_conversation(self, state: SharedRetailState) -> List[Dict]:
        """Get the conversation history from all agents"""
        return state.get('agent_messages', [])
    
    def get_final_report(self, state: SharedRetailState) -> Dict:
        """Generate a comprehensive final report"""
        
        report = {
            "scenario": state['scenario'],
            "execution_time": state['timestamp'],
            "mode": state['mode'],
            "priority": state['priority_level'],
            
            "analysis_summary": {
                "demand_trends": {
                    "products_trending_up": len(state.get('demand_analysis', {}).get('trending_up', [])),
                    "products_trending_down": len(state.get('demand_analysis', {}).get('trending_down', [])),
                },
                "shelf_life_status": {
                    "critical_expiries": len([a for a in state.get('shelf_life_alerts', []) if a.get('urgency') == 'critical']),
                    "total_alerts": len(state.get('shelf_life_alerts', [])),
                    "value_at_risk": sum(a.get('value_at_risk', 0) for a in state.get('shelf_life_alerts', []))
                },
                "inventory_health": {
                    "critical_stock": len(state.get('inventory_levels', {}).get('critical_stock', [])),
                    "overstock": len(state.get('inventory_levels', {}).get('overstock', [])),
                    "restock_needed": len(state.get('restock_recommendations', []))
                }
            },
            
            "actions_taken": {
                "pricing_changes": len(state.get('pricing_changes', [])),
                "waste_diversions": len(state.get('diversion_actions', [])),
                "total_discounts_applied": sum(p.get('discount_percentage', 0) for p in state.get('pricing_changes', []))
            },
            
            "sustainability_impact": state.get('environmental_metrics', {}),
            "compliance_status": state.get('compliance_status', {}),
            
            "final_recommendations": state.get('final_actions', []),
            "execution_status": state.get('execution_status', 'unknown')
        }
        
        return report