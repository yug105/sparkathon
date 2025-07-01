# supervisor_agent.py - Main orchestrator agent
import re
from typing import Dict, List, Literal
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from shared_state import SharedRetailState, Priority, add_agent_message
import json

class SupervisorAgent:
    """Supervisor agent that orchestrates the multi-agent workflow"""
    
    def __init__(self, openai_api_key: str):
        self.llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            openai_api_key=openai_api_key,
            temperature=0.3
        )
        
        self.system_prompt = """You are the Supervisor Agent for a retail sustainability system. 
        Your role is to analyze scenarios and determine:
        1. Which agents need to be activated
        2. The priority level (critical/high/medium/low)
        3. Whether agents can run in parallel
        4. The optimal routing path

        Available agents:
        - demand_agent: Analyzes demand trends and market conditions
        - shelf_life_agent: Monitors product expiration dates
        - inventory_agent: Manages stock levels and reordering
        - pricing_agent: Handles discounts and pricing strategies
        - waste_diversion_agent: Manages donations and recycling
        - sustainability_agent: Tracks environmental metrics

        Routing strategies:
        - "demand_first": Start with demand analysis
        - "urgent_expiry": Prioritize shelf life monitoring
        - "parallel_analysis": Run demand and shelf life in parallel
        - "full_pipeline": Run all agents sequentially
        - "emergency_waste": Direct to waste diversion for critical expiry

        Respond with a JSON object containing:
        {
            "routing": "routing_strategy",
            "priority": "priority_level",
            "parallel_agents": ["agent1", "agent2"],
            "reasoning": "explanation",
            "expected_outcomes": ["outcome1", "outcome2"]
        }
        """

    def analyze_scenario(self, state: SharedRetailState) -> SharedRetailState:
        """Analyze the scenario and determine routing"""
        
        # Construct the prompt
        prompt = f"""
        Analyze this scenario and determine the optimal agent routing:
        
        Scenario: {state['scenario']}
        Mode: {state['mode']}
        Timestamp: {state['timestamp']}
        
        Consider:
        - Urgency of the situation
        - Which agents are most relevant
        - Potential for parallel execution
        - Expected business impact
        """
        
        try:
            # Call LLM for routing decision
            messages = [
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=prompt)
            ]
            
            response = self.llm.invoke(messages)
            
            # Parse JSON response
            json_match = re.search(r'\{.*\}', response.content, re.DOTALL)
            if json_match:
                routing_decision = json.loads(json_match.group())
            else:
                # Fallback routing
                routing_decision = {
                    "routing": "full_pipeline",
                    "priority": "medium",
                    "parallel_agents": [],
                    "reasoning": "Could not parse routing decision, using default",
                    "expected_outcomes": ["Complete analysis"]
                }
            
            # Update state with routing decision
            state['routing_decision'] = routing_decision['routing']
            state['priority_level'] = routing_decision['priority']
            
            # Add supervisor message
            add_agent_message(
                state, 
                "supervisor",
                f"Routing decision: {routing_decision['routing']} with {routing_decision['priority']} priority",
                routing_decision
            )
            
            # Determine urgency based on keywords
            scenario_lower = state['scenario'].lower()
            if any(word in scenario_lower for word in ['urgent', 'critical', 'emergency', 'immediately']):
                state['priority_level'] = Priority.CRITICAL.value
            elif any(word in scenario_lower for word in ['expiring', 'expired', 'tomorrow', 'today']):
                state['priority_level'] = Priority.HIGH.value
                
        except Exception as e:
            add_agent_message(state, "supervisor", f"Error in routing analysis: {str(e)}")
            # Default routing on error
            state['routing_decision'] = "full_pipeline"
            state['priority_level'] = Priority.MEDIUM.value
            
        return state

    def route_to_agents(self, state: SharedRetailState) -> str:
        """Determine next agent(s) based on routing decision"""
        
        routing = state['routing_decision']
        
        # Define routing paths
        routing_map = {
            "demand_first": "demand",
            "urgent_expiry": "shelf_life",
            "parallel_analysis": "parallel_demand_shelf",
            "full_pipeline": "demand",
            "emergency_waste": "shelf_life"  # Goes to shelf_life first to identify items
        }
        
        next_agent = routing_map.get(routing, "demand")
        
        # Check if we're doing parallel execution
        if next_agent == "parallel_demand_shelf":
            # In a real implementation, this would trigger parallel execution
            # For now, we'll start with demand
            next_agent = "demand"
            add_agent_message(
                state,
                "supervisor",
                "Starting parallel execution with demand and shelf_life agents"
            )
        
        return next_agent

    def check_completion(self, state: SharedRetailState) -> bool:
        """Check if all necessary agents have completed their work"""
        
        # Check which agents have provided output
        has_demand = bool(state.get('demand_analysis'))
        has_shelf_life = bool(state.get('shelf_life_alerts') is not None)
        has_inventory = bool(state.get('inventory_levels'))
        has_pricing = bool(state.get('pricing_changes') is not None)
        has_waste = bool(state.get('diversion_actions') is not None)
        has_sustainability = bool(state.get('environmental_metrics'))
        
        routing = state['routing_decision']
        
        # Define completion criteria based on routing
        if routing == "emergency_waste":
            return has_shelf_life and has_waste and has_sustainability
        elif routing == "demand_first":
            return has_demand and has_inventory and has_pricing
        elif routing == "urgent_expiry":
            return has_shelf_life and has_pricing and has_waste
        else:  # full_pipeline
            return all([has_demand, has_shelf_life, has_inventory, 
                       has_pricing, has_waste, has_sustainability])

    def summarize_execution(self, state: SharedRetailState) -> SharedRetailState:
        """Create final execution summary"""
        
        summary_prompt = f"""
        Summarize the execution results and create a final action plan:
        
        Scenario: {state['scenario']}
        Priority: {state['priority_level']}
        
        Results:
        - Demand Analysis: {len(state.get('demand_analysis', {}).get('trending_up', []))} products trending up
        - Shelf Life Alerts: {len(state.get('shelf_life_alerts', []))} products expiring soon
        - Pricing Changes: {len(state.get('pricing_changes', []))} price adjustments
        - Waste Diversion: {len(state.get('diversion_actions', []))} diversion actions
        
        Create a concise action plan with priorities.
        """
        
        messages = [
            SystemMessage(content="You are creating a final execution summary for the retail system."),
            HumanMessage(content=summary_prompt)
        ]
        
        response = self.llm.invoke(messages)
        
        # Create final actions
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
            },
            {
                "action": "Update inventory orders",
                "priority": 3,
                "details": f"{len(state.get('restock_recommendations', []))} products to reorder"
            }
        ]
        
        state['execution_status'] = 'completed'
        
        add_agent_message(
            state,
            "supervisor",
            "Execution completed successfully",
            {"summary": response.content}
        )
        
        return state