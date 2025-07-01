# shared_state.py - Shared state structure for all agents
from typing import TypedDict, List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

class AgentMode(Enum):
    MANUAL = "manual"
    COMMAND = "command"
    STREAMING = "streaming"

class Priority(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class SharedRetailState(TypedDict):
    """Shared state that flows through all agents"""
    
    # Scenario Context
    scenario: str
    scenario_type: str  # "user_command", "market_condition", "scheduled", "alert_triggered"
    timestamp: str
    mode: str  # manual/command/streaming
    
    # Supervisor Decisions
    routing_decision: str  # which agents to activate
    priority_level: str
    parallel_execution: List[str]  # agents that can run in parallel
    
    # Analysis Results from Demand Agent
    demand_analysis: Dict[str, Any]
    # Structure: {
    #     "trending_up": [{"product_id": 1, "name": "...", "increase_pct": 0.3}],
    #     "trending_down": [{"product_id": 2, "name": "...", "decrease_pct": 0.2}],
    #     "seasonal_impact": {...},
    #     "external_factors": [{...}]
    # }
    
    # Shelf Life Agent Results
    shelf_life_alerts: List[Dict]
    # Structure: [
    #     {
    #         "product_id": 1,
    #         "name": "...",
    #         "batch_number": "...",
    #         "expiry_date": "...",
    #         "days_until_expiry": 2,
    #         "quantity": 100,
    #         "value_at_risk": 299.99,
    #         "urgency": "critical"
    #     }
    # ]
    
    # Inventory Agent Results
    inventory_levels: Dict[str, Any]
    # Structure: {
    #     "critical_stock": [...],
    #     "overstock": [...],
    #     "optimal": [...]
    # }
    restock_recommendations: List[Dict]
    # Structure: [
    #     {
    #         "product_id": 1,
    #         "current_stock": 50,
    #         "recommended_order": 200,
    #         "reasoning": "..."
    #     }
    # ]
    
    # Pricing & Discount Agent Decisions
    pricing_changes: List[Dict]
    # Structure: [
    #     {
    #         "product_id": 1,
    #         "current_price": 3.99,
    #         "new_price": 2.99,
    #         "discount_pct": 25,
    #         "reason": "approaching_expiry",
    #         "effective_immediately": True
    #     }
    # ]
    
    # Waste Diversion Actions
    diversion_actions: List[Dict]
    # Structure: [
    #     {
    #         "product_id": 1,
    #         "quantity": 50,
    #         "diversion_type": "donation",
    #         "recipient": "Local Food Bank",
    #         "pickup_time": "...",
    #         "compliance_checked": True
    #     }
    # ]
    
    # Sustainability Metrics
    environmental_metrics: Dict[str, float]
    # Structure: {
    #     "waste_prevented_kg": 125.5,
    #     "carbon_saved_kg": 45.2,
    #     "donation_value": 450.00,
    #     "energy_usage_kwh": 234.5
    # }
    
    compliance_status: Dict[str, bool]
    # Structure: {
    #     "food_safety": True,
    #     "esg_reporting": True,
    #     "csrd_compliant": True
    # }
    
    # Execution Plan
    final_actions: List[Dict]
    human_approvals_needed: List[Dict]
    
    # Execution tracking
    execution_status: str  # "pending", "in_progress", "completed", "failed"
    errors: List[str]
    agent_messages: List[Dict]  # Communication between agents

# Helper functions for state management
def initialize_state(scenario: str, mode: AgentMode = AgentMode.COMMAND) -> SharedRetailState:
    """Initialize a new shared state"""
    return SharedRetailState(
        scenario=scenario,
        scenario_type="user_command",
        timestamp=datetime.now().isoformat(),
        mode=mode.value,
        routing_decision="",
        priority_level=Priority.MEDIUM.value,
        parallel_execution=[],
        demand_analysis={},
        shelf_life_alerts=[],
        inventory_levels={},
        restock_recommendations=[],
        pricing_changes=[],
        diversion_actions=[],
        environmental_metrics={},
        compliance_status={},
        final_actions=[],
        human_approvals_needed=[],
        execution_status="pending",
        errors=[],
        agent_messages=[]
    )

def add_agent_message(state: SharedRetailState, agent: str, message: str, data: Optional[Dict] = None) -> None:
    """Add a message from an agent to the shared state"""
    state["agent_messages"].append({
        "agent": agent,
        "message": message,
        "data": data or {},
        "timestamp": datetime.now().isoformat()
    })

def set_error(state: SharedRetailState, agent: str, error: str) -> None:
    """Record an error in the state"""
    state["errors"].append(f"[{agent}] {error}")
    state["execution_status"] = "failed"