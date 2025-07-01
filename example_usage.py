# example_usage.py - Example of how to use the retail AI workflow
import os
from dotenv import load_dotenv
from retail_workflow import RetailSustainabilityWorkflow
from shared_state import AgentMode, SharedRetailState
import json
from langgraph.checkpoint.memory import InMemorySaver

# Load environment variables
load_dotenv()

def print_agent_messages(state: SharedRetailState):
    """Pretty print agent messages"""
    print("\n=== Agent Conversation ===")
    for msg in state.get('agent_messages', []):
        print(f"\n[{msg['agent'].upper()}] at {msg['timestamp']}")
        print(f"Message: {msg['message']}")
        if msg.get('data'):
            print(f"Data: {json.dumps(msg['data'], indent=2)}")

def demo_command_mode():
    """Demo: Command mode with various scenarios"""
    
    # Initialize workflow
    workflow = RetailSustainabilityWorkflow(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        db_url=os.getenv('NEON_DATABASE_URL')
    )
    
    # Scenario 1: Heatwave affecting demand
    print("\n" + "="*60)
    print("SCENARIO 1: Heatwave Alert - Beverage Demand Spike")
    print("="*60)
    
    scenario = """
    URGENT: Weather service reports heatwave starting tomorrow with temperatures 
    reaching 95°F for the next 3 days. Historical data shows beverage sales 
    increase by 40% during heatwaves. Need immediate analysis and action plan.
    """
    
    state = workflow.run_scenario(scenario, AgentMode.COMMAND)
    print_agent_messages(state)
    
    # Print summary
    report = workflow.get_final_report(state)
    print("\n=== Execution Summary ===")
    print(f"Priority Level: {state['priority_level']}")
    print(f"Pricing Changes: {report['actions_taken']['pricing_changes']}")
    print(f"Waste Prevented: {report['sustainability_impact'].get('waste_prevented_kg', 0):.1f} kg")
    print(f"Carbon Saved: {report['sustainability_impact'].get('carbon_saved_kg', 0):.1f} kg")

def demo_manual_mode():
    """Demo: Manual step-by-step execution"""
    
    print("\n" + "="*60)
    print("SCENARIO 2: Manual Mode - Step by Step Execution")
    print("="*60)
    
    workflow = RetailSustainabilityWorkflow(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        db_url=os.getenv('NEON_DATABASE_URL')
    )
    
    # Initialize state
    scenario = "Multiple dairy products approaching expiration within 48 hours"
    from shared_state import initialize_state
    state = initialize_state(scenario, AgentMode.MANUAL)
    
    # Step 1: Supervisor analysis
    print("\nStep 1: Supervisor Analysis")
    state = workflow.supervisor_node(state)
    print(f"Routing Decision: {state['routing_decision']}")
    print(f"Priority: {state['priority_level']}")
    
    # Step 2: Shelf Life Check
    print("\nStep 2: Shelf Life Monitoring")
    state = workflow.run_manual_step(state, "check_shelf_life")
    alerts = state.get('shelf_life_alerts', [])
    print(f"Found {len(alerts)} products expiring soon")
    if alerts:
        print(f"Most urgent: {alerts[0]['name']} expires in {alerts[0]['days_until_expiry']} days")
    
    # Step 3: Pricing Optimization
    print("\nStep 3: Pricing Optimization")
    state = workflow.run_manual_step(state, "optimize_pricing")
    pricing_changes = state.get('pricing_changes', [])
    print(f"Generated {len(pricing_changes)} pricing changes")
    
    # Step 4: Waste Diversion
    print("\nStep 4: Waste Diversion Planning")
    state = workflow.run_manual_step(state, "plan_waste_diversion")
    diversions = state.get('diversion_actions', [])
    print(f"Planned {len(diversions)} diversion actions")
    
    print("\n=== Manual Execution Complete ===")

def demo_streaming_mode():
    """Demo: Simulated streaming mode"""
    
    print("\n" + "="*60)
    print("SCENARIO 3: Streaming Mode - Real-time Monitoring")
    print("="*60)
    
    workflow = RetailSustainabilityWorkflow(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        db_url=os.getenv('NEON_DATABASE_URL')
    )
    
    # Simulate real-time triggers
    streaming_scenarios = [
        {
            "trigger": "INVENTORY_ALERT",
            "scenario": "System detected 15 batches of produce expiring within 24 hours with total value $450"
        },
        {
            "trigger": "COMPETITOR_ACTION", 
            "scenario": "MegaMart just announced 40% off all dairy products for the weekend"
        },
        {
            "trigger": "DEMAND_SPIKE",
            "scenario": "Sales of frozen items increased 60% in last 2 hours, likely due to storm warning"
        }
    ]
    
    for i, trigger_event in enumerate(streaming_scenarios):
        print(f"\n--- Real-time Trigger {i+1}: {trigger_event['trigger']} ---")
        print(f"Scenario: {trigger_event['scenario']}")
        
        state = workflow.run_scenario(trigger_event['scenario'], AgentMode.STREAMING)
        
        # Show immediate actions
        urgent_actions = [a for a in state.get('final_actions', []) if a.get('priority', 99) <= 2]
        print(f"\nImmediate Actions Required: {len(urgent_actions)}")
        for action in urgent_actions[:3]:
            print(f"  - {action.get('action')}: {action.get('details')}")

def demo_sustainability_report():
    """Demo: Generate sustainability report"""
    
    print("\n" + "="*60)
    print("SUSTAINABILITY REPORT GENERATION")
    print("="*60)
    
    workflow = RetailSustainabilityWorkflow(
        openai_api_key=os.getenv('OPENAI_API_KEY'),
        db_url=os.getenv('NEON_DATABASE_URL')
    )
    
    # Run a comprehensive scenario
    scenario = """
    Weekly sustainability review: Analyze all expiring products, optimize pricing 
    for waste reduction, maximize donations, and provide environmental impact report.
    """
    
    state = workflow.run_scenario(scenario, AgentMode.COMMAND)
    
    # Generate sustainability report
    sustainability_report = workflow.sustainability_agent.generate_sustainability_report(state)
    
    print("\n=== Sustainability Report ===")
    print(f"Date: {sustainability_report['executive_summary']['date']}")
    print(f"Overall Performance: {sustainability_report['executive_summary']['overall_performance']}")
    
    print("\nKey Metrics:")
    for metric, value in sustainability_report['key_metrics'].items():
        if isinstance(value, (int, float)):
            print(f"  - {metric}: {value:.2f}")
    
    print("\nCompliance Status:")
    for regulation, status in sustainability_report['compliance_status'].items():
        print(f"  - {regulation}: {'✓' if status else '✗'}")
    
    print("\nSocial Impact:")
    print(f"  - Meals Donated: {sustainability_report['social_impact']['meals_donated']}")
    print(f"  - Community Value: {sustainability_report['social_impact']['community_value']}")

if __name__ == "__main__":
    print("Retail AI Multi-Agent System Demo")
    print("=================================")
    
    # Check for required environment variables
    if not os.getenv('OPENAI_API_KEY') or not os.getenv('NEON_DATABASE_URL'):
        print("Error: Please set OPENAI_API_KEY and NEON_DATABASE_URL environment variables")
        exit(1)
    
    # Run demos
    try:
        # Demo 1: Command Mode
        demo_command_mode()
        
        # Demo 2: Manual Mode  
        demo_manual_mode()
        
        # Demo 3: Streaming Mode
        demo_streaming_mode()
        
        # Demo 4: Sustainability Report
        demo_sustainability_report()
        
    except Exception as e:
        print(f"\nError running demo: {str(e)}")
        import traceback
        traceback.print_exc()