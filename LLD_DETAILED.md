# Retail Sustainability Multi-Agent System - Low-Level Design (LLD)

## 1. Core Components

### 1.1 Shared State Management (`shared_state.py`)

**Purpose**: Central data structure that flows through all agents

**Key Structures**:

```python
class SharedRetailState(TypedDict):
    # Scenario Context
    scenario: str                    # User-provided scenario description
    scenario_type: str              # "user_command", "market_condition", "scheduled", "alert_triggered"
    timestamp: str                  # ISO format timestamp
    mode: str                       # "manual", "command", "streaming"

    # Supervisor Decisions
    routing_decision: str           # Which agents to activate
    priority_level: str             # "critical", "high", "medium", "low"
    parallel_execution: List[str]   # Agents that can run in parallel

    # Agent Results
    demand_analysis: Dict[str, Any]     # Demand trends and predictions
    shelf_life_alerts: List[Dict]       # Products approaching expiration
    inventory_levels: Dict[str, Any]    # Stock level analysis
    pricing_changes: List[Dict]         # Recommended price adjustments
    diversion_actions: List[Dict]       # Waste diversion plans
    environmental_metrics: Dict[str, float]  # Sustainability metrics

    # Execution Control
    execution_status: str           # "pending", "in_progress", "completed", "failed"
    errors: List[str]               # Error messages
    agent_messages: List[Dict]      # Communication between agents
```

**Helper Functions**:

```python
def initialize_state(scenario: str, mode: AgentMode) -> SharedRetailState:
    """Initialize a new shared state with default values"""

def add_agent_message(state: SharedRetailState, agent: str, message: str, data: Optional[Dict] = None):
    """Add a message from an agent to the shared state"""

def set_error(state: SharedRetailState, agent: str, error: str):
    """Record an error in the state"""
```

**Responsibilities**:

- Maintains consistency across all agents
- Provides audit trail through agent messages
- Enables state persistence and recovery
- Supports different execution modes

### 1.2 Workflow Orchestrator (`retail_workflow.py`)

**Purpose**: Coordinates the execution of all agents using LangGraph

**Key Components**:

```python
class RetailSustainabilityWorkflow:
    def __init__(self, gemini_api_key: str, db_url: str):
        # Initialize all agents
        self.supervisor = SupervisorAgent(gemini_api_key)
        self.demand_agent = DemandAgent(gemini_api_key, db_url)
        self.shelf_life_agent = ShelfLifeAgent(gemini_api_key, db_url)
        self.inventory_agent = InventoryAgent(gemini_api_key, db_url)
        self.pricing_agent = PricingDiscountAgent(gemini_api_key, db_url)
        self.waste_agent = WasteDiversionAgent(gemini_api_key, db_url)
        self.sustainability_agent = SustainabilityMonitoringAgent(gemini_api_key, db_url)

        # Build workflow
        self.workflow = self._build_workflow()
        self.memory = InMemorySaver()
        self.app = self.workflow.compile(checkpointer=self.memory)
```

**Workflow Graph**:

```
supervisor → demand → inventory → pricing → waste_diversion → sustainability → END
     ↓
shelf_life → pricing → waste_diversion → sustainability → END
```

**Key Methods**:

```python
def run_scenario(self, scenario: str, mode: AgentMode) -> SharedRetailState:
    """Run a complete scenario through the workflow"""

def run_manual_step(self, state: SharedRetailState, step: str) -> SharedRetailState:
    """Run a single step in manual mode"""

def get_final_report(self, state: SharedRetailState) -> Dict:
    """Generate a comprehensive final report"""
```

## 2. Specialized Agents

### 2.1 Supervisor Agent (`supervisor_agent.py`)

**Purpose**: Intelligent routing and scenario analysis

**Key Functions**:

```python
def analyze_scenario(self, state: SharedRetailState) -> SharedRetailState:
    """Analyze scenario and determine routing strategy"""

def route_to_agents(self, state: SharedRetailState) -> str:
    """Determine next agent(s) based on routing decision"""

def check_completion(self, state: SharedRetailState) -> bool:
    """Check if all necessary agents have completed"""

def summarize_execution(self, state: SharedRetailState) -> SharedRetailState:
    """Create final execution summary"""
```

**Routing Strategies**:

- `demand_first`: Start with demand analysis
- `urgent_expiry`: Prioritize shelf life monitoring
- `emergency_waste`: Direct to waste diversion
- `full_pipeline`: Complete sequential execution
- `parallel_analysis`: Run demand and shelf life in parallel

**LLM Integration**:

- Uses Gemini 1.5 Flash for scenario analysis
- Temperature: 0.3 for consistent routing decisions
- JSON response parsing for structured decisions

### 2.2 Demand Agent (`demand_agent.py`)

**Purpose**: Analyzes demand patterns and market conditions

**Key Functions**:

```python
def _fetch_sales_trends(self) -> pd.DataFrame:
    """Fetch recent sales trends from database"""

def _fetch_market_conditions(self) -> pd.DataFrame:
    """Fetch recent market conditions"""

def _analyze_demand_patterns(self, scenario: str, sales_data: pd.DataFrame,
                           market_conditions: pd.DataFrame, forecast_data: pd.DataFrame) -> Dict[str, Any]:
    """Use LLM to analyze demand patterns"""

def _create_fallback_analysis(self, sales_data: pd.DataFrame) -> Dict[str, Any]:
    """Create a basic analysis if LLM fails"""
```

**Data Sources**:

- Sales transaction history (last 14 days)
- Market conditions (weather, competitors, events)
- Demand forecasts (next 7 days)
- Price elasticity data

**Analysis Output**:

```python
{
    "trending_up": [
        {"product_id": 1, "name": "...", "increase_pct": 0.3, "reason": "..."}
    ],
    "trending_down": [
        {"product_id": 2, "name": "...", "decrease_pct": 0.2, "reason": "..."}
    ],
    "seasonal_impact": {"summer_products": "high", "winter_products": "low"},
    "external_factors": [
        {"factor": "heatwave", "impact": "beverages +40%", "categories_affected": ["beverages"]}
    ],
    "recommendations": ["stock_up_on_beverages", "reduce_frozen_orders"]
}
```

### 2.3 Shelf Life Agent (`shelf_life_agent.py`)

**Purpose**: Monitors product expiration and shelf life

**Key Functions**:

```python
def _fetch_expiring_products(self) -> pd.DataFrame:
    """Fetch products approaching expiration"""

def _categorize_by_urgency(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Categorize products by expiration urgency"""

def _create_shelf_life_alerts(self, scenario: str, categorized: Dict[str, pd.DataFrame],
                             demand_analysis: Dict[str, Any]) -> List[Dict]:
    """Create detailed shelf life alerts using LLM"""

def _determine_urgency(self, days_until_expiry: int) -> str:
    """Determine urgency level based on days until expiry"""
```

**Urgency Levels**:

- Critical: ≤24 hours
- High: 1-3 days
- Medium: 4-7 days
- Low: >7 days

**Alert Structure**:

```python
{
    "product_id": 1,
    "name": "Organic Milk",
    "category": "dairy",
    "batch_number": "BATCH-MILK-20241201-001",
    "expiry_date": "2024-12-02",
    "days_until_expiry": 1,
    "quantity": 50,
    "unit_price": 3.99,
    "value_at_risk": 199.50,
    "urgency": "critical",
    "markdown_recommendation": 50,
    "alternative_channel": "donation",
    "expected_recovery": 0.7
}
```

### 2.4 Inventory Agent (`inventory_agent.py`)

**Purpose**: Manages inventory levels and restock recommendations

**Key Functions**:

```python
def _fetch_inventory_status(self) -> pd.DataFrame:
    """Fetch comprehensive inventory status"""

def _analyze_inventory_health(self, inventory_df: pd.DataFrame, demand_analysis: Dict[str, Any],
                             shelf_life_alerts: List[Dict], scenario: str) -> Dict[str, Any]:
    """Analyze overall inventory health"""

def _generate_restock_recommendations(self, inventory_analysis: Dict[str, Any],
                                    demand_analysis: Dict[str, Any],
                                    shelf_life_alerts: List[Dict]) -> List[Dict]:
    """Generate specific restock recommendations"""
```

**Inventory Categories**:

- Critical stock: <2 days supply
- Overstock: >14 days supply or expiring soon
- Optimal: 2-14 days supply

**Recommendation Structure**:

```python
{
    "product_id": 1,
    "name": "Organic Apples",
    "category": "produce",
    "current_stock": 25,
    "avg_daily_sales": 15.5,
    "days_of_supply": 1.6,
    "recommended_order": 200,
    "urgency": "high",
    "reasoning": "Low stock - 1.6 days supply"
}
```

### 2.5 Pricing Agent (`pricing_discount_agent.py`)

**Purpose**: Optimizes pricing and discount strategies

**Key Functions**:

```python
def _fetch_current_pricing(self) -> pd.DataFrame:
    """Fetch current product pricing"""

def _fetch_competitor_pricing(self) -> pd.DataFrame:
    """Fetch recent competitor pricing actions"""

def _calculate_price_elasticity(self) -> pd.DataFrame:
    """Calculate price elasticity for products"""

def _optimize_pricing(self, scenario: str, shelf_life_alerts: List[Dict],
                     demand_analysis: Dict[str, Any], inventory_levels: Dict[str, Any],
                     current_pricing: pd.DataFrame, competitor_data: pd.DataFrame,
                     price_elasticity: pd.DataFrame) -> List[Dict]:
    """Generate optimized pricing recommendations"""
```

**Pricing Strategies**:

- Expiry-based: Aggressive discounts for soon-to-expire items
- Demand-based: Premium pricing for high-demand items
- Competition-based: Match or beat competitor prices
- Bundle pricing: Combine slow-moving with popular items

**Pricing Change Structure**:

```python
{
    "product_id": 1,
    "name": "Organic Milk",
    "category": "dairy",
    "current_price": 3.99,
    "new_price": 1.99,
    "discount_percentage": 50,
    "reason": "Expires in 1 days",
    "effective_immediately": True,
    "expected_units_moved": 35,
    "expected_revenue_delta": -70.00
}
```

### 2.6 Waste Diversion Agent (`wate_diversion_agent.py`)

**Purpose**: Manages waste diversion and recovery strategies

**Key Functions**:

```python
def _identify_diversion_candidates(self, shelf_life_alerts: List[Dict],
                                 pricing_changes: List[Dict]) -> List[Dict]:
    """Identify products that need diversion"""

def _create_diversion_plan(self, candidates: List[Dict], scenario: str) -> List[Dict]:
    """Create detailed diversion plan for each product"""

def _select_donation_partner(self, product: Dict) -> Dict:
    """Select appropriate donation partner based on product and requirements"""

def _calculate_environmental_impact(self, diversion_actions: List[Dict]) -> Dict[str, float]:
    """Calculate environmental impact of diversion actions"""
```

**Diversion Partners**:

```python
self.diversion_partners = {
    "food_banks": {
        "Local Food Bank Network": {
            "accepts": ["produce", "dairy", "bakery", "frozen"],
            "min_days_shelf_life": 1,
            "pickup_times": ["morning", "afternoon"],
            "capacity": 500  # kg per day
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
    }
}
```

**Diversion Action Structure**:

```python
{
    "product_id": 1,
    "name": "Organic Milk",
    "quantity": 50,
    "diversion_type": "donation",
    "partner": "Local Food Bank Network",
    "pickup_time": "today_morning",
    "retail_value": 199.50,
    "tax_benefit": 59.85,
    "compliance_checked": True,
    "handling_instructions": "Maintain cold chain. Temperature must stay below 40°F.",
    "batch_number": "BATCH-MILK-20241201-001"
}
```

### 2.7 Sustainability Agent (`sustainability_monitoring_agent.py`)

**Purpose**: Tracks environmental metrics and compliance

**Key Functions**:

```python
def _collect_current_metrics(self, state: SharedRetailState) -> Dict[str, Any]:
    """Collect metrics from current workflow execution"""

def _analyze_environmental_impact(self, current_metrics: Dict[str, Any],
                                historical_metrics: pd.DataFrame,
                                state: SharedRetailState) -> Dict[str, Any]:
    """Analyze environmental impact comprehensively"""

def _check_compliance_status(self, metrics: Dict[str, Any]) -> Dict[str, bool]:
    """Check compliance with various sustainability frameworks"""

def _generate_sustainability_recommendations(self, impact_analysis: Dict[str, Any],
                                           compliance_status: Dict[str, bool],
                                           scenario: str) -> List[Dict]:
    """Generate actionable sustainability recommendations"""
```

**Sustainability Targets**:

```python
self.sustainability_targets = {
    "waste_reduction": 0.5,      # 50% reduction target
    "carbon_footprint": 0.3,     # 30% reduction
    "plastic_reduction": 0.4,    # 40% reduction
    "energy_efficiency": 0.2,    # 20% improvement
    "donation_rate": 0.8         # 80% of expiring food donated
}
```

**Environmental Metrics**:

```python
{
    "waste_reduction_rate": 0.75,
    "donation_rate": 0.85,
    "carbon_saved_kg": 125.5,
    "water_saved_liters": 125500.0,
    "plastic_reduced_kg": 2.5,
    "energy_usage_kwh": 100.0,
    "donation_meals": 42,
    "economic_value_recovered": 59.85
}
```

## 3. Data Layer

### 3.1 Database Schema (Neon PostgreSQL)

**Core Tables**:

#### Products Table

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    sku VARCHAR(50) UNIQUE,
    unit_price DECIMAL(10,2),
    supplier_id INTEGER,
    perishable BOOLEAN,
    shelf_life_days INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
```

#### Inventory Table

```sql
CREATE TABLE inventory (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    batch_number VARCHAR(50),
    expiry_date DATE,
    location VARCHAR(50),
    last_updated TIMESTAMP DEFAULT NOW()
);
```

#### Sales Transactions Table

```sql
CREATE TABLE sales_transactions (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER,
    sale_price DECIMAL(10,2),
    transaction_time TIMESTAMP DEFAULT NOW(),
    customer_segment VARCHAR(50)
);
```

#### Market Conditions Table

```sql
CREATE TABLE market_conditions (
    id SERIAL PRIMARY KEY,
    condition_type VARCHAR(50),
    condition_data JSONB,
    impact_score DECIMAL(3,2),
    recorded_at TIMESTAMP DEFAULT NOW()
);
```

### 3.2 Data Generator (`syntheticdata.py`)

**Purpose**: Creates realistic test data for development and demo

**Key Functions**:

```python
def generate_products(self) -> List[Dict]:
    """Generate complete product catalog"""

def generate_inventory_data(self, products: List[Dict], days_back: int = 7) -> List[Dict]:
    """Generate realistic inventory data with multiple batches"""

def generate_sales_history(self, products: List[Dict], days_back: int = 7) -> List[Dict]:
    """Generate realistic sales transaction history"""

def generate_market_conditions(self, days_back: int = 7) -> List[Dict]:
    """Generate various market condition events"""

def seed_database(self, clear_existing: bool = True):
    """Seed all synthetic data into Neon database"""
```

**Product Categories**:

- Produce: Fruits, vegetables
- Dairy: Milk, cheese, yogurt
- Bakery: Bread, pastries
- Beverages: Drinks, juices
- Frozen: Frozen foods
- Clothing: Apparel
- Electronics: Tech products
- Stationery: Office supplies
- Personal Care: Hygiene products
- Household: Cleaning supplies

## 4. Integration Layer

### 4.1 Example Usage (`example_usage.py`)

**Purpose**: Demonstrates system capabilities and provides usage patterns

**Demo Functions**:

```python
def demo_command_mode():
    """Demo: Command mode with various scenarios"""

def demo_manual_mode():
    """Demo: Manual step-by-step execution"""

def demo_streaming_mode():
    """Demo: Simulated streaming mode"""

def demo_sustainability_report():
    """Demo: Generate sustainability report"""
```

**Demo Scenarios**:

1. Heatwave affecting beverage demand
2. Competitor price war on dairy and produce
3. Multiple dairy products expiring tomorrow
4. Music festival expecting 10k attendees

### 4.2 Requirements (`requirements.txt`)

**Core Dependencies**:

```
langgraph>=0.0.26          # Workflow orchestration
langchain>=0.1.0           # LLM integration framework
langchain-google-genai>=0.0.5  # Gemini AI integration
sqlalchemy>=2.0.0          # Database ORM
pandas>=2.0.0              # Data manipulation
numpy>=1.24.0              # Numerical computing
python-dotenv>=1.0.0       # Environment variable management
psycopg2-binary>=2.9.0     # PostgreSQL adapter
```

**Optional Dependencies**:

```
fastapi>=0.104.0           # API framework (for future web interface)
uvicorn>=0.24.0            # ASGI server
websockets>=11.0           # WebSocket support
redis>=5.0.0               # Caching and session storage
```

## 5. Performance Optimization

### 5.1 Database Optimization

**Indexes**:

```sql
CREATE INDEX idx_inventory_expiry ON inventory(expiry_date);
CREATE INDEX idx_inventory_product ON inventory(product_id);
CREATE INDEX idx_sales_time ON sales_transactions(transaction_time);
CREATE INDEX idx_sales_product ON sales_transactions(product_id);
CREATE INDEX idx_conditions_type ON market_conditions(condition_type);
```

**Query Optimization**:

- Use CTEs (Common Table Expressions) for complex queries
- Implement proper JOIN strategies
- Use appropriate data types
- Optimize WHERE clauses

### 5.2 Application Optimization

**Caching Strategy**:

- Redis for frequently accessed data
- In-memory caching for agent results
- Database connection pooling
- Query result caching

**Memory Management**:

- Proper resource cleanup
- Garbage collection optimization
- Memory-efficient data structures
- Batch processing for large datasets

## 6. Error Handling and Resilience

### 6.1 Error Handling Strategy

**Agent-Level Errors**:

```python
try:
    response = self.llm.invoke(messages)
    # Process response
except Exception as e:
    add_agent_message(state, "agent_name", f"Error: {str(e)}")
    # Fallback to statistical analysis
    return self._create_fallback_analysis(data)
```

**Workflow-Level Errors**:

```python
def set_error(state: SharedRetailState, agent: str, error: str):
    """Record an error in the state"""
    state["errors"].append(f"[{agent}] {error}")
    state["execution_status"] = "failed"
```

### 6.2 Resilience Patterns

**Circuit Breaker Pattern**:

- Monitor LLM API calls
- Implement retry logic with exponential backoff
- Fallback to statistical analysis when LLM fails

**Graceful Degradation**:

- Continue processing with available data
- Use historical patterns when real-time data unavailable
- Provide partial results when complete analysis impossible

## 7. Security Considerations

### 7.1 Data Security

**Encryption**:

- API keys stored securely
- Database connections encrypted
- Data in transit encrypted (HTTPS/TLS)

**Access Control**:

- Role-based access control
- API key management
- Audit logging for all operations

### 7.2 Compliance

**Data Privacy**:

- GDPR compliance for personal data
- Data retention policies
- Right to be forgotten implementation

**Regulatory Compliance**:

- ESG reporting requirements
- Food safety regulations
- Environmental regulations

## 8. Monitoring and Observability

### 8.1 Metrics Collection

**System Metrics**:

- Response times
- Throughput
- Error rates
- Resource utilization

**Business Metrics**:

- Waste reduction rates
- Revenue impact
- Sustainability targets
- Customer satisfaction

### 8.2 Logging Strategy

**Structured Logging**:

```python
{
    "timestamp": "2024-12-01T10:30:00Z",
    "level": "INFO",
    "agent": "demand_agent",
    "action": "analyze_demand_patterns",
    "duration_ms": 1250,
    "success": true,
    "data_points_processed": 1500
}
```

**Audit Trail**:

- All agent decisions logged
- State changes tracked
- User actions recorded
- Compliance events documented
