import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_arbitrage_analysis(need_id: int) -> dict:
    """
    Runs a financial arbitrage analysis to determine the most cost-effective
    sourcing path for a given PCR need.
    """
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        logger.error("Database URL not configured.")
        return {"error": "Database not configured."}

    engine = create_engine(db_url, connect_args={"sslmode": "require"})

    try:
        with engine.connect() as conn:
            # a. Fetch the specific supplier need
            need_query = text("SELECT material_type, required_kg, open_market_price_per_kg FROM pcr_needs WHERE id = :need_id")
            need_result = conn.execute(need_query, {"need_id": need_id}).fetchone()

            if not need_result:
                return {"error": f"No PCR need found with ID {need_id}"}

            # b. Extract details from the need
            material_type, quantity_kg_needed, open_market_price_per_kg = need_result
            open_market_price_per_kg = float(open_market_price_per_kg) if open_market_price_per_kg else float('inf')


            # c. Get total available internal supply
            supply_query = text("""
                SELECT SUM(weight_kg) 
                FROM materials_ledger 
                WHERE material_type = :material_type AND is_verified = true AND status = 'available'
            """)
            supply_result = conn.execute(supply_query, {"material_type": material_type}).scalar()
            total_available_internal_supply_kg = float(supply_result or 0)

            # d. Simulate cost analysis
            internal_costs = {'collection': 10.0, 'logistics': 25.0, 'recycling': 105.0}
            synergia_path_cost_per_kg = sum(internal_costs.values())

            # e. Make the decision
            if total_available_internal_supply_kg < quantity_kg_needed:
                decision = {
                    'decision': 'USE_OPEN_MARKET',
                    'reason': 'Insufficient internal supply',
                    'needed_kg': float(quantity_kg_needed),
                    'available_kg': total_available_internal_supply_kg
                }
            elif synergia_path_cost_per_kg < open_market_price_per_kg:
                savings = open_market_price_per_kg - synergia_path_cost_per_kg
                decision = {
                    'decision': 'EXECUTE_SYNERGIA_PATH',
                    'synergia_cost_per_kg': synergia_path_cost_per_kg,
                    'open_market_cost_per_kg': open_market_price_per_kg,
                    'savings_per_kg': savings,
                    'potential_total_savings': savings * float(quantity_kg_needed)
                }
            else:
                decision = {
                    'decision': 'USE_OPEN_MARKET',
                    'reason': 'Open market is more cost-effective',
                    'synergia_cost_per_kg': synergia_path_cost_per_kg,
                    'open_market_cost_per_kg': open_market_price_per_kg
                }
            
            return decision

    except Exception as e:
        logger.error(f"Error during arbitrage analysis for need ID {need_id}: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        # The connection is automatically closed by the 'with' statement
        engine.dispose() 