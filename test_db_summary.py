import asyncio
import os
from datetime import date
import json

from sqlalchemy import text
from dotenv import load_dotenv

# Ensure the logger is configured before other imports
import logging
logging.basicConfig(level=logging.INFO)

from webhook_receriver import update_daily_summary
from db_utils import create_robust_engine


async def main():
    """
    Runs an isolated test of the update_daily_summary function
    to debug database insertion logic without running the full workflow.
    """
    load_dotenv()
    
    db_url = os.getenv("NEON_DATABASE_URL")
    if not db_url:
        print("FATAL: NEON_DATABASE_URL is not set. Please check your .env file.")
        return

    print("Running isolated test for update_daily_summary...")

    # A sample 'state' object mimicking the final state of a workflow run.
    # NOTE: Ensure that product_ids 1, 5, and 10 exist in your 'products' table.
    # The default syntheticdata.py script creates these.
    sample_state = {
        "environmental_metrics": {
            "waste_prevented_kg": 15.5,
            "donation_meals": 40,
            "carbon_saved_kg": 25.2,
        },
        "pricing_changes": [
            {"product_id": 1, "new_price": 2.50, "quantity": 10}, # Category: produce
            {"product_id": 5, "new_price": 4.00, "quantity": 5},  # Category: dairy
        ],
        "diversion_actions": [
            {"product_id": 10, "quantity": 20, "retail_value": 85.0}, # Category: dairy
        ],
        # Other state keys are omitted as they are not used by the function
    }

    print("\n[STEP 1] Calling update_daily_summary with sample data...")
    try:
        await update_daily_summary(sample_state)
        print("[SUCCESS] update_daily_summary executed without raising an exception.")
    except Exception as e:
        print(f"[FAILURE] update_daily_summary failed with an exception: {e}")
        # The full traceback will be printed by the logger in the function itself.
        return

    print("\n[STEP 2] Verifying data in the database...")
    try:
        engine = create_robust_engine(db_url)
        with engine.connect() as conn:
            query = text("SELECT * FROM daily_impact_summary WHERE summary_date = :today")
            result = conn.execute(query, {'today': date.today()}).fetchone()
            
            if result:
                print("[SUCCESS] Found summary row for today's date.")
                summary_data = dict(result._mapping)
                # Pretty-print the result
                for key, value in summary_data.items():
                    if isinstance(value, str):
                         # Try to parse JSON strings for better readability
                        try:
                            parsed_json = json.loads(value)
                            print(f"  - {key}: {json.dumps(parsed_json, indent=2)}")
                        except json.JSONDecodeError:
                            print(f"  - {key}: {value}")
                    else:
                        print(f"  - {key}: {value}")
            else:
                print("[FAILURE] No summary row found for today's date.")
                
    except Exception as e:
        print(f"[FAILURE] Could not verify data in database. Error: {e}")

if __name__ == "__main__":
    # To run this test, execute `python test_db_summary.py` in your terminal.
    # Make sure your virtual environment is activated.
    asyncio.run(main()) 