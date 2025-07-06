import asyncio
from schemas import WasteAssetEvent

async def verify_asset_data(asset: WasteAssetEvent) -> dict:
    """
    Simulates data verification for a waste asset.
    
    Checks for a 'fraud' flag in the asset ID and returns a verification result.
    """
    if 'fraud' in asset.asset_id.lower():
        return {
            'is_verified': False,
            'verification_notes': 'AI Anomaly Detected: Data inconsistent with independent sources.'
        }
    else:
        # Simulate time taken for processing
        await asyncio.sleep(2)
        return {
            'is_verified': True,
            'verification_notes': 'Verified against logistics & partner data.'
        } 