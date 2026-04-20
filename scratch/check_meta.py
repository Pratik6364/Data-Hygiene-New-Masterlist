import asyncio
import sys
import os

# Add the project root to sys.path
sys.path.append(os.getcwd())

# Mock motor since I can't easily install it right now without internet/long install
# Actually, I'll just try to use the user's uvicorn environment if possible.
# Wait, why not just read the file system or use grep?
# I can use grep to search for metadata keys in the masterlist if it's exported, but it's in MongoDB.
# I will try to run python with -m uvicorn's environment? No.

# Let's try to just use a regular script with a try-except for motor.
try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    print("Motor not installed in this python environment.")
    sys.exit(1)

from backend.database import get_db, MASTERLIST_COL

async def check():
    db = get_db()
    doc = await db[MASTERLIST_COL].find_one({'type': 'CPUModel', 'status': 'Published'})
    if not doc:
        print("No CPUModel found.")
        return
    
    meta = doc.get('data', {}).get('metadata', {})
    print(f"Metadata keys: {list(meta.keys())}")
    
    mappings = [k for k in meta.keys() if k.startswith('mapping_')]
    values = [k for k in meta.keys() if not k.startswith('mapping_') and k not in ['mapping', 'target_collection']]
    
    print(f"Mapping fields: {mappings}")
    print(f"Value fields: {values}")

if __name__ == "__main__":
    asyncio.run(check())
