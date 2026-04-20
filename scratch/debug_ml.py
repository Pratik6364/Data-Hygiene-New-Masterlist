import asyncio
import sys
import os

# Add the project root to sys.path
sys.path.append(os.getcwd())

from backend.database import get_db, MASTERLIST_COL

async def check():
    db = get_db()
    # Check all published types first
    types = await db[MASTERLIST_COL].distinct("type", {"status": "Published"})
    print(f"Published types: {types}")
    
    # Check a CPUModel record
    doc = await db[MASTERLIST_COL].find_one({'type': 'CPUModel', 'status': 'Published'})
    if doc:
        print(f"Sample CPUModel record: {doc}")
    else:
        print("No published CPUModel record found.")

    # Check mappings discovery logic result
    from backend.validation import build_mappings
    mappings = await build_mappings()
    print(f"Discovered mappings: {mappings}")

if __name__ == "__main__":
    asyncio.run(check())
