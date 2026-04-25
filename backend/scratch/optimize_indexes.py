import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

async def manage_indexes():
    client = AsyncIOMotorClient("mongodb://localhost:27017/")
    db = client["masterlist_db"]
    snapshot_col = db["snapshot"]

    print("Current Indexes:")
    idxs = await snapshot_col.index_information()
    for name, info in idxs.items():
        print(f"- {name}: {info['key']}")

    # Create critical performance indexes
    print("\nEnsuring performance indexes exist...")
    await snapshot_col.create_index("execution_id", unique=True)
    await snapshot_col.create_index("data.standardization_status")
    await snapshot_col.create_index([("benchmark_type", 1), ("benchmark_category", 1)])
    await snapshot_col.create_index("data.history.updatedOn")
    
    print("Optimization Complete.")
    client.close()

if __name__ == "__main__":
    asyncio.run(manage_indexes())
