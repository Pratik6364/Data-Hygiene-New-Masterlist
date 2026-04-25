import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check_all():
    load_dotenv()
    client = AsyncIOMotorClient(os.environ["MONGO_URI"])
    db = client[os.environ["DB_NAME"]]
    col_name = os.environ.get("COLLECTION_EXECUTION_INFO", "Executioninfo")
    snap_name = os.environ.get("COLLECTION_SNAPSHOT", "snapshot")
    
    total_exec = await db[col_name].count_documents({})
    invalid_exec = await db[col_name].count_documents({"isValid": False})
    valid_exec = await db[col_name].count_documents({"isValid": True})
    no_val_exec = await db[col_name].count_documents({"isValid": {"$exists": False}})
    
    total_snap = await db[snap_name].count_documents({})
    # Check for snapshots that have at least one invalid field
    invalid_snap = await db[snap_name].count_documents({"data.0.invalidFields": {"$not": {"$size": 0}}})
    valid_snap = total_snap - invalid_snap
    
    print(f"Executioninfo ({col_name}):")
    print(f"  Total: {total_exec}")
    print(f"  isValid=False: {invalid_exec}")
    print(f"  isValid=True: {valid_exec}")
    print(f"  isValid missing: {no_val_exec}")
    print(f"\nSnapshot ({snap_name}):")
    print(f"  Total: {total_snap}")
    print(f"  Invalid (with errors): {invalid_snap}")
    print(f"  Valid (preserved history): {valid_snap}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(check_all())
