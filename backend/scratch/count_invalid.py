import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def count_invalid_snapshots():
    load_dotenv()
    MONGO_URI = os.environ.get("MONGO_URI")
    DB_NAME = os.environ.get("DB_NAME")
    
    # Collection names
    EXECUTION_INFO_COL = os.environ.get("COLLECTION_EXECUTION_INFO", "Executioninfo")
    SNAPSHOT_COL = os.environ.get("COLLECTION_SNAPSHOT", "snapshot")
    
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Check if 'isValid' exists on snapshot collection
    sample = await db[SNAPSHOT_COL].find_one({})
    if sample and "isValid" in sample:
        count = await db[SNAPSHOT_COL].count_documents({"isValid": False})
        print(f"Count of snapshots with isValid: False (top-level): {count}")
    else:
        # 2. Check if it's in the data array
        count = await db[SNAPSHOT_COL].count_documents({"data.isValid": False})
        if count > 0:
            print(f"Count of snapshots with data.isValid: False: {count}")
        else:
            # 3. Check for snapshots that have non-empty invalidFields
            count = await db[SNAPSHOT_COL].count_documents({"data.0.invalidFields": {"$not": {"$size": 0}}})
            print(f"Count of snapshots with non-empty invalidFields: {count}")
            
    # 4. Also check ExecutionInfo for comparison
    exec_invalid = await db[EXECUTION_INFO_COL].count_documents({"isValid": False})
    print(f"Count of Executioninfo records with isValid: False: {exec_invalid}")

    client.close()

if __name__ == "__main__":
    asyncio.run(count_invalid_snapshots())
