import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

async def list_dates():
    client = AsyncIOMotorClient("mongodb://localhost:27017/")
    db = client["masterlist_db"]
    snapshot_col = db["snapshot"]

    print("Fetching 5 sample dates from snapshots...")
    cursor = snapshot_col.find({}, {"data.history.updatedOn": 1}).limit(5)
    async for doc in cursor:
        try:
            print(f"Date: {doc['data'][0]['history']['updatedOn']}")
        except:
            print("Error parsing doc")

    client.close()

if __name__ == "__main__":
    asyncio.run(list_dates())
