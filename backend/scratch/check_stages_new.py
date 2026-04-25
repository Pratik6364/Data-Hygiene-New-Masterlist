import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check_stages():
    load_dotenv()
    client = AsyncIOMotorClient(os.environ["MONGO_URI"])
    db = client[os.environ["DB_NAME"]]
    col_name = os.environ.get("COLLECTION_EXECUTION_INFO", "Executioninfo")
    
    pipeline = [
        {"$group": {"_id": "$stage", "count": {"$sum": 1}}}
    ]
    results = await db[col_name].aggregate(pipeline).to_list(None)
    print("Stage Counts:")
    for res in results:
        print(f"  {res['_id']}: {res['count']}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(check_stages())
