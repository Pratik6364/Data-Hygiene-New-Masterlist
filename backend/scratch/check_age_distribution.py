import asyncio
import os
from datetime import datetime, timedelta, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def check_age_buckets():
    client = AsyncIOMotorClient("mongodb://localhost:27017/")
    db = client["masterlist_db"]
    snapshot_col = db["snapshot"]

    now = datetime.now(timezone.utc)
    green_thresh = now - timedelta(days=3)
    yellow_thresh = now - timedelta(days=6)

    # Simplified strings for query comparison (ISO format)
    green_s = green_thresh.strftime("%Y-%m-%dT%H:%M:%S")
    yellow_s = yellow_thresh.strftime("%Y-%m-%dT%H:%M:%S")

    print(f"Current Time: {now.isoformat()}")
    print(f"Green Threshold (<): {green_s}")
    print(f"Yellow Threshold (<): {yellow_s}")

    # Counts
    green_count = await snapshot_col.count_documents({"data.history.updatedOn": {"$gt": green_s}})
    yellow_count = await snapshot_col.count_documents({"data.history.updatedOn": {"$lte": green_s, "$gt": yellow_s}})
    red_count = await snapshot_col.count_documents({"data.history.updatedOn": {"$lte": yellow_s}})

    print(f"\nResults:")
    print(f"Green (< 3 days): {green_count}")
    print(f"Yellow (3-6 days): {yellow_count}")
    print(f"Red (> 6 days): {red_count}")

    if yellow_count > 0:
        print("\nSample Yellow Record:")
        sample = await snapshot_col.find_one({"data.history.updatedOn": {"$lte": green_s, "$gt": yellow_s}})
        print(f"Execution ID: {sample.get('execution_id')}")
        print(f"Updated On: {sample.get('data', [{}])[0].get('history', {}).get('updatedOn')}")

    client.close()

if __name__ == "__main__":
    asyncio.run(check_age_buckets())
