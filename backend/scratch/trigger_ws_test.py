import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from datetime import datetime, timezone

async def trigger_update():
    load_dotenv()
    client = AsyncIOMotorClient(os.environ["MONGO_URI"])
    db = client[os.environ["DB_NAME"]]
    col_name = os.environ.get("COLLECTION_EXECUTION_INFO", "Executioninfo")
    
    # Find one completed record and reset its stage to trigger the pipeline
    doc = await db[col_name].find_one({"stage": "standardization completed"})
    if doc:
        print(f"Triggering update for Record ID: {doc['_id']}")
        # We unset the stage to make it look like a 'new' record for the change stream/polling
        await db[col_name].update_one(
            {"_id": doc["_id"]},
            {
                "$unset": {"stage": "", "isValid": "", "invalidFields": "", "invalidPayload": "", "fieldStatus": ""},
                "$set": {"lastModifiedOn": datetime.now(timezone.utc).isoformat()}
            }
        )
        print("Update sent to DB. Check the WebSocket client for PIPELINE_UPDATE and SUMMARY_UPDATE.")
    else:
        print("No records found to update.")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(trigger_update())
