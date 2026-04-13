import asyncio
from database import get_db, close_db, SNAPSHOT_COL

async def main():
    db = get_db()
    # Fetch a few records that are NOT pending to see how they look
    cursor = db[SNAPSHOT_COL].find({"data.0.standardization_status": {"$ne": "PENDING"}}).limit(5)
    records = await cursor.to_list(None)
    
    if not records:
        print("No non-pending records found in data.0.standardization_status. Checking root...")
        cursor = db[SNAPSHOT_COL].find({"standardization_status": {"$ne": "PENDING"}}).limit(5)
        records = await cursor.to_list(None)

    for r in records:
        exec_id = r.get("execution_id")
        data = r.get("data", [{}])[0]
        status = data.get("standardization_status")
        root_status = r.get("standardization_status")
        print(f"ExecID: {exec_id} | DataStatus: {status} | RootStatus: {root_status}")
    
    close_db()

if __name__ == "__main__":
    asyncio.run(main())
