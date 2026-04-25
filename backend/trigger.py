import asyncio
import os
import uuid
import logging
import time
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, OperationFailure
from dotenv import load_dotenv
from database import get_db, close_db, EXECUTION_INFO_COL, SNAPSHOT_COL
from validation import get_validator
from ws_manager import manager
 
# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
 
load_dotenv()

# Fields that should NOT trigger a re-validation when updated (Internal statuses)
INTERNAL_FIELDS = {
    "stage", "isValid", 
    "fieldStatus", "invalidPayload", "lastModifiedOn",
    "benchmarkExecutionID"
}

# Global cache for the last calculated summary to avoid redundant DB aggregation
_LAST_SUMMARY = {
    "PENDING": 0, "REJECTED": 0, "ACCEPTED": 0, "ON HOLD": 0, "N/A": 0,
    "VALIDATION_IN_PROGRESS": 0, "STANDARDIZATION_IN_PROGRESS": 0, "STANDARDIZATION_COMPLETED": 0
}
_SUMMARY_LOCK = asyncio.Lock()

async def get_current_summary(db):
    """Calculates and returns the global summary counts using a single optimized pass."""
    pipeline = [
        {"$match": {"stage": {"$exists": True}}},
        {"$lookup": {
            "from": SNAPSHOT_COL,
            "localField": "benchmarkExecutionID",
            "foreignField": "execution_id",
            "as": "snapshot"
        }},
        {"$unwind": {"path": "$snapshot", "preserveNullAndEmptyArrays": True}},
        {"$facet": {
            "statuses": [
                {"$project": {
                    "status": {
                        "$cond": {
                            "if": {"$and": [{"$isArray": "$snapshot.data"}, {"$gt": [{"$size": "$snapshot.data"}, 0]}]},
                            "then": {"$arrayElemAt": ["$snapshot.data.standardization_status", 0]},
                            "else": "N/A"
                        }
                    }
                }},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}}
            ],
            "stages": [
                {"$group": {"_id": "$stage", "count": {"$sum": 1}}}
            ]
        }}
    ]
    
    agg_results = await db[EXECUTION_INFO_COL].aggregate(pipeline).to_list(1)
    facet_res = agg_results[0] if agg_results else {"statuses": [], "stages": []}

    # Parse Status Counts
    status_counts = {"PENDING": 0, "REJECTED": 0, "ACCEPTED": 0, "ON HOLD": 0, "N/A": 0}
    for s in facet_res["statuses"]:
        key = str(s["_id"] or "N/A").upper()
        if key in status_counts: status_counts[key] = s["count"]
        else: status_counts["N/A"] += s["count"]

    # Parse Stage Counts
    raw_stages = {str(s["_id"] or "unknown").lower(): s["count"] for s in facet_res["stages"]}
    grouped_stages = {
        "VALIDATION_IN_PROGRESS": (
            raw_stages.get("validation inprogress", 0) + 
            raw_stages.get("validation failed", 0)
        ),
        "STANDARDIZATION_IN_PROGRESS": (
            raw_stages.get("standardization inprogress", 0) + 
            raw_stages.get("validation completed", 0) + 
            raw_stages.get("standardization failed", 0)
        ),
        "STANDARDIZATION_COMPLETED": raw_stages.get("standardization completed", 0),
        "TOTAL_INVALID_RECORDS": (
            raw_stages.get("validation inprogress", 0) + 
            raw_stages.get("validation completed", 0) + 
            raw_stages.get("validation failed", 0) +
            raw_stages.get("standardization inprogress", 0) + 
            raw_stages.get("standardization failed", 0)
        )
    }

    return {**status_counts, **grouped_stages}

async def broadcast_summary(db):
    """Calculates and broadcasts global summary counts."""
    global _LAST_SUMMARY
    summary = await get_current_summary(db)
    
    async with _SUMMARY_LOCK:
        _LAST_SUMMARY = summary

    await manager.broadcast({
        "type": "PIPELINE_UPDATE",
        "summary": summary
    })


# ============================================================================
# PIPELINE 1: VALIDATION
# Polls for unvalidated records, validates against masterlist, updates ExecutionInfo
# ============================================================================

async def validate_document(db, validator, doc):
    """
    Pipeline 1: Validates a single document against the masterlist.
    Sets: isValid, invalidPayload, fieldStatus, validated status on ExecutionInfo.
    Status flow: (none) -> "In Progress" -> "Completed" / "Failed"
    """
    try:
        exec_id = doc.get("benchmarkExecutionID")
        if not exec_id:
            exec_id = str(uuid.uuid4())

        # Mark as In Progress and remove legacy fields
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "benchmarkExecutionID": exec_id,
                    "stage": "validation inprogress"
                },
                "$unset": {"validated": "", "standardized": "", "fieldStatus": "", "invalidPayload": "", "validation": "", "standardization": ""}
            }
        )

        # Run Validation against masterlist
        invalid_payload, field_status = await validator.validate_doc(db, doc)
        is_val = len(invalid_payload) == 0

        # Update ExecutionInfo with validation result
        # Extract all field names that are in the invalid payload
        invalid_fields = sorted(list(set([p.get("field") for p in invalid_payload])))
        
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "invalidPayload": invalid_payload,
                    "invalidFields": invalid_fields, # Lightweight list for dashboard
                    "isValid": is_val,
                    "stage": "validation completed",
                    "fieldStatus": field_status,
                    "lastModifiedOn": datetime.now(timezone.utc).isoformat()
                }
            }
        )

        status_str = "VALID" if is_val else f"INVALID ({len(invalid_payload)} error groups)"
        logger.info(f"[Validation] COMPLETED: {exec_id} | {status_str}")

        # Broadcast update to UI (Combined with latest summary)
        await manager.broadcast({
            "type": "PIPELINE_UPDATE",
            "execution_id": exec_id,
            "stage": "validation completed",
            "isValid": is_val,
            "invalidFields": invalid_fields,
            "benchmarkType": doc.get("benchmarkType"),
            "benchmarkCategory": doc.get("benchmarkCategory"),
            "updatedOn": datetime.now(timezone.utc).isoformat(),
            "suggestionsCount": False, # No suggestions until standardization
            "summary": _LAST_SUMMARY
        })

    except Exception as e:
        # Mark as Failed so it can be retried
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {"$set": {"stage": "validation failed"}}
        )
        logger.error(f"[Validation] FAILED: {doc.get('_id')} | {str(e)}")


# ============================================================================
# PIPELINE 2: STANDARDIZATION
# Polls for validated-but-not-standardized records, creates/updates snapshots
# ============================================================================

async def standardize_document(db, validator, doc):
    """
    Pipeline 2: Creates/updates the snapshot for a validated document.
    Builds fuzzy suggestions and preserves history from existing snapshots.
    Sets: standardized=True on ExecutionInfo after snapshot is written.
    """
    try:
        exec_id = doc.get("benchmarkExecutionID")
        logger.info(f"[Standardization] IN_PROGRESS: {exec_id}")
        
        # Mark as In Progress in DB
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {"$set": {"stage": "standardization inprogress"}}
        )
        
        is_val = doc.get("isValid", False)
        invalid_payload = doc.get("invalidPayload", [])

        # Fetch existing snapshot (if any) for history preservation
        latest_snap = await db[SNAPSHOT_COL].find_one(
            {"execution_id": exec_id},
            {"data": {"$slice": 1}, "snapshot_id": 1}
        )

        if is_val:
            # Record is VALID - Only update if a snapshot ALREADY exists (History Preservation)
            if latest_snap and latest_snap.get("data"):
                prev_data = latest_snap["data"][0]
                current_status = str(prev_data.get("standardization_status", "")).upper()
                if current_status not in ["ACCEPTED", "REJECTED", "ON HOLD"]:
                    await db[SNAPSHOT_COL].update_one(
                        {"execution_id": exec_id},
                        {"$set": {
                            "data.0.standardization_status": "ACCEPTED",
                            "data.0.lastModifiedOn": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        }}
                    )
            logger.info(f"[Standardization] COMPLETED: {exec_id} | Valid record preserved")
        else:
            # Record is INVALID - Create or Update Snapshot with fuzzy suggestions
            prev_data = {}
            snap_id = str(uuid.uuid4())
            if latest_snap:
                snap_id = latest_snap.get("snapshot_id", snap_id)
                if latest_snap.get("data"):
                    prev_data = latest_snap["data"][0]

            # Normalize status to uppercase, defaults to PENDING
            raw_status = prev_data.get("standardization_status", "PENDING")
            status_val = raw_status.upper() if raw_status else "PENDING"

            clean_meta = []
            for p in invalid_payload:
                field = p.get("field")
                val = p.get("value")

                p_clean = {
                    "field": field,
                    "currentStatus": "invalid",
                    "value": val,
                    "datatype": validator.field_types.get(field, "STRING").lower(),
                    "validation_status": p.get("validation_status", "invalid"),
                    "mapping": p.get("mapping", "")
                }
                actual_meta_vals = {m["name"]: m.get("value", "") for m in p.get("metadata", []) if m.get("name")}

                # Get record-level suggestions using 'Mega-String' fuzzy matching
                record_suggestions = validator.get_record_level_suggestions(field, val, actual_meta_vals)

                # Build formatted suggestions for the dashboard
                primary_comparing = []
                for i, rec_sug in enumerate(record_suggestions, 1):
                    primary_comparing.append({
                        f"suggestion{i}": rec_sug["primary_value"],
                        f"score{i}": rec_sug["score"],
                        "status": "PENDING",
                        "_id": rec_sug["_id"]
                    })
                p_clean["comparingData"] = primary_comparing

                meta_list = []
                for m in p.get("metadata", []):
                    m_clean = dict(m)
                    m_name = m_clean.get("name")
                    m_clean["datatype"] = validator.field_types.get(m_name, "STRING").lower()
                    m_comparing = []
                    for i, rec_sug in enumerate(record_suggestions, 1):
                        m_comparing.append({
                            f"suggestion{i}": rec_sug["metadata"].get(m_name, ""),
                            f"score{i}": rec_sug["score"],
                            "status": "PENDING",
                            "_id": rec_sug["_id"]
                        })
                    m_clean["comparingData"] = m_comparing
                    meta_list.append(m_clean)
                p_clean["metadata"] = meta_list
                clean_meta.append(p_clean)

            snapshot_doc = {
                "snapshot_id": snap_id,
                "execution_id": exec_id,
                "benchmark_type": doc.get("benchmarkType"),
                "benchmark_category": doc.get("benchmarkCategory"),
                "data": [{
                    "invalidFields": sorted(list(set(
                        [p.get("field") for p in clean_meta if p.get("validation_status") == "invalid"] +
                        [m.get("name") for p in clean_meta for m in p.get("metadata", []) if m.get("validation_status") == "invalid"]
                    ))),
                    "invalidValues": clean_meta,
                    "standardization_status": status_val,
                    "history": prev_data.get("history", {
                        "updatedOn": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "updatedBy": "xxx@amd.com",
                        "from": [], "to": [], "valueField": [], "source": []
                    })
                }]
            }

            await db[SNAPSHOT_COL].replace_one(
                {"execution_id": exec_id},
                snapshot_doc,
                upsert=True
            )
            logger.info(f"[Standardization] COMPLETED: {exec_id} | Snapshot created")

            # Broadcast update to UI (Combined with latest summary)
            await manager.broadcast({
                "type": "PIPELINE_UPDATE",
                "execution_id": exec_id,
                "stage": "standardization completed",
                "status": status_val,
                "invalidFields": snapshot_doc["data"][0]["invalidFields"],
                "benchmarkType": doc.get("benchmarkType"),
                "benchmarkCategory": doc.get("benchmarkCategory"),
                "updatedOn": snapshot_doc["data"][0]["history"].get("updatedOn"),
                "suggestionsCount": len(snapshot_doc["data"][0]["invalidFields"]) > 0,
                "summary": _LAST_SUMMARY
            })

        # Mark as Completed and remove legacy/internal fields
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {
                "$set": {"stage": "standardization completed"},
                "$unset": {
                    "validated": "", 
                    "standardized": "",
                    "fieldStatus": "",
                    "invalidPayload": "",
                    "validation": "",
                    "standardization": ""
                }
            }
        )

    except Exception as e:
        # Mark as Failed so it can be retried
        await db[EXECUTION_INFO_COL].update_one(
            {"_id": doc["_id"]},
            {"$set": {"stage": "standardization failed"}}
        )
        logger.error(f"[Standardization] FAILED: {doc.get('_id')} | {str(e)}")


# ============================================================================
# PARALLEL PIPELINE RUNNERS
# ============================================================================

# Global timestamp to debounce broadcasts (prevent DB saturation)
LAST_BROADCAST_TIME = 0
BROADCAST_LOCK = asyncio.Lock()

async def debounced_broadcast(db):
    """Broadcasts summary at most once every 2 seconds."""
    global LAST_BROADCAST_TIME
    current_time = time.time()
    
    if current_time - LAST_BROADCAST_TIME > 2:
        async with BROADCAST_LOCK:
            # Double check inside lock
            if time.time() - LAST_BROADCAST_TIME > 2:
                await broadcast_summary(db)
                LAST_BROADCAST_TIME = time.time()

async def run_validation_pipeline(db, validator, collection_name, interval=1, max_concurrent=50):
    """Pipeline 1: Streaming Validation with debounced updates."""
    logger.info(f"[VALIDATION PIPELINE] Started (Streaming, Concurrency: {max_concurrent})")
    collection = db[collection_name]
    semaphore = asyncio.Semaphore(max_concurrent)
    active_tasks = set()

    while True:
        try:
            query = {"$or": [{"stage": {"$exists": False}}, {"stage": "validation failed"}]}
            slots_available = max_concurrent - len(active_tasks)
            
            if slots_available > 0:
                async for doc in collection.find(query).limit(slots_available):
                    async def _run_task(d):
                        async with semaphore:
                            await validate_document(db, validator, d)
                            await debounced_broadcast(db)

                    task = asyncio.create_task(_run_task(doc))
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)

            if len(active_tasks) > 0:
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"[VALIDATION PIPELINE] Error: {e}")
            await asyncio.sleep(interval)

async def run_standardization_pipeline(db, validator, collection_name, interval=1, max_concurrent=50):
    """Pipeline 2: Streaming Standardization with debounced updates."""
    logger.info(f"[STANDARDIZATION PIPELINE] Started (Streaming, Concurrency: {max_concurrent})")
    collection = db[collection_name]
    semaphore = asyncio.Semaphore(max_concurrent)
    active_tasks = set()

    while True:
        try:
            # Poll for records that are ready for standardization OR previously failed
            query = {"stage": {"$in": ["validation completed", "standardization failed"]}}
            slots_available = max_concurrent - len(active_tasks)
            
            if slots_available > 0:
                async for doc in collection.find(query).limit(slots_available):
                    async def _run_task(d):
                        async with semaphore:
                            # Mark as In-Progress immediately so UI sees it
                            await db[collection_name].update_one(
                                {"_id": d["_id"]},
                                {"$set": {"stage": "standardization inprogress"}}
                            )
                            await standardize_document(db, validator, d)
                            await debounced_broadcast(db)

                    task = asyncio.create_task(_run_task(doc))
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)

            if len(active_tasks) > 0:
                await asyncio.sleep(0.1)
            else:
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"[STANDARDIZATION PIPELINE] Error: {e}")
            await asyncio.sleep(interval)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def run_trigger():
    """
    Main entry point. Launches both pipelines in parallel.
    Falls back to polling if Change Streams are not supported.
    """
    db = get_db()
    collection = db[EXECUTION_INFO_COL]

    # --- STARTUP CLEANUP ---
    # Any records stuck in 'inprogress' from a previous run should be reset 
    # so the pipelines can pick them up again.
    logger.info("Cleaning up stale 'inprogress' records on startup...")
    
    # Validation In-Progress -> Back to initial state (No stage)
    val_reset = await collection.update_many(
        {"stage": "validation inprogress"},
        {"$unset": {"stage": ""}}
    )
    if val_reset.modified_count > 0:
        logger.info(f"Reset {val_reset.modified_count} stale validation records.")

    # Standardization In-Progress -> Back to 'validation completed'
    std_reset = await collection.update_many(
        {"stage": "standardization inprogress"},
        {"$set": {"stage": "validation completed"}}
    )
    if std_reset.modified_count > 0:
        logger.info(f"Reset {std_reset.modified_count} stale standardization records.")

    logger.info("Initializing Validator...")
    validator = await get_validator()
    logger.info("Validator ready.")

    # Try Change Stream first (requires MongoDB Replica Set)
    try:
        logger.info(f"Attempting to start Change Stream on: {EXECUTION_INFO_COL}")

        pipeline = [
            {"$match": {
                "operationType": {"$in": ["insert", "replace", "update"]}
            }}
        ]

        # For Change Stream mode, we still run both pipelines in parallel
        # Change Stream handles real-time validation; standardization pipeline picks up after
        async def _change_stream_validation():
            async with collection.watch(pipeline, full_document="updateLookup") as stream:
                logger.info("[CHANGE STREAM] Active. Listening for new records...")
                count = 0
                async for change in stream:
                    op_type = change["operationType"]
                    doc = change.get("fullDocument")
                    if not doc: continue

                    doc_id = doc.get("_id")

                    # --- AUTO-RESET LOGIC ---
                    # If an existing record is updated with NEW DATA, we must re-trigger validation.
                    if op_type == "update":
                        updated_fields = change.get("updateDescription", {}).get("updatedFields", {})
                        # Check if any of the updated fields are "Real Data" (not internal statuses)
                        real_data_changed = any(f for f in updated_fields if f not in INTERNAL_FIELDS)
                        
                        if real_data_changed:
                            logger.info(f"[TRIGGER] Data change detected for {doc_id}. Resetting stage to re-trigger pipeline...")
                            await collection.update_one(
                                {"_id": doc_id},
                                {"$unset": {"stage": "", "isValid": "", "invalidFields": "", "invalidPayload": "", "fieldStatus": ""}}
                            )
                            # The polling loop will pick it up in the next cycle
                            continue

                    # --- START PROCESSING LOGIC ---
                    # Only start processing if the record doesn't have a stage yet
                    if "stage" not in doc:
                        count += 1
                        logger.info(f"[CHANGE STREAM] Processing Record #{count} ({doc_id})")
                        await validate_document(db, validator, doc)

        # Run Change Stream validation + Standardization pipeline in parallel
        logger.info("Starting dual pipelines: Change Stream (Validation) + Polling (Standardization)...")
        await asyncio.gather(
            _change_stream_validation(),
            run_standardization_pipeline(db, validator, EXECUTION_INFO_COL)
        )

    except (OperationFailure, PyMongoError) as e:
        err_msg = str(e)
        if isinstance(e, OperationFailure) and e.code == 40573 or "not support change streams" in err_msg.lower():
            logger.warning("Change Streams not supported (Standalone MongoDB). Switching to Polling Mode.")
            logger.info("Starting dual pipelines: Validation + Standardization (both polling)...")

            # Run BOTH pipelines in parallel
            await asyncio.gather(
                run_validation_pipeline(db, validator, EXECUTION_INFO_COL),
                run_standardization_pipeline(db, validator, EXECUTION_INFO_COL)
            )
        else:
            logger.error(f"MongoDB error: {err_msg}")
            logger.info("Restarting trigger in 5 seconds...")
            await asyncio.sleep(5)
            await run_trigger()

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        await asyncio.sleep(5)
        await run_trigger()


if __name__ == "__main__":
    try:
        asyncio.run(run_trigger())
    except KeyboardInterrupt:
        logger.info("Trigger stopped by user.")
    finally:
        close_db()