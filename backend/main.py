print("🔥 MAIN.PY IS RUNNING")

import os
import uvicorn
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from fastapi.middleware.cors import CORSMiddleware
from routes import router as api_router
from database import get_db
from ws_manager import manager
from trigger import run_trigger

app = FastAPI(
    title="Data Hygiene Validation API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ DEBUG HTTP ROUTE
@app.get("/")
def test():
    return {"status": "ok"}

# ✅ REAL-TIME WEBSOCKET ENDPOINT
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Handles real-time dashboard updates. 
    Registers clients with the ConnectionManager to receive broadcasted progress updates.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive and wait for client heartbeats/messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WS Error: {e}")
        manager.disconnect(websocket)

# REST routes
app.include_router(api_router)

# Startup
@app.on_event("startup")
async def startup():
    print("🚀 Data Hygiene API: Startup Sequence Initiated")
    get_db()

    # ✅ START BACKGROUND PIPELINES
    # Validation & Standardization runners are async and safe to run here.
    asyncio.create_task(run_trigger())
    print("✅ Background Pipelines: Validation & Standardization Started")

# Shutdown
@app.on_event("shutdown")
async def shutdown():
    from database import close_db
    close_db()
    print("🛑 Data Hygiene API: Shutdown Complete")

# Run
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8005, reload=True)