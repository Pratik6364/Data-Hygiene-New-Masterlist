from fastapi import FastAPI, WebSocket

app = FastAPI()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("🔥 HIT BACKEND")
    await websocket.accept()
    await websocket.send_text("connected")