from fastapi import WebSocket
from typing import List, Dict, Any
import json
import logging

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Connected: {len(self.active_connections)} clients")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Disconnected: {len(self.active_connections)} clients")

    async def broadcast(self, message: Dict[str, Any]):
        if not self.active_connections:
            return

        message_str = json.dumps(message)
        disconnected_clients = []

        for connection in self.active_connections:
            try:
                await connection.send_text(message_str)
            except Exception as e:
                logger.error(f"Send failed: {e}")
                disconnected_clients.append(connection)

        for conn in disconnected_clients:
            self.disconnect(conn)

# Singleton instance
manager = ConnectionManager()