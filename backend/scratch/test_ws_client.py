import asyncio
import websockets
import json

async def test_websocket():
    uri = "ws://localhost:8000/ws"
    print(f"Connecting to {uri}...")
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected! Waiting for messages...")
            print("(If the background trigger is running, you should see SUMMARY_UPDATE or PIPELINE_UPDATE messages soon)\n")
            
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"Received {data.get('type')}:", flush=True)
                if data.get('type') == 'SUMMARY_UPDATE':
                    s = data.get('summary', {})
                    print(f"  Summary -> PENDING: {s.get('PENDING')}, ACCEPTED: {s.get('ACCEPTED')}, STANDARDIZATION_IN_PROGRESS: {s.get('STANDARDIZATION_IN_PROGRESS')}", flush=True)
                elif data.get('type') == 'PIPELINE_UPDATE':
                    print(f"  Record Update -> ID: {data.get('execution_id')}, Stage: {data.get('stage')}", flush=True)
                else:
                    print(f"  Raw: {data}", flush=True)
                print("-" * 20, flush=True)
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print("\nDisconnected.")
