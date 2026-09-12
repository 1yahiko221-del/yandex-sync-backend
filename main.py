import os
from typing import Dict, Set, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from yandex_music import Client

app = FastAPI(redirect_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TOKEN = os.getenv("YANDEX_TOKEN")
client = Client(TOKEN).init()


class Room:
    def __init__(self):
        self.connections: Set[WebSocket] = set()
        self.queue: List[dict] = []
        self.current_index: int = -1


class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def get_room(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room()
        return self.rooms[room_id]

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        room = self.get_room(room_id)
        room.connections.add(websocket)

    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.rooms:
            room = self.rooms[room_id]
            room.connections.discard(websocket)
            if not room.connections:
                del self.rooms[room_id]

    async def broadcast(self, message: dict, room_id: str, sender: WebSocket = None):
        if room_id not in self.rooms:
            return
        for connection in list(self.rooms[room_id].connections):
            if connection != sender:
                try:
                    await connection.send_json(message)
                except Exception:
                    self.rooms[room_id].connections.discard(connection)


manager = ConnectionManager()


class SearchRequest(BaseModel):
    query: str


class TrackRequest(BaseModel):
    track_id: str

    @field_validator("track_id", mode="before")
    @classmethod
    def coerce_to_string(cls, v):
        return str(v)


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/api/search")
async def search_tracks(req: SearchRequest):
    try:
        result = client.search(req.query)
        tracks = []
        if not result.tracks or not result.tracks.results:
            return {"tracks": []}
        for track in result.tracks.results[:10]:
            tracks.append({
                "id": track.id,
                "title": track.title,
                "artist": track.artists[0].name if track.artists else "Unknown",
                "album": track.albums[0].title if track.albums else "",
                "duration": (track.duration_ms or 0) // 1000,
            })
        return {"tracks": tracks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/get_link")
async def get_track_link(req: TrackRequest):
    try:
        track = client.tracks([req.track_id])[0]
        download_info = track.get_download_info()
        best = download_info[-1]
        link = best.get_direct_link()
        return {"url": link}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    room = manager.get_room(room_id)

    # При подключении отправляем текущее состояние очереди
    try:
        await websocket.send_json({
            "type": "queue_update",
            "queue": room.queue,
            "current_index": room.current_index,
        })
    except Exception:
        pass

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "add_to_queue":
                track_id = data.get("track_id")
                title = data.get("title", "")
                artist = data.get("artist", "")
                if track_id:
                    try:
                        # Получаем ссылку на сервере — фронтенд не ждёт HTTP
                        track_obj = client.tracks([str(track_id)])[0]
                        download_info = track_obj.get_download_info()
                        best = download_info[-1]
                        url = best.get_direct_link()

                        queue_track = {
                            "id": track_id,
                            "title": title,
                            "artist": artist,
                            "url": url,
                        }
                        room.queue.append(queue_track)

                        if room.current_index == -1:
                            room.current_index = 0
                            await manager.broadcast({
                                "type": "play_track",
                                "track": room.queue[0],
                                "index": 0,
                            }, room_id, sender=None)

                        await manager.broadcast({
                            "type": "queue_update",
                            "queue": room.queue,
                            "current_index": room.current_index,
                        }, room_id, sender=None)
                    except Exception as e:
                        print(f"Error adding track: {e}")

            elif msg_type == "remove_from_queue":
                index = data.get("index")
                if index is not None and 0 <= index < len(room.queue):
                    room.queue.pop(index)
                    if index < room.current_index:
                        room.current_index -= 1
                    elif index == room.current_index:
                        if room.current_index >= len(room.queue):
                            room.current_index = -1
                        await manager.broadcast({
                            "type": "queue_update",
                            "queue": room.queue,
                            "current_index": room.current_index,
                        }, room_id, sender=None)
                        if room.current_index >= 0:
                            await manager.broadcast({
                                "type": "play_track",
                                "track": room.queue[room.current_index],
                                "index": room.current_index,
                            }, room_id, sender=None)
                    else:
                        await manager.broadcast({
                            "type": "queue_update",
                            "queue": room.queue,
                            "current_index": room.current_index,
                        }, room_id, sender=None)

            elif msg_type == "next_track":
                if room.queue and room.current_index < len(room.queue) - 1:
                    room.current_index += 1
                    await manager.broadcast({
                        "type": "play_track",
                        "track": room.queue[room.current_index],
                        "index": room.current_index,
                    }, room_id, sender=None)
                    await manager.broadcast({
                        "type": "queue_update",
                        "queue": room.queue,
                        "current_index": room.current_index,
                    }, room_id, sender=None)

            elif msg_type == "play_track_manual":
                index = data.get("index")
                if index is not None and 0 <= index < len(room.queue):
                    room.current_index = index
                    await manager.broadcast({
                        "type": "play_track",
                        "track": room.queue[index],
                        "index": index,
                    }, room_id, sender=None)
                    await manager.broadcast({
                        "type": "queue_update",
                        "queue": room.queue,
                        "current_index": room.current_index,
                    }, room_id, sender=None)

            elif msg_type == "track_ended":
                if room.queue and room.current_index < len(room.queue) - 1:
                    room.current_index += 1
                    await manager.broadcast({
                        "type": "play_track",
                        "track": room.queue[room.current_index],
                        "index": room.current_index,
                    }, room_id, sender=None)
                    await manager.broadcast({
                        "type": "queue_update",
                        "queue": room.queue,
                        "current_index": room.current_index,
                    }, room_id, sender=None)

            else:
                # play, pause, seek — пересылаем остальным
                await manager.broadcast(data, room_id, sender=websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception as e:
        print(f"WS error: {e}")
        manager.disconnect(websocket, room_id)
