import os
from typing import Dict, Set
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


class ConnectionManager:
    def __init__(self):
        # Теперь храним комнаты: {room_id: set(websockets)}
        self.rooms: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        if room_id not in self.rooms:
            self.rooms[room_id] = set()
        self.rooms[room_id].add(websocket)

    def disconnect(self, websocket: WebSocket, room_id: str):
        if room_id in self.rooms:
            self.rooms[room_id].discard(websocket)
            if not self.rooms[room_id]:
                del self.rooms[room_id]

    async def broadcast(self, message: dict, room_id: str, sender: WebSocket):
        """Рассылает всем в комнате, КРОМЕ отправителя."""
        if room_id not in self.rooms:
            return
        for connection in list(self.rooms[room_id]):
            if connection != sender:
                try:
                    await connection.send_json(message)
                except Exception:
                    self.rooms[room_id].discard(connection)


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
    try:
        while True:
            data = await websocket.receive_json()
            # Рассылаем всем кроме отправителя
            await manager.broadcast(data, room_id, websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception:
        manager.disconnect(websocket, room_id)
