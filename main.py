import os
import json
import time
import random
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
client = None
if TOKEN:
    try:
        client = Client(TOKEN).init()
    except Exception as exc:
        print(f"[yandex] init error: {exc}")
else:
    print("[yandex] YANDEX_TOKEN is not configured")

STATE_FILE = os.getenv("STATE_FILE", "rooms_state.json")


class Room:
    def __init__(self):
        self.connections: Set[WebSocket] = set()
        self.queue: List[dict] = []
        self.current_index: int = -1
        self.current_time: float = 0.0
        self.is_playing: bool = False
        self.last_update: float = time.time()
        self.repeat_mode: str = "off"  # off | all | one
        self.shuffle: bool = False

    def get_position(self) -> float:
        if self.is_playing:
            return max(0.0, self.current_time + (time.time() - self.last_update))
        return max(0.0, self.current_time)

    def to_dict(self) -> dict:
        return {
            "queue": self.queue,
            "current_index": self.current_index,
            "current_time": self.current_time,
            "is_playing": self.is_playing,
            "last_update": self.last_update,
            "repeat_mode": self.repeat_mode,
            "shuffle": self.shuffle,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Room":
        room = cls()
        room.queue = data.get("queue", [])
        room.current_index = data.get("current_index", -1)
        room.current_time = float(data.get("current_time", 0.0) or 0.0)
        room.is_playing = bool(data.get("is_playing", False))
        room.last_update = float(data.get("last_update", time.time()))
        room.repeat_mode = data.get("repeat_mode", "off")
        if room.repeat_mode not in ("off", "all", "one"):
            room.repeat_mode = "off"
        room.shuffle = bool(data.get("shuffle", False))
        return room


class ConnectionManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                for room_id, data in raw.items():
                    self.rooms[room_id] = Room.from_dict(data)
                print(f"[state] loaded {len(self.rooms)} room(s)")
        except Exception as e:
            print(f"[state] load error: {e}")

    def save_state(self):
        try:
            snapshot = {rid: r.to_dict() for rid, r in self.rooms.items()}
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            print(f"[state] save error: {e}")

    def get_room(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room()
        return self.rooms[room_id]

    async def connect(self, websocket: WebSocket, room_id: str):
        await websocket.accept()
        self.get_room(room_id).connections.add(websocket)

    def disconnect(self, websocket: WebSocket, room_id: str):
        room = self.rooms.get(room_id)
        if room:
            room.connections.discard(websocket)

    async def broadcast(self, message: dict, room_id: str, sender: WebSocket = None):
        room = self.rooms.get(room_id)
        if not room:
            return
        for connection in list(room.connections):
            if connection != sender:
                try:
                    await connection.send_json(message)
                except Exception:
                    room.connections.discard(connection)


manager = ConnectionManager()


class SearchRequest(BaseModel):
    query: str

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str):
        value = value.strip()
        if not value:
            raise ValueError("Пустой запрос")
        if len(value) > 200:
            raise ValueError("Слишком длинный запрос")
        return value


class TrackRequest(BaseModel):
    track_id: str

    @field_validator("track_id", mode="before")
    @classmethod
    def coerce_to_string(cls, v):
        return str(v)


def require_client():
    if client is None:
        raise HTTPException(status_code=503, detail="YANDEX_TOKEN не настроен на сервере")
    return client


def get_cover_url(track_obj):
    try:
        if track_obj.albums and track_obj.albums[0].cover_uri:
            return "https://" + track_obj.albums[0].cover_uri.replace("%%", "400x400")
    except Exception:
        pass
    return ""


@app.get("/")
def root():
    return {"status": "ok", "service": "sync-music", "version": "2.0"}


@app.get("/health")
def health():
    return {"status": "ok", "yandex_configured": client is not None, "rooms": len(manager.rooms)}


@app.post("/api/search")
async def search_tracks(req: SearchRequest):
    ym = require_client()
    try:
        result = ym.search(req.query)
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
                "cover": get_cover_url(track),
            })
        return {"tracks": tracks}
    except HTTPException:
        raise
    except Exception as e:
        print(f"[search] error: {e}")
        raise HTTPException(status_code=502, detail="Ошибка поиска музыки")


@app.post("/api/get_link")
async def get_track_link(req: TrackRequest):
    ym = require_client()
    try:
        track = ym.tracks([req.track_id])[0]
        download_info = track.get_download_info()
        if not download_info:
            raise RuntimeError("Нет вариантов загрузки")
        best = download_info[-1]
        return {"url": best.get_direct_link()}
    except Exception as e:
        print(f"[link] error: {e}")
        raise HTTPException(status_code=502, detail="Не удалось получить ссылку трека")


async def send_full_state(websocket: WebSocket, room: Room):
    track = room.queue[room.current_index] if 0 <= room.current_index < len(room.queue) else None
    await websocket.send_json({
        "type": "full_state",
        "queue": room.queue,
        "current_index": room.current_index,
        "current_time": room.get_position(),
        "is_playing": room.is_playing,
        "track": track,
        "repeat_mode": room.repeat_mode,
        "shuffle": room.shuffle,
    })


async def broadcast_queue_update(room_id: str, room: Room):
    manager.save_state()
    await manager.broadcast({
        "type": "queue_update",
        "queue": room.queue,
        "current_index": room.current_index,
    }, room_id)


async def broadcast_room_settings(room_id: str, room: Room):
    manager.save_state()
    await manager.broadcast({
        "type": "room_settings",
        "repeat_mode": room.repeat_mode,
        "shuffle": room.shuffle,
    }, room_id)


async def broadcast_play_track(room_id: str, room: Room, index: int, time_: float = 0.0):
    if not (0 <= index < len(room.queue)):
        return
    room.current_index = index
    room.current_time = max(0.0, float(time_))
    room.is_playing = True
    room.last_update = time.time()
    manager.save_state()
    await manager.broadcast({
        "type": "play_track",
        "track": room.queue[index],
        "index": index,
        "time": room.current_time,
    }, room_id)
    await broadcast_queue_update(room_id, room)


def choose_next_index(room: Room):
    if not room.queue:
        return None
    if room.repeat_mode == "one" and room.current_index >= 0:
        return room.current_index
    if room.shuffle and len(room.queue) > 1:
        choices = [i for i in range(len(room.queue)) if i != room.current_index]
        return random.choice(choices)
    nxt = room.current_index + 1
    if nxt < len(room.queue):
        return nxt
    if room.repeat_mode == "all":
        return 0
    return None


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    room = manager.get_room(room_id)
    try:
        await send_full_state(websocket, room)
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "add_to_queue":
                track_id = data.get("track_id")
                if not track_id:
                    continue
                try:
                    ym = require_client()
                    track_obj = ym.tracks([str(track_id)])[0]
                    info = track_obj.get_download_info()
                    if not info:
                        continue
                    url = info[-1].get_direct_link()
                    cover = data.get("cover", "") or get_cover_url(track_obj)
                    queue_track = {
                        "id": str(track_id),
                        "title": str(data.get("title", track_obj.title)),
                        "artist": str(data.get("artist", "Unknown")),
                        "url": url,
                        "cover": cover,
                        "added_by": str(data.get("added_by", "Кто-то"))[:24],
                    }
                    room.queue.append(queue_track)
                    if room.current_index == -1:
                        await broadcast_play_track(room_id, room, 0, 0.0)
                    else:
                        await broadcast_queue_update(room_id, room)
                except Exception as e:
                    print(f"[queue:add] {e}")

            elif msg_type == "remove_from_queue":
                index = data.get("index")
                if index is None or not 0 <= index < len(room.queue):
                    continue
                was_current = index == room.current_index
                room.queue.pop(index)
                if not room.queue:
                    room.current_index, room.current_time, room.is_playing = -1, 0.0, False
                    room.last_update = time.time()
                    await broadcast_queue_update(room_id, room)
                elif was_current:
                    new_index = min(index, len(room.queue) - 1)
                    await broadcast_play_track(room_id, room, new_index, 0.0)
                else:
                    if index < room.current_index:
                        room.current_index -= 1
                    await broadcast_queue_update(room_id, room)

            elif msg_type == "reorder_queue":
                a, b = data.get("from"), data.get("to")
                if a is None or b is None or not (0 <= a < len(room.queue) and 0 <= b < len(room.queue)):
                    continue
                track = room.queue.pop(a)
                room.queue.insert(b, track)
                if room.current_index == a:
                    room.current_index = b
                elif a < room.current_index <= b:
                    room.current_index -= 1
                elif b <= room.current_index < a:
                    room.current_index += 1
                await broadcast_queue_update(room_id, room)

            elif msg_type == "next_track":
                idx = choose_next_index(room)
                if idx is not None:
                    await broadcast_play_track(room_id, room, idx, 0.0)
                else:
                    room.is_playing = False
                    room.current_time = 0.0
                    room.last_update = time.time()
                    manager.save_state()

            elif msg_type == "prev_track":
                if room.queue and room.current_index > 0:
                    await broadcast_play_track(room_id, room, room.current_index - 1, 0.0)

            elif msg_type == "play_track_manual":
                index = data.get("index")
                if index is not None and 0 <= index < len(room.queue):
                    await broadcast_play_track(room_id, room, index, 0.0)

            elif msg_type == "track_ended":
                expected = data.get("expected_index")
                if expected is not None and expected != room.current_index:
                    continue
                idx = choose_next_index(room)
                if idx is not None:
                    await broadcast_play_track(room_id, room, idx, 0.0)
                else:
                    room.is_playing = False
                    room.current_time = 0.0
                    room.last_update = time.time()
                    manager.save_state()

            elif msg_type == "set_repeat":
                mode = data.get("mode", "off")
                if mode in ("off", "all", "one"):
                    room.repeat_mode = mode
                    await broadcast_room_settings(room_id, room)

            elif msg_type == "set_shuffle":
                room.shuffle = bool(data.get("enabled"))
                await broadcast_room_settings(room_id, room)

            elif msg_type == "play":
                room.is_playing = True
                room.current_time = max(0.0, float(data.get("time", 0)))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({"type": "play", "time": room.current_time}, room_id, sender=websocket)

            elif msg_type == "pause":
                room.is_playing = False
                room.current_time = max(0.0, float(data.get("time", room.get_position())))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({"type": "pause", "time": room.current_time}, room_id, sender=websocket)

            elif msg_type == "seek":
                room.current_time = max(0.0, float(data.get("time", 0)))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({"type": "seek", "time": room.current_time}, room_id, sender=websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception as e:
        print(f"[ws] error: {e}")
        manager.disconnect(websocket, room_id)
