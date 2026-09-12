import os
import time
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
        self.current_time: float = 0.0        # позиция воспроизведения в секундах
        self.is_playing: bool = False          # играет ли сейчас
        self.last_update: float = time.time()  # когда последний раз обновляли состояние

    def get_position(self) -> float:
        """Возвращает актуальную позицию воспроизведения с учётом прошедшего времени."""
        if self.is_playing:
            return self.current_time + (time.time() - self.last_update)
        return self.current_time


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


def get_cover_url(track_obj):
    try:
        if track_obj.albums and track_obj.albums[0].cover_uri:
            cover_uri = track_obj.albums[0].cover_uri
            return "https://" + cover_uri.replace("%%", "400x400")
    except Exception:
        pass
    return ""


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
                "cover": get_cover_url(track),
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


async def send_full_state(websocket: WebSocket, room: Room):
    """Отправляет новому клиенту полное состояние комнаты."""
    track = None
    if 0 <= room.current_index < len(room.queue):
        track = room.queue[room.current_index]
    await websocket.send_json({
        "type": "full_state",
        "queue": room.queue,
        "current_index": room.current_index,
        "current_time": room.get_position(),
        "is_playing": room.is_playing,
        "track": track,
    })


async def broadcast_queue_update(room_id: str, room: Room):
    """Хелпер: рассылает актуальный queue_update всем в комнате."""
    await manager.broadcast({
        "type": "queue_update",
        "queue": room.queue,
        "current_index": room.current_index,
    }, room_id, sender=None)


async def broadcast_play_track(room_id: str, room: Room, index: int, time_: float = 0.0):
    """Хелпер: рассылает play_track и queue_update, выставляет состояние комнаты."""
    room.current_index = index
    room.current_time = time_
    room.is_playing = True
    room.last_update = time.time()
    await manager.broadcast({
        "type": "play_track",
        "track": room.queue[index],
        "index": index,
        "time": time_,
    }, room_id, sender=None)
    await broadcast_queue_update(room_id, room)


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    room = manager.get_room(room_id)

    # При подключении отправляем полное состояние
    try:
        await send_full_state(websocket, room)
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
                cover = data.get("cover", "")
                if track_id:
                    try:
                        track_obj = client.tracks([str(track_id)])[0]
                        download_info = track_obj.get_download_info()
                        best = download_info[-1]
                        url = best.get_direct_link()

                        if not cover:
                            cover = get_cover_url(track_obj)

                        queue_track = {
                            "id": track_id,
                            "title": title,
                            "artist": artist,
                            "url": url,
                            "cover": cover,
                        }
                        room.queue.append(queue_track)

                        if room.current_index == -1:
                            await broadcast_play_track(room_id, room, 0, 0.0)
                        else:
                            await broadcast_queue_update(room_id, room)
                    except Exception as e:
                        print(f"Error adding track: {e}")

            elif msg_type == "remove_from_queue":
                index = data.get("index")
                if index is not None and 0 <= index < len(room.queue):
                    was_current = (index == room.current_index)
                    room.queue.pop(index)

                    if was_current:
                        # Удалили играющий трек.
                        if not room.queue:
                            room.current_index = -1
                            room.current_time = 0.0
                            room.is_playing = False
                            room.last_update = time.time()
                            await broadcast_queue_update(room_id, room)
                        else:
                            # Играем трек, вставший на место удалённого
                            # (если удалили последний — играем предыдущий).
                            new_index = min(index, len(room.queue) - 1)
                            await broadcast_play_track(room_id, room, new_index, 0.0)
                    else:
                        # Удалили не текущий — просто корректируем индекс.
                        if index < room.current_index:
                            room.current_index -= 1
                        await broadcast_queue_update(room_id, room)

            elif msg_type == "reorder_queue":
                from_index = data.get("from")
                to_index = data.get("to")
                if (from_index is not None and to_index is not None
                        and 0 <= from_index < len(room.queue)
                        and 0 <= to_index < len(room.queue)):
                    track = room.queue.pop(from_index)
                    room.queue.insert(to_index, track)

                    # Корректируем current_index
                    if room.current_index == from_index:
                        room.current_index = to_index
                    elif from_index < room.current_index <= to_index:
                        room.current_index -= 1
                    elif to_index <= room.current_index < from_index:
                        room.current_index += 1

                    await broadcast_queue_update(room_id, room)

            elif msg_type == "next_track":
                if room.queue and room.current_index < len(room.queue) - 1:
                    await broadcast_play_track(room_id, room, room.current_index + 1, 0.0)

            elif msg_type == "prev_track":
                if room.queue and room.current_index > 0:
                    await broadcast_play_track(room_id, room, room.current_index - 1, 0.0)

            elif msg_type == "play_track_manual":
                index = data.get("index")
                if index is not None and 0 <= index < len(room.queue):
                    await broadcast_play_track(room_id, room, index, 0.0)

            elif msg_type == "track_ended":
                # Идемпотентность: клиент присылает expected_index — индекс,
                # который у него считался текущим. Если на сервере уже другой
                # (например, второй участник успел прислать track_ended),
                # игнорируем — иначе трек перепрыгнет дважды.
                expected_index = data.get("expected_index")
                if expected_index is not None and expected_index != room.current_index:
                    continue
                if room.queue and room.current_index < len(room.queue) - 1:
                    await broadcast_play_track(room_id, room, room.current_index + 1, 0.0)
                else:
                    room.is_playing = False
                    room.last_update = time.time()

            elif msg_type == "play":
                room.is_playing = True
                room.current_time = data.get("time", 0)
                room.last_update = time.time()
                await manager.broadcast(data, room_id, sender=websocket)

            elif msg_type == "pause":
                room.is_playing = False
                room.current_time = data.get("time", room.get_position())
                room.last_update = time.time()
                await manager.broadcast(data, room_id, sender=websocket)

            elif msg_type == "seek":
                room.current_time = data.get("time", 0)
                room.last_update = time.time()
                await manager.broadcast(data, room_id, sender=websocket)

            else:
                await manager.broadcast(data, room_id, sender=websocket)

    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
    except Exception as e:
        print(f"WS error: {e}")
        manager.disconnect(websocket, room_id)
