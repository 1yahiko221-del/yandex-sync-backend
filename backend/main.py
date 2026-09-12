import os
import asyncio
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from yandex_music import Client

app = FastAPI()

# CORS — разрешаем фронтенду с GitHub Pages стучаться сюда
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # для проекта на двоих сойдёт
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Токен из переменной окружения (на Render зададим вручную)
TOKEN = os.getenv("YANDEX_TOKEN")
client = Client(TOKEN).init()

# Хранилище активных WebSocket-соединений
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# --- Модели запросов ---
class SearchRequest(BaseModel):
    query: str

class TrackRequest(BaseModel):
    track_id: str

# --- HTTP эндпоинты ---
@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/api/search")
async def search_tracks(req: SearchRequest):
    """Поиск треков через Яндекс Музыку"""
    try:
        result = client.search(req.query)
        tracks = []
        for track in result.tracks.results[:10]:
            tracks.append({
                "id": track.id,
                "title": track.title,
                "artist": track.artists[0].name if track.artists else "Unknown",
                "album": track.albums[0].title if track.albums else "",
                "duration": track.duration_ms // 1000,
            })
        return {"tracks": tracks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/get_link")
async def get_track_link(req: TrackRequest):
    """Получить прямую ссылку на полный трек"""
    try:
        track = client.tracks([req.track_id])[0]
        download_info = track.get_download_info()
        best = download_info[-1]  # максимальное качество
        link = best.get_direct_link()
        return {"url": link}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- WebSocket для синхронизации ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            # Просто пересылаем всем, включая отправителя (или исключая — по желанию)
            await manager.broadcast(data)
    except WebSocketDisconnect:
        manager.disconnect(websocket)