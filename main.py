import asyncio
import json
import os
import random
import time
import hashlib
from typing import Dict, Set, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from yandex_music import Client
app = FastAPI(redirect_slashes=False)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=False, allow_methods=['*'], allow_headers=['*'])
TOKEN = os.getenv('YANDEX_TOKEN')
client = None
if TOKEN:
    try:
        client = Client(TOKEN).init()
    except Exception as exc:
        print(f'[yandex] init error: {exc}')
else:
    print('[yandex] YANDEX_TOKEN is not configured')
STATE_FILE = os.getenv('STATE_FILE', 'rooms_state.json')
MAX_CHAT = 500
CHAT_COOLDOWN = 0.7
EMPTY_ROOM_TTL = int(os.getenv('EMPTY_ROOM_TTL', '1800'))
MAX_ROOM_HISTORY = 300

class Room:

    def __init__(self):
        self.connections: Set[WebSocket] = set()
        self.users: Dict[WebSocket, dict] = {}
        self.queue: List[dict] = []
        self.current_index = -1
        self.current_time = 0.0
        self.is_playing = False
        self.last_update = time.time()
        self.repeat_mode = 'off'
        self.shuffle = False
        self.dj_mode = False
        self.owner_token: Optional[str] = None
        self.proposals: Dict[str, dict] = {}
        self.chat_last: Dict[WebSocket, float] = {}
        self.shared_playlists: Dict[str, dict] = {}
        self.blocked_tokens: Set[str] = set()
        self.history: List[dict] = []
        self.empty_since: Optional[float] = None

    def get_position(self):
        if self.is_playing:
            return max(0.0, self.current_time + (time.time() - self.last_update))
        return max(0.0, self.current_time)

    def to_dict(self):
        return {'queue': self.queue, 'current_index': self.current_index, 'current_time': self.get_position(), 'is_playing': self.is_playing, 'last_update': time.time(), 'repeat_mode': self.repeat_mode, 'shuffle': self.shuffle, 'dj_mode': self.dj_mode, 'owner_token': self.owner_token, 'shared_playlists': list(self.shared_playlists.values()), 'history': self.history[-MAX_ROOM_HISTORY:], 'blocked_tokens': list(self.blocked_tokens), 'empty_since': self.empty_since}

    @classmethod
    def from_dict(cls, data):
        r = cls()
        r.queue = data.get('queue', [])
        r.current_index = int(data.get('current_index', -1))
        r.current_time = float(data.get('current_time', 0) or 0)
        r.is_playing = bool(data.get('is_playing', False))
        r.last_update = float(data.get('last_update', time.time()))
        r.repeat_mode = data.get('repeat_mode', 'off') if data.get('repeat_mode') in ('off', 'all', 'one') else 'off'
        r.shuffle = bool(data.get('shuffle', False))
        r.dj_mode = bool(data.get('dj_mode', False))
        r.owner_token = data.get('owner_token')
        r.history = list(data.get('history', []) or [])[-MAX_ROOM_HISTORY:]
        r.blocked_tokens = set(str(x) for x in (data.get('blocked_tokens', []) or []))
        r.empty_since = data.get('empty_since')
        for playlist in data.get('shared_playlists', []) or []:
            if isinstance(playlist, dict) and playlist.get('id'):
                playlist = dict(playlist)
                owner_id = str(playlist.get('owner_id') or '')
                member_ids = [str(x) for x in (playlist.get('member_ids') or [])]
                playlist['member_ids'] = list(dict.fromkeys(([owner_id] if owner_id else []) + member_ids))
                roles = dict(playlist.get('roles') or {})
                for mid in playlist['member_ids']:
                    roles[mid] = 'owner' if mid == owner_id else roles.get(mid, 'editor')
                playlist['roles'] = roles
                r.shared_playlists[str(playlist['id'])] = playlist
        return r

class ConnectionManager:

    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self._load_state()

    def _load_state(self):
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                for rid, data in raw.items():
                    self.rooms[rid] = Room.from_dict(data)
                print(f'[state] loaded {len(self.rooms)} room(s)')
        except Exception as e:
            print(f'[state] load error: {e}')

    def save_state(self):
        try:
            snap = {rid: r.to_dict() for rid, r in self.rooms.items()}
            tmp = STATE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(snap, f, ensure_ascii=False)
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            print(f'[state] save error: {e}')

    def cleanup_empty(self):
        now = time.time()
        removed = []
        for rid, room in list(self.rooms.items()):
            if room.users:
                continue
            if room.empty_since and now - float(room.empty_since) >= EMPTY_ROOM_TTL:
                removed.append(rid)
                self.rooms.pop(rid, None)
        if removed:
            self.save_state()
            print(f'[rooms] auto-deleted empty rooms: {removed}')

    def get_room(self, rid):
        self.cleanup_empty()
        if rid not in self.rooms:
            self.rooms[rid] = Room()
        return self.rooms[rid]

    async def connect(self, ws, rid):
        await ws.accept()
        self.get_room(rid).connections.add(ws)

    def disconnect(self, ws, rid):
        r = self.rooms.get(rid)
        if r:
            departing = r.users.get(ws, {})
            departing_token = departing.get('token')
            r.connections.discard(ws)
            r.users.pop(ws, None)
            r.chat_last.pop(ws, None)
            if departing_token and departing_token == r.owner_token and r.users:
                # Если владелец вышел, передаём владение оставшемуся участнику.
                r.owner_token = next(iter(r.users.values())).get('token')
            if not r.users:
                r.owner_token = departing_token or r.owner_token
                r.empty_since = time.time()
            else:
                r.empty_since = None
            self.save_state()

    async def broadcast(self, msg, rid, sender=None):
        r = self.rooms.get(rid)
        if not r:
            return
        for c in list(r.connections):
            if c is not sender:
                try:
                    await c.send_json(msg)
                except Exception:
                    r.connections.discard(c)
                    r.users.pop(c, None)
manager = ConnectionManager()

class SearchRequest(BaseModel):
    query: str

    @field_validator('query')
    @classmethod
    def validate_query(cls, v):
        v = v.strip()
        if not v:
            raise ValueError('Пустой запрос')
        if len(v) > 200:
            raise ValueError('Слишком длинный запрос')
        return v

class TrackRequest(BaseModel):
    track_id: str

    @field_validator('track_id', mode='before')
    @classmethod
    def coerce(cls, v):
        return str(v)

def require_client():
    if client is None:
        raise HTTPException(status_code=503, detail='YANDEX_TOKEN не настроен на сервере')
    return client

def get_cover_url(track_obj):
    try:
        if track_obj.albums and track_obj.albums[0].cover_uri:
            return 'https://' + track_obj.albums[0].cover_uri.replace('%%', '400x400')
    except Exception:
        pass
    return ''

def get_duration_seconds(track_obj):
    """Return track duration in seconds across yandex-music versions."""
    try:
        value = getattr(track_obj, 'duration_ms', None)
        if value:
            return int(value) // 1000
    except Exception:
        pass
    try:
        value = getattr(track_obj, 'duration', None)
        if value:
            return int(value)
    except Exception:
        pass
    return 0

@app.get('/')
def root():
    return {'status': 'ok', 'service': 'sync-music', 'version': '3.0'}

@app.get('/health')
def health():
    return {'status': 'ok', 'yandex_configured': client is not None, 'rooms': len(manager.rooms)}

@app.post('/api/search')
async def search_tracks(req: SearchRequest):
    ym = require_client()
    try:
        result = ym.search(req.query)
        rows = []
        if not result.tracks or not result.tracks.results:
            return {'tracks': []}
        for t in result.tracks.results[:10]:
            rows.append({'id': t.id, 'title': t.title, 'artist': t.artists[0].name if t.artists else 'Unknown', 'album': t.albums[0].title if t.albums else '', 'duration': get_duration_seconds(t), 'cover': get_cover_url(t)})
        return {'tracks': rows}
    except HTTPException:
        raise
    except Exception as e:
        print(f'[search] {e}')
        raise HTTPException(status_code=502, detail='Ошибка поиска музыки')

@app.post('/api/get_link')
async def get_track_link(req: TrackRequest):
    ym = require_client()
    try:
        t = ym.tracks([req.track_id])[0]
        info = t.get_download_info()
        if not info:
            raise RuntimeError('Нет вариантов загрузки')
        return {'url': info[-1].get_direct_link()}
    except Exception as e:
        print(f'[link] {e}')
        raise HTTPException(status_code=502, detail='Не удалось получить ссылку трека')

def participant_id(token: str) -> str:
    return hashlib.sha256(token.encode('utf-8')).hexdigest()[:12]

def public_participants(room):
    out = []
    for ws, u in room.users.items():
        token = u.get('token', '')
        out.append({
            'id': participant_id(token),
            'name': u.get('name') or 'Гость',
            'color': u.get('color', '#6d5dfc'),
            'avatar': u.get('avatar', ''),
            'is_owner': token == room.owner_token,
            'is_playing': room.is_playing
        })
    return out

def public_shared_playlists(room):
    result = []
    for p in room.shared_playlists.values():
        result.append({
            'id': p.get('id'),
            'name': p.get('name', 'Совместный плейлист'),
            'description': p.get('description', ''),
            'owner_id': p.get('owner_id'),
            'member_ids': list(p.get('member_ids', [])),
            'roles': dict(p.get('roles', {})),
            'cover': p.get('cover', ''),
            'tracks': list(p.get('tracks', [])),
            'created_at': p.get('created_at')
        })
    return result

async def send_full_state(ws, room):
    track = room.queue[room.current_index] if 0 <= room.current_index < len(room.queue) else None
    u = room.users.get(ws, {})
    await ws.send_json({'type': 'full_state', 'queue': room.queue, 'current_index': room.current_index, 'current_time': room.get_position(), 'is_playing': room.is_playing, 'track': track, 'repeat_mode': room.repeat_mode, 'shuffle': room.shuffle, 'dj_mode': room.dj_mode, 'is_owner': u.get('token') == room.owner_token, 'participants': public_participants(room), 'self_id': participant_id(u.get('token', '')), 'shared_playlists': public_shared_playlists(room), 'room_history': room.history[-MAX_ROOM_HISTORY:]})

async def participant_update(rid):
    await manager.broadcast({'type': 'participant_update', 'participants': public_participants(manager.get_room(rid))}, rid)

async def queue_update(rid, room):
    manager.save_state()
    await manager.broadcast({'type': 'queue_update', 'queue': room.queue, 'current_index': room.current_index}, rid)

async def settings_update(rid, room):
    manager.save_state()
    for ws, user in list(room.users.items()):
        try:
            await ws.send_json({
                'type': 'room_settings',
                'repeat_mode': room.repeat_mode,
                'shuffle': room.shuffle,
                'dj_mode': room.dj_mode,
                'is_owner': user.get('token') == room.owner_token
            })
        except Exception:
            pass


async def shared_playlists_update(rid, room):
    manager.save_state()
    await manager.broadcast({'type': 'shared_playlists', 'playlists': public_shared_playlists(room)}, rid)

def add_room_history(room, action, track=None, actor='Гость'):
    item = {
        'id': hashlib.sha256(f"{time.time_ns()}:{actor}:{action}".encode()).hexdigest()[:16],
        'action': action,
        'actor': actor[:24],
        'time': time.time(),
    }
    if track:
        item['track'] = {
            'id': str(track.get('id', '')),
            'title': str(track.get('title', ''))[:200],
            'artist': str(track.get('artist', ''))[:120],
            'cover': str(track.get('cover', ''))[:1000],
            'duration': int(track.get('duration', 0) or 0),
        }
    room.history.append(item)
    room.history = room.history[-MAX_ROOM_HISTORY:]


async def room_history_update(rid, room):
    manager.save_state()
    await manager.broadcast({'type': 'room_history', 'history': room.history[-MAX_ROOM_HISTORY:]}, rid)

async def play_track(rid, room, index, t=0):
    if not 0 <= index < len(room.queue):
        return
    room.current_index = index
    room.current_time = max(0, float(t))
    room.is_playing = True
    room.last_update = time.time()
    if 0 <= index < len(room.queue):
        add_room_history(room, 'play', room.queue[index], 'Система')
    manager.save_state()
    await manager.broadcast({'type': 'play_track', 'track': room.queue[index], 'index': index, 'time': room.current_time}, rid)
    await manager.broadcast({'type': 'system_message', 'text': f'Сейчас играет: {room.queue[index].get("title", "трек")}', 'time': time.time()}, rid)
    await queue_update(rid, room)

def choose_next(room):
    if not room.queue:
        return None
    if room.repeat_mode == 'one' and room.current_index >= 0:
        return room.current_index
    if room.shuffle and len(room.queue) > 1:
        return random.choice([i for i in range(len(room.queue)) if i != room.current_index])
    n = room.current_index + 1
    if n < len(room.queue):
        return n
    return 0 if room.repeat_mode == 'all' else None

def can_control(room, ws):
    return not room.dj_mode or room.users.get(ws, {}).get('token') == room.owner_token

@app.on_event('startup')
async def cleanup_loop():
    async def loop():
        while True:
            await asyncio.sleep(60)
            manager.cleanup_empty()
    asyncio.create_task(loop())


@app.websocket('/ws/{room_id}')
async def websocket_endpoint(websocket: WebSocket, room_id: str):
    await manager.connect(websocket, room_id)
    room = manager.get_room(room_id)
    try:
        while True:
            data = await websocket.receive_json()
            typ = data.get('type')
            if typ == 'hello':
                token = str(data.get('token') or '')[:100]
                uname = str(data.get('name') or 'Гость').strip()[:24] or 'Гость'
                if not token:
                    token = f'server-{id(websocket)}'
                if token in room.blocked_tokens:
                    await websocket.send_json({'type': 'kicked', 'text': 'Ты был удалён из комнаты владельцем.'})
                    await websocket.close(code=4003)
                    manager.disconnect(websocket, room_id)
                    continue
                if room.owner_token is None:
                    room.owner_token = token
                room.users[websocket] = {'token': token, 'name': uname, 'color': data.get('color') or '#6d5dfc', 'avatar': str(data.get('avatar') or '')[:300000]}
                await send_full_state(websocket, room)
                await participant_update(room_id)
                continue
            if websocket not in room.users:
                continue
            user = room.users[websocket]
            name = user.get('name') or 'Гость'
            if typ == 'profile_update':
                user['name'] = str(data.get('name') or user.get('name') or 'Гость').strip()[:24] or 'Гость'
                user['avatar'] = str(data.get('avatar') or '')[:300000]
                user['color'] = str(data.get('color') or user.get('color') or '#6d5dfc')[:32]
                await participant_update(room_id)
                continue
            if typ == 'leave_room':
                await websocket.send_json({'type': 'left_room'})
                manager.disconnect(websocket, room_id)
                await participant_update(room_id)
                try:
                    await websocket.close(code=1000)
                except Exception:
                    pass
                break
            if typ == 'transfer_owner':
                if user.get('token') != room.owner_token:
                    continue
                target_id = str(data.get('participant_id') or '')
                target_ws = next((candidate for candidate, candidate_user in room.users.items() if participant_id(candidate_user.get('token', '')) == target_id), None)
                if target_ws is None:
                    continue
                room.owner_token = room.users[target_ws].get('token')
                room.dj_mode = False
                manager.save_state()
                await manager.broadcast({'type': 'system_message', 'text': f'{name} передал права владельца', 'time': time.time()}, room_id)
                await participant_update(room_id)
                await settings_update(room_id, room)
                continue
            if typ == 'kick_participant':
                if user.get('token') != room.owner_token:
                    continue
                target_id = str(data.get('participant_id') or '')
                target_ws = next((candidate for candidate, candidate_user in room.users.items() if participant_id(candidate_user.get('token', '')) == target_id), None)
                if target_ws is None or target_ws is websocket:
                    continue
                target_user = room.users.get(target_ws, {})
                target_token = target_user.get('token', '')
                room.blocked_tokens.add(target_token)
                try:
                    await target_ws.send_json({'type': 'kicked', 'text': 'Владелец удалил тебя из комнаты.'})
                    await target_ws.close(code=4003)
                except Exception:
                    pass
                manager.disconnect(target_ws, room_id)
                await participant_update(room_id)
                await manager.broadcast({'type': 'system_message', 'text': f'{name} удалил участника из комнаты', 'time': time.time()}, room_id)
                continue
            if typ == 'shared_playlist_create':
                playlist_name = str(data.get('name') or '').strip()[:80]
                if not playlist_name:
                    continue
                owner_id = participant_id(user.get('token', ''))
                member_ids = [str(x)[:32] for x in (data.get('member_ids') or []) if x]
                member_ids = list(dict.fromkeys([owner_id] + member_ids))
                roles = {str(x): ('owner' if str(x) == owner_id else 'editor') for x in member_ids}
                playlist_id = str(data.get('id') or '')[:64] or hashlib.sha256(f'{room_id}:{time.time()}:{owner_id}'.encode()).hexdigest()[:16]
                room.shared_playlists[playlist_id] = {
                    'id': playlist_id,
                    'name': playlist_name,
                    'description': str(data.get('description') or '').strip()[:240],
                    'owner_id': owner_id,
                    'member_ids': member_ids,
                    'roles': roles,
                    'cover': str(data.get('cover') or '')[:300000],
                    'tracks': [],
                    'created_at': time.time()
                }
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'shared_playlist_update':
                playlist = room.shared_playlists.get(str(data.get('playlist_id') or ''))
                owner_id = participant_id(user.get('token', ''))
                if not playlist or (playlist.get('owner_id') != owner_id and user.get('token') != room.owner_token):
                    continue
                playlist['name'] = str(data.get('name') or playlist.get('name') or 'Совместный плейлист').strip()[:80]
                playlist['description'] = str(data.get('description') or '').strip()[:240]
                playlist['cover'] = str(data.get('cover') or '')[:300000]
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'shared_playlist_update_members':
                playlist = room.shared_playlists.get(str(data.get('playlist_id') or ''))
                owner_id = participant_id(user.get('token', ''))
                if not playlist or (playlist.get('owner_id') != owner_id and user.get('token') != room.owner_token):
                    continue
                member_ids = [str(x)[:32] for x in (data.get('member_ids') or []) if x]
                member_ids = list(dict.fromkeys([owner_id] + member_ids))
                roles = dict(playlist.get('roles', {}))
                incoming_roles = data.get('roles') or {}
                playlist['member_ids'] = member_ids
                playlist['roles'] = {mid: ('owner' if mid == owner_id else ('viewer' if incoming_roles.get(mid) == 'viewer' else 'editor')) for mid in member_ids}
                await shared_playlists_update(room_id, room)
                await manager.broadcast({'type': 'system_message', 'text': f'{name} изменил доступ к совместному плейлисту', 'time': time.time()}, room_id)
                continue
            if typ == 'shared_playlist_add_track':
                playlist = room.shared_playlists.get(str(data.get('playlist_id') or ''))
                user_id = participant_id(user.get('token', ''))
                role = playlist.get('roles', {}).get(user_id, 'editor') if playlist else 'viewer'
                if not playlist or (user_id not in playlist.get('member_ids', []) and user.get('token') != room.owner_token) or (role not in ('owner', 'editor') and user.get('token') != room.owner_token):
                    continue
                track = data.get('track')
                if not isinstance(track, dict) or not track.get('id'):
                    continue
                if any(str(x.get('id')) == str(track.get('id')) for x in playlist.get('tracks', [])):
                    continue
                safe_track = {
                    'id': str(track.get('id'))[:100],
                    'title': str(track.get('title') or '')[:200],
                    'artist': str(track.get('artist') or 'Unknown')[:120],
                    'album': str(track.get('album') or '')[:160],
                    'cover': str(track.get('cover') or '')[:1000],
                    'duration': max(0, int(track.get('duration') or 0)),
                    'added_by': name
                }
                playlist.setdefault('tracks', []).append(safe_track)
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'shared_playlist_reorder':
                playlist = room.shared_playlists.get(str(data.get('playlist_id') or ''))
                user_id = participant_id(user.get('token', ''))
                role = playlist.get('roles', {}).get(user_id, 'editor') if playlist else 'viewer'
                if not playlist or (user_id not in playlist.get('member_ids', []) and user.get('token') != room.owner_token) or (role not in ('owner', 'editor') and user.get('token') != room.owner_token):
                    continue
                a, b = data.get('from'), data.get('to')
                if not isinstance(a, int) or not isinstance(b, int) or not (0 <= a < len(playlist.get('tracks', [])) and 0 <= b < len(playlist.get('tracks', []))):
                    continue
                tr = playlist['tracks'].pop(a)
                playlist['tracks'].insert(b, tr)
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'shared_playlist_remove_track':
                playlist = room.shared_playlists.get(str(data.get('playlist_id') or ''))
                user_id = participant_id(user.get('token', ''))
                role = playlist.get('roles', {}).get(user_id, 'editor') if playlist else 'viewer'
                if not playlist or (user_id not in playlist.get('member_ids', []) and user.get('token') != room.owner_token) or (role not in ('owner', 'editor') and user.get('token') != room.owner_token):
                    continue
                track_id = str(data.get('track_id') or '')
                playlist['tracks'] = [x for x in playlist.get('tracks', []) if str(x.get('id')) != track_id]
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'shared_playlist_delete':
                playlist_id = str(data.get('playlist_id') or '')
                playlist = room.shared_playlists.get(playlist_id)
                owner_id = participant_id(user.get('token', ''))
                if not playlist or (playlist.get('owner_id') != owner_id and user.get('token') != room.owner_token):
                    continue
                room.shared_playlists.pop(playlist_id, None)
                await shared_playlists_update(room_id, room)
                continue
            if typ == 'add_to_queue':
                tid = data.get('track_id')
                if not tid:
                    continue
                try:
                    ym = require_client()
                    tr = ym.tracks([str(tid)])[0]
                    info = tr.get_download_info()
                    if not info:
                        continue
                    if any(str(item.get('id')) == str(tid) for item in room.queue):
                        await websocket.send_json({'type': 'system_message', 'text': 'Этот трек уже находится в очереди', 'time': time.time()})
                        continue

                    duration = int(data.get('duration') or 0)
                    if duration <= 0:
                        duration = get_duration_seconds(tr)

                    room.queue.append({
                        'id': str(tid),
                        'title': str(data.get('title') or tr.title)[:200],
                        'artist': str(data.get('artist') or 'Unknown')[:120],
                        'album': str(data.get('album') or (tr.albums[0].title if tr.albums else ''))[:160],
                        'url': info[-1].get_direct_link(),
                        'cover': str(data.get('cover') or get_cover_url(tr))[:1000],
                        'duration': duration,
                        'added_by': name
                    })
                    add_room_history(room, 'add', room.queue[-1], name)
                    if room.current_index == -1:
                        await play_track(room_id, room, 0, 0)
                    else:
                        await queue_update(room_id, room)
                    await room_history_update(room_id, room)
                    await manager.broadcast({'type': 'system_message', 'text': f'{name} добавил «{room.queue[-1]["title"]}» в очередь', 'time': time.time()}, room_id)
                except Exception as e:
                    print(f'[queue:add] {e}')
            elif typ in {'remove_from_queue', 'clear_queue', 'reorder_queue', 'play_track_manual', 'play_track_manual_by_id', 'next_track', 'prev_track', 'track_ended', 'play', 'pause', 'seek', 'set_repeat', 'set_shuffle'} and (not can_control(room, websocket)):
                await websocket.send_json({'type': 'system_message', 'text': 'DJ Mode: управлять воспроизведением может только владелец комнаты', 'time': time.time()})
                continue
            elif typ == 'remove_many_from_queue':
                indices = data.get('indices') or []
                if not isinstance(indices, list):
                    continue
                valid = sorted({int(x) for x in indices if isinstance(x, int) and 0 <= x < len(room.queue)}, reverse=True)
                if not valid:
                    continue
                current_track_id = room.queue[room.current_index].get('id') if 0 <= room.current_index < len(room.queue) else None
                current_was_removed = room.current_index in valid
                removed = [room.queue[i] for i in valid]
                valid_set = set(valid)
                room.queue = [track for idx, track in enumerate(room.queue) if idx not in valid_set]
                if current_track_id is not None:
                    room.current_index = next((i for i, track in enumerate(room.queue) if str(track.get('id')) == str(current_track_id)), -1)
                else:
                    room.current_index = -1
                for track in removed[:20]:
                    add_room_history(room, 'remove', track, name)
                if not room.queue:
                    room.current_index = -1
                    room.current_time = 0
                    room.is_playing = False
                    room.last_update = time.time()
                    await queue_update(room_id, room)
                    await room_history_update(room_id, room)
                elif current_was_removed:
                    next_index = min(max(0, room.current_index), len(room.queue) - 1)
                    await play_track(room_id, room, next_index, 0)
                    await room_history_update(room_id, room)
                else:
                    await queue_update(room_id, room)
                    await room_history_update(room_id, room)
                continue
            elif typ == 'clear_queue':
                if not room.queue:
                    continue
                current = room.queue[room.current_index] if 0 <= room.current_index < len(room.queue) else None
                room.queue = [current] if current else []
                room.current_index = 0 if current else -1
                if not current:
                    room.current_time = 0
                    room.is_playing = False
                room.last_update = time.time()
                add_room_history(room, 'clear', current, name)
                await queue_update(room_id, room)
                await room_history_update(room_id, room)
                await manager.broadcast({'type': 'system_message', 'text': f'{name} очистил очередь', 'time': time.time()}, room_id)
            elif typ == 'remove_from_queue':
                i = data.get('index')
                if not isinstance(i, int) or not 0 <= i < len(room.queue):
                    continue
                was = i == room.current_index
                removed_track = room.queue[i]
                room.queue.pop(i)
                add_room_history(room, 'remove', removed_track, name)
                if not room.queue:
                    room.current_index = -1
                    room.current_time = 0
                    room.is_playing = False
                    room.last_update = time.time()
                    await queue_update(room_id, room)
                    await room_history_update(room_id, room)
                elif was:
                    await play_track(room_id, room, min(i, len(room.queue) - 1), 0)
                    await room_history_update(room_id, room)
                else:
                    if i < room.current_index:
                        room.current_index -= 1
                    await queue_update(room_id, room)
                    await room_history_update(room_id, room)
            elif typ == 'reorder_queue':
                a, b = (data.get('from'), data.get('to'))
                if not all((isinstance(x, int) for x in (a, b))) or not (0 <= a < len(room.queue) and 0 <= b < len(room.queue)):
                    continue
                tr = room.queue.pop(a)
                room.queue.insert(b, tr)
                if room.current_index == a:
                    room.current_index = b
                elif a < room.current_index <= b:
                    room.current_index -= 1
                elif b <= room.current_index < a:
                    room.current_index += 1
                await queue_update(room_id, room)
            elif typ in ('next_track', 'track_ended'):
                if typ == 'track_ended' and data.get('expected_index') is not None and (data.get('expected_index') != room.current_index):
                    continue
                idx = choose_next(room)
                if idx is None:
                    room.is_playing = False
                    room.current_time = 0
                    room.last_update = time.time()
                    manager.save_state()
                    await manager.broadcast({'type': 'pause', 'time': 0}, room_id)
                else:
                    await play_track(room_id, room, idx, 0)
            elif typ == 'prev_track':
                if room.current_index > 0:
                    await play_track(room_id, room, room.current_index - 1, 0)
            elif typ == 'play_track_manual':
                i = data.get('index')
                if isinstance(i, int) and 0 <= i < len(room.queue):
                    await play_track(room_id, room, i, 0)
            elif typ == 'play_track_manual_by_id':
                tid = str(data.get('track_id'))
                i = next((i for i, t in enumerate(room.queue) if str(t.get('id')) == tid), None)
                if i is not None:
                    await play_track(room_id, room, i, 0)
            elif typ == 'set_repeat':
                mode = data.get('mode', 'off')
                if mode in ('off', 'all', 'one'):
                    room.repeat_mode = mode
                    await settings_update(room_id, room)
            elif typ == 'set_shuffle':
                room.shuffle = bool(data.get('enabled'))
                await settings_update(room_id, room)
            elif typ == 'play':
                room.is_playing = True
                room.current_time = max(0, float(data.get('time', 0)))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({'type': 'play', 'time': room.current_time}, room_id, websocket)
                await manager.broadcast({'type': 'system_message', 'text': f'{name} включил воспроизведение', 'time': time.time()}, room_id)
                await participant_update(room_id)
            elif typ == 'pause':
                room.is_playing = False
                room.current_time = max(0, float(data.get('time', room.get_position())))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({'type': 'pause', 'time': room.current_time}, room_id, websocket)
                await manager.broadcast({'type': 'system_message', 'text': f'{name} поставил паузу', 'time': time.time()}, room_id)
                await participant_update(room_id)
            elif typ == 'seek':
                room.current_time = max(0, float(data.get('time', 0)))
                room.last_update = time.time()
                manager.save_state()
                await manager.broadcast({'type': 'seek', 'time': room.current_time}, room_id, websocket)
            elif typ == 'set_dj_mode':
                if user.get('token') != room.owner_token:
                    continue
                room.dj_mode = bool(data.get('enabled'))
                await settings_update(room_id, room)
                await manager.broadcast({'type': 'system_message', 'text': f"{name} {('включил' if room.dj_mode else 'выключил')} DJ Mode", 'time': time.time()}, room_id)
            elif typ == 'clear_room_history':
                if user.get('token') != room.owner_token:
                    continue
                room.history.clear()
                await room_history_update(room_id, room)
                continue
            elif typ == 'chat_message':
                now = time.monotonic()
                last = room.chat_last.get(websocket, 0)
                if now - last < CHAT_COOLDOWN:
                    continue
                text = str(data.get('text') or '').strip()[:MAX_CHAT]
                if not text:
                    continue
                room.chat_last[websocket] = now
                await manager.broadcast({'type': 'chat_message', 'message': {'id': hashlib.sha256(f'{time.time_ns()}:{name}'.encode()).hexdigest()[:16], 'name': name, 'text': text, 'time': time.time(), 'reply_to': data.get('reply_to') if isinstance(data.get('reply_to'), dict) else None}}, room_id)
            elif typ == 'chat_reaction':
                message_id = str(data.get('message_id') or '')[:32]
                reaction = str(data.get('reaction') or '')[:8]
                allowed = {'❤️', '🔥', '😂', '👍', '👎', '👏', '🎵'}
                if message_id and reaction in allowed:
                    await manager.broadcast({'type': 'reaction_message', 'message_id': message_id, 'reaction': reaction, 'name': name}, room_id)
                continue
            elif typ == 'reaction':
                reaction = str(data.get('reaction') or '')[:4]
                allowed = {'❤️', '🔥', '😂', '👍'}
                if reaction in allowed:
                    await manager.broadcast({'type': 'reaction', 'reaction': reaction, 'name': name}, room_id)
    except WebSocketDisconnect:
        manager.disconnect(websocket, room_id)
        await participant_update(room_id)
    except Exception as e:
        print(f'[ws] error: {e}')
        manager.disconnect(websocket, room_id)
        await participant_update(room_id)
