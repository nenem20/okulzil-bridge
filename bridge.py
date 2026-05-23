"""
NeneOkulZil - WebSocket Köprü Sunucusu
Okul bilgisayarı bu sunucuya bağlanır.
Telefon bu sunucu üzerinden komut gönderir.
"""
import asyncio
import json
import os
import time
import hashlib
import secrets
from aiohttp import web, WSMsgType

# Her okul kendi topic'i ile çalışır
# topic -> {"school_ws": websocket, "clients": [websocket, ...]}
rooms = {}
rooms_lock = asyncio.Lock()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

async def handle_school(request):
    """Okul bilgisayarı buraya bağlanır (server.py)"""
    topic = request.match_info["topic"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    
    log(f"Okul bağlandı: {topic}")
    
    async with rooms_lock:
        if topic not in rooms:
            rooms[topic] = {"school_ws": None, "clients": []}
        rooms[topic]["school_ws"] = ws
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Okuldan gelen mesajları (durum bildirimi vs.) telefona ilet
                data = msg.data
                async with rooms_lock:
                    if topic in rooms:
                        dead = []
                        for client_ws in rooms[topic]["clients"]:
                            try:
                                await client_ws.send_str(data)
                            except:
                                dead.append(client_ws)
                        for d in dead:
                            rooms[topic]["clients"].remove(d)
            elif msg.type == WSMsgType.ERROR:
                log(f"Okul WS hatası: {topic}")
                break
    finally:
        async with rooms_lock:
            if topic in rooms:
                rooms[topic]["school_ws"] = None
        log(f"Okul ayrıldı: {topic}")
    
    return ws

async def handle_remote(request):
    """Telefon buraya bağlanır (remote.html)"""
    topic = request.match_info["topic"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    
    log(f"Telefon bağlandı: {topic}")
    
    async with rooms_lock:
        if topic not in rooms:
            rooms[topic] = {"school_ws": None, "clients": []}
        rooms[topic]["clients"].append(ws)
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Telefondan gelen komutları okul bilgisayarına ilet
                async with rooms_lock:
                    if topic in rooms and rooms[topic]["school_ws"]:
                        try:
                            await rooms[topic]["school_ws"].send_str(msg.data)
                        except:
                            rooms[topic]["school_ws"] = None
                            log(f"Okul bağlantısı koptu: {topic}")
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        async with rooms_lock:
            if topic in rooms and ws in rooms[topic]["clients"]:
                rooms[topic]["clients"].remove(ws)
        log(f"Telefon ayrıldı: {topic}")
    
    return ws

async def handle_status(request):
    """Okul bağlı mı kontrol et"""
    topic = request.match_info["topic"]
    async with rooms_lock:
        room = rooms.get(topic)
        connected = room is not None and room["school_ws"] is not None
    return web.json_response({"connected": connected, "topic": topic})

async def handle_health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

app = web.Application()
app.router.add_get("/ws/school/{topic}", handle_school)
app.router.add_get("/ws/remote/{topic}", handle_remote)
app.router.add_get("/status/{topic}", handle_status)
app.router.add_get("/health", handle_health)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log(f"Köprü sunucusu başlıyor: port={port}")
    web.run_app(app, host="0.0.0.0", port=port)
