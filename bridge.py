"""
NeneOkulZil - WebSocket Köprü Sunucusu
Okul bilgisayarı bu sunucuya WS ile bağlanır.
Telefon HTTP API çağrılarını bu sunucuya yapar.
Sunucu, HTTP isteklerini WS üzerinden okul bilgisayarına iletir ve cevabı döner.
"""
import asyncio
import json
import os
import time
import uuid
from aiohttp import web, WSMsgType

# topic -> {"school_ws": websocket, "pending": {req_id: Future}}
rooms = {}
rooms_lock = asyncio.Lock()

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Okul bilgisayarı WS bağlantısı ──────────────────────────────────────────
async def handle_school(request):
    """server.py buraya WS ile bağlanır, gelen HTTP isteklerini işler."""
    topic = request.match_info["topic"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    log(f"Okul bağlandı: {topic}")

    async with rooms_lock:
        if topic not in rooms:
            rooms[topic] = {"school_ws": None, "pending": {}}
        rooms[topic]["school_ws"] = ws

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    req_id = data.get("_req_id")
                    if req_id:
                        async with rooms_lock:
                            fut = rooms.get(topic, {}).get("pending", {}).pop(req_id, None)
                        if fut and not fut.done():
                            fut.set_result(data)
                except Exception as e:
                    log(f"Okul mesaj hatası: {e}")
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        async with rooms_lock:
            if topic in rooms:
                if rooms[topic]["school_ws"] is ws:
                    rooms[topic]["school_ws"] = None
                    # Yalnızca bu WS aktifken gelen bekleyen istekleri iptal et
                    for fut in rooms[topic]["pending"].values():
                        if not fut.done():
                            fut.set_exception(Exception("Okul bağlantısı kesildi"))
                    rooms[topic]["pending"].clear()
        log(f"Okul ayrıldı: {topic}")
    return ws

# ── Proxy: HTTP → WS → HTTP ─────────────────────────────────────────────────
async def handle_proxy(request):
    """Telefondan gelen HTTP isteğini WS üzerinden okul PC'ye iletir."""
    topic    = request.match_info["topic"]
    endpoint = "/" + request.match_info["endpoint"]
    qs       = request.query_string

    # ── Guvenlik: kopru YALNIZCA /api/remote/* uclarini gecirir ──────────────
    # Okul PC'si (server.py) gelen _endpoint'i oldugu gibi kendi 127.0.0.1'ine
    # iletir. Oradaki yetki sinirlari: /api/remote/* -> PIN, diger /api/* ->
    # API_TOKEN, statik dosyalar -> HIC KONTROL YOK (do_GET yalnizca "/api/" ile
    # baslayanlari kontrol eder). Kopru token tasimadigi icin /api/* zaten 403
    # alir; asil acik statik GET'lerdi (/index.html vb.) — ntfy topic'ini bilen
    # herkes cekebilirdi. Sinir bu yuzden kopru kenarinda da uygulanir.
    # ".." ayrica reddedilir: "/api/remote/../index.html" oneki gecer ama
    # urllib istegi normallestirince whitelist disina cikardi.
    if not endpoint.startswith("/api/remote/") or ".." in endpoint:
        log(f"Proxy REDDEDILDI (whitelist disi): {endpoint} topic={topic}")
        return web.json_response(
            {"ok": False, "error": "Bu uc kopru uzerinden kullanilamaz"},
            status=403,
            headers={"Access-Control-Allow-Origin": "*"}
        )

    # WS yeni bağlanıyor / yeniden bağlanıyor olabilir — 8 saniye bekle
    school_ws = None
    for attempt in range(4):
        async with rooms_lock:
            room = rooms.get(topic)
            sw = room["school_ws"] if room else None
        if sw is None:
            state = "None"
        elif sw.closed:
            state = "closed"
        else:
            state = "open"
        log(f"Proxy deneme {attempt+1}: school_ws={state} bool={bool(sw) if sw is not None else 'N/A'} topic={topic}")
        if sw is not None and not sw.closed:
            school_ws = sw
            break
        if attempt < 3:
            await asyncio.sleep(2)

    if school_ws is None:
        return web.json_response(
            {"ok": False, "error": "Okul bilgisayarı bağlı değil"},
            status=503,
            headers={"Access-Control-Allow-Origin": "*"}
        )

    req_id = str(uuid.uuid4())[:8]
    loop   = asyncio.get_event_loop()
    fut    = loop.create_future()

    async with rooms_lock:
        rooms[topic]["pending"][req_id] = fut

    # İsteği paketle
    method = request.method
    body   = {}
    if method == "POST":
        try:
            body = await request.json()
        except Exception:
            body = {}

    packet = {
        "_req_id":  req_id,
        "_method":  method,
        "_endpoint": endpoint,
        "_qs":      qs,
        **body
    }

    # K-7 (adim 1): PIN'i adres satirindan (?pin=) cikarabilmek icin telefonun
    # X-Remote-Pin header'ini pakete tasi. EK ozellik, mevcut akisi BOZMAZ:
    # govde/qs'te pin varsa dokunulmaz; header YALNIZCA pakette pin yokken doldurur.
    # (server.py packet["pin"]'i okur; boylece GET'lerde de PIN header'dan gecebilir.)
    if not packet.get("pin"):
        hdr_pin = request.headers.get("X-Remote-Pin", "")
        if hdr_pin:
            packet["pin"] = hdr_pin

    try:
        await school_ws.send_str(json.dumps(packet, ensure_ascii=False))
        result = await asyncio.wait_for(fut, timeout=10.0)
        result.pop("_req_id", None)
        status = result.pop("_status", 200)
        return web.json_response(result, status=status,
                                 headers={"Access-Control-Allow-Origin": "*"})
    except asyncio.TimeoutError:
        async with rooms_lock:
            rooms.get(topic, {}).get("pending", {}).pop(req_id, None)
        return web.json_response(
            {"ok": False, "error": "Okul bilgisayarı cevap vermedi"},
            status=504,
            headers={"Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return web.json_response(
            {"ok": False, "error": str(e)},
            status=500,
            headers={"Access-Control-Allow-Origin": "*"}
        )

# ── SSE proxy ────────────────────────────────────────────────────────────────
async def handle_sse_proxy(request):
    """SSE bağlantısını köprü üzerinden okul PC'ye iletir."""
    topic = request.match_info["topic"]
    pin   = request.rel_url.query.get("pin", "")

    async with rooms_lock:
        room = rooms.get(topic)
        school_ws = room["school_ws"] if room else None

    if not school_ws or school_ws.closed:
        return web.Response(
            status=503,
            text="data: {\"error\": \"Okul bağlı değil\"}\n\n",
            content_type="text/event-stream"
        )

    response = web.StreamResponse(headers={
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*"
    })
    await response.prepare(request)

    # SSE kuyruğu: okul PC'den gelen SSE mesajları buraya düşer
    sse_queue = asyncio.Queue(maxsize=50)

    sse_id = f"sse_{uuid.uuid4().hex[:8]}"
    async with rooms_lock:
        if topic not in rooms:
            rooms[topic] = {"school_ws": None, "pending": {}}
        rooms[topic].setdefault("sse_clients", {})[sse_id] = sse_queue

    # Okul PC'ye SSE başlat bildirimi gönder
    await school_ws.send_str(json.dumps({
        "_sse_start": True,
        "_sse_id": sse_id,
        "pin": pin
    }))

    try:
        await response.write(b"event: connected\ndata: {}\n\n")
        while True:
            try:
                msg = await asyncio.wait_for(sse_queue.get(), timeout=25)
                await response.write(msg.encode())
            except asyncio.TimeoutError:
                await response.write(b": keepalive\n\n")
    except Exception:
        pass
    finally:
        async with rooms_lock:
            rooms.get(topic, {}).get("sse_clients", {}).pop(sse_id, None)
        try:
            await school_ws.send_str(json.dumps({"_sse_stop": True, "_sse_id": sse_id}))
        except Exception:
            pass

    return response

# ── OPTIONS (CORS) ───────────────────────────────────────────────────────────
async def handle_options(request):
    return web.Response(headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Api-Token, X-Remote-Pin"
    })

# ── Durum / sağlık ───────────────────────────────────────────────────────────
async def handle_status(request):
    topic = request.match_info["topic"]
    async with rooms_lock:
        room = rooms.get(topic)
        sw = room.get("school_ws") if room else None
        connected = sw is not None and not sw.closed
    return web.json_response(
        {"connected": connected, "topic": topic},
        headers={"Access-Control-Allow-Origin": "*"}
    )

async def handle_health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

# NOT: /debug ucu kaldirildi (K-8) — auth'suz olarak bagli tum okullarin topic'ini
# listeliyordu; tum uzaktan guvenlik "topic'i kimse bilmez" varsayimina dayandigi icin
# bu bir sizinti idi. Hicbir mesru istemci /debug cagirmiyordu (yalnizca hata ayiklama
# amacliydi, 409eb10). /health rooms SAYISINI verir ama topic'leri ASLA acmaz.

# ── Uygulama ─────────────────────────────────────────────────────────────────
app = web.Application()
app.router.add_get ("/ws/school/{topic}",               handle_school)
app.router.add_get ("/status/{topic}",                  handle_status)
app.router.add_get ("/health",                          handle_health)
app.router.add_get ("/proxy/{topic}/api/remote/sse",    handle_sse_proxy)
app.router.add_get ("/proxy/{topic}/{endpoint:.*}",     handle_proxy)
app.router.add_post("/proxy/{topic}/{endpoint:.*}",     handle_proxy)
app.router.add_route("OPTIONS", "/proxy/{topic}/{endpoint:.*}", handle_options)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    log(f"Köprü sunucusu başlıyor: port={port}")
    web.run_app(app, host="0.0.0.0", port=port)
