# NeneOkulZil - WebSocket Köprü Sunucusu

Okul bilgisayarı ile telefon arasında köprü görevi görür.

## Nasıl Çalışır

```
Telefon (4G) <---> Bu Sunucu (Render) <---> Okul Bilgisayarı (Okul Ağı)
```

## Endpoints

- `GET /ws/school/{topic}` — Okul bilgisayarı bağlanır
- `GET /ws/remote/{topic}` — Telefon bağlanır  
- `GET /status/{topic}` — Okul bağlı mı?
- `GET /health` — Sunucu sağlık kontrolü
