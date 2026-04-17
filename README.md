# ChatWave

WebSocket chat server written from scratch in Java. No frameworks, no Netty, no Spring —
just raw sockets and the standard library.

Started this because I wanted to actually understand how WebSockets work at the protocol
level, not just use a library that handles it. Ended up going further than planned.

## What it does

- WebSocket handshake + frame parsing (RFC 6455)
- User auth with salted SHA-256
- Roles: user / mod / admin / dev
- Public channels with persistent history
- Direct messages with offline delivery
- File uploads (images, videos, other)
- WebRTC signaling for audio/video calls
- Chat commands (/mute, /ban, /kick, /setrole, ...)
- Inventory system (custom titles + name colors per user)
- Beta channel access via title system

## Requirements

- Java 21 (uses virtual threads)

## Running

```bash
javac ChatWaveServer.java ChannelManager.java
java ChatWaveServer
```

Server starts on port `12345`. Data is stored in the working directory:
`users.db`, `dm/`, `channels/`, `uploads/`.

## Notes

The frontend is not included here. This is just the server.

`ChannelManager` handles channel history I/O and is used by `ChatWaveServer`.
