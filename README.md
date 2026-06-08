# objtrsf

A reusable, self-contained transport stack, originally developed for
`github.com/on-keyday/agent-harness` and shared with related projects.

- `transport/` — pluggable byte-transport endpoints that objproto runs over:
  UDP (with PMTUD), WebSocket (including a `GOOS=js`/wasm build), a UDP+WebSocket
  dual stack, and an in-memory pipe for tests. Each yields an `objproto.Endpoint`.
- `objproto/` — encrypted object protocol: an authenticated-encryption datagram
  layer (AEAD + packet numbers + replay/dedup + a reorder buffer) carried over any
  `transport/` endpoint. It is best-effort, **not** reliable — there is no
  retransmission or loss recovery here. The handshake is an anonymous ECDH
  exchange; **authentication is delegated to the application layer** (the wire
  intentionally carries no peer auth).
- `trsf/` — QUIC-like **reliable** multiplexed transport layered over objproto;
  this is where reliable delivery lives: streams, retransmission/ack handling,
  congestion control, flow control, MTU probing.

## Layering

The transport core owns only the transport-range wire kinds
(`ping`/`pong`/`close` + the stream kinds, values `0x00`–`0x3F`). Application
payload kinds are defined by the consumer and use the reserved app range
(`0x40`–`0xFF`); the core passes any non-transport kind through to the
application via its receive seam.

## Status

Work in progress. Until a tagged release exists, consumers wire it in with a
local `replace` directive.

**Personal / experimental — only for me.** This is built for my own projects and
outside use is not really intended or supported. There is no stability promise:
the API and the wire format may change without notice.

**Not security-audited.** The cryptography is a self-rolled, toy-scope protocol,
not a reviewed standard. The objproto handshake is an anonymous ECDH exchange,
unauthenticated by design — it gives confidentiality against a passive
eavesdropper, but resisting an active MITM requires the application layer to bind
an authenticator (e.g. a PSK) to the exported handshake transcript. Don't use
this for anything security-sensitive.
