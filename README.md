# objtrsf

A reusable, self-contained transport stack, originally developed for
`github.com/on-keyday/agent-harness` and shared with related projects.

- `objproto/` — encrypted, reliable UDP object protocol. The handshake is an
  anonymous ECDH exchange; **authentication is delegated to the application
  layer** (the wire intentionally carries no peer auth).
- `trsf/` — QUIC-like multiplexed transport layered over objproto: streams,
  congestion control, flow control, MTU probing, ack handling.

## Layering

The transport core owns only the transport-range wire kinds
(`ping`/`pong`/`close` + the stream kinds, values `0x00`–`0x3F`). Application
payload kinds are defined by the consumer and use the reserved app range
(`0x40`–`0xFF`); the core passes any non-transport kind through to the
application via its receive seam.

## Status

Work in progress. Until a tagged release exists, consumers wire it in with a
local `replace` directive.
