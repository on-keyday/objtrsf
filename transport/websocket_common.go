package transport

import (
	"crypto/tls"
	"log/slog"

	"github.com/on-keyday/objtrsf/objproto"
)

// WebSocketConfig configures a WebSocket-backed objproto Endpoint. The same
// struct is used for Client / Server / Mutual modes; the Path field is
// interpreted by Client/Mutual as the dial Location.Path, and by
// Server/Mutual as the mount path passed to mux.Handle.
//
// The transport package does not own a path convention. Callers are expected
// to align Client and Server values; cli.WebSocketPath is the canonical
// harness-side default.
//
// TLS is consulted for Origin scheme decisions (ws:// vs wss://). The
// listen-side TLS for Server / Mutual is owned by the caller's *http.Server.
//
// Mode selects Client / Server / Mutual semantics. The mux argument of
// WebSocketEndpoint must be nil for Client and non-nil for Server / Mutual.
//
// This file (websocket_common.go) is build-constraint-free so the struct is
// shared between native (websocket.go, !js) and wasm (websocket_wasm.go, js)
// implementations.
type WebSocketConfig struct {
	Logger *slog.Logger
	Path   string
	TLS    *tls.Config
	Mode   objproto.EndpointMode
}
