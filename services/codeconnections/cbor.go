package codeconnections

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// cborBridge serves the Smithy RPCv2 CBOR protocol the SDK now prefers.
var cborBridge = service.RPCv2CBORBridge{ //nolint:gochecknoglobals // immutable wire-shape table
	Name:     "CodeConnections_20231201",
	TimeKeys: map[string]bool{"CreatedAt": true, "ResolvedAt": true, "StartedAt": true, "Time": true},
}
