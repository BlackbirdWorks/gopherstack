package outposts

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// randomKeyBytes is the byte length of the synthetic public key material
// randomBase64Key generates -- 32 bytes base64-encodes to a ~44-char string,
// in the same general shape a WireGuard public key takes.
const randomKeyBytes = 32

// randomBase64Key returns a synthetic, non-cryptographic placeholder public
// key string in the same general shape a WireGuard public key takes
// (base64, ~44 chars). It is NOT real key material -- see PARITY.md's
// "Connection/private-connectivity" note: StartConnection/GetConnection
// model a narrow, install-time-only tunnel, and this backend does not
// attempt to simulate real cryptography.
func randomBase64Key() string {
	buf := make([]byte, randomKeyBytes)
	if _, err := rand.Read(buf); err != nil {
		return base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%0*d", randomKeyBytes, 0))
	}

	return base64.StdEncoding.EncodeToString(buf)
}

// StartConnection starts a new installation tunnel against an existing
// Asset (the Outpost server being installed).
func (b *InMemoryBackend) StartConnection(req *startConnectionRequest) (*Connection, error) {
	if req.AssetId == "" {
		return nil, validationError("AssetId is required")
	}

	if req.ClientPublicKey == "" {
		return nil, validationError("ClientPublicKey is required")
	}

	if req.NetworkInterfaceDeviceIndex < 0 {
		return nil, validationError("NetworkInterfaceDeviceIndex must not be negative")
	}

	b.mu.Lock("StartConnection")
	defer b.mu.Unlock()

	if _, ok := b.findAssetLocked(req.AssetId); !ok {
		return nil, notFoundError(resourceAsset, req.AssetId)
	}

	id := newConnectionID()

	c := &Connection{
		ID:                  id,
		AssetID:             req.AssetId,
		ClientPublicKey:     req.ClientPublicKey,
		ServerPublicKey:     randomBase64Key(),
		ClientTunnelAddress: "169.254.0.1/30",
		ServerTunnelAddress: "169.254.0.2/30",
		ServerEndpoint:      fmt.Sprintf("%s.outposts.%s.amazonaws.com:443", id, b.region),
		UnderlayIPAddress:   "100.64.0.1",
		AllowedIps:          []string{"169.254.0.0/30"},
	}

	b.connections.Put(c)

	return c.clone(), nil
}

// clone returns a deep copy of c, so the returned Connection shares no
// mutable memory with the backend's stored copy -- AllowedIps is a slice.
func (c *Connection) clone() *Connection {
	cp := *c
	cp.AllowedIps = cloneStrs(c.AllowedIps)

	return &cp
}

// GetConnection returns a copy of the connection with the given ID.
func (b *InMemoryBackend) GetConnection(id string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	c, ok := b.connections.Get(id)
	if !ok {
		return nil, notFoundError(resourceConnection, id)
	}

	return c.clone(), nil
}
