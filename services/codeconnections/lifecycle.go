package codeconnections

import "time"

const statusAvailable = "AVAILABLE"

// SetConnectionHandshakeDelay keeps new connections PENDING for d, the way a real
// connection waits for the provider handshake (default 0: immediately AVAILABLE).
func (b *InMemoryBackend) SetConnectionHandshakeDelay(d time.Duration) {
	b.mu.Lock("SetConnectionHandshakeDelay")
	defer b.mu.Unlock()

	b.connectionHandshakeDelay = d
}

// SetHostSetupDelay keeps new hosts PENDING for d (default 0: immediately AVAILABLE).
func (b *InMemoryBackend) SetHostSetupDelay(d time.Duration) {
	b.mu.Lock("SetHostSetupDelay")
	defer b.mu.Unlock()

	b.hostSetupDelay = d
}

func (b *InMemoryBackend) connectionStatus(c *Connection) string {
	if c.Status == statusAvailable && b.connectionHandshakeDelay > 0 &&
		time.Since(c.CreatedAt) < b.connectionHandshakeDelay {
		return "PENDING"
	}

	return c.Status
}

func (b *InMemoryBackend) hostStatus(h *Host) string {
	if h.Status == statusAvailable && b.hostSetupDelay > 0 && time.Since(h.CreatedAt) < b.hostSetupDelay {
		return "PENDING"
	}

	return h.Status
}
