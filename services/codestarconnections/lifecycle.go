package codestarconnections

import (
	"time"
)

// SetConnectionHandshakeDelay keeps new connections PENDING for d, the way a real
// connection waits for the provider handshake (default 0: immediately AVAILABLE).
func (b *InMemoryBackend) SetConnectionHandshakeDelay(d time.Duration) {
	b.mu.Lock("SetConnectionHandshakeDelay")
	defer b.mu.Unlock()

	b.connectionHandshakeDelay = d
}

// SetHostSetupDelay makes hosts move from PENDING to AVAILABLE d after creation
// (default 0: hosts stay PENDING until the setup that only the console can complete).
func (b *InMemoryBackend) SetHostSetupDelay(d time.Duration) {
	b.mu.Lock("SetHostSetupDelay")
	defer b.mu.Unlock()

	b.hostSetupDelay = d
}

// stampCreated records a creation time for status progression. Caller holds the write lock.
func (b *InMemoryBackend) stampCreated(arn string) {
	if b.createdAt == nil {
		b.createdAt = make(map[string]time.Time)
	}

	b.createdAt[arn] = time.Now()
}

func (b *InMemoryBackend) connectionStatus(c *Connection) string {
	at, ok := b.createdAt[c.ConnectionArn]
	if ok && c.ConnectionStatus == ConnectionStatusAvailable &&
		b.connectionHandshakeDelay > 0 && time.Since(at) < b.connectionHandshakeDelay {
		return ConnectionStatusPending
	}

	return c.ConnectionStatus
}

func (b *InMemoryBackend) hostStatus(h *Host) string {
	if h.Status != HostStatusPending || b.hostSetupDelay <= 0 {
		return h.Status
	}

	if at, ok := b.createdAt[h.HostArn]; ok && time.Since(at) < b.hostSetupDelay {
		return h.Status
	}

	return HostStatusAvailable
}
