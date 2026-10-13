package managedblockchain

import (
	"time"

	"github.com/google/uuid"
)

const statusCreating = "CREATING"

// SetCreateDelay keeps new networks, members and nodes in CREATING for d before
// they report AVAILABLE (default 0: AVAILABLE immediately).
func (b *InMemoryBackend) SetCreateDelay(d time.Duration) {
	b.mu.Lock("SetCreateDelay")
	defer b.mu.Unlock()

	b.createDelay = d
}

func (b *InMemoryBackend) initialStatus() string {
	if b.createDelay > 0 {
		return statusCreating
	}

	return networkStatusAvailable
}

func (b *InMemoryBackend) settledStatus(status string, created *time.Time) string {
	if b.createDelay > 0 && status == statusCreating && created != nil &&
		time.Since(*created) >= b.createDelay {
		return networkStatusAvailable
	}

	return status
}

func (b *InMemoryBackend) viewNetwork(n *Network) *Network {
	cp := cloneNetwork(n)
	cp.Status = b.settledStatus(cp.Status, cp.CreationDate)

	return cp
}

func (b *InMemoryBackend) viewMember(m *Member) *Member {
	cp := cloneMember(m)
	cp.Status = b.settledStatus(cp.Status, cp.CreationDate)

	return cp
}

func (b *InMemoryBackend) viewNode(n *Node) *Node {
	cp := cloneNode(n)
	cp.Status = b.settledStatus(cp.Status, cp.CreationDate)

	return cp
}

func (b *InMemoryBackend) memberReadyLocked(m *Member) bool {
	return b.settledStatus(m.Status, m.CreationDate) != statusCreating
}

func (b *InMemoryBackend) nodeReadyLocked(n *Node) bool {
	return b.settledStatus(n.Status, n.CreationDate) != statusCreating
}

func (b *InMemoryBackend) networkReadyLocked(n *Network) bool {
	return b.settledStatus(n.Status, n.CreationDate) != statusCreating
}

const (
	idRandomLen = 26
	bitsPerByte = 8
	idAlphaBits = 5
	idAlphaMask = 31
)

const idAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

// newResourceID returns prefix plus 26 base32 characters, the shape of real
// ids such as n-MWY63ZJZU5HGNCMBQER7IN6OIU (ResourceIdString max 32).
func newResourceID(prefix string) string {
	raw := uuid.New()
	out := make([]byte, 0, len(prefix)+idRandomLen)
	out = append(out, prefix...)

	var acc uint64

	bits := 0

	for _, by := range raw {
		acc = acc<<bitsPerByte | uint64(by)
		bits += bitsPerByte

		for bits >= idAlphaBits && len(out) < len(prefix)+idRandomLen {
			bits -= idAlphaBits
			out = append(out, idAlphabet[(acc>>bits)&idAlphaMask])
		}
	}

	for len(out) < len(prefix)+idRandomLen {
		out = append(out, idAlphabet[0])
	}

	return string(out)
}
