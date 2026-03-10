package bedrockruntime

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Invocation records a single model invocation.
type Invocation struct {
	CreatedAt time.Time
	ModelID   string
	Operation string
	Input     string
	Output    string
}

// InMemoryBackend stores Bedrock Runtime state in memory.
type InMemoryBackend struct {
	invocations []*Invocation
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		invocations: make([]*Invocation, 0),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("bedrockruntime"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RecordInvocation stores a completed invocation in memory.
func (b *InMemoryBackend) RecordInvocation(operation, modelID, input, output string) *Invocation {
	b.mu.Lock("RecordInvocation")
	defer b.mu.Unlock()

	inv := &Invocation{
		Operation: operation,
		ModelID:   modelID,
		Input:     input,
		Output:    output,
		CreatedAt: time.Now().UTC(),
	}
	b.invocations = append(b.invocations, inv)

	cp := *inv

	return &cp
}

// ListInvocations returns all recorded invocations.
func (b *InMemoryBackend) ListInvocations() []*Invocation {
	b.mu.RLock("ListInvocations")
	defer b.mu.RUnlock()

	out := make([]*Invocation, 0, len(b.invocations))

	for _, inv := range b.invocations {
		cp := *inv
		out = append(out, &cp)
	}

	return out
}
