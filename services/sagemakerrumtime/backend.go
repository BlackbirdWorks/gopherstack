package sagemakerrumtime

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// maxInvocationHistory is the maximum number of invocations retained in memory.
const maxInvocationHistory = 1000

// MaxInvocationHistory is the exported value for testing.
const MaxInvocationHistory = maxInvocationHistory

// Invocation records a single SageMaker Runtime endpoint invocation.
type Invocation struct {
	CreatedAt    time.Time `json:"createdAt"`
	EndpointName string    `json:"endpointName"`
	Operation    string    `json:"operation"`
	Input        string    `json:"input"`
	Output       string    `json:"output"`
}

// InMemoryBackend stores SageMaker Runtime state in memory.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
	invocations []*Invocation
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		invocations: make([]*Invocation, 0),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("sagemakerrumtime"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RecordInvocation stores a completed invocation in memory.
func (b *InMemoryBackend) RecordInvocation(operation, endpointName, input, output string) *Invocation {
	b.mu.Lock("RecordInvocation")
	defer b.mu.Unlock()

	inv := &Invocation{
		Operation:    operation,
		EndpointName: endpointName,
		Input:        input,
		Output:       output,
		CreatedAt:    time.Now().UTC(),
	}
	b.invocations = append(b.invocations, inv)

	if len(b.invocations) > maxInvocationHistory {
		b.invocations = b.invocations[len(b.invocations)-maxInvocationHistory:]
	}

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
