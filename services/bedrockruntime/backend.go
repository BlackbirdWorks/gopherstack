package bedrockruntime

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// maxInvocationHistory is the maximum number of invocations retained in memory.
const maxInvocationHistory = 1000

// MaxInvocationHistory is the exported value for testing.
const MaxInvocationHistory = maxInvocationHistory

// Async invoke status values.
const (
	AsyncInvokeStatusInProgress = "InProgress"
	AsyncInvokeStatusCompleted  = "Completed"
	AsyncInvokeStatusFailed     = "Failed"
)

// Invocation records a single model invocation.
type Invocation struct {
	CreatedAt time.Time
	ModelID   string
	Operation string
	Input     string
	Output    string
}

// AsyncInvoke records an asynchronous model invocation.
type AsyncInvoke struct {
	SubmitTime         time.Time
	LastModifiedTime   time.Time
	EndTime            *time.Time
	FailureMessage     *string
	ClientRequestToken *string
	InvocationArn      string
	ModelArn           string
	OutputS3URI        string
	Status             string
}

// InMemoryBackend stores Bedrock Runtime state in memory.
type InMemoryBackend struct {
	mu                 *lockmetrics.RWMutex
	asyncInvokes       map[string]*AsyncInvoke
	accountID          string
	region             string
	invocations        []*Invocation
	asyncInvokeCounter int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		invocations:  make([]*Invocation, 0),
		asyncInvokes: make(map[string]*AsyncInvoke),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("bedrockruntime"),
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

// Purge removes all model invocations recorded before the cutoff time.
func (b *InMemoryBackend) Purge(cutoff time.Time) {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	n := 0
	for _, inv := range b.invocations {
		if !inv.CreatedAt.Before(cutoff) {
			b.invocations[n] = inv
			n++
		}
	}
	b.invocations = b.invocations[:n]
}

// StartAsyncInvoke creates a new asynchronous model invocation and returns it.
func (b *InMemoryBackend) StartAsyncInvoke(modelID, s3URI, clientToken string) *AsyncInvoke {
	b.mu.Lock("StartAsyncInvoke")
	defer b.mu.Unlock()

	b.asyncInvokeCounter++
	id := strconv.Itoa(b.asyncInvokeCounter)
	arn := fmt.Sprintf("arn:aws:bedrock:%s:%s:async-invoke/%s", b.region, b.accountID, id)
	modelArn := fmt.Sprintf("arn:aws:bedrock:%s::foundation-model/%s", b.region, modelID)
	now := time.Now().UTC()

	var token *string
	if clientToken != "" {
		t := clientToken
		token = &t
	}

	inv := &AsyncInvoke{
		InvocationArn:      arn,
		ModelArn:           modelArn,
		OutputS3URI:        s3URI,
		Status:             AsyncInvokeStatusInProgress,
		SubmitTime:         now,
		LastModifiedTime:   now,
		ClientRequestToken: token,
	}

	b.asyncInvokes[arn] = inv

	cp := *inv

	return &cp
}

// GetAsyncInvoke returns the async invocation with the given ARN, or false if not found.
func (b *InMemoryBackend) GetAsyncInvoke(invocationArn string) (*AsyncInvoke, bool) {
	b.mu.RLock("GetAsyncInvoke")
	defer b.mu.RUnlock()

	inv, ok := b.asyncInvokes[invocationArn]
	if !ok {
		return nil, false
	}

	cp := *inv

	return &cp, true
}

// ListAsyncInvokes returns all async invocations sorted by submit time.
func (b *InMemoryBackend) ListAsyncInvokes() []*AsyncInvoke {
	b.mu.RLock("ListAsyncInvokes")
	defer b.mu.RUnlock()

	out := make([]*AsyncInvoke, 0, len(b.asyncInvokes))

	for _, inv := range b.asyncInvokes {
		cp := *inv
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SubmitTime.Before(out[j].SubmitTime)
	})

	return out
}
