package bedrockruntime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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

// Sentinel errors for the bedrockruntime backend.
var (
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = errors.New("ValidationException")
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
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
	Tags               map[string]string
	InvocationArn      string
	ModelArn           string
	OutputS3URI        string
	Status             string
}

// ListAsyncInvokesFilter holds optional filter criteria for listing async invocations.
type ListAsyncInvokesFilter struct {
	// StatusEquals filters to invocations with the given status; empty means no filter.
	StatusEquals string
}

// InMemoryBackend stores Bedrock Runtime state in memory.
type InMemoryBackend struct {
	mu                 *lockmetrics.RWMutex
	asyncInvokes       map[string]*AsyncInvoke
	tokenIndex         map[string]string // clientRequestToken → invocationArn (idempotency)
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
		tokenIndex:   make(map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("bedrockruntime"),
	}
}

// Reset clears all backend state, returning the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.invocations = make([]*Invocation, 0)
	b.asyncInvokes = make(map[string]*AsyncInvoke)
	b.tokenIndex = make(map[string]string)
	b.asyncInvokeCounter = 0
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
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	n := 0
	for _, inv := range b.invocations {
		if ctx.Err() != nil {
			return
		}
		if !inv.CreatedAt.Before(cutoff) {
			b.invocations[n] = inv
			n++
		}
	}
	b.invocations = b.invocations[:n]
}

// StartAsyncInvoke creates a new asynchronous model invocation and returns it.
// If clientToken is non-empty and an invocation with that token already exists,
// the existing invocation is returned (idempotency).
// Returns an error if required parameters are missing.
func (b *InMemoryBackend) StartAsyncInvoke(
	modelID, s3URI, clientToken string,
	tags map[string]string,
) (*AsyncInvoke, error) {
	if modelID == "" {
		return nil, fmt.Errorf("%w: modelId is required", ErrValidation)
	}

	if s3URI == "" {
		return nil, fmt.Errorf("%w: s3Uri is required", ErrValidation)
	}

	b.mu.Lock("StartAsyncInvoke")
	defer b.mu.Unlock()

	// Idempotency: if clientToken is set and already seen, return existing invocation.
	if clientToken != "" {
		if existingArn, ok := b.tokenIndex[clientToken]; ok {
			if existing, found := b.asyncInvokes[existingArn]; found {
				cp := *existing
				cp.Tags = copyTags(existing.Tags)

				return &cp, nil
			}
		}
	}

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
		Tags:               copyTags(tags),
	}

	b.asyncInvokes[arn] = inv

	if clientToken != "" {
		b.tokenIndex[clientToken] = arn
	}

	cp := *inv
	cp.Tags = copyTags(inv.Tags)

	return &cp, nil
}

// GetAsyncInvoke returns the async invocation with the given ARN.
// Returns ErrNotFound if the invocation does not exist.
func (b *InMemoryBackend) GetAsyncInvoke(invocationArn string) (*AsyncInvoke, error) {
	b.mu.RLock("GetAsyncInvoke")
	defer b.mu.RUnlock()

	inv, ok := b.asyncInvokes[invocationArn]
	if !ok {
		return nil, fmt.Errorf("%w: async-invoke %q", ErrNotFound, invocationArn)
	}

	cp := *inv
	cp.Tags = copyTags(inv.Tags)

	return &cp, nil
}

// ListAsyncInvokes returns async invocations sorted by submit time (oldest first).
// An optional filter may restrict results by status.
func (b *InMemoryBackend) ListAsyncInvokes(filter ListAsyncInvokesFilter) []*AsyncInvoke {
	b.mu.RLock("ListAsyncInvokes")
	defer b.mu.RUnlock()

	out := make([]*AsyncInvoke, 0, len(b.asyncInvokes))

	for _, inv := range b.asyncInvokes {
		if filter.StatusEquals != "" && inv.Status != filter.StatusEquals {
			continue
		}

		cp := *inv
		cp.Tags = copyTags(inv.Tags)
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].SubmitTime.Before(out[j].SubmitTime)
	})

	return out
}

// copyTags returns a shallow copy of the given tag map; nil-safe.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	cp := make(map[string]string, len(tags))

	maps.Copy(cp, tags)

	return cp
}
