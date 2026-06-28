package bedrockruntime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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

// invocationRing is a fixed-capacity circular buffer for Invocation records.
// Once full, new writes overwrite the oldest entry (FIFO eviction).
type invocationRing struct {
	buf       []*Invocation
	head      int // index of the oldest entry
	count     int // number of valid entries (0 ≤ count ≤ sz)
	evictions int // total evictions since last reset
}

// newInvocationRing allocates a ring with the given capacity.
func newInvocationRing(sz int) invocationRing {
	return invocationRing{buf: make([]*Invocation, sz)}
}

// push appends inv, evicting the oldest entry when the ring is full.
func (r *invocationRing) push(inv *Invocation) {
	sz := len(r.buf)
	if r.count < sz {
		r.buf[(r.head+r.count)%sz] = inv
		r.count++
	} else {
		// Overwrite oldest slot and advance head — oldest entry is evicted.
		r.evictions++
		r.buf[r.head] = inv
		r.head = (r.head + 1) % sz
	}
}

// snapshot returns all entries in insertion order (oldest first).
func (r *invocationRing) snapshot() []*Invocation {
	out := make([]*Invocation, r.count)
	sz := len(r.buf)

	for i := range r.count {
		out[i] = r.buf[(r.head+i)%sz]
	}

	return out
}

// reset discards all entries without reallocating the buffer.
func (r *invocationRing) reset() {
	r.head = 0
	r.count = 0
}

// InMemoryBackend stores Bedrock Runtime state in memory.
type InMemoryBackend struct {
	svcCtx             context.Context
	mu                 *lockmetrics.RWMutex
	asyncInvokes       map[string]*AsyncInvoke
	tokenIndex         map[string]string
	accountID          string
	region             string
	invocations        invocationRing
	asyncInvokeCounter int
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		invocations:  newInvocationRing(maxInvocationHistory),
		asyncInvokes: make(map[string]*AsyncInvoke),
		tokenIndex:   make(map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("bedrockruntime"),
		svcCtx:       context.Background(),
	}
}

// Reset clears all backend state, returning the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.invocations.reset()
	b.asyncInvokes = make(map[string]*AsyncInvoke)
	b.tokenIndex = make(map[string]string)
	b.asyncInvokeCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RecordInvocation stores a completed invocation in memory.
// When the ring is full, the oldest entry is evicted and a warning is logged via the background context.
func (b *InMemoryBackend) RecordInvocation(operation, modelID, input, output string) *Invocation {
	b.mu.Lock("RecordInvocation")
	defer b.mu.Unlock()

	prevEvictions := b.invocations.evictions
	inv := &Invocation{
		Operation: operation,
		ModelID:   modelID,
		Input:     input,
		Output:    output,
		CreatedAt: time.Now().UTC(),
	}
	b.invocations.push(inv)

	if b.invocations.evictions > prevEvictions {
		logger.Load(b.svcCtx).Warn(
			"bedrockruntime: invocationRing full, oldest entry evicted",
			"capacity", len(b.invocations.buf),
		)
	}

	cp := *inv

	return &cp
}

// ListInvocations returns all recorded invocations in insertion order (oldest first).
func (b *InMemoryBackend) ListInvocations() []*Invocation {
	b.mu.RLock("ListInvocations")
	defer b.mu.RUnlock()

	raw := b.invocations.snapshot()
	out := make([]*Invocation, len(raw))

	for i, inv := range raw {
		cp := *inv
		out[i] = &cp
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

	keep := b.invocations.snapshot()
	b.invocations.reset()

	for _, inv := range keep {
		if ctx.Err() != nil {
			return
		}
		if !inv.CreatedAt.Before(cutoff) {
			b.invocations.push(inv)
		}
	}
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

	if !strings.HasPrefix(s3URI, "s3://") {
		return nil, fmt.Errorf("%w: s3Uri must start with s3://", ErrValidation)
	}

	// Ensure there is a non-empty bucket name after "s3://".
	rest := strings.TrimPrefix(s3URI, "s3://")
	if rest == "" || strings.HasPrefix(rest, "/") {
		return nil, fmt.Errorf("%w: s3Uri must include a bucket name", ErrValidation)
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
	arnStr := arn.Build("bedrock", b.region, b.accountID, fmt.Sprintf("async-invoke/%s", id))
	modelArn := arn.Build("bedrock", b.region, "", fmt.Sprintf("foundation-model/%s", modelID))
	now := time.Now().UTC()

	var token *string
	if clientToken != "" {
		t := clientToken
		token = &t
	}

	inv := &AsyncInvoke{
		InvocationArn:      arnStr,
		ModelArn:           modelArn,
		OutputS3URI:        s3URI,
		Status:             AsyncInvokeStatusInProgress,
		SubmitTime:         now,
		LastModifiedTime:   now,
		ClientRequestToken: token,
		Tags:               copyTags(tags),
	}

	b.asyncInvokes[arnStr] = inv

	if clientToken != "" {
		b.tokenIndex[clientToken] = arnStr
	}

	cp := *inv
	cp.Tags = copyTags(inv.Tags)

	return &cp, nil
}

// AdvanceAsyncInvokesForTest is a test helper that immediately advances all InProgress
// invocations whose age exceeds minAge. Pass 0 to advance all immediately.
func (b *InMemoryBackend) AdvanceAsyncInvokesForTest(minAge time.Duration) {
	b.mu.Lock("AdvanceAsyncInvokesForTest")
	defer b.mu.Unlock()

	now := time.Now()

	for _, inv := range b.asyncInvokes {
		if inv.Status != AsyncInvokeStatusInProgress {
			continue
		}

		if now.Sub(inv.SubmitTime) >= minAge {
			endTime := now.UTC()
			inv.Status = AsyncInvokeStatusCompleted
			inv.EndTime = &endTime
			inv.LastModifiedTime = now.UTC()
		}
	}
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
