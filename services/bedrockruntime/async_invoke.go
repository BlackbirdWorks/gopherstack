package bedrockruntime

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// StartAsyncInvoke creates a new asynchronous model invocation and returns it.
// If clientToken is non-empty and an invocation with that token already exists,
// the existing invocation is returned (idempotency).
// Returns an error if required parameters are missing.
func (b *InMemoryBackend) StartAsyncInvoke(
	modelID, s3URI, clientToken string,
	tags map[string]string,
) (*AsyncInvoke, error) {
	return b.StartAsyncInvokeFingerprinted(modelID, s3URI, clientToken, "", tags)
}

// StartAsyncInvokeFingerprinted is StartAsyncInvoke plus a request fingerprint:
// replaying a token with a different non-empty fingerprint is a ConflictException.
func (b *InMemoryBackend) StartAsyncInvokeFingerprinted(
	modelID, s3URI, clientToken, fingerprint string,
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

	if replay, replayed, err := b.replayAsyncInvoke(clientToken, fingerprint); replayed || err != nil {
		return replay, err
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
		Fingerprint:        fingerprint,
	}

	b.asyncInvokes.Put(inv)

	if clientToken != "" {
		b.tokenIndex[clientToken] = arnStr
	}

	cp := *inv
	cp.Tags = copyTags(inv.Tags)

	return &cp, nil
}

// replayAsyncInvoke returns the invocation recorded for clientToken, or ConflictException when the
// token was used with other parameters. Caller holds b.mu.
func (b *InMemoryBackend) replayAsyncInvoke(clientToken, fingerprint string) (*AsyncInvoke, bool, error) {
	existingArn, ok := b.tokenIndex[clientToken]
	if clientToken == "" || !ok {
		return nil, false, nil
	}

	existing, found := b.asyncInvokes.Get(existingArn)
	if !found {
		return nil, false, nil
	}

	if fingerprint != "" && existing.Fingerprint != "" && existing.Fingerprint != fingerprint {
		return nil, false, fmt.Errorf("%w: client request token was already used with different parameters",
			ErrConflict)
	}

	cp := *existing
	cp.Tags = copyTags(existing.Tags)

	return &cp, true, nil
}

// AdvanceAsyncInvokesForTest is a test helper that immediately advances all InProgress
// invocations whose age exceeds minAge. Pass 0 to advance all immediately.
func (b *InMemoryBackend) AdvanceAsyncInvokesForTest(minAge time.Duration) {
	b.mu.Lock("AdvanceAsyncInvokesForTest")
	defer b.mu.Unlock()

	now := time.Now()

	for _, inv := range b.asyncInvokes.All() {
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

	inv, ok := b.asyncInvokes.Get(invocationArn)
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

	all := b.asyncInvokes.All()
	out := make([]*AsyncInvoke, 0, len(all))

	for _, inv := range all {
		if !matchesAsyncInvokeFilter(inv, filter) {
			continue
		}

		cp := *inv
		cp.Tags = copyTags(inv.Tags)
		out = append(out, &cp)
	}

	descending := filter.SortOrder == asyncInvokeSortOrderDescending
	sort.Slice(out, func(i, j int) bool {
		if descending {
			return out[i].SubmitTime.After(out[j].SubmitTime)
		}

		return out[i].SubmitTime.Before(out[j].SubmitTime)
	})

	return out
}

// matchesAsyncInvokeFilter reports whether inv satisfies filter's statusEquals,
// submitTimeAfter and submitTimeBefore criteria (ListAsyncInvokesInput fields --
// bedrockruntime@v1.57.1 api_op_ListAsyncInvokes.go).
func matchesAsyncInvokeFilter(inv *AsyncInvoke, filter ListAsyncInvokesFilter) bool {
	if filter.StatusEquals != "" && inv.Status != filter.StatusEquals {
		return false
	}
	if filter.SubmitTimeAfter != nil && !inv.SubmitTime.After(*filter.SubmitTimeAfter) {
		return false
	}
	if filter.SubmitTimeBefore != nil && !inv.SubmitTime.Before(*filter.SubmitTimeBefore) {
		return false
	}

	return true
}
