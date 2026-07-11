package dlm

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	errResourceNotFound = "ResourceNotFoundException"
	errInvalidRequest   = "InvalidRequestException"
	policyIDPrefix      = "policy-"
	stateEnabled        = "ENABLED"
	stateDisabled       = "DISABLED"
)

var (
	// ErrPolicyNotFound is returned when a lifecycle policy does not exist.
	ErrPolicyNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrInvalidRequest is returned on invalid input.
	ErrInvalidRequest = awserr.New(errInvalidRequest, awserr.ErrInvalidParameter)
)

// storedPolicy holds a lifecycle policy with all persisted fields.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type storedPolicy struct {
	DateCreated      time.Time         `json:"dateCreated"`
	DateModified     time.Time         `json:"dateModified"`
	Tags             map[string]string `json:"tags"`
	PolicyDetails    map[string]any    `json:"policyDetails,omitempty"`
	Description      string            `json:"description"`
	ExecutionRoleARN string            `json:"executionRoleArn"`
	PolicyArn        string            `json:"policyArn"`
	PolicyID         string            `json:"policyId"`
	State            string            `json:"state"`
}

func (p *storedPolicy) toPolicy() *Policy {
	tags := make(map[string]string)
	maps.Copy(tags, p.Tags)

	return &Policy{
		DateCreated:      p.DateCreated,
		DateModified:     p.DateModified,
		Description:      p.Description,
		ExecutionRoleARN: p.ExecutionRoleARN,
		PolicyArn:        p.PolicyArn,
		PolicyID:         p.PolicyID,
		State:            p.State,
		Tags:             tags,
		PolicyDetails:    p.PolicyDetails,
	}
}

func (p *storedPolicy) toSummary() *PolicySummary {
	tags := make(map[string]string)
	maps.Copy(tags, p.Tags)

	return &PolicySummary{
		PolicyID:    p.PolicyID,
		Description: p.Description,
		State:       p.State,
		Tags:        tags,
	}
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	registry  *store.Registry
	policies  *store.Table[storedPolicy] // policyID → policy
	accountID string
	region    string
	counter   int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("dlm"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
	}

	registerAllTables(b)

	return b
}

// CreateLifecyclePolicy creates a new lifecycle policy and returns it.
func (b *InMemoryBackend) CreateLifecyclePolicy(
	description, executionRoleARN, state string,
	tags map[string]string,
	policyDetails map[string]any,
) (*Policy, error) {
	b.mu.Lock("CreateLifecyclePolicy")
	defer b.mu.Unlock()

	b.counter++
	policyID := fmt.Sprintf("%s%016x", policyIDPrefix, b.counter)
	policyARN := arn.Build("dlm", b.region, b.accountID, fmt.Sprintf("policy/%s", policyID))

	now := time.Now().UTC()
	resolvedState := state
	if resolvedState == "" {
		resolvedState = stateEnabled
	}

	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	p := &storedPolicy{
		DateCreated:      now,
		DateModified:     now,
		Description:      description,
		ExecutionRoleARN: executionRoleARN,
		PolicyArn:        policyARN,
		PolicyID:         policyID,
		State:            resolvedState,
		Tags:             storedTags,
		PolicyDetails:    policyDetails,
	}
	b.policies.Put(p)

	return p.toPolicy(), nil
}

// DeleteLifecyclePolicy removes a lifecycle policy.
func (b *InMemoryBackend) DeleteLifecyclePolicy(policyID string) error {
	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	if !b.policies.Has(policyID) {
		return ErrPolicyNotFound
	}

	b.policies.Delete(policyID)

	return nil
}

// GetLifecyclePolicies returns summary info for lifecycle policies, optionally
// filtered by policyIDs and/or state.
func (b *InMemoryBackend) GetLifecyclePolicies(policyIDs []string, state string) ([]*PolicySummary, error) {
	b.mu.RLock("GetLifecyclePolicies")
	defer b.mu.RUnlock()

	idFilter := make(map[string]struct{}, len(policyIDs))
	for _, id := range policyIDs {
		idFilter[id] = struct{}{}
	}

	var result []*PolicySummary

	for _, p := range b.policies.All() {
		if len(idFilter) > 0 {
			if _, ok := idFilter[p.PolicyID]; !ok {
				continue
			}
		}

		if state != "" && !strings.EqualFold(p.State, state) {
			continue
		}

		result = append(result, p.toSummary())
	}

	sort.Slice(result, func(i, j int) bool { return result[i].PolicyID < result[j].PolicyID })

	return result, nil
}

// GetLifecyclePolicy returns full details for a lifecycle policy.
func (b *InMemoryBackend) GetLifecyclePolicy(policyID string) (*Policy, error) {
	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies.Get(policyID)
	if !ok {
		return nil, ErrPolicyNotFound
	}

	return p.toPolicy(), nil
}

// UpdateLifecyclePolicy updates mutable fields of an existing policy.
func (b *InMemoryBackend) UpdateLifecyclePolicy(
	policyID, description, executionRoleARN, state string,
	policyDetails map[string]any,
) error {
	b.mu.Lock("UpdateLifecyclePolicy")
	defer b.mu.Unlock()

	p, ok := b.policies.Get(policyID)
	if !ok {
		return ErrPolicyNotFound
	}

	if description != "" {
		p.Description = description
	}

	if executionRoleARN != "" {
		p.ExecutionRoleARN = executionRoleARN
	}

	if state != "" {
		p.State = state
	}

	if policyDetails != nil {
		p.PolicyDetails = policyDetails
	}

	p.DateModified = time.Now().UTC()

	return nil
}

// TagResource applies tags to a DLM resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return ErrPolicyNotFound
	}

	maps.Copy(p.Tags, tags)

	return nil
}

// UntagResource removes tags from a DLM resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return ErrPolicyNotFound
	}

	for _, k := range tagKeys {
		delete(p.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a DLM resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return nil, ErrPolicyNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, p.Tags)

	return result, nil
}

// findPolicyByARNLocked returns the policy matching the given ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findPolicyByARNLocked(policyARN string) (*storedPolicy, bool) {
	var found *storedPolicy

	b.policies.Range(func(p *storedPolicy) bool {
		if p.PolicyArn == policyARN {
			found = p

			return false
		}

		return true
	})

	return found, found != nil
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.counter = 0
}
