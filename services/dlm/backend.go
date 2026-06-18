package dlm

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// backendSnapshot holds serializable backend state.
type backendSnapshot struct {
	Policies map[string]*storedPolicy     `json:"policies"`
	Tags     map[string]map[string]string `json:"tags"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	policies  map[string]*storedPolicy     // policyID → policy
	tags      map[string]map[string]string // resourceARN → tags
	accountID string
	region    string
	counter   int
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:        lockmetrics.New("dlm"),
		policies:  make(map[string]*storedPolicy),
		tags:      make(map[string]map[string]string),
		accountID: accountID,
		region:    region,
	}
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
	policyARN := fmt.Sprintf("arn:aws:dlm:%s:%s:policy/%s", b.region, b.accountID, policyID)

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
	b.policies[policyID] = p
	b.tags[policyARN] = storedTags

	return p.toPolicy(), nil
}

// DeleteLifecyclePolicy removes a lifecycle policy.
func (b *InMemoryBackend) DeleteLifecyclePolicy(policyID string) error {
	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	p, ok := b.policies[policyID]
	if !ok {
		return ErrPolicyNotFound
	}

	delete(b.tags, p.PolicyArn)
	delete(b.policies, policyID)

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

	for _, p := range b.policies {
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

	return result, nil
}

// GetLifecyclePolicy returns full details for a lifecycle policy.
func (b *InMemoryBackend) GetLifecyclePolicy(policyID string) (*Policy, error) {
	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies[policyID]
	if !ok {
		return nil, ErrPolicyNotFound
	}

	return p.toPolicy(), nil
}

// UpdateLifecyclePolicy updates mutable fields of an existing policy.
func (b *InMemoryBackend) UpdateLifecyclePolicy(policyID, description, executionRoleARN, state string) error {
	b.mu.Lock("UpdateLifecyclePolicy")
	defer b.mu.Unlock()

	p, ok := b.policies[policyID]
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

	p.DateModified = time.Now().UTC()

	return nil
}

// TagResource applies tags to a DLM resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrPolicyNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a DLM resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceARN) {
		return ErrPolicyNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a DLM resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownResource(resourceARN) {
		return nil, ErrPolicyNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

// isKnownResource returns true if the ARN corresponds to a known policy.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownResource(arn string) bool {
	for _, p := range b.policies {
		if p.PolicyArn == arn {
			return true
		}
	}

	return false
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.policies = make(map[string]*storedPolicy)
	b.tags = make(map[string]map[string]string)
	b.counter = 0
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(backendSnapshot{
		Policies: b.policies,
		Tags:     b.tags,
	})

	return data
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	if snap.Policies != nil {
		b.policies = snap.Policies
	} else {
		b.policies = make(map[string]*storedPolicy)
	}

	if snap.Tags != nil {
		b.tags = snap.Tags
	} else {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}
