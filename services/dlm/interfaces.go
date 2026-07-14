package dlm

import (
	"context"
	"time"
)

// StorageBackend is the interface for DLM storage operations.
type StorageBackend interface {
	CreateLifecyclePolicy(
		description, executionRoleARN, state string, tags map[string]string, policyDetails map[string]any,
	) (*Policy, error)
	DeleteLifecyclePolicy(policyID string) error
	GetLifecyclePolicies(filter PolicyFilter) ([]*PolicySummary, error)
	GetLifecyclePolicy(policyID string) (*Policy, error)
	UpdateLifecyclePolicy(policyID, description, executionRoleARN, state string, policyDetails map[string]any) error

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Policy holds full lifecycle policy details.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type Policy struct {
	DateCreated      time.Time
	DateModified     time.Time
	Tags             map[string]string
	PolicyDetails    map[string]any
	Description      string
	ExecutionRoleARN string
	PolicyArn        string
	PolicyID         string
	State            string
}

// PolicySummary holds summary lifecycle policy info.
type PolicySummary struct {
	Tags        map[string]string
	PolicyID    string
	Description string
	State       string
	PolicyType  string
}

// PolicyFilter narrows the results of GetLifecyclePolicies. A zero-value
// field (nil slice or empty string) imposes no restriction along that
// dimension. Matching AWS's documented semantics for the
// GetLifecyclePolicies query parameters, PolicyIDs/ResourceTypes/TargetTags/
// TagsToAdd each apply an ANY-of match within the list, and the dimensions
// are ANDed together.
type PolicyFilter struct {
	PolicyIDs []string
	State     string
	// ResourceTypes matches against PolicyDetails.ResourceTypes.
	ResourceTypes []string
	// TargetTags holds "key=value" pairs matched against
	// PolicyDetails.TargetTags.
	TargetTags []string
	// TagsToAdd holds "key=value" pairs matched against the TagsToAdd of any
	// of PolicyDetails.Schedules.
	TagsToAdd []string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
