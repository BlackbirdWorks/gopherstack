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
	GetLifecyclePolicies(policyIDs []string, state string) ([]*PolicySummary, error)
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
}

var _ StorageBackend = (*InMemoryBackend)(nil)
