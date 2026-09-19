package bedrock

// Code in this file backs core bedrock's PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy: POST /resource-policy, GET/DELETE
// /resource-policy/{resourceArn}. Request/response field is "resourcePolicy"
// (not "policy"), there is no revisionId concurrency control, and real AWS's
// docs describe the target only as "a Bedrock resource" with no documented
// ARN-pattern allowlist. This backend narrows that to resource ARNs that both
// look like a Bedrock ARN AND resolve to one of the ARN-addressable resource
// families this package already models (see resourcePolicyTargetExists) --
// validating the ARN rather than accepting any string, while staying honest
// about which real AWS resource types are documented as eligible
// (undocumented in this SDK version).
import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// bedrockResourceArnPattern matches a syntactically well-formed Bedrock ARN
// (arn:{partition}:bedrock:{region}:{account}:{resource-type}/{id}). Real
// AWS's core PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy accept
// "the ARN of the Bedrock resource" generically, with no documented
// resource-type pattern beyond a 1-255 character length bound -- this
// backend still rejects anything that isn't shaped like a Bedrock ARN at
// all, rather than accepting any string.
var bedrockResourceArnPattern = regexp.MustCompile(`^arn:aws[a-z0-9-]*:bedrock:[a-z0-9-]*:\d{12}:.+$`)

// resourcePolicyTargetExists reports whether resourceArn refers to a
// resource actually present in this backend. Resource-based policies in the
// core bedrock domain may attach to any of the ARN-addressable resource
// families this package already models: guardrails, custom models, custom
// model deployments, provisioned model throughput, automated reasoning
// policies, prompt routers, inference profiles, and marketplace model
// endpoints. Caller must hold at least a read lock.
func (b *InMemoryBackend) resourcePolicyTargetExists(resourceArn string) bool {
	if _, ok := b.guardrailsByARN[resourceArn]; ok {
		return true
	}

	return b.customModels.Has(resourceArn) ||
		b.customModelDeployments.Has(resourceArn) ||
		b.provisionedModelThroughputs.Has(resourceArn) ||
		b.automatedReasoningPolicies.Has(resourceArn) ||
		b.promptRouters.Has(resourceArn) ||
		b.inferenceProfiles.Has(resourceArn) ||
		b.marketplaceEndpoints.Has(resourceArn)
}

// PutResourcePolicy creates or replaces the resource policy attached to
// resourceArn (core bedrock domain).
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policyDocument string) (*ResourcePolicy, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if policyDocument == "" {
		return nil, fmt.Errorf("%w: resourcePolicy is required", ErrValidation)
	}
	if !bedrockResourceArnPattern.MatchString(resourceArn) {
		return nil, fmt.Errorf("%w: resourceArn is not a valid Bedrock resource ARN", ErrValidation)
	}
	if !b.resourcePolicyTargetExists(resourceArn) {
		// Core bedrock's PutResourcePolicy declares no ResourceNotFoundException
		// (bedrock@v1.66.4 deserializers.go) -- ErrValidation is the closest
		// type it does declare. ConflictException is also declared here but
		// describes a conflicting operation, not a missing target.
		return nil, fmt.Errorf("%w: resource %s not found", ErrValidation, resourceArn)
	}

	rp := b.putResourcePolicyRecord(resourceArn, policyDocument)

	cp := *rp

	return &cp, nil
}

// GetResourcePolicy returns the resource policy attached to resourceArn
// (core bedrock domain).
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (*ResourcePolicy, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	rp, ok := b.resourcePolicies.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// DeleteResourcePolicy removes the resource policy attached to resourceArn
// (core bedrock domain).
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if !b.resourcePolicies.Has(resourceArn) {
		return fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceArn)
	}

	b.resourcePolicies.Delete(resourceArn)

	return nil
}

// putResourcePolicyRecord creates or replaces the stored ResourcePolicy row
// for resourceArn, bumping the shared revision counter. Caller must hold
// b.mu.Lock and have already validated resourceArn/policyDocument.
func (b *InMemoryBackend) putResourcePolicyRecord(resourceArn, policyDocument string) *ResourcePolicy {
	now := time.Now().UTC()

	createdAt := now
	if existing, had := b.resourcePolicies.Get(resourceArn); had {
		createdAt = existing.CreatedAt
	}

	b.resourcePolicyRevisionCounter++
	rp := &ResourcePolicy{
		ResourceArn:    resourceArn,
		PolicyDocument: policyDocument,
		RevisionID:     strconv.Itoa(b.resourcePolicyRevisionCounter),
		CreatedAt:      createdAt,
		UpdatedAt:      now,
	}
	b.resourcePolicies.Put(rp)

	return rp
}
