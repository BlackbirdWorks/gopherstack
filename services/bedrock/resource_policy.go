package bedrock

// Code in this file backs TWO distinct real AWS operation families that
// happen to share a name -- PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy exist on BOTH the core bedrock client and the
// bedrock-agent client, with different wire shapes and different eligible
// resource types (verified via `go doc` against both
// aws-sdk-go-v2/service/bedrock and aws-sdk-go-v2/service/bedrockagent):
//
//   - Core bedrock (Handler): POST /resource-policy, GET/DELETE
//     /resource-policy/{resourceArn}. Request/response field is
//     "resourcePolicy" (not "policy"), there is no revisionId concurrency
//     control, and real AWS's docs describe the target only as "a Bedrock
//     resource" with no documented ARN-pattern allowlist. This backend
//     narrows that to resource ARNs that both look like a Bedrock ARN AND
//     resolve to one of the ARN-addressable resource families this package
//     already models (see resourcePolicyTargetExists) -- validating the ARN
//     rather than accepting any string, per this campaign's requirement,
//     while staying honest about which real AWS resource types are
//     documented as eligible (undocumented in this SDK version).
//   - bedrock-agent (AgentsHandler): PUT/GET/DELETE
//     /resourcepolicy/{resourceArn} (no hyphen, singular). Request/response
//     field is "policy", and both SDK doc comments AND the ARN pattern the
//     SDK validates client-side
//     (arn:aws(-[^:]+)?:bedrock:{region}:{account}:knowledge-base/{id})
//     agree this family is scoped to knowledge bases ONLY -- see
//     handler_agent_resource_policy.go.
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

// knowledgeBaseResourceArnPattern matches real AWS's documented ARN pattern
// for bedrock-agent's PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy: arn:aws(-[^:]+)?:bedrock:{region}:{account}:
// knowledge-base/{id}. This family is scoped to knowledge bases only (see
// this file's package doc comment). The ID character class is widened to
// "[0-9a-zA-Z-]+" (real AWS documents pure alphanumeric only) because this
// backend's own CreateKnowledgeBase generates hyphenated IDs like
// "kb-00000001" (see knowledge_bases.go) -- narrowing to the real character
// class would make every gopherstack-issued KB ARN unmatchable.
var knowledgeBaseResourceArnPattern = regexp.MustCompile(
	`^arn:aws[a-z0-9-]*:bedrock:[a-z0-9-]*:\d{12}:knowledge-base/([0-9a-zA-Z-]+)$`,
)

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
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
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
// for resourceArn, bumping the shared revision counter. Shared by both the
// core bedrock and bedrock-agent Put flavors. Caller must hold b.mu.Lock and
// have already validated resourceArn/policyDocument.
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

// knowledgeBaseIDFromResourceArn extracts the knowledge base ID from
// resourceArn if it matches the real bedrock-agent resource-policy ARN
// pattern, or "" if it does not.
func knowledgeBaseIDFromResourceArn(resourceArn string) string {
	m := knowledgeBaseResourceArnPattern.FindStringSubmatch(resourceArn)
	if m == nil {
		return ""
	}

	return m[1]
}

// PutKnowledgeBaseResourcePolicy creates or replaces the resource policy
// attached to a knowledge base ARN (bedrock-agent domain; see this file's
// package doc comment for why this flavor is scoped to knowledge bases
// only). When expectedRevisionID is non-empty it must match the stored
// policy's current RevisionID (or no policy may yet exist when
// expectedRevisionID is also empty) -- a mismatch fails with ErrAlreadyExists
// (-> ConflictException, matching real AWS's documented 409 for this op),
// modeling real AWS's optimistic-concurrency semantics.
func (b *InMemoryBackend) PutKnowledgeBaseResourcePolicy(
	resourceArn, policyDocument, expectedRevisionID string,
) (*ResourcePolicy, error) {
	b.mu.Lock("PutKnowledgeBaseResourcePolicy")
	defer b.mu.Unlock()

	if policyDocument == "" {
		return nil, fmt.Errorf("%w: policy is required", ErrValidation)
	}

	kbID := knowledgeBaseIDFromResourceArn(resourceArn)
	if kbID == "" {
		return nil, fmt.Errorf("%w: resourceArn must be a knowledge base ARN", ErrValidation)
	}
	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %s not found", ErrNotFound, kbID)
	}

	if err := b.checkResourcePolicyRevision(resourceArn, expectedRevisionID); err != nil {
		return nil, err
	}

	rp := b.putResourcePolicyRecord(resourceArn, policyDocument)

	cp := *rp

	return &cp, nil
}

// checkResourcePolicyRevision validates expectedRevisionID (when non-empty)
// against the resource policy currently stored for resourceArn. Caller must
// hold b.mu.Lock.
func (b *InMemoryBackend) checkResourcePolicyRevision(resourceArn, expectedRevisionID string) error {
	if expectedRevisionID == "" {
		return nil
	}

	existing, had := b.resourcePolicies.Get(resourceArn)
	if !had || existing.RevisionID != expectedRevisionID {
		return fmt.Errorf(
			"%w: expectedRevisionId %s does not match the current policy revision",
			ErrAlreadyExists, expectedRevisionID,
		)
	}

	return nil
}

// GetKnowledgeBaseResourcePolicy returns the resource policy attached to a
// knowledge base ARN (bedrock-agent domain).
func (b *InMemoryBackend) GetKnowledgeBaseResourcePolicy(resourceArn string) (*ResourcePolicy, error) {
	b.mu.RLock("GetKnowledgeBaseResourcePolicy")
	defer b.mu.RUnlock()

	rp, ok := b.resourcePolicies.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// DeleteKnowledgeBaseResourcePolicy removes the resource policy attached to
// a knowledge base ARN (bedrock-agent domain), returning the revision ID the
// deleted policy had. See PutKnowledgeBaseResourcePolicy's doc comment for
// expectedRevisionID semantics.
func (b *InMemoryBackend) DeleteKnowledgeBaseResourcePolicy(resourceArn, expectedRevisionID string) (string, error) {
	b.mu.Lock("DeleteKnowledgeBaseResourcePolicy")
	defer b.mu.Unlock()

	existing, ok := b.resourcePolicies.Get(resourceArn)
	if !ok {
		return "", fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceArn)
	}

	if expectedRevisionID != "" && existing.RevisionID != expectedRevisionID {
		return "", fmt.Errorf(
			"%w: expectedRevisionId %s does not match the current policy revision",
			ErrAlreadyExists, expectedRevisionID,
		)
	}

	revisionID := existing.RevisionID
	b.resourcePolicies.Delete(resourceArn)

	return revisionID, nil
}
