package bedrockagent

// Code in this file backs bedrock-agent's PutResourcePolicy/
// GetResourcePolicy/DeleteResourcePolicy -- a DIFFERENT operation family
// from core Bedrock's own PutResourcePolicy/GetResourcePolicy/
// DeleteResourcePolicy despite the shared names: core bedrock uses
// POST/GET/DELETE /resource-policy (hyphenated) with request/response field
// "resourcePolicy" and no revision concurrency control (see
// services/bedrock/resource_policy.go), while bedrock-agent uses PUT/GET/
// DELETE /resourcepolicy/{resourceArn} (no hyphen, singular) with field
// "policy" and a "revisionId"/"expectedRevisionId" optimistic-concurrency
// pair.
//
// Verified directly against aws-sdk-go-v2/service/bedrockagent v1.58.0's
// api_op_{Put,Get,Delete}ResourcePolicy.go, serializers.go (the
// awsRestjson1_serializeOpHttpBindings*/awsRestjson1_serializeOpDocument*
// helpers), deserializers.go, and validators.go, plus AWS's published API
// reference (docs.aws.amazon.com/bedrock/latest/APIReference/API_agent_Put
// ResourcePolicy.html and .../API_agent_DeleteResourcePolicy.html):
//
//   - PUT/GET/DELETE /resourcepolicy/{resourceArn}.
//   - Put's request body: {"policy": "...", "expectedRevisionId": "..."}
//     (expectedRevisionId omitted when absent). Delete's expectedRevisionId
//     is a QUERY parameter, not a body field (Get has no body at all).
//   - Every response carries "resourceArn" and "revisionId"; Get/Put also
//     carry "policy".
//   - resourceArn is scoped to knowledge bases only: the SDK's own doc
//     comments say so ("Associates a resource policy with a knowledge
//     base"/"...for a knowledge base"), and AWS's API reference documents
//     the resourceArn URI-label pattern as exactly
//     `arn:aws(-[^:]+)?:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:knowledge-base/[0-9a-zA-Z]+`.
//     NOTE: validators.go in this SDK version does NOT itself regexp-check
//     that pattern client-side -- it only requires resourceArn/policy be
//     non-nil. The knowledge-base-only ARN shape below comes from the
//     published API reference, not a client-side smithy validator.
//   - expectedRevisionId is documented "Required: No" on both Put and
//     Delete: real AWS lets a caller omit it and proceed unconditionally
//     (it exists purely as an opt-in optimistic-concurrency guard, the same
//     contract as an HTTP If-Match/ETag pair), so PutResourcePolicy/
//     DeleteResourcePolicy below only enforce a match when the caller
//     actually supplies one -- a supplied-but-stale (or supplied against a
//     resource with no existing policy) value fails with the real
//     ConflictException/409 rather than silently overwriting.
import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// knowledgeBaseResourceArnPattern matches AWS's documented resourceArn
// pattern for this operation family:
// arn:aws(-[^:]+)?:bedrock:{region}:{account}:knowledge-base/{id}. The ID
// character class is widened to "[0-9a-zA-Z-]+" (AWS's docs specify pure
// alphanumeric, no hyphen) because this backend's own CreateKnowledgeBase
// mints hyphenated IDs like "kb-00000001" (see knowledge_bases.go) --
// narrowing to the documented class would make every gopherstack-issued KB
// ARN unmatchable by its own resource-policy API.
var knowledgeBaseResourceArnPattern = regexp.MustCompile(
	`^arn:aws[a-z0-9-]*:bedrock:[a-z0-9-]*:\d{12}:knowledge-base/([0-9a-zA-Z-]+)$`,
)

// knowledgeBaseIDFromResourceArn extracts the knowledge base ID from
// resourceArn if it matches knowledgeBaseResourceArnPattern, or "" if it does
// not.
func knowledgeBaseIDFromResourceArn(resourceArn string) string {
	m := knowledgeBaseResourceArnPattern.FindStringSubmatch(resourceArn)
	if m == nil {
		return ""
	}

	return m[1]
}

// PutResourcePolicy creates or replaces the resource policy attached to a
// knowledge base ARN. resourceArn must match the real, documented
// knowledge-base-only ARN pattern AND resolve to a knowledge base this
// backend actually has -- a policy must not attach to a nonexistent
// resource. See this file's package doc comment for expectedRevisionID
// semantics.
func (b *InMemoryBackend) PutResourcePolicy(
	_ context.Context, resourceArn, policy, expectedRevisionID string,
) (*ResourcePolicy, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if policy == "" {
		return nil, fmt.Errorf("%w: policy is required", ErrValidation)
	}

	kbID := knowledgeBaseIDFromResourceArn(resourceArn)
	if kbID == "" {
		return nil, fmt.Errorf("%w: resourceArn must be a knowledge base ARN", ErrValidation)
	}

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if err := b.checkResourcePolicyRevision(resourceArn, expectedRevisionID); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	createdAt := now

	if existing, had := b.resourcePolicies.Get(resourceArn); had {
		createdAt = existing.CreatedAt
	}

	b.resourcePolicyCounter++
	rp := &ResourcePolicy{
		ResourceArn: resourceArn,
		Policy:      policy,
		RevisionID:  strconv.Itoa(b.resourcePolicyCounter),
		CreatedAt:   createdAt,
		UpdatedAt:   now,
	}
	b.resourcePolicies.Put(rp)

	cp := *rp

	return &cp, nil
}

// checkResourcePolicyRevision validates expectedRevisionID (when non-empty)
// against the resource policy currently stored for resourceArn -- a mismatch
// (including when no policy yet exists) fails with ErrAlreadyExists, which
// handleErr maps to ConflictException/409, matching real AWS's documented
// error for this op family. Caller must hold b.mu.Lock.
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

// GetResourcePolicy returns the resource policy attached to a knowledge base
// ARN.
func (b *InMemoryBackend) GetResourcePolicy(_ context.Context, resourceArn string) (*ResourcePolicy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rp, ok := b.resourcePolicies.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// DeleteResourcePolicy removes the resource policy attached to a knowledge
// base ARN, returning the revision ID the deleted policy had -- AWS's
// documented DeleteResourcePolicyOutput.revisionId is "the revision
// identifier after the resource policy was deleted", which this backend
// interprets as the last revision the (now-removed) policy carried, matching
// how PutResourcePolicy's response documents "[u]se this value in the
// expectedRevisionId field of a subsequent ... DeleteResourcePolicy
// request" -- i.e. revisionId is a handle on a specific policy revision, not
// a live counter that keeps advancing after the row is gone. See this
// file's package doc comment for expectedRevisionID semantics.
func (b *InMemoryBackend) DeleteResourcePolicy(
	_ context.Context, resourceArn, expectedRevisionID string,
) (string, error) {
	b.mu.Lock()
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
