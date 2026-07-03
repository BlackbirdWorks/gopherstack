package s3

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// Data-plane access control. Bucket policies and ACLs are stored by the
// Put*Policy / Put*Acl handlers, but historically nothing consulted them on the
// object data plane, so a Deny policy or a private ACL had no effect on real GET
// / PUT / DELETE / List requests. This file evaluates the stored policy + ACL
// (subject to the bucket's Public Access Block) and returns AccessDenied when a
// request is not permitted.
//
// Identity model: this emulator is single-account, so a request is either the
// bucket *owner* (any SigV4/SigV2-signed or presigned request) or *anonymous*
// (no Authorization header and not presigned). An explicit Deny in the bucket
// policy applies to everyone including the owner (matching real S3, where an
// explicit bucket-policy Deny overrides even the account root), so Deny is
// always enforced. Anonymous / public-ACL gating is only enforced when the
// handler runs in authorization-enforcement mode (a PresignSecret is set), so
// the default, credential-agnostic behaviour of existing callers is preserved.

// s3Action is the canonical IAM action name for an S3 operation.
type s3Action string

const (
	actionGetObject    s3Action = "s3:GetObject"
	actionPutObject    s3Action = "s3:PutObject"
	actionDeleteObject s3Action = "s3:DeleteObject"
	actionListBucket   s3Action = "s3:ListBucket"
)

// policyEffect is the outcome of evaluating a policy against a request.
type policyEffect int

const (
	effectDefault policyEffect = iota // no matching statement
	effectAllow
	effectDeny
)

// caller identifies the principal making a request under the single-account
// model.
type caller struct {
	anonymous bool
}

// callerIdentity classifies r as the bucket owner or an anonymous request.
func callerIdentity(r *http.Request) caller {
	if r.Header.Get("Authorization") != "" || isPresignedRequest(r) {
		return caller{anonymous: false}
	}

	return caller{anonymous: true}
}

// enforcementEnabled reports whether the handler applies anonymous / public-ACL
// gating (as opposed to only always-on explicit-Deny enforcement).
func (h *S3Handler) enforcementEnabled() bool {
	return h.PresignSecret != ""
}

// authorizeObjectAccess evaluates the bucket policy and ACL for an object-plane
// request and returns ErrAccessDenied when it is not permitted. It is safe to
// call for buckets with no policy/ACL configured (returns nil).
func (h *S3Handler) authorizeObjectAccess(
	ctx context.Context, r *http.Request, bucket, key string, action s3Action,
) error {
	who := callerIdentity(r)

	pab, _ := loadPublicAccessBlock(ctx, h.Backend, bucket)

	switch h.evaluateBucketPolicy(ctx, bucket, key, action, who, pab) {
	case effectDeny:
		return ErrAccessDenied
	case effectAllow:
		return nil
	case effectDefault:
	}

	// No policy statement matched. Owners are allowed by default; anonymous
	// callers need an explicit public grant (policy or ACL), enforced only in
	// authorization-enforcement mode.
	if !who.anonymous || !h.enforcementEnabled() {
		return nil
	}

	if h.aclGrantsAnonymous(ctx, bucket, action, pab) {
		return nil
	}

	return ErrAccessDenied
}

// evaluateBucketPolicy loads and evaluates the bucket policy JSON, returning the
// combined effect (Deny wins over Allow). A public Allow is suppressed when the
// bucket's PublicAccessBlock has RestrictPublicBuckets set.
func (h *S3Handler) evaluateBucketPolicy(
	ctx context.Context,
	bucket, key string,
	action s3Action,
	who caller,
	pab PublicAccessBlockConfiguration,
) policyEffect {
	policyJSON, err := h.Backend.GetBucketPolicy(ctx, bucket)
	if err != nil || policyJSON == "" {
		return effectDefault
	}

	var doc bucketPolicyDocument
	if uErr := json.Unmarshal([]byte(policyJSON), &doc); uErr != nil {
		return effectDefault
	}

	resource := objectResourceARN(bucket, key, action)

	result := effectDefault
	for i := range doc.Statement {
		stmt := &doc.Statement[i]
		if !stmt.matches(action, resource) {
			continue
		}

		applies, public := stmt.principalApplies(who)
		if !applies {
			continue
		}

		switch strings.ToLower(stmt.Effect) {
		case "deny":
			return effectDeny
		case "allow":
			if public && pab.RestrictPublicBuckets {
				continue // public Allow ignored under RestrictPublicBuckets
			}
			result = effectAllow
		}
	}

	return result
}

// objectResourceARN builds the S3 resource ARN a request targets. List actions
// address the bucket itself; object actions address bucket/key.
func objectResourceARN(bucket, key string, action s3Action) string {
	if action == actionListBucket {
		return "arn:aws:s3:::" + bucket
	}

	return "arn:aws:s3:::" + bucket + "/" + key
}

// bucketPolicyDocument is the subset of an IAM/bucket policy we evaluate.
type bucketPolicyDocument struct {
	Statement []bucketPolicyStatement `json:"Statement"`
}

// bucketPolicyStatement is one policy statement. Action / Resource / Principal
// may each be a single string or a list, so they are decoded as `any` and
// normalised on access.
type bucketPolicyStatement struct {
	Principal any    `json:"Principal"`
	NotAction any    `json:"NotAction"`
	Action    any    `json:"Action"`
	Resource  any    `json:"Resource"`
	Effect    string `json:"Effect"`
}

// matches reports whether the statement's Action and Resource cover the request.
func (s *bucketPolicyStatement) matches(action s3Action, resource string) bool {
	if !s.actionMatches(action) {
		return false
	}

	return s.resourceMatches(resource)
}

// actionMatches reports whether the statement Action (or NotAction) covers the
// request action, honouring wildcards (s3:*, s3:Get*, *).
func (s *bucketPolicyStatement) actionMatches(action s3Action) bool {
	if s.NotAction != nil {
		return !anyStringMatches(s.NotAction, string(action))
	}

	return anyStringMatches(s.Action, string(action))
}

// resourceMatches reports whether the statement Resource covers the request
// resource ARN. A statement with no Resource matches everything (used by
// identity-style statements); wildcards are honoured.
func (s *bucketPolicyStatement) resourceMatches(resource string) bool {
	if s.Resource == nil {
		return true
	}

	return anyStringMatches(s.Resource, resource)
}

// principalApplies reports whether the statement's Principal covers the caller,
// and whether that Principal is the public wildcard "*".
func (s *bucketPolicyStatement) principalApplies(who caller) (bool, bool) {
	if principalIsWildcard(s.Principal) {
		return true, true
	}

	// A specific (non-wildcard) principal is treated as the account owner in
	// this single-account model, so it applies to a non-anonymous caller only.
	return !who.anonymous, false
}

// anyStringMatches reports whether raw (a string or []any of strings) contains a
// pattern matching target, using IAM-style '*'/'?' wildcards, case-insensitively
// for the action namespace.
func anyStringMatches(raw any, target string) bool {
	switch v := raw.(type) {
	case string:
		return wildcardMatch(v, target)
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && wildcardMatch(s, target) {
				return true
			}
		}
	}

	return false
}

// wildcardMatch performs IAM-style glob matching supporting '*' (any run) and
// '?' (single char). Matching is case-insensitive to tolerate action casing.
func wildcardMatch(pattern, s string) bool {
	pattern = strings.ToLower(pattern)
	s = strings.ToLower(s)

	return globMatch(pattern, s)
}

// globMatch is an iterative '*'/'?' matcher (no regex, linear backtracking).
func globMatch(pattern, s string) bool {
	var (
		px, sx         int
		nextPx, nextSx = -1, 0
	)

	for sx < len(s) || px < len(pattern) {
		if px < len(pattern) {
			switch pattern[px] {
			case '*':
				nextPx = px
				nextSx = sx + 1
				px++

				continue
			case '?':
				if sx < len(s) {
					px++
					sx++

					continue
				}
			default:
				if sx < len(s) && pattern[px] == s[sx] {
					px++
					sx++

					continue
				}
			}
		}

		if nextPx == -1 || nextSx > len(s) {
			return false
		}
		px = nextPx + 1
		sx = nextSx
		nextSx++
	}

	return true
}

// aclGrantsAnonymous reports whether the bucket ACL grants the requested action
// to anonymous (AllUsers) requests, subject to the PublicAccessBlock's
// IgnorePublicAcls flag (which, when set, makes S3 disregard public ACL grants).
func (h *S3Handler) aclGrantsAnonymous(
	ctx context.Context, bucket string, action s3Action, pab PublicAccessBlockConfiguration,
) bool {
	if pab.IgnorePublicAcls {
		return false
	}

	acl, err := h.Backend.GetBucketACL(ctx, bucket)
	if err != nil {
		return false
	}

	switch action {
	case actionGetObject, actionListBucket:
		return acl == aclPublicRead || acl == aclPublicReadWrite
	case actionPutObject, actionDeleteObject:
		return acl == aclPublicReadWrite
	}

	return false
}
