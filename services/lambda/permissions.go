package lambda

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddPermission adds a permission statement to a function's resource-based policy.
// When qualifier is non-empty the statement is scoped to that version/alias,
// matching real Lambda: the invoker must then use the qualified ARN to invoke.
func (b *InMemoryBackend) AddPermission(
	functionName, qualifier string,
	input *AddPermissionInput,
) (*AddPermissionOutput, error) {
	b.mu.Lock("AddPermission")
	defer b.mu.Unlock()

	name, qualifier := resolvePermissionTarget(functionName, qualifier)

	if qualifier == versionLatest {
		return nil, fmt.Errorf(
			"%w: we currently do not support adding policies for $LATEST",
			ErrInvalidParameterValue,
		)
	}

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	if qualifier != "" && !b.qualifierExistsLocked(name, qualifier) {
		return nil, ErrVersionNotFound
	}

	key := permissionMapKey(name, qualifier)

	if _, exists := b.permissions.Get(key + "|" + input.StatementID); exists {
		return nil, ErrFunctionAlreadyExists
	}

	if input.RevisionID != "" && input.RevisionID != policyRevisionID(b.permissionsForTarget(key)) {
		return nil, ErrPreconditionFailed
	}

	perm := &FunctionPermission{
		StatementID:           input.StatementID,
		Action:                input.Action,
		Principal:             input.Principal,
		SourceArn:             input.SourceArn,
		SourceAccount:         input.SourceAccount,
		EventSourceToken:      input.EventSourceToken,
		PrincipalOrgID:        input.PrincipalOrgID,
		Effect:                effectAllow,
		FunctionName:          name,
		Qualifier:             qualifier,
		FunctionURLAuthType:   input.FunctionURLAuthType,
		InvokedViaFunctionURL: input.InvokedViaFunctionURL,
	}

	b.permissions.Put(perm)

	resource := "function:" + name
	if qualifier != "" {
		resource += ":" + qualifier
	}

	resourceArn := arn.Build("lambda", b.region, b.accountID, resource)
	stmtJSON := buildPermissionStatementJSON(perm, resourceArn)

	return &AddPermissionOutput{Statement: &stmtJSON}, nil
}

// RemovePermission removes a permission statement from a function's resource-based policy.
// When revisionID is non-empty it must match the policy's current revision (as
// returned by GetPolicy), matching real Lambda's optimistic-concurrency check;
// a mismatch returns ErrPreconditionFailed without mutating the policy.
func (b *InMemoryBackend) RemovePermission(functionName, qualifier, statementID, revisionID string) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	name, qualifier := resolvePermissionTarget(functionName, qualifier)

	if _, ok := b.functions.Get(name); !ok {
		return ErrFunctionNotFound
	}

	key := permissionMapKey(name, qualifier)

	if revisionID != "" && revisionID != policyRevisionID(b.permissionsForTarget(key)) {
		return ErrPreconditionFailed
	}

	if !b.permissions.Delete(key + "|" + statementID) {
		return ErrFunctionNotFound
	}

	return nil
}

// GetPolicy returns the resource-based policy JSON for a function, scoped to
// qualifier when non-empty.
func (b *InMemoryBackend) GetPolicy(functionName, qualifier string) (*GetPolicyOutput, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	name, qualifier := resolvePermissionTarget(functionName, qualifier)

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	// GetPolicy (legacy) and GetResourcePolicy (newer) read the same
	// underlying policy document -- a PutResourcePolicy override must be
	// visible here too, not just through GetResourcePolicy.
	policy, rev, ok := b.effectivePolicyLocked(name, qualifier)
	if !ok {
		return nil, ErrNoPolicyFound
	}

	return &GetPolicyOutput{Policy: &policy, RevisionID: &rev}, nil
}

// functionResourceArn builds the ARN a function/qualifier target's policy
// statements name as their Resource -- the same shape AddPermission uses.
func functionResourceArn(b *InMemoryBackend, name, qualifier string) string {
	resource := "function:" + name
	if qualifier != "" {
		resource += ":" + qualifier
	}

	return arn.Build("lambda", b.region, b.accountID, resource)
}

// statementPolicyLocked renders the current statement-based (AddPermission)
// policy JSON and revision for a function/qualifier target, or ok=false when
// no statements exist for it. Caller must hold b.mu (read or write).
func (b *InMemoryBackend) statementPolicyLocked(name, qualifier string) (string, string, bool) {
	perms := b.permissionsForTarget(permissionMapKey(name, qualifier))
	if len(perms) == 0 {
		return "", "", false
	}

	resourceArn := functionResourceArn(b, name, qualifier)

	// Sort statements for deterministic output.
	sortedPerms := make([]*FunctionPermission, len(perms))
	copy(sortedPerms, perms)
	sort.Slice(sortedPerms, func(i, j int) bool {
		return sortedPerms[i].StatementID < sortedPerms[j].StatementID
	})

	stmts := make([]string, 0, len(sortedPerms))
	for _, p := range sortedPerms {
		stmts = append(stmts, buildPermissionStatementJSON(p, resourceArn))
	}

	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, strings.Join(stmts, ","))

	return policy, policyRevisionID(perms), true
}

// clearPermissionsForTargetLocked removes every statement-based
// FunctionPermission for a function/qualifier target. Used by
// PutResourcePolicy/DeleteResourcePolicy, which operate on the whole policy
// document and must not leave stale statements a subsequent GetPolicy/
// GetResourcePolicy would otherwise still render. Caller must hold b.mu.Lock.
func (b *InMemoryBackend) clearPermissionsForTargetLocked(name, qualifier string) {
	key := permissionMapKey(name, qualifier)
	for _, p := range b.permissionsForTarget(key) {
		b.permissions.Delete(key + "|" + p.StatementID)
	}
}

// policyRevisionID derives a stable opaque revision identifier for a policy
// target (function+qualifier scope) from its current set of statement IDs.
// Real AWS returns an opaque string here that changes on every AddPermission/
// RemovePermission and stays stable otherwise; since statement content is
// immutable once added (there is no UpdatePermission op — a StatementId can
// only be added once and then removed), hashing the sorted StatementId set is
// sufficient to detect every real mutation. Deriving it from b.permissions
// (which IS persisted) instead of separate backend state keeps this correct
// across Snapshot/Restore without needing its own persistence wiring.
func policyRevisionID(perms []*FunctionPermission) string {
	if len(perms) == 0 {
		return ""
	}

	ids := make([]string, len(perms))
	for i, p := range perms {
		ids[i] = p.StatementID
	}

	sort.Strings(ids)

	h := sha256.Sum256([]byte(strings.Join(ids, "\x00")))

	return hex.EncodeToString(h[:])
}

const effectAllow = "Allow"

type permissionStatement struct {
	Principal any                  `json:"Principal"`
	Condition *permissionCondition `json:"Condition,omitempty"`
	Sid       string               `json:"Sid"`
	Effect    string               `json:"Effect"`
	Action    string               `json:"Action"`
	Resource  string               `json:"Resource"`
}

type permissionCondition struct {
	ArnLike      map[string]string `json:"ArnLike,omitempty"`
	StringEquals map[string]string `json:"StringEquals,omitempty"`
	Bool         map[string]string `json:"Bool,omitempty"`
}

// buildPermissionStatementJSON builds the IAM policy statement JSON for a FunctionPermission,
// with a Condition block when SourceArn, SourceAccount etc. are set. Values are JSON-encoded.
func buildPermissionStatementJSON(p *FunctionPermission, resourceArn string) string {
	var principal any
	switch {
	case p.Principal == "*":
		principal = "*"
	case strings.Contains(p.Principal, ".amazonaws.com") || strings.Contains(p.Principal, ".aws.amazon.com"):
		principal = map[string]string{"Service": p.Principal}
	default:
		principal = map[string]string{"AWS": p.Principal}
	}

	stmt := permissionStatement{
		Sid: p.StatementID, Effect: effectAllow, Principal: principal, Action: p.Action, Resource: resourceArn,
	}

	var cond permissionCondition
	if p.SourceArn != "" {
		cond.ArnLike = map[string]string{"AWS:SourceArn": p.SourceArn}
	}

	for k, v := range map[string]string{
		"AWS:SourceAccount":          p.SourceAccount,
		"aws:PrincipalOrgID":         p.PrincipalOrgID,
		"lambda:EventSourceToken":    p.EventSourceToken,
		"lambda:FunctionUrlAuthType": p.FunctionURLAuthType,
	} {
		if v == "" {
			continue
		}

		if cond.StringEquals == nil {
			cond.StringEquals = map[string]string{}
		}

		cond.StringEquals[k] = v
	}

	if p.InvokedViaFunctionURL != nil {
		cond.Bool = map[string]string{"lambda:InvokedViaFunctionUrl": strconv.FormatBool(*p.InvokedViaFunctionURL)}
	}

	if cond.ArnLike != nil || cond.StringEquals != nil || cond.Bool != nil {
		stmt.Condition = &cond
	}

	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if err := enc.Encode(stmt); err != nil {
		return ""
	}

	return strings.TrimSuffix(buf.String(), "\n")
}
