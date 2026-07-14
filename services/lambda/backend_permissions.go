package lambda

import (
	"fmt"
	"sort"
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

	perm := &FunctionPermission{
		StatementID:      input.StatementID,
		Action:           input.Action,
		Principal:        input.Principal,
		SourceArn:        input.SourceArn,
		SourceAccount:    input.SourceAccount,
		EventSourceToken: input.EventSourceToken,
		PrincipalOrgID:   input.PrincipalOrgID,
		Effect:           "Allow",
		FunctionName:     name,
		Qualifier:        qualifier,
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
func (b *InMemoryBackend) RemovePermission(functionName, qualifier, statementID string) error {
	b.mu.Lock("RemovePermission")
	defer b.mu.Unlock()

	name, qualifier := resolvePermissionTarget(functionName, qualifier)

	if _, ok := b.functions.Get(name); !ok {
		return ErrFunctionNotFound
	}

	key := permissionMapKey(name, qualifier)

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

	perms := b.permissionsForTarget(permissionMapKey(name, qualifier))
	if len(perms) == 0 {
		return nil, ErrNoPolicyFound
	}

	stmts := make([]string, 0, len(perms))

	resource := "function:" + name
	if qualifier != "" {
		resource += ":" + qualifier
	}

	resourceArn := arn.Build("lambda", b.region, b.accountID, resource)

	// Sort statements for deterministic output.
	sortedPerms := make([]*FunctionPermission, len(perms))
	copy(sortedPerms, perms)
	sort.Slice(sortedPerms, func(i, j int) bool {
		return sortedPerms[i].StatementID < sortedPerms[j].StatementID
	})

	for _, p := range sortedPerms {
		stmts = append(stmts, buildPermissionStatementJSON(p, resourceArn))
	}

	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, strings.Join(stmts, ","))
	rev := "1"

	return &GetPolicyOutput{Policy: &policy, RevisionID: &rev}, nil
}

// buildPermissionStatementJSON builds the IAM policy statement JSON for a FunctionPermission.
// It includes a Condition block when SourceArn or SourceAccount are set, matching real AWS output.
func buildPermissionStatementJSON(p *FunctionPermission, resourceArn string) string {
	// Determine principal format: account IDs and "*" use root principal; services use Service key.
	var principalJSON string
	switch {
	case p.Principal == "*":
		principalJSON = `"*"`
	case strings.Contains(p.Principal, ".amazonaws.com") || strings.Contains(p.Principal, ".aws.amazon.com"):
		principalJSON = fmt.Sprintf(`{"Service":%q}`, p.Principal)
	default:
		// Account principal: arn:aws:iam::{account}:root
		principalJSON = fmt.Sprintf(`{"AWS":%q}`, p.Principal)
	}

	base := fmt.Sprintf(
		`{"Sid":%q,"Effect":"Allow","Principal":%s,"Action":%q,"Resource":%q`,
		p.StatementID, principalJSON, p.Action, resourceArn,
	)

	// Build the Condition block. ArnLike and StringEquals are each a single
	// JSON object — SourceAccount, PrincipalOrgID, and EventSourceToken all
	// use the StringEquals operator and must be merged into ONE object
	// (naively appending separate "StringEquals":{...} entries would emit
	// duplicate JSON keys, which real AWS never does).
	var arnLike []string
	if p.SourceArn != "" {
		arnLike = append(arnLike, fmt.Sprintf(`"AWS:SourceArn":%q`, p.SourceArn))
	}

	var stringEquals []string
	if p.SourceAccount != "" {
		stringEquals = append(stringEquals, fmt.Sprintf(`"AWS:SourceAccount":%q`, p.SourceAccount))
	}
	if p.PrincipalOrgID != "" {
		stringEquals = append(stringEquals, fmt.Sprintf(`"aws:PrincipalOrgID":%q`, p.PrincipalOrgID))
	}
	if p.EventSourceToken != "" {
		stringEquals = append(stringEquals, fmt.Sprintf(`"lambda:EventSourceToken":%q`, p.EventSourceToken))
	}

	var conditions []string
	if len(arnLike) > 0 {
		conditions = append(conditions, `"ArnLike":{`+strings.Join(arnLike, ",")+`}`)
	}
	if len(stringEquals) > 0 {
		conditions = append(conditions, `"StringEquals":{`+strings.Join(stringEquals, ",")+`}`)
	}

	if len(conditions) > 0 {
		return base + `,"Condition":{` + strings.Join(conditions, ",") + `}}`
	}

	return base + "}"
}
