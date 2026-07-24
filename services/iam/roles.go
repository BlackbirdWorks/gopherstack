package iam

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateRole creates a new IAM role.
func (b *InMemoryBackend) CreateRole(
	roleName, path, assumeRolePolicyDocument, permissionsBoundary string,
) (*Role, error) {
	b.mu.Lock("CreateRole")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); exists {
		return nil, fmt.Errorf("%w: role %q already exists", ErrRoleAlreadyExists, roleName)
	}

	if assumeRolePolicyDocument != "" && !json.Valid([]byte(assumeRolePolicyDocument)) {
		return nil, fmt.Errorf(
			"%w: invalid JSON in AssumeRolePolicyDocument",
			ErrMalformedPolicyDocument,
		)
	}

	if err := b.validateTrustPolicyPrincipal(assumeRolePolicyDocument); err != nil {
		return nil, err
	}

	p := normPath(path)
	r := Role{
		RoleName:                 roleName,
		RoleID:                   newID("AROA"),
		Arn:                      arn.Build("iam", "", b.accountID, "role"+p+roleName),
		Path:                     p,
		AssumeRolePolicyDocument: assumeRolePolicyDocument,
		CreateDate:               time.Now().UTC(),
		PermissionsBoundary:      permissionsBoundary,
	}
	b.roles.Put(&r)
	b.roleByARN[r.Arn] = roleName
	b.sortedRoleNames = insertSorted(b.sortedRoleNames, roleName)

	return &r, nil
}

// DeleteRole deletes an IAM role by name.
//
// Matching real AWS: DeleteRole is rejected with DeleteConflictException if
// the role still has inline policies, attached managed policies, or is
// attached to an instance profile — the caller must DeleteRolePolicy /
// DetachRolePolicy / RemoveRoleFromInstanceProfile first. See
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteRole.html.
func (b *InMemoryBackend) DeleteRole(roleName string) error {
	b.mu.Lock("DeleteRole")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if len(b.rolePolicies[roleName]) > 0 {
		return fmt.Errorf("%w: role %q has attached policies", ErrDeleteConflict, roleName)
	}

	if len(b.roleInlinePolicies[roleName]) > 0 {
		return fmt.Errorf("%w: role %q has inline policies", ErrDeleteConflict, roleName)
	}

	for _, ip := range b.instanceProfiles.All() {
		if slices.Contains(ip.Roles, roleName) {
			return fmt.Errorf(
				"%w: role %q is attached to instance profile %q",
				ErrDeleteConflict, roleName, ip.InstanceProfileName,
			)
		}
	}

	role, _ := b.roles.Get(roleName)
	b.roles.Delete(roleName)
	delete(b.roleByARN, role.Arn)
	b.sortedRoleNames = deleteSorted(b.sortedRoleNames, roleName)

	return nil
}

// ListRoles returns a paginated list of IAM roles sorted by name.
func (b *InMemoryBackend) ListRoles(marker string, maxItems int) (page.Page[Role], error) {
	b.mu.RLock("ListRoles")
	defer b.mu.RUnlock()

	return pageFromSortedNames(
		b.sortedRoleNames,
		b.roles.Get,
		marker,
		maxItems,
		iamDefaultMaxItems,
	), nil
}

// GetRole retrieves a single IAM role by name.
func (b *InMemoryBackend) GetRole(roleName string) (*Role, error) {
	b.mu.RLock("GetRole")
	defer b.mu.RUnlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	return r, nil
}

// GetRoleByArn retrieves a single IAM role by its full ARN.
func (b *InMemoryBackend) GetRoleByArn(roleArn string) (*Role, error) {
	b.mu.RLock("GetRoleByArn")
	defer b.mu.RUnlock()

	roleName, exists := b.roleByARN[roleArn]
	if !exists {
		return nil, fmt.Errorf("%w: role with ARN %q not found", ErrRoleNotFound, roleArn)
	}

	role, exists := b.roles.Get(roleName)
	if !exists {
		return nil, fmt.Errorf("%w: role with ARN %q not found", ErrRoleNotFound, roleArn)
	}

	return role, nil
}

// UpdateRoleMaxSessionDuration sets the maximum session duration for a role.
func (b *InMemoryBackend) UpdateRoleMaxSessionDuration(
	roleName string,
	maxSessionDuration int32,
) error {
	b.mu.Lock("UpdateRoleMaxSessionDuration")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.MaxSessionDuration = maxSessionDuration
	b.roles.Put(r)

	return nil
}

// ListAllRoles returns all roles (for dashboard).
func (b *InMemoryBackend) ListAllRoles() []Role {
	b.mu.RLock("ListAllRoles")
	defer b.mu.RUnlock()

	roles := make([]Role, 0, b.roles.Len())
	for _, r := range b.roles.All() {
		roles = append(roles, *r)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	return roles
}

// ListAttachedRolePolicies returns all policy ARNs attached to the named role.
func (b *InMemoryBackend) ListAttachedRolePolicies(roleName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedRolePolicies")
	defer b.mu.RUnlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	arns := b.rolePolicies[roleName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, arn := range arns {
		name := policyNameFromARN(arn)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: arn})
	}

	return result, nil
}

// PutRolePolicy creates or replaces an inline policy on a role.
func (b *InMemoryBackend) PutRolePolicy(roleName, policyName, policyDocument string) error {
	b.mu.Lock("PutRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if err := validateIdentityPolicyDocument(policyDocument); err != nil {
		return err
	}

	if len(policyDocument) > maxRolePolicySize {
		return fmt.Errorf("%w: inline policy for role %q exceeds %d bytes",
			ErrLimitExceeded, roleName, maxRolePolicySize)
	}

	if b.roleInlinePolicies[roleName] == nil {
		b.roleInlinePolicies[roleName] = make(map[string]string)
	}

	b.roleInlinePolicies[roleName][policyName] = policyDocument

	return nil
}

// GetRolePolicy retrieves an inline policy document from a role.
func (b *InMemoryBackend) GetRolePolicy(roleName, policyName string) (string, error) {
	b.mu.RLock("GetRolePolicy")
	defer b.mu.RUnlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return "", fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	doc, exists := b.roleInlinePolicies[roleName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on role %q",
			ErrInlinePolicyNotFound,
			policyName,
			roleName,
		)
	}

	return doc, nil
}

// DeleteRolePolicy removes an inline policy from a role.
func (b *InMemoryBackend) DeleteRolePolicy(roleName, policyName string) error {
	b.mu.Lock("DeleteRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if _, exists := b.roleInlinePolicies[roleName][policyName]; !exists {
		return fmt.Errorf(
			"%w: inline policy %q not found on role %q",
			ErrInlinePolicyNotFound,
			policyName,
			roleName,
		)
	}

	delete(b.roleInlinePolicies[roleName], policyName)

	return nil
}

// ListRolePolicies returns sorted inline policy names for a role.
func (b *InMemoryBackend) ListRolePolicies(roleName string) ([]string, error) {
	b.mu.RLock("ListRolePolicies")
	defer b.mu.RUnlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	names := collections.SortedKeys(b.roleInlinePolicies[roleName])

	return names, nil
}

// PutRolePermissionsBoundary sets the permissions boundary on a role.
func (b *InMemoryBackend) PutRolePermissionsBoundary(roleName, policyArn string) error {
	b.mu.Lock("PutRolePermissionsBoundary")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.PermissionsBoundary = policyArn
	b.roles.Put(r)

	return nil
}

// DeleteRolePermissionsBoundary clears the permissions boundary on a role.
func (b *InMemoryBackend) DeleteRolePermissionsBoundary(roleName string) error {
	b.mu.Lock("DeleteRolePermissionsBoundary")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.PermissionsBoundary = ""
	b.roles.Put(r)

	return nil
}

// UpdateAssumeRolePolicy updates the assume-role policy document on a role.
func (b *InMemoryBackend) UpdateAssumeRolePolicy(roleName, policyDocument string) error {
	b.mu.Lock("UpdateAssumeRolePolicy")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return fmt.Errorf(
			"%w: invalid JSON in AssumeRolePolicyDocument",
			ErrMalformedPolicyDocument,
		)
	}

	if err := b.validateTrustPolicyPrincipal(policyDocument); err != nil {
		return err
	}

	r.AssumeRolePolicyDocument = policyDocument
	b.roles.Put(r)

	return nil
}

// purgeRolesLocked removes roles created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeRolesLocked(cutoff time.Time) {
	for _, r := range b.roles.All() {
		if r.CreateDate.Before(cutoff) {
			name := r.RoleName
			b.roles.Delete(name)
			delete(b.rolePolicies, name)
			delete(b.roleInlinePolicies, name)
		}
	}
}

// UpdateRole updates the description of an IAM role.
func (b *InMemoryBackend) UpdateRole(roleName, description string) error {
	b.mu.Lock("UpdateRole")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.Description = description
	b.roles.Put(r)

	return nil
}

// parseJSON is a thin wrapper around json.Unmarshal for readability.
func parseJSON(s string, v any) error {
	return json.Unmarshal([]byte(s), v) //nolint:wrapcheck // thin delegation; callers handle the error
}

// TagRole merges the given key-value pairs into the role's Tags field.
func (b *InMemoryBackend) TagRole(roleName string, tags map[string]string) error {
	b.mu.Lock("TagRole")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if r.Tags == nil {
		r.Tags = make(map[string]string, len(tags))
	}

	maps.Copy(r.Tags, tags)

	b.roles.Put(r)

	return nil
}

// UntagRole removes the given keys from the role's Tags field.
func (b *InMemoryBackend) UntagRole(roleName string, keys []string) error {
	b.mu.Lock("UntagRole")
	defer b.mu.Unlock()

	r, exists := b.roles.Get(roleName)
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	for _, k := range keys {
		delete(r.Tags, k)
	}

	b.roles.Put(r)

	return nil
}

// trustStatement is a parsed trust policy statement for AssumeRole evaluation.
type trustStatement struct {
	Action    any                       `json:"Action"`
	Condition map[string]map[string]any `json:"Condition,omitempty"`
	Effect    string                    `json:"Effect,omitempty"`
	Principal trustPrincipal            `json:"Principal"`
}

// trustPrincipal holds the parsed Principal from a trust policy statement.
// AWS Principal can be "*", a single ARN string, or a map of type→ARNs.
type trustPrincipal struct {
	AWS       []string `json:"aws,omitempty"`
	Service   []string `json:"service,omitempty"`
	Federated []string `json:"federated,omitempty"`
	AllowAll  bool     `json:"allowAll,omitempty"`
}

// UnmarshalJSON handles the three AWS Principal forms:
//   - "*"                         → AllowAll=true
//   - "arn:aws:iam::..."          → AWS=[arn]
//   - {"AWS":"...", "Service":"..."} → typed map
func (p *trustPrincipal) UnmarshalJSON(data []byte) error {
	// Try string first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s == "*" {
			p.AllowAll = true
		} else {
			p.AWS = []string{s}
		}

		return nil
	}

	// Try typed map.
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err //nolint:wrapcheck // UnmarshalJSON interface; wrapping changes the type
	}

	for k, v := range m {
		var vals []string

		// Value can be a string or []string.
		var single string
		if err := json.Unmarshal(v, &single); err == nil {
			vals = []string{single}
		} else {
			if err2 := json.Unmarshal(v, &vals); err2 != nil {
				continue
			}
		}

		switch strings.ToUpper(k) {
		case "AWS":
			p.AWS = append(p.AWS, vals...)
		case "SERVICE":
			p.Service = append(p.Service, vals...)
		case "FEDERATED":
			p.Federated = append(p.Federated, vals...)
		}
	}

	return nil
}

// EvaluateAssumeRoleTrustPolicy evaluates a role's trust policy against an AssumeRole request.
// Returns true if the principal is allowed to assume the role under the given conditions.
//
// Parameters:
//   - trustPolicyJSON: the role's AssumeRolePolicyDocument
//   - principalARN: the ARN of the entity trying to assume the role
//   - externalID: sts:ExternalId value from the request (empty if not provided)
//   - mfaPresent: whether MFA was used in the request
//   - ctx: additional condition context (e.g. source IP)
func EvaluateAssumeRoleTrustPolicy(
	trustPolicyJSON, principalARN, externalID string, mfaPresent bool, ctx ConditionContext,
) bool {
	if trustPolicyJSON == "" {
		return false
	}

	// Inject STS-specific condition keys into the context.
	if ctx.Extra == nil {
		ctx.Extra = make(map[string]string)
	}

	const (
		mfaKey   = "aws:multifactorauthpresent"
		mfaTrue  = "true"
		mfaFalse = "false"
	)

	ctx.Extra["sts:externalid"] = externalID

	if mfaPresent {
		ctx.Extra[mfaKey] = mfaTrue
	} else {
		ctx.Extra[mfaKey] = mfaFalse
	}

	var doc struct {
		Statement []trustStatement `json:"Statement,omitempty"`
	}

	expanded := SubstituteVariables(trustPolicyJSON, ctx)

	if err := parseJSON(expanded, &doc); err != nil {
		return false
	}

	for _, stmt := range doc.Statement {
		if !strings.EqualFold(stmt.Effect, "Allow") {
			continue
		}

		if !trustPrincipalMatches(stmt.Principal, principalARN) {
			continue
		}

		if !stmtActionMatchesTrust(stmt.Action, "sts:AssumeRole") {
			continue
		}

		if !conditionMatches(stmt.Condition, ctx) {
			continue
		}

		return true
	}

	return false
}

// trustPrincipalMatches returns true if principalARN is covered by the trust principal.
func trustPrincipalMatches(p trustPrincipal, principalARN string) bool {
	if p.AllowAll {
		return true
	}

	lower := strings.ToLower(principalARN)

	for _, arn := range p.AWS {
		if wildcardMatchCaseInsensitive(arn, lower) {
			return true
		}
	}

	for _, svc := range p.Service {
		if wildcardMatchCaseInsensitive(svc, lower) {
			return true
		}
	}

	for _, fed := range p.Federated {
		if wildcardMatchCaseInsensitive(fed, lower) {
			return true
		}
	}

	return false
}

// stmtActionMatchesTrust checks whether the trust statement Action includes the given action.
func stmtActionMatchesTrust(action any, want string) bool {
	for _, a := range anyStrings(action) {
		if wildcardMatchCaseInsensitive(a, want) {
			return true
		}
	}

	return false
}

// validateTrustPolicyPrincipal parses the trust policy and validates the Principal field.
// Allowed shapes: "*", a string ARN, or a map with keys AWS/Service/Federated.
// Returns nil if the policy is empty or valid; returns ErrMalformedPolicyDocument otherwise.
// maxARNParts is the number of components in a parsed AWS ARN.
const maxARNParts = 6

func (b *InMemoryBackend) validateTrustPolicyPrincipal(policyJSON string) error {
	if policyJSON == "" {
		return nil
	}

	var doc struct {
		Statement []struct {
			Principal json.RawMessage `json:"Principal"`
		} `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil //nolint:nilerr // JSON validity confirmed upstream; this path handles schema mismatch
	}

	for i, stmt := range doc.Statement {
		if stmt.Principal == nil {
			continue
		}

		var p trustPrincipal
		if err := json.Unmarshal(stmt.Principal, &p); err != nil {
			return fmt.Errorf(
				"%w: statement[%d] Principal has unsupported shape",
				ErrMalformedPolicyDocument, i,
			)
		}

		if err := b.validateTrustPrincipalBlock(p, i); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) validateTrustPrincipalBlock(p trustPrincipal, i int) error {
	// Validate ARN format for AWS principals.
	for _, a := range p.AWS {
		if err := b.validateAWSPrincipal(a, i); err != nil {
			return err
		}
	}

	for _, s := range p.Service {
		if !strings.HasSuffix(s, ".amazonaws.com") {
			return fmt.Errorf("%w: invalid service principal", ErrMalformedPolicyDocument)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateAWSPrincipal(a string, i int) error {
	if a == "*" {
		return nil
	}

	if !strings.HasPrefix(strings.ToLower(a), "arn:aws") {
		return fmt.Errorf(
			"%w: statement[%d] Principal.AWS %q is not a valid ARN",
			ErrMalformedPolicyDocument, i, a,
		)
	}

	parts := strings.SplitN(a, ":", maxARNParts)
	if len(parts) == maxARNParts {
		accountID := parts[4]
		resource := parts[5]

		if accountID != b.accountID && accountID != "" {
			return fmt.Errorf("%w: invalid cross-account principal", ErrMalformedPolicyDocument)
		}
		if accountID == b.accountID {
			if err := b.validatePrincipalResource(resource); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) validatePrincipalResource(resource string) error {
	if userName, ok := strings.CutPrefix(resource, "user/"); ok {
		if _, exists := b.users.Get(userName); !exists {
			return fmt.Errorf("%w: user %q not found", ErrMalformedPolicyDocument, userName)
		}
	} else if roleName, okRole := strings.CutPrefix(resource, "role/"); okRole {
		if _, exists := b.roles.Get(roleName); !exists {
			return fmt.Errorf("%w: role %q not found", ErrMalformedPolicyDocument, roleName)
		}
	}

	return nil
}
