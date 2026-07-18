package serverlessrepo

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// clonePolicyStatement returns a deep copy of s.
func clonePolicyStatement(s *ApplicationPolicyStatement) *ApplicationPolicyStatement {
	return &ApplicationPolicyStatement{
		StatementID:     s.StatementID,
		Actions:         cloneStringSlice(s.Actions),
		Principals:      cloneStringSlice(s.Principals),
		PrincipalOrgIDs: cloneStringSlice(s.PrincipalOrgIDs),
	}
}

// clonePolicyStatements returns deep copies of all policy statements.
// Returns an empty (non-nil) slice when stmts is nil.
func clonePolicyStatements(stmts []*ApplicationPolicyStatement) []*ApplicationPolicyStatement {
	out := make([]*ApplicationPolicyStatement, len(stmts))
	for i, s := range stmts {
		out[i] = clonePolicyStatement(s)
	}

	return out
}

// GetApplicationPolicy returns the policy statements for an application.
func (b *InMemoryBackend) GetApplicationPolicy(appName string) ([]*ApplicationPolicyStatement, error) {
	b.mu.RLock("GetApplicationPolicy")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	return clonePolicyStatements(b.appPolicies[appName]), nil
}

// PutApplicationPolicy sets the policy statements for an application.
func (b *InMemoryBackend) PutApplicationPolicy(
	appName string,
	statements []*ApplicationPolicyStatement,
) ([]*ApplicationPolicyStatement, error) {
	b.mu.Lock("PutApplicationPolicy")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: could not find application %q", ErrApplicationNotFound, appName)
	}

	// Validate and auto-generate statementIds.
	for i, s := range statements {
		if len(s.Actions) == 0 {
			return nil, fmt.Errorf("%w: statement %d has no actions", ErrValidation, i)
		}

		for _, action := range s.Actions {
			if !isValidPolicyAction(action) {
				return nil, fmt.Errorf("%w: statement %d contains unsupported action %q", ErrValidation, i, action)
			}
		}

		if len(s.Principals) == 0 {
			return nil, fmt.Errorf("%w: statement %d has no principals", ErrValidation, i)
		}

		if s.StatementID == "" {
			statements[i].StatementID = uuid.NewString()
		}
	}

	b.appPolicies[appName] = clonePolicyStatements(statements)

	return clonePolicyStatements(b.appPolicies[appName]), nil
}

// validPolicyActionVariantCount is the number of case variants (PascalCase and all-lowercase)
// registered per documented policy action in [validPolicyActionsSet].
const validPolicyActionVariantCount = 2

// validPolicyActionsSet returns the set of AWS SAR application policy actions documented in
// the "Application Permissions" table of the SAR access-control guide: GetApplication,
// CreateCloudFormationChangeSet, CreateCloudFormationTemplate, ListApplicationVersions,
// ListApplicationDependencies, SearchApplications, Deploy (which implies all of the
// preceding), and UnshareApplication (used to revoke an AWS Organization share). Both the
// documented PascalCase spelling and an all-lowercase variant are accepted per each action.
func validPolicyActionsSet() map[string]struct{} {
	pascalCase := []string{
		"GetApplication",
		"CreateCloudFormationChangeSet",
		"CreateCloudFormationTemplate",
		"ListApplicationVersions",
		"ListApplicationDependencies",
		"SearchApplications",
		"Deploy",
		"UnshareApplication",
	}

	set := make(map[string]struct{}, len(pascalCase)*validPolicyActionVariantCount)
	for _, action := range pascalCase {
		set[action] = struct{}{}
		set[strings.ToLower(action)] = struct{}{}
	}

	return set
}

// isValidPolicyAction returns true if the given action is a supported SAR policy action.
// AWS SAR is case-insensitive for action names; we accept both mixed-case and lowercase variants.
func isValidPolicyAction(action string) bool {
	_, ok := validPolicyActionsSet()[action]

	return ok
}
