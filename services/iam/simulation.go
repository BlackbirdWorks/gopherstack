package iam

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// GetAccountAuthorizationDetails returns a full dump of all IAM entities and their policies.
func (b *InMemoryBackend) GetAccountAuthorizationDetails() AccountAuthorizationDetails {
	b.mu.RLock("GetAccountAuthorizationDetails")
	defer b.mu.RUnlock()

	// Build reverse group-membership map: userName → []groupName.
	userGroupMap := make(map[string][]string, b.users.Len())
	for groupName, members := range b.groupMembers {
		for _, member := range members {
			userGroupMap[member] = append(userGroupMap[member], groupName)
		}
	}

	// Build reverse instance-profile map: roleName → []InstanceProfile, mirroring
	// ListInstanceProfilesForRole (same real backend now feeds both), so
	// RoleDetail.InstanceProfileList is populated instead of always empty.
	roleInstanceProfiles := make(map[string][]InstanceProfile)
	for _, ip := range b.instanceProfiles.All() {
		for _, roleName := range ip.Roles {
			roleInstanceProfiles[roleName] = append(roleInstanceProfiles[roleName], *ip)
		}
	}

	for roleName, profiles := range roleInstanceProfiles {
		sort.Slice(profiles, func(i, j int) bool {
			return profiles[i].InstanceProfileName < profiles[j].InstanceProfileName
		})
		roleInstanceProfiles[roleName] = profiles
	}

	// Build user details.
	users := make([]UserDetail, 0, b.users.Len())
	for _, u := range b.users.All() {
		user := *u
		attached := attachedFromARNs(b.userPolicies[u.UserName])
		inline := inlineEntries(b.userInlinePolicies[u.UserName])
		groupNames := userGroupMap[u.UserName]
		sort.Strings(groupNames)
		users = append(
			users,
			UserDetail{User: user, AttachedPolicies: attached, InlinePolicies: inline, GroupNames: groupNames},
		)
	}

	sort.Slice(users, func(i, j int) bool { return users[i].UserName < users[j].UserName })

	// Build group details.
	groups := make([]GroupDetail, 0, b.groups.Len())
	for _, g := range b.groups.All() {
		group := *g
		attached := attachedFromARNs(b.groupPolicies[g.GroupName])
		inline := inlineEntries(b.groupInlinePolicies[g.GroupName])
		groups = append(
			groups,
			GroupDetail{Group: group, AttachedPolicies: attached, InlinePolicies: inline},
		)
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	// Build role details.
	roles := make([]RoleDetail, 0, b.roles.Len())
	for _, r := range b.roles.All() {
		role := *r
		attached := attachedFromARNs(b.rolePolicies[r.RoleName])
		inline := inlineEntries(b.roleInlinePolicies[r.RoleName])
		profiles := roleInstanceProfiles[r.RoleName]
		roles = append(
			roles,
			RoleDetail{Role: role, AttachedPolicies: attached, InlinePolicies: inline, InstanceProfiles: profiles},
		)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	// Build managed policy list.
	policies := make([]Policy, 0, b.policies.Len())
	for _, p := range b.policies.All() {
		policies = append(policies, *p)
	}

	sort.Slice(
		policies,
		func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName },
	)

	return AccountAuthorizationDetails{
		Users:    users,
		Groups:   groups,
		Roles:    roles,
		Policies: policies,
	}
}

// attachedFromARNs converts a slice of policy ARNs to AttachedPolicy entries.
func attachedFromARNs(arns []string) []AttachedPolicy {
	result := make([]AttachedPolicy, 0, len(arns))

	for _, a := range arns {
		result = append(result, AttachedPolicy{PolicyName: policyNameFromARN(a), PolicyArn: a})
	}

	return result
}

// inlineEntries converts a policyName→document map to sorted InlinePolicyEntry slices.
func inlineEntries(m map[string]string) []InlinePolicyEntry {
	result := make([]InlinePolicyEntry, 0, len(m))

	for name, doc := range m {
		result = append(result, InlinePolicyEntry{PolicyName: name, PolicyDocument: doc})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].PolicyName < result[j].PolicyName })

	return result
}

// SimulatePrincipalPolicy evaluates a set of actions against a set of resources
// for the given principal ARN, returning a result per action×resource pair.
//
// Supported principal ARN formats:
//   - arn:aws:iam::<account>:user/<name>
//   - arn:aws:iam::<account>:role/<name>
//
// Permission boundaries are enforced: effective permissions = identity policies ∩ boundary.
// An allow is only returned if both the identity policies allow AND the boundary allows.
func (b *InMemoryBackend) SimulatePrincipalPolicy(
	principalArn, callerArn, resourceOwner string,
	resourcePolicyList, actionNames, resourceArns []string, ctx ConditionContext,
) ([]SimulationResult, error) {
	b.mu.RLock("SimulatePrincipalPolicy")
	defer b.mu.RUnlock()

	namedPolicies, err := b.collectNamedPrincipalPolicies(principalArn)
	if err != nil {
		return nil, err
	}

	// Collect permission boundary document (if any).
	boundaryDoc := b.collectBoundaryDoc(principalArn)
	hasBoundary := boundaryDoc != ""

	if len(resourceArns) == 0 {
		resourceArns = []string{"*"}
	}

	// Build plain docs slice for combined evaluation.
	docs := make([]string, 0, len(namedPolicies))
	for _, np := range namedPolicies {
		docs = append(docs, np.Doc)
	}

	results := make([]SimulationResult, 0, len(actionNames)*len(resourceArns))

	principalAccount := parseAccountFromArn(principalArn)
	resourceAccount := resourceOwner
	if resourceAccount == "" {
		if callerArn != "" {
			resourceAccount = parseAccountFromArn(callerArn)
		} else {
			resourceAccount = principalAccount
		}
	}

	isCrossAccount := resourceAccount != principalAccount && resourceAccount != "" && principalAccount != ""

	for _, action := range actionNames {
		for _, resource := range resourceArns {
			results = append(results, b.evaluateSingleSimulation(
				action, resource, docs, resourcePolicyList,
				ctx, hasBoundary, boundaryDoc, namedPolicies, isCrossAccount,
			))
		}
	}

	return results, nil
}

func (b *InMemoryBackend) evaluateSingleSimulation(
	action, resource string,
	docs, resourcePolicyList []string,
	ctx ConditionContext,
	hasBoundary bool, boundaryDoc string,
	namedPolicies []namedPolicyDoc,
	isCrossAccount bool,
) SimulationResult {
	// Identity Policies evaluation
	idResult := EvaluatePolicies(docs, action, resource, ctx)

	// Resource Policies evaluation
	var resDocs []string
	resDocs = append(resDocs, resourcePolicyList...)

	// Auto-inject role trust policy for sts:AssumeRole
	if action == "sts:AssumeRole" {
		if r, errGet := b.GetRoleByArn(resource); errGet == nil && r.AssumeRolePolicyDocument != "" {
			resDocs = append(resDocs, r.AssumeRolePolicyDocument)
		}
	}

	resResult := EvalImplicitDeny
	if len(resDocs) > 0 {
		resResult = EvaluatePolicies(resDocs, action, resource, ctx)
	}

	// Combine logic (Intra-account vs Cross-account)
	evalResult := combineSimulationResults(idResult, resResult, isCrossAccount)

	// Per-policy detail map.
	detail := make(map[string]string, len(namedPolicies))
	for _, np := range namedPolicies {
		r := EvaluatePolicies([]string{np.Doc}, action, resource, ctx)
		detail[np.SourceID] = evalDecisionStr(r)
	}

	// Boundary enforcement.
	var allowedByBoundary *bool

	if hasBoundary {
		boundaryResult := EvaluatePolicies(
			[]string{boundaryDoc},
			action,
			resource,
			ctx,
		)
		allowed := boundaryResult == EvalAllow

		allowedByBoundary = &allowed

		if evalResult == EvalAllow && !allowed {
			evalResult = EvalImplicitDeny
		}
	}

	return SimulationResult{
		ActionName:                   action,
		ResourceName:                 resource,
		Decision:                     evalDecisionStr(evalResult),
		EvalDecisionDetails:          detail,
		AllowedByPermissionsBoundary: allowedByBoundary,
	}
}

func combineSimulationResults(idResult, resResult EvaluationResult, isCrossAccount bool) EvaluationResult {
	if idResult == EvalExplicitDeny || resResult == EvalExplicitDeny {
		return EvalExplicitDeny
	}

	if isCrossAccount {
		if idResult == EvalAllow && resResult == EvalAllow {
			return EvalAllow
		}

		return EvalImplicitDeny
	}

	if idResult == EvalAllow || resResult == EvalAllow {
		return EvalAllow
	}

	return EvalImplicitDeny
}

func parseAccountFromArn(arnStr string) string {
	const minArnParts = 5
	const arnAccountIndex = 4

	parts := strings.Split(arnStr, ":")
	if len(parts) >= minArnParts {
		return parts[arnAccountIndex]
	}

	return ""
}

// evalDecisionStr converts an EvalResult to the AWS-compatible decision string.
func evalDecisionStr(r EvaluationResult) string {
	switch r {
	case EvalAllow:
		return "allowed"
	case EvalExplicitDeny:
		return "explicitDeny"
	default:
		return "implicitDeny"
	}
}

// collectBoundaryDoc returns the policy document for the principal's permission boundary, or "".
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectBoundaryDoc(principalArn string) string {
	const (
		userPrefix = ":user/"
		rolePrefix = ":role/"
	)

	switch {
	case strings.Contains(principalArn, userPrefix):
		idx := strings.LastIndex(principalArn, userPrefix)

		return b.boundaryDocForUser(principalArn[idx+len(userPrefix):])
	case strings.Contains(principalArn, rolePrefix):
		idx := strings.LastIndex(principalArn, rolePrefix)

		return b.boundaryDocForRole(principalArn[idx+len(rolePrefix):])
	}

	return ""
}

// collectNamedPrincipalPolicies returns named policy documents for the given principal ARN.
// Each entry contains the policy source ID (ARN for managed, name for inline) and document.
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectNamedPrincipalPolicies(
	principalArn string,
) ([]namedPolicyDoc, error) {
	const (
		userPrefix = ":user/"
		rolePrefix = ":role/"
	)

	switch {
	case strings.Contains(principalArn, userPrefix):
		idx := strings.LastIndex(principalArn, userPrefix)
		userName := principalArn[idx+len(userPrefix):]

		if _, exists := b.users.Get(userName); !exists {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
		}

		named := b.collectNamedEntityPolicies(
			b.userPolicies[userName],
			b.userInlinePolicies[userName],
		)

		// Add group-inherited policies.
		for groupName, members := range b.groupMembers {
			if !slices.Contains(members, userName) {
				continue
			}

			named = append(
				named,
				b.collectNamedEntityPolicies(
					b.groupPolicies[groupName],
					b.groupInlinePolicies[groupName],
				)...)
		}

		return named, nil

	case strings.Contains(principalArn, rolePrefix):
		idx := strings.LastIndex(principalArn, rolePrefix)
		roleName := principalArn[idx+len(rolePrefix):]

		if _, exists := b.roles.Get(roleName); !exists {
			return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
		}

		return b.collectNamedEntityPolicies(
			b.rolePolicies[roleName],
			b.roleInlinePolicies[roleName],
		), nil

	default:
		return nil, fmt.Errorf(
			"%w: unsupported principal ARN format %q",
			ErrUserNotFound,
			principalArn,
		)
	}
}

// collectNamedEntityPolicies collects named policy docs from attached ARNs and inline policies.
// Uses policyByARN for O(1) ARN-to-name resolution instead of O(n) map scan.
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectNamedEntityPolicies(
	attachedARNs []string, inlinePols map[string]string,
) []namedPolicyDoc {
	var named []namedPolicyDoc

	for _, policyArn := range attachedARNs {
		polName, ok := b.policyByARN[policyArn]
		if !ok {
			continue
		}

		p, ok := b.policies.Get(polName)
		if ok && p.PolicyDocument != "" {
			named = append(named, namedPolicyDoc{SourceID: p.Arn, Doc: p.PolicyDocument})
		}
	}

	for name, doc := range inlinePols {
		if doc != "" {
			named = append(named, namedPolicyDoc{SourceID: name, Doc: doc})
		}
	}

	return named
}
