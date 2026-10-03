package iam

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

var _ roleauth.ResourceAuthorizer = (*RoleAuthorizer)(nil)

// AuthorizeServiceResource evaluates the resource's own policy for a service principal,
// with aws:SourceArn from sourceARN and aws:SourceAccount from its account (else the resource's).
func (a *RoleAuthorizer) AuthorizeServiceResource(servicePrincipal, action, resource, sourceARN string) error {
	docs := collectResourcePolicies(context.Background(), a.providers, resource)

	sourceAccount := arnAccount(sourceARN)
	if sourceAccount == "" {
		sourceAccount = arnAccount(resource)
	}

	condCtx := ConditionContext{
		RequestedRegion: arnRegion(resource),
		Extra: map[string]string{
			"aws:sourcearn":     sourceARN,
			"aws:sourceaccount": sourceAccount,
		},
	}

	if EvaluateServicePolicies(docs, servicePrincipal, action, resource, condCtx) != EvalAllow {
		return fmt.Errorf("%w: %s is not authorized to perform %s on resource %s",
			roleauth.ErrAccessDenied, servicePrincipal, action, resource)
	}

	return nil
}

// EvaluateServicePolicies is EvaluatePolicies for resource policies: only statements whose
// Principal covers servicePrincipal participate.
func EvaluateServicePolicies(
	policyDocs []string, servicePrincipal, action, resource string, ctx ConditionContext,
) EvaluationResult {
	result := EvalImplicitDeny

	for _, doc := range policyDocs {
		pd, ok := parsePolicyDocumentCached(SubstituteVariables(doc, ctx))
		if !ok {
			continue
		}

		for _, stmt := range pd.Statement {
			if !principalCoversService(stmt.Principal, servicePrincipal) ||
				!stmtActionMatches(stmt, action) || !stmtResourceMatches(stmt, resource) ||
				!conditionMatches(stmt.Condition, ctx) {
				continue
			}

			switch strings.ToUpper(stmt.Effect) {
			case "DENY":
				return EvalExplicitDeny
			case "ALLOW":
				result = EvalAllow
			}
		}
	}

	return result
}

func principalCoversService(principal any, servicePrincipal string) bool {
	switch p := principal.(type) {
	case string:
		return p == "*"
	case map[string]any:
		for _, s := range anyStrings(p["Service"]) {
			if s == "*" || strings.EqualFold(s, servicePrincipal) {
				return true
			}
		}

		return slices.Contains(anyStrings(p["AWS"]), "*")
	}

	return false
}
