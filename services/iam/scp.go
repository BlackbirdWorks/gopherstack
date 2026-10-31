package iam

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

// SCPPolicy is one service control policy attached to a level.
type SCPPolicy struct {
	ARN      string
	Document string
}

// SCPLevel is the SCPs attached directly to one node (root, OU or account) on an account's path.
type SCPLevel struct {
	TargetID string
	Policies []SCPPolicy
}

// SCPProvider resolves the SCPs governing an account.
type SCPProvider interface {
	// SCPsForAccount returns the levels root-first. ok is false when SCPs do not
	// apply: no organization, SCPs not enabled on the root, a management
	// account, or an account outside the organization.
	SCPsForAccount(accountID string) (levels []SCPLevel, ok bool)
}

const serviceLinkedRoleNamePrefix = "AWSServiceRoleFor"

// scpExemptPrincipal reports whether SCPs never restrict principalArn: service-linked roles.
func scpExemptPrincipal(principalArn string) bool {
	if strings.Contains(principalArn, ":role/aws-service-role/") {
		return true
	}

	return strings.HasPrefix(extractRoleNameFromArn(principalArn), serviceLinkedRoleNamePrefix)
}

// scpVerdict is the outcome of evaluating the SCP chain.
type scpVerdict struct {
	policyArn string
	match     EvalMatch
	denied    bool
	explicit  bool
}

// evaluateSCPs applies the documented SCP rules: any explicit Deny at any level
// denies; otherwise every level needs at least one Allow.
func evaluateSCPs(levels []SCPLevel, action, resource string, condCtx ConditionContext) scpVerdict {
	var implicit bool

	for _, lvl := range levels {
		docs := make([]string, len(lvl.Policies))
		for i, p := range lvl.Policies {
			docs[i] = arn.Canonical(p.Document)
		}

		res, m := ExplainPolicies(docs, action, resource, condCtx)

		switch res {
		case EvalExplicitDeny:
			v := scpVerdict{denied: true, explicit: true, match: m}
			if m.PolicyIndex >= 0 && m.PolicyIndex < len(lvl.Policies) {
				v.policyArn = lvl.Policies[m.PolicyIndex].ARN
			}

			return v
		case EvalImplicitDeny:
			implicit = true
		case EvalAllow:
		}
	}

	return scpVerdict{denied: implicit}
}

// scpDecision gates an otherwise-allowed request on the caller account's SCPs.
func (cfg EnforcementConfig) scpDecision(
	action, resource string, condCtx ConditionContext,
) (scpVerdict, bool) {
	if cfg.SCPs == nil || condCtx.PrincipalAccount == "" || scpExemptPrincipal(condCtx.PrincipalARN) {
		return scpVerdict{}, false
	}

	levels, ok := cfg.SCPs.SCPsForAccount(condCtx.PrincipalAccount)
	if !ok || len(levels) == 0 {
		return scpVerdict{}, false
	}

	v := evaluateSCPs(levels, action, resource, condCtx)

	return v, v.denied
}

// scpDeniedMessage is the AccessDenied text AWS documents for SCP denials
// (IAM User Guide, "Troubleshoot access denied error messages").
func scpDeniedMessage(principalArn, action string, v scpVerdict) string {
	msg := "User: " + principalArn + " is not authorized to perform: " + action

	if !v.explicit {
		return msg + " because no service control policy allows the " + action + " action"
	}

	msg += " with an explicit deny in a service control policy"
	if v.policyArn != "" {
		msg += ": " + v.policyArn
	}

	return msg
}

func (v scpVerdict) toDecision() requestDecision {
	d := requestDecision{
		reason: reasonServiceControlPolicy, source: sourceSCP, match: v.match, explicit: v.explicit,
		policy: &iampolicystream.PolicyRef{Type: iampolicystream.PolicySCP, Arn: v.policyArn},
	}

	if !v.explicit {
		d.match = EvalMatch{PolicyIndex: -1, StatementIdx: -1}
	}

	return d
}

