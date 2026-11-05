package iam

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

// RCPProvider resolves the resource control policies governing resources owned by an account.
type RCPProvider interface {
	// RCPsForAccount returns the levels root-first. ok is false when RCPs do not
	// apply: no organization, RCPs not enabled, the management account, or an
	// account outside the organization.
	RCPsForAccount(accountID string) (levels []SCPLevel, ok bool)
	// OrgIDForAccount returns the organization ID that accountID belongs to, or "".
	OrgIDForAccount(accountID string) string
}

// ResourceOwnerResolver names the account owning a resource whose ARN carries no account ID (S3 buckets).
type ResourceOwnerResolver interface {
	ResourceOwnerAccount(ctx context.Context, resourceARN string) (accountID string, ok bool)
}

const ctxKeyPrincipalOrgID = "aws:principalorgid"

// rcpSupportedServices are the action prefixes listed under "List of AWS services that support RCPs"
// in the Organizations User Guide.
const rcpSupportedServices = "appstream dsql clouddirectory cloudfront cloudsearch logs networkmonitor " +
	"cognito-identity cognito-idp comprehend comprehendmedical firehose dynamodb dax autoscaling ecr ecr-public " +
	"gamelift inspector-scan kendra kinesisvideo memorydb aoss personalize polly s3 swf sqs textract " +
	"timestream-influxdb transcribe translate workspaces appconfig autoscaling-plans budgets servicediscovery " +
	"cloudtrail-data codeartifact codebuild codecommit codepipeline compute-optimizer cost-optimization-hub " +
	"events fms fis health rolesanywhere kms opensearch pricing pca-connector-ad resource-groups secretsmanager " +
	"sts signin support transfer notifications wafv2 xray"

// RCPSupportsService reports whether service (an IAM action prefix) supports RCPs.
func RCPSupportsService(service string) bool {
	return slices.Contains(strings.Fields(rcpSupportedServices), strings.ToLower(service))
}

func (cfg EnforcementConfig) rcpProvider() RCPProvider {
	p, _ := cfg.SCPs.(RCPProvider)

	return p
}

// resourceOwnerAccount is the account owning resourceARN: the ARN's own account
// segment, else the first resolver that knows the resource.
func (cfg EnforcementConfig) resourceOwnerAccount(ctx context.Context, resourceARN string) (string, bool) {
	if acct, ok := arnAccountID(resourceARN); ok {
		return acct, true
	}

	for _, r := range cfg.ResourceOwners {
		if acct, ok := r.ResourceOwnerAccount(ctx, resourceARN); ok && acct != "" {
			return acct, true
		}
	}

	return "", false
}

// withPrincipalOrg exposes aws:PrincipalOrgID for principals of an organization account.
func withPrincipalOrg(p RCPProvider, condCtx ConditionContext) ConditionContext {
	orgID := p.OrgIDForAccount(condCtx.PrincipalAccount)
	if orgID == "" || condCtx.PrincipalAccount == "" {
		return condCtx
	}

	extra := make(map[string]string, len(condCtx.Extra)+1)
	maps.Copy(extra, condCtx.Extra)

	extra[ctxKeyPrincipalOrgID] = orgID
	condCtx.Extra = extra

	return condCtx
}

// rcpVerdict evaluates the RCPs of the account owning resourceARN. denied is false
// when RCPs do not apply (unsupported service, service-linked role, management or non-member owner).
func (cfg EnforcementConfig) rcpVerdict(
	ctx context.Context, action, resourceARN, resource string, condCtx ConditionContext,
) (scpVerdict, bool) {
	p := cfg.rcpProvider()
	svc, _, _ := strings.Cut(action, ":")

	if p == nil || scpExemptPrincipal(condCtx.PrincipalARN) || !RCPSupportsService(svc) {
		return scpVerdict{}, false
	}

	owner, ok := cfg.resourceOwnerAccount(ctx, resourceARN)
	if !ok {
		return scpVerdict{}, false
	}

	levels, ok := p.RCPsForAccount(owner)
	if !ok || len(levels) == 0 {
		return scpVerdict{}, false
	}

	v := evaluateSCPs(levels, action, resource, withPrincipalOrg(p, condCtx))
	v.rcp = true
	v.resource = resource

	return v, v.denied
}

// applyRCPs turns an allow into an RCP deny when the resource owner's RCPs do not permit the request.
func (cfg EnforcementConfig) applyRCPs(
	ctx context.Context, dec requestDecision, action, resourceARN, resource string, condCtx ConditionContext,
) (requestDecision, string) {
	if !dec.allow {
		return dec, ""
	}

	v, denied := cfg.rcpVerdict(ctx, action, resourceARN, resource, condCtx)
	if !denied {
		return dec, ""
	}

	return v.toDecision(), scpDeniedMessage(condCtx.PrincipalARN, action, v)
}

func (v scpVerdict) rcpDecision() requestDecision {
	d := requestDecision{
		reason: reasonResourceControlPolicy, source: sourceRCP, match: v.match, explicit: v.explicit,
		policy: &iampolicystream.PolicyRef{Type: iampolicystream.PolicyRCP, Arn: v.policyArn},
	}

	if !v.explicit {
		d.match = EvalMatch{PolicyIndex: -1, StatementIdx: -1}
	}

	return d
}
