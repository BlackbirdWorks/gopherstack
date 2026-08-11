package iot

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// allIoTHTTPPathsRaw is every distinct HTTP path template used by the
// pinned iot@v1.77.4 SDK (github.com/aws/aws-sdk-go-v2/service/iot),
// extracted via:
//
//	grep -oP 'httpbinding.SplitURI\("\K[^"]+' serializers.go | sort -u
//
// in $(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/iot@v1.77.4.
// Regenerate this list (and re-run TestRouteMatcher_ExhaustiveCoverage) on
// every iot SDK version bump -- a newly added real path with no
// corresponding entry here is exactly the shape of gap this test exists to
// catch (matchIoTPath never listing "/tags" at all, gopherstack-2mwl).
//
// One newline-delimited literal, not a []string of 161 individually
// repeated literals: each of these path strings already appears (2x) in
// handler_routing.go and the matching resolve*Ops function, so a []string
// form trips goconst across a dozen unrelated production files the moment
// this test file adds a third occurrence. A single blob is one AST string
// node, so it can't collide.
const allIoTHTTPPathsRaw = `
/accept-certificate-transfer/{certificateId}
/active-violations
/attached-policies/{target}
/audit/configuration
/audit/findings
/audit/findings/{findingId}
/audit/mitigationactions/executions
/audit/mitigationactions/tasks
/audit/mitigationactions/tasks/{taskId}
/audit/mitigationactions/tasks/{taskId}/cancel
/audit/relatedResources
/audit/scheduledaudits
/audit/scheduledaudits/{scheduledAuditName}
/audit/suppressions/create
/audit/suppressions/delete
/audit/suppressions/describe
/audit/suppressions/list
/audit/suppressions/update
/audit/tasks
/audit/tasks/{taskId}
/audit/tasks/{taskId}/cancel
/authorizer/{authorizerName}
/authorizer/{authorizerName}/test
/authorizers
/behavior-model-training/summaries
/billing-groups
/billing-groups/addThingToBillingGroup
/billing-groups/{billingGroupName}
/billing-groups/{billingGroupName}/things
/billing-groups/removeThingFromBillingGroup
/cacertificate
/cacertificate/{certificateId}
/cacertificates
/cancel-certificate-transfer/{certificateId}
/certificate-providers
/certificate-providers/{certificateProviderName}
/certificate/register
/certificate/register-no-ca
/certificates
/certificates-by-ca/{caCertificateId}
/certificates/{certificateId}
/certificates-out-going
/command-executions
/command-executions/{executionId}
/commands
/commands/{commandId}
/confirmdestination/{confirmationToken+}
/custom-metric/{metricName}
/custom-metrics
/default-authorizer
/destinations
/destinations/{arn+}
/detect/mitigationactions/executions
/detect/mitigationactions/tasks
/detect/mitigationactions/tasks/{taskId}
/detect/mitigationactions/tasks/{taskId}/cancel
/dimensions
/dimensions/{name}
/domainConfigurations
/domainConfigurations/{domainConfigurationName}
/dynamic-thing-groups/{thingGroupName}
/effective-policies
/encryption-configuration
/endpoint
/event-configurations
/fleet-metric/{metricName}
/fleet-metrics
/indexing/config
/indices
/indices/buckets
/indices/cardinality
/indices/{indexName}
/indices/percentiles
/indices/search
/indices/statistics
/jobs
/jobs/{jobId}
/jobs/{jobId}/cancel
/jobs/{jobId}/job-document
/jobs/{jobId}/targets
/jobs/{jobId}/things
/job-templates
/job-templates/{jobTemplateId}
/keys-and-certificate
/loggingOptions
/managed-job-templates
/managed-job-templates/{templateName}
/metric-values
/mitigationactions/actions
/mitigationactions/actions/{actionName}
/otaUpdates
/otaUpdates/{otaUpdateId}
/package-configuration
/packages
/packages/{packageName}
/packages/{packageName}/versions
/packages/{packageName}/versions/{versionName}
/packages/{packageName}/versions/{versionName}/sbom
/packages/{packageName}/versions/{versionName}/sbom-validation-results
/policies
/policies/{policyName}
/policies/{policyName}/version
/policies/{policyName}/version/{policyVersionId}
/policy-principals
/policy-targets/{policyName}
/principal-policies
/principal-policies/{policyName}
/principals/things
/principals/things-v2
/provisioning-templates
/provisioning-templates/{templateName}
/provisioning-templates/{templateName}/provisioning-claim
/provisioning-templates/{templateName}/versions
/provisioning-templates/{templateName}/versions/{versionId}
/registrationcode
/reject-certificate-transfer/{certificateId}
/role-aliases
/role-aliases/{roleAlias}
/rules
/rules/{ruleName}
/rules/{ruleName}/disable
/rules/{ruleName}/enable
/security-profile-behaviors/validate
/security-profiles
/security-profiles-for-target
/security-profiles/{securityProfileName}
/security-profiles/{securityProfileName}/targets
/streams
/streams/{streamId}
/tags
/target-policies/{policyName}
/test-authorization
/thing-groups
/thing-groups/addThingToThingGroup
/thing-groups/removeThingFromThingGroup
/thing-groups/{thingGroupName}
/thing-groups/{thingGroupName}/things
/thing-groups/updateThingGroupsForThing
/thing-registration-tasks
/thing-registration-tasks/{taskId}
/thing-registration-tasks/{taskId}/cancel
/thing-registration-tasks/{taskId}/reports
/things
/things/{thingName}
/things/{thingName}/connectivity-data
/things/{thingName}/jobs
/things/{thingName}/jobs/{jobId}
/things/{thingName}/jobs/{jobId}/cancel
/things/{thingName}/jobs/{jobId}/executionNumber/{executionNumber}
/things/{thingName}/principals
/things/{thingName}/principals-v2
/things/{thingName}/thing-groups
/thing-types
/thing-types/{thingTypeName}
/thing-types/{thingTypeName}/deprecate
/transfer-certificate/{certificateId}
/untag
/v2LoggingLevel
/v2LoggingOptions
/violation-events
/violations/verification-state/{violationId}
`

// "/destinations" is CreateTopicRuleDestination's real path, which accepts
// no Tags -- out of scope regardless. Separately, NOT fixed here: this
// repo's own pathRuleDestinations constant is "/rule-destinations", which
// doesn't match this real path either -- a pre-existing, unrelated
// path-name bug for a future pass.
//
// knownUnmatchedIoTPathsRaw lists real paths from allIoTHTTPPathsRaw that
// matchIoTPath deliberately does not match yet, each with the reason it is
// out of gopherstack-2mwl's scope: none of these operations accept Tags at
// creation, so a real client 404ing on them is a genuine, separate,
// pre-existing "operation unreachable" bug -- tracked for follow-up, not
// fixed here. Listed explicitly (not omitted) so the gap is visible instead
// of silently absent, the same discipline sesv2/memorydb's exhaustiveness
// tests use for untaggable/known-gap resource kinds. One "path|reason" per
// line, in the same single-literal shape as allIoTHTTPPathsRaw and for the
// same goconst reason.
const knownUnmatchedIoTPathsRaw = `
/cacertificate|RegisterCACertificate/ListCACertificates: no Tags field
/cacertificate/{certificateId}|DescribeCACertificate/UpdateCACertificate/DeleteCACertificate: no Tags field
/cacertificates|ListCACertificates: no Tags field
/cancel-certificate-transfer/{certificateId}|CancelCertificateTransfer: no Tags field
/certificates-by-ca/{caCertificateId}|ListCertificatesByCA: no Tags field
/reject-certificate-transfer/{certificateId}|RejectCertificateTransfer: no Tags field
/transfer-certificate/{certificateId}|TransferCertificate: no Tags field
/keys-and-certificate|CreateKeysAndCertificate: no Tags field (api_op_CreateKeysAndCertificate.go)
/default-authorizer|SetDefaultAuthorizer/DescribeDefaultAuthorizer: no Tags field
/loggingOptions|SetLoggingOptions: no Tags field
/v2LoggingLevel|SetV2LoggingLevel: no Tags field
/v2LoggingOptions|SetV2LoggingOptions/GetV2LoggingOptions: no Tags field
/attached-policies/{target}|ListAttachedPolicies: no Tags field
/effective-policies|GetEffectivePolicies: no Tags field
/policy-principals|ListPolicyPrincipals: no Tags field
/policy-targets/{policyName}|ListTargetsForPolicy family: no Tags field
/principal-policies|ListPrincipalPolicies: no Tags field
/principals/things|ListPrincipalThings: no Tags field
/principals/things-v2|ListPrincipalThingsV2: no Tags field
/destinations|CreateTopicRuleDestination: no Tags field (see doc comment re pathRuleDestinations)
/destinations/{arn+}|GetTopicRuleDestination/DeleteTopicRuleDestination: no Tags field
/command-executions|ListCommandExecutions: no Tags field
/event-configurations|DescribeEventConfigurations/UpdateEventConfigurations: no Tags field
/package-configuration|GetPackageConfiguration/UpdatePackageConfiguration: no Tags field
/registrationcode|GetRegistrationCode/DeleteRegistrationCode: no Tags field
`

// pathParamPattern matches a Smithy URI label like "{certificateId}" or the
// greedy form "{arn+}".
var pathParamPattern = regexp.MustCompile(`\{[^}]+\}`)

// parseLines splits a newline-delimited raw block into trimmed, non-empty
// lines.
func parseLines(raw string) []string {
	var out []string

	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}

	return out
}

// parseKnownUnmatched parses knownUnmatchedIoTPathsRaw's "path|reason" lines
// into a lookup map.
func parseKnownUnmatched(raw string) map[string]string {
	out := make(map[string]string)

	for _, line := range parseLines(raw) {
		path, reason, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}

		out[path] = reason
	}

	return out
}

// TestRouteMatcher_ExhaustiveCoverage asserts matchIoTPath's verdict for
// every real HTTP path the pinned iot SDK uses (allIoTHTTPPathsRaw), rather
// than a hand-picked handful. A path this test doesn't know about at all
// can't be checked -- allIoTHTTPPathsRaw itself must be regenerated on SDK
// bumps (see its doc comment) -- but every path IN the list is asserted
// either matched or explicitly, visibly excused via
// knownUnmatchedIoTPathsRaw, so a route silently dropped from the gate (the
// exact gopherstack-2mwl shape: "/tags" was never listed at all) fails here
// instead of only failing a handler-level test that bypasses routing
// entirely.
//
// Chose a fixed embedded list over parsing the SDK's serializers.go at test
// time: parsing external module-cache source is slower, depends on
// GOMODCACHE being populated in CI, and breaks silently across SDK
// generator format changes -- a hardcoded, version-cited list matches this
// repo's existing exhaustiveness-test convention (sesv2/memorydb) and keeps
// the test itself deterministic and fast.
func TestRouteMatcher_ExhaustiveCoverage(t *testing.T) {
	t.Parallel()

	knownUnmatched := parseKnownUnmatched(knownUnmatchedIoTPathsRaw)

	for _, tmpl := range parseLines(allIoTHTTPPathsRaw) {
		concrete := pathParamPattern.ReplaceAllString(tmpl, "x")

		reason, isKnownGap := knownUnmatched[tmpl]
		got := matchIoTPath(concrete)

		if isKnownGap {
			assert.Falsef(t, got, "%s is listed in knownUnmatchedIoTPathsRaw (%s) but matchIoTPath now matches "+
				"it -- move it out of knownUnmatchedIoTPathsRaw", tmpl, reason)

			continue
		}

		assert.Truef(t, got, "%s does not match matchIoTPath -- either add it to the route gate "+
			"(handler_routing.go) or, if it genuinely accepts no Tags and is out of scope, "+
			"add it to knownUnmatchedIoTPathsRaw with a reason", tmpl)
	}
}
