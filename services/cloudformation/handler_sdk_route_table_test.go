package cloudformation_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative Action value for every real
// CloudFormation operation, extracted from cloudformation@v1.76.1
// serializers.go: each op's awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- CloudFormation
// is AWS Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// form field. ExtractOperation and Handler() both read r.Form.Get("Action")
// directly, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case) -- not a
// route-template mismatch. Query protocol is case-insensitive for XML field
// names on the wire, but gopherstack's own dispatch is a Go string
// switch/map, which is always exact-match regardless of protocol.
//
// This table covers all 90 real CloudFormation ops (cloudformation@v1.76.1),
// which is also gopherstack's full implemented set (h.GetSupportedOperations(),
// 90/90) -- confirmed by diffing the dispatch-table keys (89 switch cases
// across 11 chained dispatchXOps functions, plus one "DescribeType"
// if-branch in Handler.dispatch that isn't a switch case) against this exact
// list, zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"ActivateOrganizationsAccess",
		"ActivateType",
		"BatchDescribeTypeConfigurations",
		"CancelUpdateStack",
		"ContinueUpdateRollback",
		"CreateChangeSet",
		"CreateGeneratedTemplate",
		"CreateStack",
		"CreateStackInstances",
		"CreateStackRefactor",
		"CreateStackSet",
		"DeactivateOrganizationsAccess",
		"DeactivateType",
		"DeleteChangeSet",
		"DeleteGeneratedTemplate",
		"DeleteStack",
		"DeleteStackInstances",
		"DeleteStackSet",
		"DeregisterType",
		"DescribeAccountLimits",
		"DescribeChangeSet",
		"DescribeChangeSetHooks",
		"DescribeEvents",
		"DescribeGeneratedTemplate",
		"DescribeOrganizationsAccess",
		"DescribePublisher",
		"DescribeResourceScan",
		"DescribeStackDriftDetectionStatus",
		"DescribeStackEvents",
		"DescribeStackInstance",
		"DescribeStackRefactor",
		"DescribeStackResource",
		"DescribeStackResourceDrifts",
		"DescribeStackResources",
		"DescribeStackSet",
		"DescribeStackSetOperation",
		"DescribeStacks",
		"DescribeType",
		"DescribeTypeRegistration",
		"DetectStackDrift",
		"DetectStackResourceDrift",
		"DetectStackSetDrift",
		"EstimateTemplateCost",
		"ExecuteChangeSet",
		"ExecuteStackRefactor",
		"GetGeneratedTemplate",
		"GetHookResult",
		"GetStackPolicy",
		"GetTemplate",
		"GetTemplateSummary",
		"ImportStacksToStackSet",
		"ListChangeSets",
		"ListExports",
		"ListGeneratedTemplates",
		"ListHookResults",
		"ListImports",
		"ListResourceScanRelatedResources",
		"ListResourceScanResources",
		"ListResourceScans",
		"ListStackInstanceResourceDrifts",
		"ListStackInstances",
		"ListStackRefactorActions",
		"ListStackRefactors",
		"ListStackResources",
		"ListStackSetAutoDeploymentTargets",
		"ListStackSetOperationResults",
		"ListStackSetOperations",
		"ListStackSets",
		"ListStacks",
		"ListTypeRegistrations",
		"ListTypeVersions",
		"ListTypes",
		"PublishType",
		"RecordHandlerProgress",
		"RegisterPublisher",
		"RegisterType",
		"RollbackStack",
		"SetStackPolicy",
		"SetTypeConfiguration",
		"SetTypeDefaultVersion",
		"SignalResource",
		"StartResourceScan",
		"StopStackSetOperation",
		"TestType",
		"UpdateGeneratedTemplate",
		"UpdateStack",
		"UpdateStackInstances",
		"UpdateStackSet",
		"UpdateTerminationProtection",
		"ValidateTemplate",
	}
}

// TestExtractOperation_SDKRouteTable drives every real CloudFormation
// operation's authoritative Action value through ExtractOperation and
// Handler(), asserting the form field resolves to the right op name and that
// Handler() does not fall through to the "unknown action: " sentinel that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: ",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
