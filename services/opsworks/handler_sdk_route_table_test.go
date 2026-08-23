package opsworks_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteOps is the authoritative operation list for OpsWorks, extracted
// from opsworks@v1.31.0 serializers.go: every real op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("OpsWorks_20130218.<Op>")
// and always POSTs to "/" -- OpsWorks is JSON-RPC, so unlike a REST-family
// service there is no path template to drift, only the target-header op
// name. Cross-checked all three of OpsWorks' op-name tables against each
// other and against this list (gopherstack-jqh2 shape 3: an
// isXPath-switch/buildOps-map/GetSupportedOperations-list trio must agree):
// GetSupportedOperations' literal slice and buildOps' dispatch map both
// match this list 74/74, zero mismatches in any direction.
//
// Regenerate by grepping serializers.go for every
// `"OpsWorks_20130218.` and pulling the trailing op name.
func sdkRouteOps() []string {
	return []string{
		"AssignInstance",
		"AssignVolume",
		"AssociateElasticIp",
		"AttachElasticLoadBalancer",
		"CloneStack",
		"CreateApp",
		"CreateDeployment",
		"CreateInstance",
		"CreateLayer",
		"CreateStack",
		"CreateUserProfile",
		"DeleteApp",
		"DeleteInstance",
		"DeleteLayer",
		"DeleteStack",
		"DeleteUserProfile",
		"DeregisterEcsCluster",
		"DeregisterElasticIp",
		"DeregisterInstance",
		"DeregisterRdsDbInstance",
		"DeregisterVolume",
		"DescribeAgentVersions",
		"DescribeApps",
		"DescribeCommands",
		"DescribeDeployments",
		"DescribeEcsClusters",
		"DescribeElasticIps",
		"DescribeElasticLoadBalancers",
		"DescribeInstances",
		"DescribeLayers",
		"DescribeLoadBasedAutoScaling",
		"DescribeMyUserProfile",
		"DescribeOperatingSystems",
		"DescribePermissions",
		"DescribeRaidArrays",
		"DescribeRdsDbInstances",
		"DescribeServiceErrors",
		"DescribeStackProvisioningParameters",
		"DescribeStackSummary",
		"DescribeStacks",
		"DescribeTimeBasedAutoScaling",
		"DescribeUserProfiles",
		"DescribeVolumes",
		"DetachElasticLoadBalancer",
		"DisassociateElasticIp",
		"GetHostnameSuggestion",
		"GrantAccess",
		"ListTags",
		"RebootInstance",
		"RegisterEcsCluster",
		"RegisterElasticIp",
		"RegisterInstance",
		"RegisterRdsDbInstance",
		"RegisterVolume",
		"SetLoadBasedAutoScaling",
		"SetPermission",
		"SetTimeBasedAutoScaling",
		"StartInstance",
		"StartStack",
		"StopInstance",
		"StopStack",
		"TagResource",
		"UnassignInstance",
		"UnassignVolume",
		"UntagResource",
		"UpdateApp",
		"UpdateElasticIp",
		"UpdateInstance",
		"UpdateLayer",
		"UpdateMyUserProfile",
		"UpdateRdsDbInstance",
		"UpdateStack",
		"UpdateUserProfile",
		"UpdateVolume",
	}
}

// TestExtractOperation_SDKRouteTable drives every real OpsWorks op's
// authoritative X-Amz-Target header (see sdkRouteOps) through
// ExtractOperation and asserts the dispatch table resolves it -- not the
// "unknown action" ValidationException dispatch's map-lookup miss emits
// (handler.go:262-263). gopherstack-jqh2 pass 4: re-extracted all 74
// OpsWorks ops from the pinned SDK and confirmed GetSupportedOperations and
// buildOps both already match exactly.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", "OpsWorks_20130218."+op)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			require.Equal(t, op, h.ExtractOperation(c))

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"op=%s: dispatched to the unknown-action handler", op)
		})
	}
}
