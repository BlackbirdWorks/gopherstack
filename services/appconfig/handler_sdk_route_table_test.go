package appconfig_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real AppConfig
// operation, extracted from appconfig@v1.48.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// DeleteDeploymentStrategy is a real AWS API typo: it alone uses
// "/deployementstrategies/{Id}" (extra "e") while every other
// deployment-strategy op uses "/deploymentstrategies" -- verified directly
// in serializers.go, not an extraction artifact.
//
// DeploymentNumber, VersionNumber, and Run are genuinely wire-serialized as
// integers (encoder.SetURI(...).Integer(...) in serializers.go, not
// .String()), and this handler's route parser requires them to parse as
// int32 to resolve GetDeployment/StopDeployment,
// Get/DeleteHostedConfigurationVersion, and the four Run-numbered
// experiment-run ops -- so those entries use a numeric literal ("42")
// instead of PLACEHOLDER; a non-numeric value there would never arrive from
// a real SDK client.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApplication", "POST", "/applications"},
		{"CreateConfigurationProfile", "POST", "/applications/PLACEHOLDER/configurationprofiles"},
		{"CreateDeploymentStrategy", "POST", "/deploymentstrategies"},
		{"CreateEnvironment", "POST", "/applications/PLACEHOLDER/environments"},
		{"CreateExperimentDefinition", "POST", "/applications/PLACEHOLDER/experimentdefinitions"},
		{"CreateExtension", "POST", "/extensions"},
		{"CreateExtensionAssociation", "POST", "/extensionassociations"},
		{
			"CreateHostedConfigurationVersion", "POST",
			"/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER/hostedconfigurationversions",
		},
		{"DeleteApplication", "DELETE", "/applications/PLACEHOLDER"},
		{"DeleteConfigurationProfile", "DELETE", "/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER"},
		{"DeleteDeploymentStrategy", "DELETE", "/deployementstrategies/PLACEHOLDER"},
		{"DeleteEnvironment", "DELETE", "/applications/PLACEHOLDER/environments/PLACEHOLDER"},
		{"DeleteExperimentDefinition", "DELETE", "/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER"},
		{"DeleteExtension", "DELETE", "/extensions/PLACEHOLDER"},
		{"DeleteExtensionAssociation", "DELETE", "/extensionassociations/PLACEHOLDER"},
		{
			"DeleteHostedConfigurationVersion", "DELETE",
			"/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER/hostedconfigurationversions/42",
		},
		{"GetAccountSettings", "GET", "/settings"},
		{"GetApplication", "GET", "/applications/PLACEHOLDER"},
		{"GetConfiguration", "GET", "/applications/PLACEHOLDER/environments/PLACEHOLDER/configurations/PLACEHOLDER"},
		{"GetConfigurationProfile", "GET", "/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER"},
		{"GetDeployment", "GET", "/applications/PLACEHOLDER/environments/PLACEHOLDER/deployments/42"},
		{"GetDeploymentStrategy", "GET", "/deploymentstrategies/PLACEHOLDER"},
		{"GetEnvironment", "GET", "/applications/PLACEHOLDER/environments/PLACEHOLDER"},
		{"GetExperimentDefinition", "GET", "/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER"},
		{
			"GetExperimentRun", "GET",
			"/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns/42",
		},
		{"GetExtension", "GET", "/extensions/PLACEHOLDER"},
		{"GetExtensionAssociation", "GET", "/extensionassociations/PLACEHOLDER"},
		{
			"GetHostedConfigurationVersion", "GET",
			"/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER/hostedconfigurationversions/42",
		},
		{"ListApplications", "GET", "/applications"},
		{"ListConfigurationProfiles", "GET", "/applications/PLACEHOLDER/configurationprofiles"},
		{"ListDeploymentStrategies", "GET", "/deploymentstrategies"},
		{"ListDeployments", "GET", "/applications/PLACEHOLDER/environments/PLACEHOLDER/deployments"},
		{"ListEnvironments", "GET", "/applications/PLACEHOLDER/environments"},
		{"ListExperimentDefinitions", "GET", "/experimentdefinitions"},
		{
			"ListExperimentRunEvents", "GET",
			"/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns/42/events",
		},
		{
			"ListExperimentRuns", "GET",
			"/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns",
		},
		{"ListExtensionAssociations", "GET", "/extensionassociations"},
		{"ListExtensions", "GET", "/extensions"},
		{
			"ListHostedConfigurationVersions", "GET",
			"/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER/hostedconfigurationversions",
		},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"StartDeployment", "POST", "/applications/PLACEHOLDER/environments/PLACEHOLDER/deployments"},
		{"StartExperimentRun", "POST", "/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns"},
		{"StopDeployment", "DELETE", "/applications/PLACEHOLDER/environments/PLACEHOLDER/deployments/42"},
		{
			"StopExperimentRun", "PATCH",
			"/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns/42/stop",
		},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAccountSettings", "PATCH", "/settings"},
		{"UpdateApplication", "PATCH", "/applications/PLACEHOLDER"},
		{"UpdateConfigurationProfile", "PATCH", "/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER"},
		{"UpdateDeploymentStrategy", "PATCH", "/deploymentstrategies/PLACEHOLDER"},
		{"UpdateEnvironment", "PATCH", "/applications/PLACEHOLDER/environments/PLACEHOLDER"},
		{"UpdateExperimentDefinition", "PATCH", "/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER"},
		{
			"UpdateExperimentRun", "PATCH",
			"/applications/PLACEHOLDER/experimentdefinitions/PLACEHOLDER/experimentruns/42/update",
		},
		{"UpdateExtension", "PATCH", "/extensions/PLACEHOLDER"},
		{"UpdateExtensionAssociation", "PATCH", "/extensionassociations/PLACEHOLDER"},
		{
			"ValidateConfiguration", "POST",
			"/applications/PLACEHOLDER/configurationprofiles/PLACEHOLDER/validators",
		},
	}
}

// TestExtractOperation_SDKRouteTable drives every real AppConfig op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 56 appconfig ops from the pinned SDK and confirmed the
// existing route table already correct, including the real AWS
// DeleteDeploymentStrategy path typo and the account-wide (non-nested)
// ListExperimentDefinitions path -- both already deliberately handled with
// doc comments in handler.go before this pass.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "not found" body that appConfigDispatch's
// map-lookup miss emits (handler.go:977-995) -- guarding against an
// operation name that resolves correctly but has no entry in
// appConfigDispatch (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), `"not found"`,
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
