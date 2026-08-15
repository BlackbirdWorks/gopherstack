package databrew_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real DataBrew
// operation, extracted from databrew@v1.42.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchDeleteRecipeVersion", "POST", "/recipes/PLACEHOLDER/batchDeleteRecipeVersion"},
		{"CreateDataset", "POST", "/datasets"},
		{"CreateProfileJob", "POST", "/profileJobs"},
		{"CreateProject", "POST", "/projects"},
		{"CreateRecipe", "POST", "/recipes"},
		{"CreateRecipeJob", "POST", "/recipeJobs"},
		{"CreateRuleset", "POST", "/rulesets"},
		{"CreateSchedule", "POST", "/schedules"},
		{"DeleteDataset", "DELETE", "/datasets/PLACEHOLDER"},
		{"DeleteJob", "DELETE", "/jobs/PLACEHOLDER"},
		{"DeleteProject", "DELETE", "/projects/PLACEHOLDER"},
		{"DeleteRecipeVersion", "DELETE", "/recipes/PLACEHOLDER/recipeVersion/PLACEHOLDER"},
		{"DeleteRuleset", "DELETE", "/rulesets/PLACEHOLDER"},
		{"DeleteSchedule", "DELETE", "/schedules/PLACEHOLDER"},
		{"DescribeDataset", "GET", "/datasets/PLACEHOLDER"},
		{"DescribeJob", "GET", "/jobs/PLACEHOLDER"},
		{"DescribeJobRun", "GET", "/jobs/PLACEHOLDER/jobRun/PLACEHOLDER"},
		{"DescribeProject", "GET", "/projects/PLACEHOLDER"},
		{"DescribeRecipe", "GET", "/recipes/PLACEHOLDER"},
		{"DescribeRuleset", "GET", "/rulesets/PLACEHOLDER"},
		{"DescribeSchedule", "GET", "/schedules/PLACEHOLDER"},
		{"ListDatasets", "GET", "/datasets"},
		{"ListJobRuns", "GET", "/jobs/PLACEHOLDER/jobRuns"},
		{"ListJobs", "GET", "/jobs"},
		{"ListProjects", "GET", "/projects"},
		{"ListRecipeVersions", "GET", "/recipeVersions"},
		{"ListRecipes", "GET", "/recipes"},
		{"ListRulesets", "GET", "/rulesets"},
		{"ListSchedules", "GET", "/schedules"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PublishRecipe", "POST", "/recipes/PLACEHOLDER/publishRecipe"},
		{"SendProjectSessionAction", "PUT", "/projects/PLACEHOLDER/sendProjectSessionAction"},
		{"StartJobRun", "POST", "/jobs/PLACEHOLDER/startJobRun"},
		{"StartProjectSession", "PUT", "/projects/PLACEHOLDER/startProjectSession"},
		{"StopJobRun", "POST", "/jobs/PLACEHOLDER/jobRun/PLACEHOLDER/stopJobRun"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateDataset", "PUT", "/datasets/PLACEHOLDER"},
		{"UpdateProfileJob", "PUT", "/profileJobs/PLACEHOLDER"},
		{"UpdateProject", "PUT", "/projects/PLACEHOLDER"},
		{"UpdateRecipe", "PUT", "/recipes/PLACEHOLDER"},
		{"UpdateRecipeJob", "PUT", "/recipeJobs/PLACEHOLDER"},
		{"UpdateRuleset", "PUT", "/rulesets/PLACEHOLDER"},
		{"UpdateSchedule", "PUT", "/schedules/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DataBrew op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 44 databrew ops from the pinned SDK and confirmed the
// existing route table already correct, including the generic
// "/jobs/{Name}" Delete/Describe path shared by both job subtypes
// (ProfileJob/RecipeJob use type-specific paths only for Create/Update) and
// every same-path/different-method collision.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown action: " error that h.dispatch's
// final default emits (handler.go:495) -- guarding against an action name
// that resolves correctly but has no matching case in any dispatchXxx
// family (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

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
			assert.NotContains(t, rec.Body.String(), "unknown action: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
