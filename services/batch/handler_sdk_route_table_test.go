package batch_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Batch
// operation, extracted from batch@v1.68.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for the {resourceArn} URI label on the three tag ops -- ExtractOperation
// (handler.go) dispatches those purely off the "/v1/tags/" prefix plus
// method, never validating ARN shape, so the literal value doesn't matter
// here, only that the path matches Op. 45 real ops here, matching batch's
// real op count exactly (also matches GetSupportedOperations's own 45
// entries one-for-one).
//
// A systematic check for a shared method+path across all 45 ops found zero
// collisions: every non-tag op has its own unique literal path, and the
// three tag ops share "/v1/tags/{resourceArn}" but are disambiguated by
// method (GET/POST/DELETE), which ExtractOperation and Handler() both
// already switch on -- so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any route
// in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CancelJob", "POST", "/v1/canceljob"},
		{"CreateComputeEnvironment", "POST", "/v1/createcomputeenvironment"},
		{"CreateConsumableResource", "POST", "/v1/createconsumableresource"},
		{"CreateJobQueue", "POST", "/v1/createjobqueue"},
		{"CreateQuotaShare", "POST", "/v1/createquotashare"},
		{"CreateSchedulingPolicy", "POST", "/v1/createschedulingpolicy"},
		{"CreateServiceEnvironment", "POST", "/v1/createserviceenvironment"},
		{"DeleteComputeEnvironment", "POST", "/v1/deletecomputeenvironment"},
		{"DeleteConsumableResource", "POST", "/v1/deleteconsumableresource"},
		{"DeleteJobQueue", "POST", "/v1/deletejobqueue"},
		{"DeleteQuotaShare", "POST", "/v1/deletequotashare"},
		{"DeleteSchedulingPolicy", "POST", "/v1/deleteschedulingpolicy"},
		{"DeleteServiceEnvironment", "POST", "/v1/deleteserviceenvironment"},
		{"DeregisterJobDefinition", "POST", "/v1/deregisterjobdefinition"},
		{"DescribeComputeEnvironments", "POST", "/v1/describecomputeenvironments"},
		{"DescribeConsumableResource", "POST", "/v1/describeconsumableresource"},
		{"DescribeJobDefinitions", "POST", "/v1/describejobdefinitions"},
		{"DescribeJobQueues", "POST", "/v1/describejobqueues"},
		{"DescribeJobs", "POST", "/v1/describejobs"},
		{"DescribeQuotaShare", "POST", "/v1/describequotashare"},
		{"DescribeSchedulingPolicies", "POST", "/v1/describeschedulingpolicies"},
		{"DescribeServiceEnvironments", "POST", "/v1/describeserviceenvironments"},
		{"DescribeServiceJob", "POST", "/v1/describeservicejob"},
		{"GetJobQueueSnapshot", "POST", "/v1/getjobqueuesnapshot"},
		{"ListConsumableResources", "POST", "/v1/listconsumableresources"},
		{"ListJobs", "POST", "/v1/listjobs"},
		{"ListJobsByConsumableResource", "POST", "/v1/listjobsbyconsumableresource"},
		{"ListQuotaShares", "POST", "/v1/listquotashares"},
		{"ListSchedulingPolicies", "POST", "/v1/listschedulingpolicies"},
		{"ListServiceJobs", "POST", "/v1/listservicejobs"},
		{"ListTagsForResource", "GET", "/v1/tags/PLACEHOLDER"},
		{"RegisterJobDefinition", "POST", "/v1/registerjobdefinition"},
		{"SubmitJob", "POST", "/v1/submitjob"},
		{"SubmitServiceJob", "POST", "/v1/submitservicejob"},
		{"TagResource", "POST", "/v1/tags/PLACEHOLDER"},
		{"TerminateJob", "POST", "/v1/terminatejob"},
		{"TerminateServiceJob", "POST", "/v1/terminateservicejob"},
		{"UntagResource", "DELETE", "/v1/tags/PLACEHOLDER"},
		{"UpdateComputeEnvironment", "POST", "/v1/updatecomputeenvironment"},
		{"UpdateConsumableResource", "POST", "/v1/updateconsumableresource"},
		{"UpdateJobQueue", "POST", "/v1/updatejobqueue"},
		{"UpdateQuotaShare", "POST", "/v1/updatequotashare"},
		{"UpdateSchedulingPolicy", "POST", "/v1/updateschedulingpolicy"},
		{"UpdateServiceEnvironment", "POST", "/v1/updateserviceenvironment"},
		{"UpdateServiceJob", "POST", "/v1/updateservicejob"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Batch op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 45 ops against batch's real op
// count. It then drives the same request through the real Handler() and
// asserts the response does not contain the exact literal "unknown
// operation for path" that Handler's ops-map-miss branch (handler.go) emits
// under UnknownOperationException -- this service's only dispatch-miss
// mode, grepped across every non-test .go file in this package and
// confirmed to appear nowhere else (every domain error instead carries
// ClientException/InternalFailure built from a dynamic err.Error(), never
// this literal).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation for path",
				"method=%s path=%s op=%s: dispatched to the unmatched-action default", tc.method, tc.path, tc.op)
		})
	}
}
