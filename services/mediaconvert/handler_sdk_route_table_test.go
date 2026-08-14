package mediaconvert_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real MediaConvert
// operation, extracted from mediaconvert@v1.97.1 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for any {Id}/{Name}/{Arn} URI label -- parseRoute (handler.go) does not
// validate ID shape, so the literal value doesn't matter here, only that the
// path matches Op. This table deliberately excludes opUpdateJob: it is not a
// real MediaConvert SDK operation (no UpdateJobInput/UpdateJobOutput/
// Client.UpdateJob exist in the pinned SDK, and real MediaConvert jobs are
// immutable once created) -- see handler.go's doc comment on opUpdateJob for
// the full citation. 34 real ops here, not 35.
//
// This service has two wire shapes easy to get backwards from REST habit,
// both already correctly handled in handler_jobs.go/handler_tags.go and
// pinned by their own cases below: CancelJob is DELETE (not POST), and
// UntagResource is PUT /tags/{Arn} (not DELETE) while TagResource is POST
// /tags with no Arn in the path at all.
//
// A systematic check for a shared method+path template across all 34 ops
// found zero collisions, so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any route
// in this table. The one prefix-collision trap in this service --
// "/2017-08-29/jobsQueries" being a superstring of the "/2017-08-29/jobs"
// prefix -- is already resolved correctly in parseRoute by checking
// jobsQueriesPath before jobsPath; StartJobsQuery is kept in this table as
// its own case specifically to guard that ordering.
//
// All 34 real ops were confirmed wired across dispatchReadOnly/
// dispatchReadOnlyNewOps/dispatchMutating/dispatchMutatingNewOps
// (handler.go) before writing this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateCertificate", "POST", "/2017-08-29/certificates"},
		{"CancelJob", "DELETE", "/2017-08-29/jobs/PLACEHOLDER"},
		{"CreateJob", "POST", "/2017-08-29/jobs"},
		{"CreateJobTemplate", "POST", "/2017-08-29/jobTemplates"},
		{"CreatePreset", "POST", "/2017-08-29/presets"},
		{"CreateQueue", "POST", "/2017-08-29/queues"},
		{"CreateResourceShare", "POST", "/2017-08-29/resourceShares"},
		{"DeleteJobTemplate", "DELETE", "/2017-08-29/jobTemplates/PLACEHOLDER"},
		{"DeletePolicy", "DELETE", "/2017-08-29/policy"},
		{"DeletePreset", "DELETE", "/2017-08-29/presets/PLACEHOLDER"},
		{"DeleteQueue", "DELETE", "/2017-08-29/queues/PLACEHOLDER"},
		{"DescribeEndpoints", "POST", "/2017-08-29/endpoints"},
		{"DisassociateCertificate", "DELETE", "/2017-08-29/certificates/PLACEHOLDER"},
		{"GetJob", "GET", "/2017-08-29/jobs/PLACEHOLDER"},
		{"GetJobTemplate", "GET", "/2017-08-29/jobTemplates/PLACEHOLDER"},
		{"GetJobsQueryResults", "GET", "/2017-08-29/jobsQueries/PLACEHOLDER"},
		{"GetPolicy", "GET", "/2017-08-29/policy"},
		{"GetPreset", "GET", "/2017-08-29/presets/PLACEHOLDER"},
		{"GetQueue", "GET", "/2017-08-29/queues/PLACEHOLDER"},
		{"ListJobTemplates", "GET", "/2017-08-29/jobTemplates"},
		{"ListJobs", "GET", "/2017-08-29/jobs"},
		{"ListPresets", "GET", "/2017-08-29/presets"},
		{"ListQueues", "GET", "/2017-08-29/queues"},
		{"ListTagsForResource", "GET", "/2017-08-29/tags/PLACEHOLDER"},
		{"ListVersions", "GET", "/2017-08-29/versions"},
		{"Probe", "POST", "/2017-08-29/probe"},
		{"PutPolicy", "PUT", "/2017-08-29/policy"},
		{"SearchJobs", "GET", "/2017-08-29/search"},
		{"StartJobsQuery", "POST", "/2017-08-29/jobsQueries"},
		{"TagResource", "POST", "/2017-08-29/tags"},
		{"UntagResource", "PUT", "/2017-08-29/tags/PLACEHOLDER"},
		{"UpdateJobTemplate", "PUT", "/2017-08-29/jobTemplates/PLACEHOLDER"},
		{"UpdatePreset", "PUT", "/2017-08-29/presets/PLACEHOLDER"},
		{"UpdateQueue", "PUT", "/2017-08-29/queues/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MediaConvert op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseRoute resolves it to the right op, all 34 ops against
// mediaconvert's real op count. It then drives the same request through the
// real Handler() and asserts it did not fall through to the "unknown
// operation: " prefix that dispatchMutatingNewOps's final default case
// (handler.go) emits under the "NotFoundException" code when no dispatch*
// function claims the route -- distinct from every domain-specific error
// this service writes via writeError (NotFoundException/ConflictException/
// BadRequestException/InternalError), whose messages come from ErrNotFound/
// ErrAlreadyExists/ErrValidation-wrapped errors and never contain this
// literal prefix.
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
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
