package scheduler_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// sdkRouteCases is the authoritative method+path for every real EventBridge
// Scheduler operation, extracted from scheduler@v1.20.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for the {Name}/{ResourceArn} URI label -- parseSchedulerRESTPath
// (handler.go) never validates identifier shape, so the literal value
// doesn't matter here, only path depth and static segments. 12 real ops
// here, matching scheduler's real op count exactly (also matches
// GetSupportedOperations's own 12 entries one-for-one).
//
// A systematic check for a shared method+path across all 12 ops found zero
// collisions: every op has its own unique (method, path) pair -- even
// GetSchedule/DeleteSchedule/UpdateSchedule sharing "/schedules/{Name}" are
// disambiguated by method (GET/DELETE/PUT), which
// parseSchedulerRESTPath/handleREST already switch on -- so no *required
// dynamic* (non-template) member -- the s3/glacier vacuity-trap class --
// was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateSchedule", "POST", "/schedules/PLACEHOLDER"},
		{"CreateScheduleGroup", "POST", "/schedule-groups/PLACEHOLDER"},
		{"DeleteSchedule", "DELETE", "/schedules/PLACEHOLDER"},
		{"DeleteScheduleGroup", "DELETE", "/schedule-groups/PLACEHOLDER"},
		{"GetSchedule", "GET", "/schedules/PLACEHOLDER"},
		{"GetScheduleGroup", "GET", "/schedule-groups/PLACEHOLDER"},
		{"ListScheduleGroups", "GET", "/schedule-groups"},
		{"ListSchedules", "GET", "/schedules"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateSchedule", "PUT", "/schedules/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Scheduler op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseSchedulerRESTPath resolves it to the right op, all 12 ops
// against scheduler's real op count. It then drives the same request
// through the real Handler() and asserts the response body is not the exact
// plain-text literal "not found" that handleREST's restOpUnknown branch
// (handler.go) emits via c.String(http.StatusNotFound, "not found") when
// parseSchedulerRESTPath fails to match.
//
// This handler has the two-miss-mode shape the campaign has burned agents
// on before (sesv2, mwaa): a second, JSON-target-only miss path exists in
// dispatch() (fmt.Errorf("%w: %s", errUnknownAction, action), surfaced as a
// ValidationException with message "unknown action: ..."), but it is
// unreachable from any REST request -- it only fires when an X-Amz-Target
// header selects an action absent from h.ops, and every op this table
// drives is dispatched via the REST branch, never that path, so only the
// first miss mode applies here.
//
// A bare substring check on "not found" is NOT safe for this service --
// unlike ram/grafana/batch/accessanalyzer/resourcegroups, scheduler's own
// legitimate ErrNotFound messages (tags.go, schedules.go,
// schedule_groups.go) are built as e.g. "ResourceNotFoundException:
// schedule PLACEHOLDER not found", which *contains* the same "not found"
// substring as the miss sentinel -- exactly the amplify/xray collision trap
// called out for this campaign. Every op below targets a PLACEHOLDER name
// that does not exist in the fresh backend, so GetSchedule/DeleteSchedule/
// etc. legitimately 404 with that substring present; the routing-miss
// sentinel is instead matched by *exact* body equality, since
// handleREST's Unknown branch writes bare text via c.String while every
// domain error (found or not) writes a JSON object via c.JSONBlob.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			h := scheduler.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotEqual(t, "not found", rec.Body.String(),
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
