package resourcegroups_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// sdkRouteCases is the authoritative method+path for every real Resource
// Groups operation, extracted from resourcegroups@v1.36.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// the {Arn} URI label on the three tag ops (GetTags/Tag/Untag) --
// isResourceTagsPath (handler.go) only checks the "/resources/" prefix and
// "/tags" suffix, never ARN shape, so the literal value doesn't matter
// here, only that the path matches Op. 23 real ops here, matching
// resourcegroups's real op count exactly (also matches
// GetSupportedOperations's own 23 entries one-for-one).
//
// A systematic check for a shared method+path across all 23 ops found zero
// collisions: every static-path op has its own unique literal path, and the
// three tag ops share "/resources/{Arn}/tags" but are disambiguated by
// method (GET/PUT/PATCH), which ExtractOperation and handleResourceTags
// both already switch on -- so no *required dynamic* (non-template) member
// -- the s3/glacier vacuity-trap class -- was needed to disambiguate any
// route in this table.
//
// Note: Untag's real wire method is PATCH, not DELETE -- confirmed directly
// against serializers.go:1743 ("request.Method = \"PATCH\""), contradicting
// this package's own handleResourceTags comment ("AWS uses DELETE"). The
// routing itself is correct (PATCH is handled, and DELETE is accepted too
// as extra leniency), so this is a stale comment, not a routing bug -- not
// fixed here since it doesn't affect dispatch correctness.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CancelTagSyncTask", "POST", "/cancel-tag-sync-task"},
		{"CreateGroup", "POST", "/groups"},
		{"DeleteGroup", "POST", "/delete-group"},
		{"GetAccountSettings", "POST", "/get-account-settings"},
		{"GetGroup", "POST", "/get-group"},
		{"GetGroupConfiguration", "POST", "/get-group-configuration"},
		{"GetGroupQuery", "POST", "/get-group-query"},
		{"GetTagSyncTask", "POST", "/get-tag-sync-task"},
		{"GetTags", "GET", "/resources/PLACEHOLDER/tags"},
		{"GroupResources", "POST", "/group-resources"},
		{"ListGroupResources", "POST", "/list-group-resources"},
		{"ListGroupingStatuses", "POST", "/list-grouping-statuses"},
		{"ListGroups", "POST", "/groups-list"},
		{"ListTagSyncTasks", "POST", "/list-tag-sync-tasks"},
		{"PutGroupConfiguration", "POST", "/put-group-configuration"},
		{"SearchResources", "POST", "/resources/search"},
		{"StartTagSyncTask", "POST", "/start-tag-sync-task"},
		{"Tag", "PUT", "/resources/PLACEHOLDER/tags"},
		{"UngroupResources", "POST", "/ungroup-resources"},
		{"Untag", "PATCH", "/resources/PLACEHOLDER/tags"},
		{"UpdateAccountSettings", "POST", "/update-account-settings"},
		{"UpdateGroup", "POST", "/update-group"},
		{"UpdateGroupQuery", "POST", "/update-group-query"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Resource Groups op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 23 ops against resourcegroups's
// real op count. It then drives the same request through the real Handler()
// and asserts the response does not contain the exact literal
// "UnknownOperationException" that dispatch's ops-map-miss branch
// (handler.go) emits via ErrUnknownOperation -- this service's only
// dispatch-miss mode, grepped across every non-test .go file in this
// package and confirmed to appear nowhere else (every domain error instead
// carries NotFoundException/BadRequestException/InternalServerErrorException
// built from a dynamic err.Error(), never this literal).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			h := resourcegroups.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-action default", tc.method, tc.path, tc.op)
		})
	}
}
