package neptune_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// sdkRouteCases is the authoritative Action value for every real Neptune
// operation, extracted from neptune@v1.48.4 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets body.Key("Action").String("<Op>")
// and always POSTs to "/" -- Neptune is AWS Query/XML (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one form field. ExtractOperation and Handler()
// both read the Action value from the parsed form (r.Form.Get("Action")), so
// the class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 70 real Neptune ops (neptune@v1.48.4) -- confirmed
// by diffing both GetSupportedOperations() (a hand-written literal list) and
// the actual dispatch chain (dispatchDBClusterAction ->
// dispatchDBInstanceAction -> dispatchSubnetAndClusterParamGroupAction ->
// dispatchParameterGroupAction -> dispatchSnapshotAndEndpointAction ->
// dispatchEventSubscriptionAction -> dispatchGlobalClusterAndTagAction,
// each a separate switch chained via its own default case, the same
// extraction idiom used by ses's eight-deep helper chain) against this exact
// list: zero mismatches in either direction, no dead or excluded keys. The
// two diffs are genuinely independent -- GetSupportedOperations is a
// separately maintained literal, not built by ranging over the dispatch
// chain. Note per gopherstack-n1mb: neptune's handlers were edited recently
// for unrelated query-protocol list-parsing bugs (reading list members under
// "member" instead of the per-type element name), so its dispatch keys were
// not assumed correct from that pass -- they were independently re-extracted
// here.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AddRoleToDBCluster",
		"AddSourceIdentifierToSubscription",
		"AddTagsToResource",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CreateDBCluster",
		"CreateDBClusterEndpoint",
		"CreateDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"CreateDBInstance",
		"CreateDBParameterGroup",
		"CreateDBSubnetGroup",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"DeleteDBCluster",
		"DeleteDBClusterEndpoint",
		"DeleteDBClusterParameterGroup",
		"DeleteDBClusterSnapshot",
		"DeleteDBInstance",
		"DeleteDBParameterGroup",
		"DeleteDBSubnetGroup",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DescribeDBClusterEndpoints",
		"DescribeDBClusterParameterGroups",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBClusterSnapshots",
		"DescribeDBClusters",
		"DescribeDBEngineVersions",
		"DescribeDBInstances",
		"DescribeDBParameterGroups",
		"DescribeDBParameters",
		"DescribeDBSubnetGroups",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeGlobalClusters",
		"DescribeOrderableDBInstanceOptions",
		"DescribePendingMaintenanceActions",
		"DescribeValidDBInstanceModifications",
		"FailoverDBCluster",
		"FailoverGlobalCluster",
		"ListTagsForResource",
		"ModifyDBCluster",
		"ModifyDBClusterEndpoint",
		"ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBInstance",
		"ModifyDBParameterGroup",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"PromoteReadReplicaDBCluster",
		"RebootDBInstance",
		"RemoveFromGlobalCluster",
		"RemoveRoleFromDBCluster",
		"RemoveSourceIdentifierFromSubscription",
		"RemoveTagsFromResource",
		"ResetDBClusterParameterGroup",
		"ResetDBParameterGroup",
		"RestoreDBClusterFromSnapshot",
		"RestoreDBClusterToPointInTime",
		"StartDBCluster",
		"StopDBCluster",
		"SwitchoverGlobalCluster",
	}
}

// TestExtractOperation_SDKRouteTable drives every real Neptune operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "InvalidAction" sentinel (ErrUnknownAction,
// handler.go's dispatchGlobalClusterAndTagAction default case -- the last
// link in the chain) that a dispatch-table key mismatch would produce.
// ErrUnknownAction is a plain errors.New sentinel (not wrapped around a
// shared category like awserr.ErrInvalidParameter), and "InvalidAction" is
// not reused by any other entry in neptuneErrorCode's mapping table
// (grepped) -- so asserting on the wire code is safe here, unlike
// workmail/transfer, where the dispatch-miss sentinel shares its wire type
// with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := neptune.NewHandler(neptune.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			body := "Action=" + op + "&Version=2014-10-31"
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
