package elasticache_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative Action value for every real ElastiCache
// operation, extracted from elasticache@v1.56.4 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- ElastiCache is
// AWS Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// form field. ExtractOperation and Handler() both read the Action field from
// the parsed form body directly, so the class of bug this table catches is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case) -- not a route-template mismatch. Query protocol is
// case-insensitive for XML field names on the wire, but gopherstack's own
// dispatch is a Go map lookup in dispatchTable(), which is always
// exact-match regardless of protocol.
//
// Handler()'s own RouteMatcher additionally requires the Action to already
// be in GetSupportedOperations() before a request is even routed here (see
// RouteMatcher, handler.go) -- this test calls ExtractOperation/Handler()
// directly, bypassing that pre-filter, so it still exercises dispatchTable()
// on its own terms.
//
// This table covers all 75 real ElastiCache ops (elasticache@v1.56.4)
// -- confirmed by diffing GetSupportedOperations() and the dispatchTable()
// map's 75 keys against this exact list: zero mismatches in either
// direction, and no dead or excluded keys found.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AddTagsToResource",
		"AuthorizeCacheSecurityGroupIngress",
		"BatchApplyUpdateAction",
		"BatchStopUpdateAction",
		"CompleteMigration",
		"CopyServerlessCacheSnapshot",
		"CopySnapshot",
		"CreateCacheCluster",
		"CreateCacheParameterGroup",
		"CreateCacheSecurityGroup",
		"CreateCacheSubnetGroup",
		"CreateGlobalReplicationGroup",
		"CreateReplicationGroup",
		"CreateServerlessCache",
		"CreateServerlessCacheSnapshot",
		"CreateSnapshot",
		"CreateUser",
		"CreateUserGroup",
		"DecreaseNodeGroupsInGlobalReplicationGroup",
		"DecreaseReplicaCount",
		"DeleteCacheCluster",
		"DeleteCacheParameterGroup",
		"DeleteCacheSecurityGroup",
		"DeleteCacheSubnetGroup",
		"DeleteGlobalReplicationGroup",
		"DeleteReplicationGroup",
		"DeleteServerlessCache",
		"DeleteServerlessCacheSnapshot",
		"DeleteSnapshot",
		"DeleteUser",
		"DeleteUserGroup",
		"DescribeCacheClusters",
		"DescribeCacheEngineVersions",
		"DescribeCacheParameterGroups",
		"DescribeCacheParameters",
		"DescribeCacheSecurityGroups",
		"DescribeCacheSubnetGroups",
		"DescribeEngineDefaultParameters",
		"DescribeEvents",
		"DescribeGlobalReplicationGroups",
		"DescribeReplicationGroups",
		"DescribeReservedCacheNodes",
		"DescribeReservedCacheNodesOfferings",
		"DescribeServerlessCaches",
		"DescribeServerlessCacheSnapshots",
		"DescribeServiceUpdates",
		"DescribeSnapshots",
		"DescribeUpdateActions",
		"DescribeUserGroups",
		"DescribeUsers",
		"DisassociateGlobalReplicationGroup",
		"ExportServerlessCacheSnapshot",
		"FailoverGlobalReplicationGroup",
		"IncreaseNodeGroupsInGlobalReplicationGroup",
		"IncreaseReplicaCount",
		"ListAllowedNodeTypeModifications",
		"ListTagsForResource",
		"ModifyCacheCluster",
		"ModifyCacheParameterGroup",
		"ModifyCacheSubnetGroup",
		"ModifyGlobalReplicationGroup",
		"ModifyReplicationGroup",
		"ModifyReplicationGroupShardConfiguration",
		"ModifyServerlessCache",
		"ModifyUser",
		"ModifyUserGroup",
		"PurchaseReservedCacheNodesOffering",
		"RebalanceSlotsInGlobalReplicationGroup",
		"RebootCacheCluster",
		"RemoveTagsFromResource",
		"ResetCacheParameterGroup",
		"RevokeCacheSecurityGroupIngress",
		"StartMigration",
		"TestFailover",
		"TestMigration",
	}
}

// TestExtractOperation_SDKRouteTable drives every real ElastiCache
// operation's authoritative Action value through ExtractOperation and
// Handler(), asserting the form field resolves to the right op name and that
// Handler() does not fall through to the "unknown action: " sentinel text
// (handler.go's Handler(), the sole production site that writes it) that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action:",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
