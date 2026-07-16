package elasticache

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	elasticacheVersion = "2015-02-02"
	elasticacheNS      = "http://elasticache.amazonaws.com/doc/2015-02-02/"
	unknownOp          = "Unknown"
)

// Handler is the Echo HTTP handler for ElastiCache operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new ElastiCache handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ElastiCache" }

// GetSupportedOperations returns all supported ElastiCache operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCacheCluster",
		"DeleteCacheCluster",
		"DescribeCacheClusters",
		"ModifyCacheCluster",
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"CreateReplicationGroup",
		"DeleteReplicationGroup",
		"DescribeReplicationGroups",
		"ModifyReplicationGroup",
		"TestFailover",
		"CreateCacheParameterGroup",
		"DeleteCacheParameterGroup",
		"DescribeCacheParameterGroups",
		"ModifyCacheParameterGroup",
		"ResetCacheParameterGroup",
		"DescribeCacheParameters",
		"CreateCacheSubnetGroup",
		"DeleteCacheSubnetGroup",
		"DescribeCacheSubnetGroups",
		"ModifyCacheSubnetGroup",
		"CreateSnapshot",
		"DeleteSnapshot",
		"DescribeSnapshots",
		"CopySnapshot",
		"DescribeEvents",
		// New ops
		"CreateCacheSecurityGroup",
		"AuthorizeCacheSecurityGroupIngress",
		"CreateGlobalReplicationGroup",
		"CreateServerlessCache",
		"CreateServerlessCacheSnapshot",
		"CopyServerlessCacheSnapshot",
		"CreateUser",
		"BatchApplyUpdateAction",
		"BatchStopUpdateAction",
		"CompleteMigration",
		// Ops2
		"DeleteUser",
		"DescribeUsers",
		"ModifyUser",
		"CreateUserGroup",
		"DeleteUserGroup",
		"DescribeUserGroups",
		"ModifyUserGroup",
		"DeleteGlobalReplicationGroup",
		"DescribeGlobalReplicationGroups",
		"DisassociateGlobalReplicationGroup",
		"FailoverGlobalReplicationGroup",
		"IncreaseNodeGroupsInGlobalReplicationGroup",
		"DecreaseNodeGroupsInGlobalReplicationGroup",
		"ModifyGlobalReplicationGroup",
		"RebalanceSlotsInGlobalReplicationGroup",
		"DescribeReservedCacheNodes",
		"DescribeReservedCacheNodesOfferings",
		"PurchaseReservedCacheNodesOffering",
		"DeleteServerlessCache",
		"DeleteServerlessCacheSnapshot",
		"DescribeServerlessCaches",
		"DescribeServerlessCacheSnapshots",
		"ExportServerlessCacheSnapshot",
		"ModifyServerlessCache",
		"StartMigration",
		"TestMigration",
		"IncreaseReplicaCount",
		"DecreaseReplicaCount",
		"ModifyReplicationGroupShardConfiguration",
		"DescribeCacheEngineVersions",
		"RebootCacheCluster",
		"DeleteCacheSecurityGroup",
		"DescribeCacheSecurityGroups",
		"RevokeCacheSecurityGroupIngress",
		"DescribeEngineDefaultParameters",
		"DescribeServiceUpdates",
		"DescribeUpdateActions",
		"ListAllowedNodeTypeModifications",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elasticache" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ElastiCache instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a matcher for ElastiCache query-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}
		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == elasticacheVersion &&
			slices.Contains(h.GetSupportedOperations(), vals.Get("Action"))
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// ExtractOperation extracts the Action from the form body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOp
	}
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOp
	}
	action := vals.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}
	for _, key := range []string{
		"CacheClusterId",
		"ReplicationGroupId",
		"CacheParameterGroupName",
		"CacheSubnetGroupName",
		"SnapshotName",
		"ResourceName",
		"CacheSecurityGroupName",
		"GlobalReplicationGroupIdSuffix",
		"ServerlessCacheName",
		"ServerlessCacheSnapshotName",
		"UserId",
	} {
		if v := vals.Get(key); v != "" {
			return v
		}
	}

	return ""
}

type elasticacheActionFn func(ctx context.Context, c *echo.Context, form url.Values) error

func (h *Handler) regionFromRequest(c *echo.Context) string {
	return httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
}

func (h *Handler) dispatchTable() map[string]elasticacheActionFn {
	return map[string]elasticacheActionFn{
		"CreateCacheCluster":           h.createCacheCluster,
		"DeleteCacheCluster":           h.deleteCacheCluster,
		"DescribeCacheClusters":        h.describeCacheClusters,
		"ModifyCacheCluster":           h.modifyCacheCluster,
		"ListTagsForResource":          h.listTagsForResource,
		"AddTagsToResource":            h.addTagsToResource,
		"RemoveTagsFromResource":       h.removeTagsFromResource,
		"CreateReplicationGroup":       h.createReplicationGroup,
		"DeleteReplicationGroup":       h.deleteReplicationGroup,
		"DescribeReplicationGroups":    h.describeReplicationGroups,
		"ModifyReplicationGroup":       h.modifyReplicationGroup,
		"TestFailover":                 h.testFailoverReplicationGroup,
		"CreateCacheParameterGroup":    h.createCacheParameterGroup,
		"DeleteCacheParameterGroup":    h.deleteCacheParameterGroup,
		"DescribeCacheParameterGroups": h.describeCacheParameterGroups,
		"ModifyCacheParameterGroup":    h.modifyCacheParameterGroup,
		"ResetCacheParameterGroup":     h.resetCacheParameterGroup,
		"DescribeCacheParameters":      h.describeCacheParameters,
		"CreateCacheSubnetGroup":       h.createCacheSubnetGroup,
		"DeleteCacheSubnetGroup":       h.deleteCacheSubnetGroup,
		"DescribeCacheSubnetGroups":    h.describeCacheSubnetGroups,
		"ModifyCacheSubnetGroup":       h.modifyCacheSubnetGroup,
		"CreateSnapshot":               h.createSnapshot,
		"DeleteSnapshot":               h.deleteSnapshot,
		"DescribeSnapshots":            h.describeSnapshots,
		"CopySnapshot":                 h.copySnapshot,
		"DescribeEvents":               h.describeEvents,
		// New ops
		"CreateCacheSecurityGroup":           h.createCacheSecurityGroup,
		"AuthorizeCacheSecurityGroupIngress": h.authorizeCacheSecurityGroupIngress,
		"CreateGlobalReplicationGroup":       h.createGlobalReplicationGroup,
		"CreateServerlessCache":              h.createServerlessCache,
		"CreateServerlessCacheSnapshot":      h.createServerlessCacheSnapshot,
		"CopyServerlessCacheSnapshot":        h.copyServerlessCacheSnapshot,
		"CreateUser":                         h.createUser,
		"BatchApplyUpdateAction":             h.batchApplyUpdateAction,
		"BatchStopUpdateAction":              h.batchStopUpdateAction,
		"CompleteMigration":                  h.completeMigration,
		// Ops2
		"DeleteUser":                                 h.deleteUser,
		"DescribeUsers":                              h.describeUsers,
		"ModifyUser":                                 h.modifyUser,
		"CreateUserGroup":                            h.createUserGroup,
		"DeleteUserGroup":                            h.deleteUserGroup,
		"DescribeUserGroups":                         h.describeUserGroups,
		"ModifyUserGroup":                            h.modifyUserGroup,
		"DeleteGlobalReplicationGroup":               h.deleteGlobalReplicationGroup,
		"DescribeGlobalReplicationGroups":            h.describeGlobalReplicationGroups,
		"DisassociateGlobalReplicationGroup":         h.disassociateGlobalReplicationGroup,
		"FailoverGlobalReplicationGroup":             h.failoverGlobalReplicationGroup,
		"IncreaseNodeGroupsInGlobalReplicationGroup": h.increaseNodeGroupsInGlobalReplicationGroup,
		"DecreaseNodeGroupsInGlobalReplicationGroup": h.decreaseNodeGroupsInGlobalReplicationGroup,
		"ModifyGlobalReplicationGroup":               h.modifyGlobalReplicationGroup,
		"RebalanceSlotsInGlobalReplicationGroup":     h.rebalanceSlotsInGlobalReplicationGroup,
		"DescribeReservedCacheNodes":                 h.describeReservedCacheNodes,
		"DescribeReservedCacheNodesOfferings":        h.describeReservedCacheNodesOfferings,
		"PurchaseReservedCacheNodesOffering":         h.purchaseReservedCacheNodesOffering,
		"DeleteServerlessCache":                      h.deleteServerlessCache,
		"DeleteServerlessCacheSnapshot":              h.deleteServerlessCacheSnapshot,
		"DescribeServerlessCaches":                   h.describeServerlessCaches,
		"DescribeServerlessCacheSnapshots":           h.describeServerlessCacheSnapshots,
		"ExportServerlessCacheSnapshot":              h.exportServerlessCacheSnapshot,
		"ModifyServerlessCache":                      h.modifyServerlessCache,
		"StartMigration":                             h.startMigration,
		"TestMigration":                              h.testMigration,
		"IncreaseReplicaCount":                       h.increaseReplicaCount,
		"DecreaseReplicaCount":                       h.decreaseReplicaCount,
		"ModifyReplicationGroupShardConfiguration":   h.modifyReplicationGroupShardConfiguration,
		"DescribeCacheEngineVersions":                h.describeCacheEngineVersions,
		"RebootCacheCluster":                         h.rebootCacheCluster,
		"DeleteCacheSecurityGroup":                   h.deleteCacheSecurityGroup,
		"DescribeCacheSecurityGroups":                h.describeCacheSecurityGroups,
		"RevokeCacheSecurityGroupIngress":            h.revokeCacheSecurityGroupIngress,
		"DescribeEngineDefaultParameters":            h.describeEngineDefaultParameters,
		"DescribeServiceUpdates":                     h.describeServiceUpdates,
		"DescribeUpdateActions":                      h.describeUpdateActions,
		"ListAllowedNodeTypeModifications":           h.listAllowedNodeTypeModifications,
	}
}

// Handler returns the Echo handler function for ElastiCache requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot read body")
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot parse form")
		}
		action := vals.Get("Action")
		fn, ok := h.dispatchTable()[action]
		if !ok {
			return c.String(http.StatusBadRequest, "unknown action: "+action)
		}

		region := h.regionFromRequest(c)
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)

		return fn(ctx, c, vals)
	}
}

// parseFormTags extracts Tags.Tag.N.Key/Value pairs from a form.
func parseFormTags(form url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			break
		}
		val := form.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))
		tags[key] = val
	}

	return tags
}

// parsePagination extracts Marker and MaxRecords from query form values.
func parsePagination(form url.Values) (string, int) {
	marker := form.Get("Marker")
	maxRecords := 0

	if s := form.Get("MaxRecords"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxRecords = n
		}
	}

	return marker, maxRecords
}

// parseRepeatedField extracts a list of values from form fields with numeric suffixes.
// e.g., "ReplicationGroupIds.member.1", "ReplicationGroupIds.member.2", etc.
func parseRepeatedField(form url.Values, prefix string) []string {
	var items []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}
		items = append(items, v)
	}

	return items
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	type resetter interface{ Reset() }
	if r, ok := h.Backend.(resetter); ok {
		r.Reset()
	}
}

func xmlResp(c *echo.Context, status int, v any) error {
	data, err := xml.Marshal(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	c.Response().Header().Set("Content-Type", "text/xml; charset=utf-8")
	c.Response().WriteHeader(status)
	_, _ = c.Response().Write([]byte(xml.Header))
	_, _ = c.Response().Write(data)

	return nil
}

// xmlErrorDetail holds the fault type, code, and message for an ElastiCache XML
// error, matching the AWS query-protocol error envelope.
type xmlErrorDetail struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type xmlErrorResp struct {
	XMLName   xml.Name       `xml:"ErrorResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	Error     xmlErrorDetail `xml:"Error"`
	RequestID string         `xml:"RequestId"`
}

// faultType classifies an HTTP status into the AWS query-protocol fault Type.
// Client-side faults (4xx: validation, not-found, conflict) are "Sender";
// server-side faults (5xx) are "Receiver".
func faultType(status int) string {
	if status >= http.StatusInternalServerError {
		return "Receiver"
	}

	return "Sender"
}

// newRequestID returns a fresh correlation ID for a response, mirroring the
// per-request x-amzn-RequestId AWS attaches to every call.
func newRequestID() string {
	return uuid.NewString()
}

func xmlError(c *echo.Context, status int, code, message string) error {
	resp := xmlErrorResp{
		Xmlns:     elasticacheNS,
		RequestID: newRequestID(),
	}
	resp.Error.Type = faultType(status)
	resp.Error.Code = code
	resp.Error.Message = message

	return xmlResp(c, status, resp)
}
