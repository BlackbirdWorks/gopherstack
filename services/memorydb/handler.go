package memorydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	memorydbService       = "memorydb"
	memorydbMatchPriority = 87
	memorydbTargetPrefix  = "AmazonMemoryDB."
)

// Handler is the HTTP handler for the AWS MemoryDB JSON 1.1 API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new MemoryDB handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "MemoryDB" }

// Reset clears all state by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns the list of supported MemoryDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchUpdateCluster",
		"CopySnapshot",
		"CreateACL",
		"CreateCluster",
		"CreateMultiRegionCluster",
		"CreateParameterGroup",
		"CreateSnapshot",
		"CreateSubnetGroup",
		"CreateUser",
		"DeleteACL",
		"DeleteCluster",
		"DeleteMultiRegionCluster",
		"DeleteParameterGroup",
		"DeleteSnapshot",
		"DeleteSubnetGroup",
		"DeleteUser",
		"DescribeACLs",
		"DescribeClusters",
		"DescribeEngineVersions",
		"DescribeEvents",
		"DescribeMultiRegionClusters",
		"DescribeMultiRegionParameterGroups",
		"DescribeMultiRegionParameters",
		"DescribeParameterGroups",
		"DescribeParameters",
		"DescribeReservedNodes",
		"DescribeReservedNodesOfferings",
		"DescribeServiceUpdates",
		"DescribeSnapshots",
		"DescribeSubnetGroups",
		"DescribeUsers",
		"FailoverShard",
		"ListAllowedMultiRegionClusterUpdates",
		"ListAllowedNodeTypeUpdates",
		"ListTags",
		"PurchaseReservedNodesOffering",
		"ResetParameterGroup",
		"TagResource",
		"UntagResource",
		"UpdateACL",
		"UpdateCluster",
		"UpdateMultiRegionCluster",
		"UpdateParameterGroup",
		"UpdateSubnetGroup",
		"UpdateUser",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return memorydbService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches MemoryDB JSON 1.1 API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		if strings.HasPrefix(target, memorydbTargetPrefix) {
			return true
		}

		return httputils.ExtractServiceFromRequest(c.Request()) == memorydbService
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return memorydbMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	if !strings.HasPrefix(target, memorydbTargetPrefix) {
		return "Unknown"
	}

	return strings.TrimPrefix(target, memorydbTargetPrefix)
}

// ExtractResource extracts the primary resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	resourceKeys := []string{
		"ClusterName", "ACLName", "SubnetGroupName",
		"UserName", "ParameterGroupName", "SnapshotName",
		"ResourceArn",
	}

	for _, key := range resourceKeys {
		if v, ok := data[key]; ok {
			if s, isStr := v.(string); isStr {
				return s
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for MemoryDB requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		target := c.Request().Header.Get("X-Amz-Target")

		if !strings.HasPrefix(target, memorydbTargetPrefix) {
			return writeError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValueException",
				"missing or invalid X-Amz-Target header",
			)
		}

		op := strings.TrimPrefix(target, memorydbTargetPrefix)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "memorydb: failed to read request body", "error", err)

			return writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "memorydb request", "op", op)

		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		regionCtx := context.WithValue(ctx, regionContextKey{}, region)

		return h.dispatch(regionCtx, c, op, body)
	}
}

// dispatch routes to the appropriate handler based on the operation name.
func (h *Handler) dispatch(ctx context.Context, c *echo.Context, op string, body []byte) error {
	if handled, result := h.dispatchCoreOps(ctx, c, op, body); handled {
		return result
	}

	if handled, result := h.dispatchNewOps(ctx, c, op, body); handled {
		return result
	}

	return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
}

// memorydbCoreOps maps operation names to handler functions for core MemoryDB operations.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var memorydbCoreOps = map[string]func(*Handler, context.Context, *echo.Context, []byte) error{
	"CreateCluster":           (*Handler).handleCreateCluster,
	"DescribeClusters":        (*Handler).handleDescribeClusters,
	"DeleteCluster":           (*Handler).handleDeleteCluster,
	"UpdateCluster":           (*Handler).handleUpdateCluster,
	"CreateACL":               (*Handler).handleCreateACL,
	"DescribeACLs":            (*Handler).handleDescribeACLs,
	"DeleteACL":               (*Handler).handleDeleteACL,
	"UpdateACL":               (*Handler).handleUpdateACL,
	"CreateSubnetGroup":       (*Handler).handleCreateSubnetGroup,
	"DescribeSubnetGroups":    (*Handler).handleDescribeSubnetGroups,
	"DeleteSubnetGroup":       (*Handler).handleDeleteSubnetGroup,
	"UpdateSubnetGroup":       (*Handler).handleUpdateSubnetGroup,
	"CreateUser":              (*Handler).handleCreateUser,
	"DescribeUsers":           (*Handler).handleDescribeUsers,
	"DeleteUser":              (*Handler).handleDeleteUser,
	"UpdateUser":              (*Handler).handleUpdateUser,
	"CreateParameterGroup":    (*Handler).handleCreateParameterGroup,
	"DescribeParameterGroups": (*Handler).handleDescribeParameterGroups,
	"DeleteParameterGroup":    (*Handler).handleDeleteParameterGroup,
	"UpdateParameterGroup":    (*Handler).handleUpdateParameterGroup,
	"ListTags":                (*Handler).handleListTags,
	"TagResource":             (*Handler).handleTagResource,
	"UntagResource":           (*Handler).handleUntagResource,
}

// dispatchCoreOps handles the original core operations.
func (h *Handler) dispatchCoreOps(ctx context.Context, c *echo.Context, op string, body []byte) (bool, error) {
	if fn, ok := memorydbCoreOps[op]; ok {
		return true, fn(h, ctx, c, body)
	}

	return false, nil
}

// dispatchNewOps handles the new operations added in this release.
func (h *Handler) dispatchNewOps(ctx context.Context, c *echo.Context, op string, body []byte) (bool, error) {
	if ok, err := h.dispatchSnapshotAndEngineOps(ctx, c, op, body); ok {
		return true, err
	}

	if ok, err := h.dispatchMultiRegionOps(ctx, c, op, body); ok {
		return true, err
	}

	return h.dispatchParameterAndShardOps(ctx, c, op, body)
}

// dispatchSnapshotAndEngineOps handles snapshot, engine-version, and event operations.
func (h *Handler) dispatchSnapshotAndEngineOps(
	ctx context.Context,
	c *echo.Context,
	op string,
	body []byte,
) (bool, error) {
	switch op {
	case "CreateSnapshot":

		return true, h.handleCreateSnapshot(ctx, c, body)
	case "DescribeSnapshots":

		return true, h.handleDescribeSnapshots(ctx, c, body)
	case "CopySnapshot":

		return true, h.handleCopySnapshot(ctx, c, body)
	case "DeleteSnapshot":

		return true, h.handleDeleteSnapshot(ctx, c, body)
	case "DescribeEngineVersions":

		return true, h.handleDescribeEngineVersions(ctx, c, body)
	case "DescribeEvents":

		return true, h.handleDescribeEvents(ctx, c, body)
	case "BatchUpdateCluster":

		return true, h.handleBatchUpdateCluster(ctx, c, body)
	case "DescribeServiceUpdates":

		return true, h.handleDescribeServiceUpdates(ctx, c, body)
	}

	return false, nil
}

// dispatchMultiRegionOps handles multi-region cluster and parameter group operations.
func (h *Handler) dispatchMultiRegionOps(ctx context.Context, c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreateMultiRegionCluster":

		return true, h.handleCreateMultiRegionCluster(ctx, c, body)
	case "DeleteMultiRegionCluster":

		return true, h.handleDeleteMultiRegionCluster(ctx, c, body)
	case "DescribeMultiRegionClusters":

		return true, h.handleDescribeMultiRegionClusters(ctx, c, body)
	case "DescribeMultiRegionParameterGroups":

		return true, h.handleDescribeMultiRegionParameterGroups(ctx, c, body)
	case "UpdateMultiRegionCluster":

		return true, h.handleUpdateMultiRegionCluster(ctx, c, body)
	case "ListAllowedMultiRegionClusterUpdates":

		return true, h.handleListAllowedMultiRegionClusterUpdates(ctx, c, body)
	}

	return false, nil
}

// dispatchParameterAndShardOps handles parameter group and shard operations.
func (h *Handler) dispatchParameterAndShardOps(
	ctx context.Context,
	c *echo.Context,
	op string,
	body []byte,
) (bool, error) {
	switch op {
	case "DescribeParameters":

		return true, h.handleDescribeParameters(ctx, c, body)
	case "ResetParameterGroup":

		return true, h.handleResetParameterGroup(ctx, c, body)
	case "FailoverShard":

		return true, h.handleFailoverShard(ctx, c, body)
	case "ListAllowedNodeTypeUpdates":

		return true, h.handleListAllowedNodeTypeUpdates(ctx, c, body)
	case "DescribeReservedNodes":

		return true, h.handleDescribeReservedNodes(ctx, c, body)
	case "DescribeReservedNodesOfferings":

		return true, h.handleDescribeReservedNodesOfferings(ctx, c, body)
	case "PurchaseReservedNodesOffering":

		return true, h.handlePurchaseReservedNodesOffering(ctx, c, body)
	case "DescribeMultiRegionParameters":

		return true, h.handleDescribeMultiRegionParameters(ctx, c, body)
	}

	return false, nil
}

// -- Cluster handlers ------------------------------------------------------------

func (h *Handler) handleCreateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req createClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	if req.NodeType == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "NodeType is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	cluster, err := h.Backend.CreateCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterResponse{Cluster: toClusterObject(cluster, true)})
}

func (h *Handler) handleDescribeClusters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	clusters, err := h.Backend.DescribeClusters(ctx, req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Apply cursor-based pagination when listing all clusters.
	start := 0

	if req.NextToken != "" {
		for i, cl := range clusters {
			if cl.Name == req.NextToken {
				start = i + 1

				break
			}
		}
	}

	clusters = clusters[start:]

	var nextToken string

	if req.MaxResults != nil && int(*req.MaxResults) < len(clusters) {
		nextToken = clusters[*req.MaxResults].Name
		clusters = clusters[:*req.MaxResults]
	}

	showShards := req.ShowShardDetails != nil && *req.ShowShardDetails

	objs := make([]clusterObject, 0, len(clusters))

	for _, cl := range clusters {
		objs = append(objs, toClusterObject(cl, showShards))
	}

	return c.JSON(http.StatusOK, describeClusterResponse{Clusters: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	var (
		cluster *Cluster
		err     error
	)

	if req.FinalSnapshotName != "" {
		cluster, err = h.Backend.DeleteClusterWithSnapshot(ctx, req.ClusterName, req.FinalSnapshotName)
	} else {
		cluster, err = h.Backend.DeleteCluster(ctx, req.ClusterName)
	}

	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteClusterResponse{Cluster: toClusterObject(cluster, true)})
}

func (h *Handler) handleUpdateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cluster, err := h.Backend.UpdateCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateClusterResponse{Cluster: toClusterObject(cluster, true)})
}

// -- ACL handlers ----------------------------------------------------------------

func (h *Handler) handleCreateACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req createACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	acl, err := h.Backend.CreateACL(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createACLResponse{ACL: toACLObject(acl, []string{})})
}

func (h *Handler) handleDescribeACLs(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	acls, err := h.Backend.DescribeACLs(ctx, req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Fetch all clusters once to compute the Clusters field on each ACL.
	allClusters, _ := h.Backend.DescribeClusters(ctx, "")

	acls, nextToken := paginateItems(acls, req.NextToken, req.MaxResults, func(a *ACL) string { return a.Name })

	objs := make([]aclObject, 0, len(acls))

	for _, a := range acls {
		clusterNames := clustersForACL(allClusters, a.Name)
		objs = append(objs, toACLObject(a, clusterNames))
	}

	return c.JSON(http.StatusOK, describeACLResponse{ACLs: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.DeleteACL(ctx, req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteACLResponse{ACL: toACLObject(acl, []string{})})
}

func (h *Handler) handleUpdateACL(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.UpdateACL(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	allClusters, _ := h.Backend.DescribeClusters(ctx, "")
	clusterNames := clustersForACL(allClusters, acl.Name)

	return c.JSON(http.StatusOK, updateACLResponse{ACL: toACLObject(acl, clusterNames)})
}

// -- SubnetGroup handlers --------------------------------------------------------

func (h *Handler) handleCreateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	sg, err := h.Backend.CreateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleDescribeSubnetGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	sgs, err := h.Backend.DescribeSubnetGroups(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	sgs, nextToken := paginateItems(sgs, req.NextToken, req.MaxResults, func(sg *SubnetGroup) string { return sg.Name })

	objs := make([]subnetGroupObject, 0, len(sgs))

	for _, sg := range sgs {
		objs = append(objs, toSubnetGroupObject(sg))
	}

	return c.JSON(http.StatusOK, describeSubnetGroupResponse{SubnetGroups: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.DeleteSubnetGroup(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleUpdateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.UpdateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

// -- User handlers ---------------------------------------------------------------

func (h *Handler) handleCreateUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req createUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	user, err := h.Backend.CreateUser(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createUserResponse{User: toUserObject(user, 0)})
}

func (h *Handler) handleDescribeUsers(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	users, err := h.Backend.DescribeUsers(ctx, req.UserName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	users, nextToken := paginateItems(users, req.NextToken, req.MaxResults, func(u *User) string { return u.Name })

	allACLs, _ := h.Backend.DescribeACLs(ctx, "")

	objs := make([]userObject, 0, len(users))

	for _, u := range users {
		count := countUserGroupMemberships(allACLs, u.Name)
		objs = append(objs, toUserObject(u, count))
	}

	return c.JSON(http.StatusOK, describeUserResponse{Users: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	user, err := h.Backend.DeleteUser(ctx, req.UserName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteUserResponse{User: toUserObject(user, 0)})
}

func (h *Handler) handleUpdateUser(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	user, err := h.Backend.UpdateUser(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateUserResponse{User: toUserObject(user, 0)})
}

// -- ParameterGroup handlers -----------------------------------------------------

func (h *Handler) handleCreateParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	pg, err := h.Backend.CreateParameterGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleDescribeParameterGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	pgs, err := h.Backend.DescribeParameterGroups(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pgs, nextToken := paginateItems(
		pgs,
		req.NextToken,
		req.MaxResults,
		func(pg *ParameterGroup) string { return pg.Name },
	)

	objs := make([]parameterGroupObject, 0, len(pgs))

	for _, pg := range pgs {
		objs = append(objs, toParameterGroupObject(pg))
	}

	return c.JSON(http.StatusOK, describeParameterGroupResponse{ParameterGroups: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.DeleteParameterGroup(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleUpdateParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.UpdateParameterGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

// -- Tag handlers ----------------------------------------------------------------

func (h *Handler) handleListTags(ctx context.Context, c *echo.Context, body []byte) error {
	var req listTagsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	tags, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(tags)})
}

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var req tagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	tags := tagsFromSlice(req.Tags)

	if err := h.Backend.TagResource(ctx, req.ResourceArn, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	// Return the resulting tag list (AWS behaviour).
	result, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(result)})
}

func (h *Handler) handleUntagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var req untagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	// Return the remaining tag list (AWS behaviour).
	result, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(result)})
}

// -- Snapshot handlers -----------------------------------------------------------

func (h *Handler) handleCreateSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req createSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SnapshotName is required")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	s, err := h.Backend.CreateSnapshot(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleCopySnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req copySnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SourceSnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SourceSnapshotName is required")
	}

	if req.TargetSnapshotName == "" && req.TargetBucket == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"TargetSnapshotName or TargetBucket is required",
		)
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	s, err := h.Backend.CopySnapshot(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, copySnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleDeleteSnapshot(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SnapshotName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SnapshotName is required")
	}

	s, err := h.Backend.DeleteSnapshot(ctx, req.SnapshotName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSnapshotResponse{Snapshot: toSnapshotObject(s)})
}

func (h *Handler) handleDescribeSnapshots(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeSnapshotRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	snapshots, err := h.Backend.DescribeSnapshots(ctx, req.SnapshotName, req.ClusterName, req.SnapshotType, req.Source)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	snapshots, nextToken := paginateItems(
		snapshots,
		req.NextToken,
		req.MaxResults,
		func(s *Snapshot) string { return s.Name },
	)

	objs := make([]snapshotObject, 0, len(snapshots))

	for _, s := range snapshots {
		objs = append(objs, toSnapshotObject(s))
	}

	return c.JSON(http.StatusOK, describeSnapshotResponse{Snapshots: objs, NextToken: nextToken})
}

// -- EngineVersion handlers ------------------------------------------------------

func (h *Handler) handleDescribeEngineVersions(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeEngineVersionsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	versions, err := h.Backend.DescribeEngineVersions(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]engineVersionObject, 0, len(versions))

	for _, ev := range versions {
		objs = append(objs, engineVersionObject{
			Engine:               ev.Engine,
			EngineVersion:        ev.EngineVersion,
			EnginePatchVersion:   ev.EnginePatchVersion,
			ParameterGroupFamily: ev.ParameterGroupFamily,
			Description:          ev.Description,
		})
	}

	return c.JSON(http.StatusOK, describeEngineVersionsResponse{EngineVersions: objs})
}

// -- Event handlers --------------------------------------------------------------

func (h *Handler) handleDescribeEvents(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeEventsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	events, err := h.Backend.DescribeEvents(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]eventObject, 0, len(events))

	for _, ev := range events {
		objs = append(objs, eventObject{
			Date:       ev.Date.Format(time.RFC3339),
			SourceName: ev.SourceName,
			SourceType: ev.SourceType,
			Message:    ev.Message,
		})
	}

	return c.JSON(http.StatusOK, describeEventsResponse{Events: objs})
}

// -- MultiRegionCluster handlers -------------------------------------------------

func (h *Handler) handleCreateMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req createMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterNameSuffix == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterNameSuffix is required",
		)
	}

	if req.NodeType == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "NodeType is required")
	}

	mrc, err := h.Backend.CreateMultiRegionCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
}

func (h *Handler) handleDeleteMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	mrc, err := h.Backend.DeleteMultiRegionCluster(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
}

func (h *Handler) handleDescribeMultiRegionClusters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionClustersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	mrcs, err := h.Backend.DescribeMultiRegionClusters(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]multiRegionClusterObject, 0, len(mrcs))

	for _, mrc := range mrcs {
		objs = append(objs, toMultiRegionClusterObject(mrc))
	}

	return c.JSON(http.StatusOK, describeMultiRegionClustersResponse{MultiRegionClusters: objs})
}

// -- MultiRegionParameterGroup handlers ------------------------------------------

func (h *Handler) handleDescribeMultiRegionParameterGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParameterGroupsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	mrpgs, err := h.Backend.DescribeMultiRegionParameterGroups(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]multiRegionParameterGroupObject, 0, len(mrpgs))

	for _, mrpg := range mrpgs {
		objs = append(objs, multiRegionParameterGroupObject{
			ARN:         mrpg.ARN,
			Name:        mrpg.Name,
			Description: mrpg.Description,
			Family:      mrpg.Family,
		})
	}

	return c.JSON(http.StatusOK, describeMultiRegionParameterGroupsResponse{MultiRegionParameterGroups: objs})
}

// -- BatchUpdateCluster handler --------------------------------------------------

func (h *Handler) handleBatchUpdateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req batchUpdateClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if len(req.ClusterNames) == 0 {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterNames is required")
	}

	found := h.Backend.BatchUpdateCluster(ctx, req.ClusterNames)

	processedObjs := make([]clusterObject, 0, len(found))
	unprocessedObjs := make([]unprocessedCluster, 0, len(req.ClusterNames))

	for _, name := range req.ClusterNames {
		if cl, ok := found[name]; ok {
			processedObjs = append(processedObjs, toClusterObject(cl, true))
		} else {
			unprocessedObjs = append(unprocessedObjs, unprocessedCluster{
				ClusterName:  name,
				ErrorType:    "ClusterNotFoundFault",
				ErrorMessage: "cluster not found: " + name,
			})
		}
	}

	return c.JSON(http.StatusOK, batchUpdateClusterResponse{
		ProcessedClusters:   processedObjs,
		UnprocessedClusters: unprocessedObjs,
	})
}

// -- New handler functions (refinement check 2) ----------------------------------

func (h *Handler) handleDescribeParameters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeParametersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	params, err := h.Backend.DescribeParameters(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]parameterObject, 0, len(params))

	for k, v := range params {
		objs = append(objs, parameterObject{
			Name:                 k,
			Value:                v,
			DataType:             "string",
			ChangeType:           "immediate",
			Source:               "system",
			MinimumEngineVersion: engineVersion62,
		})
	}

	sort.Slice(objs, func(i, j int) bool { return objs[i].Name < objs[j].Name })

	return c.JSON(http.StatusOK, describeParametersResponse{Parameters: objs})
}

func (h *Handler) handleResetParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req resetParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	pg, err := h.Backend.ResetParameterGroup(ctx, req.ParameterGroupName, req.ParameterNames, req.AllParameters)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, resetParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleFailoverShard(ctx context.Context, c *echo.Context, body []byte) error {
	var req failoverShardRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cl, err := h.Backend.FailoverShard(ctx, req.ClusterName, req.ShardName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, failoverShardResponse{Cluster: toClusterObject(cl, true)})
}

func (h *Handler) handleListAllowedNodeTypeUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req listAllowedNodeTypeUpdatesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	nodeTypes, err := h.Backend.ListAllowedNodeTypeUpdates(ctx, req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listAllowedNodeTypeUpdatesResponse{
		ScaleUpNodeTypes:   nodeTypes,
		ScaleDownNodeTypes: nodeTypes,
	})
}

func (h *Handler) handleListAllowedMultiRegionClusterUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req listAllowedMultiRegionClusterUpdatesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	nodeTypes, err := h.Backend.ListAllowedMultiRegionClusterUpdates(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listAllowedMultiRegionClusterUpdatesResponse{
		ScaleUpNodeTypes:   nodeTypes,
		ScaleDownNodeTypes: nodeTypes,
	})
}

func (h *Handler) handleUpdateMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	mrc, err := h.Backend.UpdateMultiRegionCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
}

func (h *Handler) handleDescribeServiceUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeServiceUpdatesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}
	updates, err := h.Backend.DescribeServiceUpdates(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}
	updates, nextToken := paginateItems(
		updates,
		req.NextToken,
		req.MaxResults,
		func(su *ServiceUpdate) string { return su.ServiceUpdateName },
	)
	objs := make([]serviceUpdateObject, 0, len(updates))
	for _, su := range updates {
		objs = append(objs, serviceUpdateObject{
			ServiceUpdateName:   su.ServiceUpdateName,
			ReleaseDate:         su.ReleaseDate,
			Description:         su.Description,
			Status:              su.Status,
			Type:                su.Type,
			AutoUpdateStartDate: su.AutoUpdateStartDate,
		})
	}

	return c.JSON(http.StatusOK, describeServiceUpdatesResponse{ServiceUpdates: objs, NextToken: nextToken})
}

// -- ReservedNode handlers -------------------------------------------------------

func (h *Handler) handleDescribeReservedNodes(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeReservedNodesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	nodes, err := h.Backend.DescribeReservedNodes(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeReservedNodesResponse{ReservedNodes: toReservedNodeSlice(nodes)})
}

func (h *Handler) handleDescribeReservedNodesOfferings(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeReservedNodesOfferingsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	offerings, err := h.Backend.DescribeReservedNodesOfferings(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		describeReservedNodesOfferingsResponse{ReservedNodesOfferings: toReservedNodesOfferingSlice(offerings)},
	)
}

func (h *Handler) handlePurchaseReservedNodesOffering(ctx context.Context, c *echo.Context, body []byte) error {
	var req purchaseReservedNodesOfferingRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ReservedNodesOfferingID == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"ReservedNodesOfferingId is required",
		)
	}

	rn, err := h.Backend.PurchaseReservedNodesOffering(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, purchaseReservedNodesOfferingResponse{ReservedNode: rn})
}

// -- DescribeMultiRegionParameters handler ---------------------------------------

func (h *Handler) handleDescribeMultiRegionParameters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParametersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	params, err := h.Backend.DescribeMultiRegionParameters(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]parameterObject, 0, len(params))

	for k, v := range params {
		objs = append(objs, parameterObject{
			Name:     k,
			Value:    v,
			DataType: "string",
		})
	}

	sort.Slice(objs, func(i, j int) bool { return objs[i].Name < objs[j].Name })

	return c.JSON(http.StatusOK, describeMultiRegionParametersResponse{Parameters: objs})
}

// -- helpers ---------------------------------------------------------------------

// paginateItems applies cursor-based pagination to a slice of named items.
// getName extracts the name used as a pagination cursor from each item.
func paginateItems[T any](items []T, token string, maxResults *int32, getName func(T) string) ([]T, string) {
	if token != "" {
		items = items[findStartIndex(items, token, getName):]
	}

	limit := 100
	if maxResults != nil && *maxResults > 0 && int(*maxResults) < limit {
		limit = int(*maxResults)
	}

	var nextToken string

	if limit < len(items) {
		nextToken = getName(items[limit])
		items = items[:limit]
	}

	return items, nextToken
}

// findStartIndex returns the index after the item whose name equals token,
// or 0 if not found.
func findStartIndex[T any](items []T, token string, getName func(T) string) int {
	for i, item := range items {
		if getName(item) == token {
			return i + 1
		}
	}

	return 0
}

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):

		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):

		return writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):

		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	case errors.Is(err, awserr.ErrConflict):

		return writeError(c, http.StatusConflict, "InvalidRequestException", err.Error())
	default:

		return writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagsPerRes  = 50
)

var (
	errTagQuotaExceeded = fmt.Errorf("tag quota exceeded: a resource may have at most %d tags", maxTagsPerRes)
	errTagKeyLength     = fmt.Errorf("tag key length must be between 1 and %d characters", maxTagKeyLen)
	errTagKeyPrefix     = errors.New(`tag key must not start with reserved prefix "aws:"`)
	errTagValueLength   = fmt.Errorf("tag value length must be 0-%d characters", maxTagValueLen)
)

// validateTagEntries enforces AWS MemoryDB tag constraints:
// key 1-128 chars, no "aws:" prefix; value 0-256 chars; at most 50 tags.
func validateTagEntries(tags []tagEntry) error {
	if len(tags) > maxTagsPerRes {
		return errTagQuotaExceeded
	}

	for _, t := range tags {
		klen := utf8.RuneCountInString(t.Key)
		if klen == 0 || klen > maxTagKeyLen {
			return errTagKeyLength
		}

		if strings.HasPrefix(t.Key, "aws:") {
			return errTagKeyPrefix
		}

		if vlen := utf8.RuneCountInString(t.Value); vlen > maxTagValueLen {
			return errTagValueLength
		}
	}

	return nil
}

// writeError writes a JSON error response using the standard AWS JSON 1.1 envelope.
func writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// enginePatchVersionFor looks up the EnginePatchVersion for a given engine+version pair.
func enginePatchVersionFor(engine, engineVersion string) string {
	for _, ev := range defaultEngineVersions() {
		if ev.Engine == engine && ev.EngineVersion == engineVersion {
			return ev.EnginePatchVersion
		}
	}

	return engineVersion
}

// toClusterObject converts a Cluster to its JSON representation.
// showShards controls whether per-shard node detail is populated.
func toClusterObject(c *Cluster, showShards bool) clusterObject {
	region := c.Region
	if region == "" {
		region = config.DefaultRegion
	}

	var shards []shardObject
	if showShards {
		shards = buildShards(c.Name, c.NumShards, c.NumReplicasPerShard, c.Port, region)
	}

	sgs := make([]securityGroupMembership, 0, len(c.SecurityGroupIDs))
	for _, id := range c.SecurityGroupIDs {
		sgs = append(sgs, securityGroupMembership{SecurityGroupID: id, Status: "active"})
	}

	return clusterObject{
		Name:                     c.Name,
		ARN:                      c.ARN,
		Description:              c.Description,
		Status:                   c.Status,
		NodeType:                 c.NodeType,
		EngineVersion:            c.EngineVersion,
		EnginePatchVersion:       enginePatchVersionFor(c.Engine, c.EngineVersion),
		Engine:                   c.Engine,
		DataTiering:              c.DataTiering,
		NetworkType:              c.NetworkType,
		IPDiscovery:              c.IPDiscovery,
		AutoMinorVersionUpgrade:  c.AutoMinorVersionUpgrade,
		ACLName:                  c.ACLName,
		SubnetGroupName:          c.SubnetGroupName,
		ParameterGroupName:       c.ParameterGroupName,
		KmsKeyID:                 c.KmsKeyID,
		SnsTopicArn:              c.SnsTopicArn,
		SnsTopicStatus:           c.SnsTopicStatus,
		MaintenanceWindow:        c.MaintenanceWindow,
		SnapshotWindow:           c.SnapshotWindow,
		NumberOfShards:           c.NumShards,
		TLSEnabled:               c.TLSEnabled,
		SnapshotRetentionLimit:   c.SnapshotRetentionLimit,
		Shards:                   shards,
		AvailabilityMode:         c.AvailabilityMode,
		NumberOfReplicasPerShard: c.NumReplicasPerShard,
		SecurityGroups:           sgs,
		ClusterEndpoint: &endpointObject{
			Address: c.Name + ".memorydb." + region + ".amazonaws.com",
			Port:    c.Port,
		},
	}
}

// buildShards constructs a slice of shardObjects with evenly-distributed slots and nodes.
func buildShards(clusterName string, numShards, numReplicas, port int32, region string) []shardObject {
	const totalSlots = 16384

	const maxShards = 256

	// Clamp nShards to [1, maxShards] before use. Converting through a
	// clamped int prevents CodeQL from treating the make size as
	// attacker-controlled (go/slice-memory-allocation-excessive-size).
	nShards := max(1, min(maxShards, int(numShards)))

	slotsPerShard := totalSlots / nShards

	zones := []string{region + "a", region + "b", region + "c"}

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. nShards is only used for the loop count below (safe).
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	shards := make([]shardObject, 0)

	for i := range nShards {
		start := i * slotsPerShard
		end := start + slotsPerShard - 1

		if i == nShards-1 {
			end = totalSlots - 1
		}

		nodes := make([]nodeObject, 0, 1+int(numReplicas))
		for ni := range 1 + int(numReplicas) {
			role := "primary"
			if ni > 0 {
				role = "replica"
			}
			nodeName := fmt.Sprintf("%s-0001-%04d-%04d", clusterName, i, ni)
			nodes = append(nodes, nodeObject{
				Name:             nodeName,
				Status:           clusterStatusAvailable,
				AvailabilityZone: zones[ni%len(zones)],
				CreateTime:       float64(time.Now().Unix()),
				Endpoint: &endpointObject{
					Address: nodeName + ".memorydb." + region + ".amazonaws.com",
					Port:    port,
				},
			})
			_ = role
		}

		// Shard name follows the AWS MemoryDB convention: <cluster>-<nodegroup>-<shardindex>
		// where nodegroup is always "0001" for single-shard-group clusters.
		shards = append(shards, shardObject{
			Name:          fmt.Sprintf("%s-0001-%04d", clusterName, i),
			Status:        clusterStatusAvailable,
			Slots:         fmt.Sprintf("%d-%d", start, end),
			NumberOfNodes: int32(1 + int(numReplicas)), //nolint:gosec // clamped above
			Nodes:         nodes,
		})
	}

	return shards
}

// clustersForACL returns the names of clusters that reference the given ACL name.
func clustersForACL(clusters []*Cluster, aclName string) []string {
	names := make([]string, 0, len(clusters))

	for _, c := range clusters {
		if c.ACLName == aclName {
			names = append(names, c.Name)
		}
	}

	return names
}

// toACLObject converts an ACL to its JSON representation.
// clusterNames is the list of cluster names that reference this ACL.
func toACLObject(a *ACL, clusterNames []string) aclObject {
	return aclObject{
		Name:                 a.Name,
		ARN:                  a.ARN,
		Status:               a.Status,
		UserNames:            a.UserNames,
		Clusters:             clusterNames,
		MinimumEngineVersion: engineVersion62,
		PendingChanges:       &aclPendingChangesObject{UserNamesToAdd: []string{}, UserNamesToRemove: []string{}},
	}
}

// toSubnetGroupObject converts a SubnetGroup to its JSON representation.
func toSubnetGroupObject(sg *SubnetGroup) subnetGroupObject {
	subnets := make([]subnetEntry, 0, len(sg.SubnetIDs))

	for _, id := range sg.SubnetIDs {
		subnets = append(subnets, subnetEntry{Identifier: id})
	}

	return subnetGroupObject{
		Name:        sg.Name,
		ARN:         sg.ARN,
		Description: sg.Description,
		VPCID:       sg.VPCID,
		Subnets:     subnets,
	}
}

// toUserObject converts a User to its JSON representation.
func toUserObject(u *User, userGroupCount int32) userObject {
	auth := &authenticationObject{Type: u.AuthType}
	if u.AuthType == "password" && len(u.Passwords) > 0 {
		count := min(len(u.Passwords), math.MaxInt32)
		auth.PasswordCount = int32(count) //nolint:gosec // count is clamped to math.MaxInt32 above
	}

	return userObject{
		Name:                 u.Name,
		ARN:                  u.ARN,
		AccessString:         u.AccessString,
		Status:               u.Status,
		Authentication:       auth,
		MinimumEngineVersion: engineVersion62,
		UserGroupCount:       userGroupCount,
	}
}

// countUserGroupMemberships returns the number of ACLs that contain userName.
func countUserGroupMemberships(acls []*ACL, userName string) int32 {
	var count int32
	for _, a := range acls {
		if slices.Contains(a.UserNames, userName) {
			count++
		}
	}

	return count
}

// toParameterGroupObject converts a ParameterGroup to its JSON representation.
func toParameterGroupObject(pg *ParameterGroup) parameterGroupObject {
	return parameterGroupObject{
		Name:        pg.Name,
		ARN:         pg.ARN,
		Description: pg.Description,
		Family:      pg.Family,
	}
}

// toSnapshotObject converts a Snapshot to its JSON representation.
func toSnapshotObject(s *Snapshot) snapshotObject {
	createdAt := ""
	if !s.CreatedAt.IsZero() {
		createdAt = s.CreatedAt.UTC().Format(time.RFC3339)
	}

	var clusterConfig *snapshotClusterConfig
	if s.ClusterConfiguration.Name != "" {
		cfg := s.ClusterConfiguration
		clusterConfig = &cfg
	}

	return snapshotObject{
		Name:                 s.Name,
		ARN:                  s.ARN,
		ClusterConfiguration: clusterConfig,
		Status:               s.Status,
		KmsKeyID:             s.KmsKeyID,
		SnapshotType:         s.SnapshotType,
		Source:               s.Source,
		CreatedAt:            createdAt,
	}
}

// toMultiRegionClusterObject converts a MultiRegionCluster to its JSON representation.
func toMultiRegionClusterObject(mrc *MultiRegionCluster) multiRegionClusterObject {
	return multiRegionClusterObject{
		MultiRegionClusterName:        mrc.MultiRegionClusterName,
		ARN:                           mrc.ARN,
		Description:                   mrc.Description,
		NodeType:                      mrc.NodeType,
		Engine:                        mrc.Engine,
		EngineVersion:                 mrc.EngineVersion,
		MultiRegionParameterGroupName: mrc.MultiRegionParameterGroupName,
		Status:                        mrc.Status,
	}
}

// toReservedNodeSlice converts a slice of ReservedNode pointers to values.
func toReservedNodeSlice(nodes []*ReservedNode) []ReservedNode {
	result := make([]ReservedNode, 0, len(nodes))

	for _, n := range nodes {
		result = append(result, *n)
	}

	return result
}

// toReservedNodesOfferingSlice converts a slice of ReservedNodesOffering pointers to values.
func toReservedNodesOfferingSlice(offerings []*ReservedNodesOffering) []ReservedNodesOffering {
	result := make([]ReservedNodesOffering, 0, len(offerings))

	for _, o := range offerings {
		result = append(result, *o)
	}

	return result
}

// Purge implements service.Purgeable by removing all MemoryDB resources older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}
