package memorydb

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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

// GetSupportedOperations returns the list of supported MemoryDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DescribeClusters",
		"DeleteCluster",
		"UpdateCluster",
		"CreateACL",
		"DescribeACLs",
		"DeleteACL",
		"UpdateACL",
		"CreateSubnetGroup",
		"DescribeSubnetGroups",
		"DeleteSubnetGroup",
		"UpdateSubnetGroup",
		"CreateUser",
		"DescribeUsers",
		"DeleteUser",
		"UpdateUser",
		"CreateParameterGroup",
		"DescribeParameterGroups",
		"DeleteParameterGroup",
		"UpdateParameterGroup",
		"ListTags",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return memorydbService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

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
		"UserName", "ParameterGroupName", "ResourceArn",
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

		return h.dispatch(c, op, body)
	}
}

// dispatch routes to the appropriate handler based on the operation name.
//
//nolint:cyclop // switch-based operation dispatch; each case is a single delegation.
func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	switch op {
	case "CreateCluster":
		return h.handleCreateCluster(c, body)
	case "DescribeClusters":
		return h.handleDescribeClusters(c, body)
	case "DeleteCluster":
		return h.handleDeleteCluster(c, body)
	case "UpdateCluster":
		return h.handleUpdateCluster(c, body)
	case "CreateACL":
		return h.handleCreateACL(c, body)
	case "DescribeACLs":
		return h.handleDescribeACLs(c, body)
	case "DeleteACL":
		return h.handleDeleteACL(c, body)
	case "UpdateACL":
		return h.handleUpdateACL(c, body)
	case "CreateSubnetGroup":
		return h.handleCreateSubnetGroup(c, body)
	case "DescribeSubnetGroups":
		return h.handleDescribeSubnetGroups(c, body)
	case "DeleteSubnetGroup":
		return h.handleDeleteSubnetGroup(c, body)
	case "UpdateSubnetGroup":
		return h.handleUpdateSubnetGroup(c, body)
	case "CreateUser":
		return h.handleCreateUser(c, body)
	case "DescribeUsers":
		return h.handleDescribeUsers(c, body)
	case "DeleteUser":
		return h.handleDeleteUser(c, body)
	case "UpdateUser":
		return h.handleUpdateUser(c, body)
	case "CreateParameterGroup":
		return h.handleCreateParameterGroup(c, body)
	case "DescribeParameterGroups":
		return h.handleDescribeParameterGroups(c, body)
	case "DeleteParameterGroup":
		return h.handleDeleteParameterGroup(c, body)
	case "UpdateParameterGroup":
		return h.handleUpdateParameterGroup(c, body)
	case "ListTags":
		return h.handleListTags(c, body)
	case "TagResource":
		return h.handleTagResource(c, body)
	case "UntagResource":
		return h.handleUntagResource(c, body)
	}

	return writeError(c, http.StatusBadRequest, "UnknownOperationException", "unknown operation: "+op)
}

// -- Cluster handlers ------------------------------------------------------------

func (h *Handler) handleCreateCluster(c *echo.Context, body []byte) error {
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

	cluster, err := h.Backend.CreateCluster(h.DefaultRegion, h.AccountID, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterResponse{Cluster: toClusterObject(cluster)})
}

func (h *Handler) handleDescribeClusters(c *echo.Context, body []byte) error {
	var req describeClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	clusters, err := h.Backend.DescribeClusters(req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]clusterObject, 0, len(clusters))

	for _, c := range clusters {
		objs = append(objs, toClusterObject(c))
	}

	return c.JSON(http.StatusOK, describeClusterResponse{Clusters: objs})
}

func (h *Handler) handleDeleteCluster(c *echo.Context, body []byte) error {
	var req deleteClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cluster, err := h.Backend.DeleteCluster(req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteClusterResponse{Cluster: toClusterObject(cluster)})
}

func (h *Handler) handleUpdateCluster(c *echo.Context, body []byte) error {
	var req updateClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cluster, err := h.Backend.UpdateCluster(&req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateClusterResponse{Cluster: toClusterObject(cluster)})
}

// -- ACL handlers ----------------------------------------------------------------

func (h *Handler) handleCreateACL(c *echo.Context, body []byte) error {
	var req createACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.CreateACL(h.DefaultRegion, h.AccountID, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createACLResponse{ACL: toACLObject(acl)})
}

func (h *Handler) handleDescribeACLs(c *echo.Context, body []byte) error {
	var req describeACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	acls, err := h.Backend.DescribeACLs(req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]aclObject, 0, len(acls))

	for _, a := range acls {
		objs = append(objs, toACLObject(a))
	}

	return c.JSON(http.StatusOK, describeACLResponse{ACLs: objs})
}

func (h *Handler) handleDeleteACL(c *echo.Context, body []byte) error {
	var req deleteACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.DeleteACL(req.ACLName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteACLResponse{ACL: toACLObject(acl)})
}

func (h *Handler) handleUpdateACL(c *echo.Context, body []byte) error {
	var req updateACLRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ACLName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ACLName is required")
	}

	acl, err := h.Backend.UpdateACL(&req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateACLResponse{ACL: toACLObject(acl)})
}

// -- SubnetGroup handlers --------------------------------------------------------

func (h *Handler) handleCreateSubnetGroup(c *echo.Context, body []byte) error {
	var req createSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.CreateSubnetGroup(h.DefaultRegion, h.AccountID, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleDescribeSubnetGroups(c *echo.Context, body []byte) error {
	var req describeSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	sgs, err := h.Backend.DescribeSubnetGroups(req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]subnetGroupObject, 0, len(sgs))

	for _, sg := range sgs {
		objs = append(objs, toSubnetGroupObject(sg))
	}

	return c.JSON(http.StatusOK, describeSubnetGroupResponse{SubnetGroups: objs})
}

func (h *Handler) handleDeleteSubnetGroup(c *echo.Context, body []byte) error {
	var req deleteSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.DeleteSubnetGroup(req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleUpdateSubnetGroup(c *echo.Context, body []byte) error {
	var req updateSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.UpdateSubnetGroup(&req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

// -- User handlers ---------------------------------------------------------------

func (h *Handler) handleCreateUser(c *echo.Context, body []byte) error {
	var req createUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	user, err := h.Backend.CreateUser(h.DefaultRegion, h.AccountID, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createUserResponse{User: toUserObject(user)})
}

func (h *Handler) handleDescribeUsers(c *echo.Context, body []byte) error {
	var req describeUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	users, err := h.Backend.DescribeUsers(req.UserName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]userObject, 0, len(users))

	for _, u := range users {
		objs = append(objs, toUserObject(u))
	}

	return c.JSON(http.StatusOK, describeUserResponse{Users: objs})
}

func (h *Handler) handleDeleteUser(c *echo.Context, body []byte) error {
	var req deleteUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	user, err := h.Backend.DeleteUser(req.UserName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteUserResponse{User: toUserObject(user)})
}

func (h *Handler) handleUpdateUser(c *echo.Context, body []byte) error {
	var req updateUserRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.UserName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "UserName is required")
	}

	user, err := h.Backend.UpdateUser(&req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateUserResponse{User: toUserObject(user)})
}

// -- ParameterGroup handlers -----------------------------------------------------

func (h *Handler) handleCreateParameterGroup(c *echo.Context, body []byte) error {
	var req createParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.CreateParameterGroup(h.DefaultRegion, h.AccountID, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleDescribeParameterGroups(c *echo.Context, body []byte) error {
	var req describeParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	pgs, err := h.Backend.DescribeParameterGroups(req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]parameterGroupObject, 0, len(pgs))

	for _, pg := range pgs {
		objs = append(objs, toParameterGroupObject(pg))
	}

	return c.JSON(http.StatusOK, describeParameterGroupResponse{ParameterGroups: objs})
}

func (h *Handler) handleDeleteParameterGroup(c *echo.Context, body []byte) error {
	var req deleteParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.DeleteParameterGroup(req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleUpdateParameterGroup(c *echo.Context, body []byte) error {
	var req updateParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.UpdateParameterGroup(&req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

// -- Tag handlers ----------------------------------------------------------------

func (h *Handler) handleListTags(c *echo.Context, body []byte) error {
	var req listTagsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	tags, err := h.Backend.ListTags(req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(tags)})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req tagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	tags := tagsFromSlice(req.Tags)

	if err := h.Backend.TagResource(req.ResourceArn, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req untagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// -- helpers ---------------------------------------------------------------------

// writeBackendError translates a backend error to an HTTP response.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	default:
		return writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

// writeError writes a JSON error response using the standard AWS JSON 1.1 envelope.
func writeError(c *echo.Context, status int, errType, message string) error {
	return c.JSON(status, errorResponse{Type: errType, Message: message})
}

// toClusterObject converts a Cluster to its JSON representation.
func toClusterObject(c *Cluster) clusterObject {
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}

	shards := buildShards(c.Name, c.NumShards)

	return clusterObject{
		Name:                   c.Name,
		ARN:                    c.ARN,
		Description:            c.Description,
		Status:                 c.Status,
		NodeType:               c.NodeType,
		EngineVersion:          c.EngineVersion,
		EnginePatchVersion:     c.EngineVersion,
		ACLName:                c.ACLName,
		SubnetGroupName:        c.SubnetGroupName,
		ParameterGroupName:     c.ParameterGroupName,
		KmsKeyID:               c.KmsKeyID,
		SnsTopicArn:            c.SnsTopicArn,
		MaintenanceWindow:      c.MaintenanceWindow,
		SnapshotWindow:         c.SnapshotWindow,
		NumberOfShards:         c.NumShards,
		TLSEnabled:             c.TLSEnabled,
		SnapshotRetentionLimit: c.SnapshotRetentionLimit,
		Shards:                 shards,
		ClusterEndpoint: &endpointObject{
			Address: c.Name + ".memorydb." + region + ".amazonaws.com",
			Port:    c.Port,
		},
	}
}

// buildShards constructs a slice of shardObjects with evenly-distributed slots.
func buildShards(clusterName string, numShards int32) []shardObject {
	const totalSlots = 16384

	if numShards <= 0 {
		numShards = 1
	}

	shards := make([]shardObject, numShards)
	n := int(numShards)
	slotsPerShard := totalSlots / n

	for i := range shards {
		start := i * slotsPerShard
		end := start + slotsPerShard - 1

		if i == n-1 {
			end = totalSlots - 1
		}

		// Shard name follows the AWS MemoryDB convention: <cluster>-<nodegroup>-<shardindex>
		// where nodegroup is always "0001" for single-shard-group clusters.
		shards[i] = shardObject{
			Name:          fmt.Sprintf("%s-0001-%04d", clusterName, i),
			Status:        clusterStatusAvailable,
			Slots:         fmt.Sprintf("%d-%d", start, end),
			NumberOfNodes: 1,
		}
	}

	return shards
}

// toACLObject converts an ACL to its JSON representation.
func toACLObject(a *ACL) aclObject {
	return aclObject{
		Name:      a.Name,
		ARN:       a.ARN,
		Status:    a.Status,
		UserNames: a.UserNames,
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
func toUserObject(u *User) userObject {
	return userObject{
		Name:         u.Name,
		ARN:          u.ARN,
		AccessString: u.AccessString,
		Status:       u.Status,
	}
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
