package neptune

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	engineDescriptionAmazonNeptune = "Amazon Neptune"
	pgFamilyNeptune12              = "neptune1.2"
	pgFamilyNeptune13              = "neptune1.3"
	sourceTypeNotification         = "notification"
	formTrue                       = "true"
)

var errNoServerlessV2Config = errors.New("noServerlessV2Config")

const (
	neptuneVersion = "2014-10-31"
	neptuneXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
)

// Handler is the Echo HTTP handler for Neptune operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Neptune handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Neptune" }

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// clusterARN builds a Neptune cluster ARN using the backend's region and account.
func (h *Handler) clusterARN(id string) string {
	return arn.Build("neptune", h.Backend.Region(), h.Backend.AccountID(), "cluster:"+id)
}

// GetSupportedOperations returns supported Neptune operations (sorted).
func (h *Handler) GetSupportedOperations() []string {
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

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "neptune" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Neptune instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Neptune requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}
		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}
		ua := r.Header.Get("User-Agent")
		if !strings.Contains(ua, "api/neptune") {
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

		return vals.Get("Version") == neptuneVersion
	}
}

// MatchPriority returns the routing priority for Neptune (higher than RDS to intercept Neptune requests first).
func (h *Handler) MatchPriority() int { return service.PriorityFormNeptune }

// ExtractOperation extracts the Neptune action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}
	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource returns the DB cluster identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("DBClusterIdentifier")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}
		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}
		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}
		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBCluster":
		return h.handleCreateDBCluster(vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(vals)
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(vals)
	case "StopDBCluster":
		return h.handleStopDBCluster(vals)
	case "StartDBCluster":
		return h.handleStartDBCluster(vals)
	case "FailoverDBCluster":
		return h.handleFailoverDBCluster(vals)
	case "CreateDBInstance":
		return h.handleCreateDBInstance(vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(vals)
	default:
		return h.dispatchExtended(action, vals)
	}
}

func (h *Handler) dispatchExtended(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(vals)
	case "DeleteDBClusterParameterGroup":
		return h.handleDeleteDBClusterParameterGroup(vals)
	case "ModifyDBClusterParameterGroup":
		return h.handleModifyDBClusterParameterGroup(vals)
	default:
		return h.dispatchExtended2(action, vals)
	}
}

func (h *Handler) dispatchExtended2(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(vals)
	case "DeleteDBClusterSnapshot":
		return h.handleDeleteDBClusterSnapshot(vals)
	case "ListTagsForResource":
		return h.handleListTagsForResource(vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(vals)
	case "DescribeGlobalClusters":
		return h.handleDescribeGlobalClusters(vals)
	default:
		return h.dispatchNewOps(action, vals)
	}
}

func (h *Handler) dispatchNewOps(action string, vals url.Values) (any, error) {
	switch action {
	case "AddRoleToDBCluster":
		return h.handleAddRoleToDBCluster(vals)
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(vals)
	case "CopyDBParameterGroup":
		return h.handleCopyDBParameterGroup(vals)
	case "CreateDBClusterEndpoint":
		return h.handleCreateDBClusterEndpoint(vals)
	case "CreateDBParameterGroup":
		return h.handleCreateDBParameterGroup(vals)
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(vals)
	default:
		return h.dispatchNewOps2(action, vals)
	}
}

func (h *Handler) dispatchNewOps2(action string, vals url.Values) (any, error) {
	switch action {
	case "DeleteDBClusterEndpoint":
		return h.handleDeleteDBClusterEndpoint(vals)
	case "DescribeDBClusterEndpoints":
		return h.handleDescribeDBClusterEndpoints(vals)
	case "ModifyDBClusterEndpoint":
		return h.handleModifyDBClusterEndpoint(vals)
	case "DeleteDBParameterGroup":
		return h.handleDeleteDBParameterGroup(vals)
	case "DescribeDBParameterGroups":
		return h.handleDescribeDBParameterGroups(vals)
	case "DescribeDBParameters":
		return h.handleDescribeDBParameters(vals)
	case "ModifyDBParameterGroup":
		return h.handleModifyDBParameterGroup(vals)
	case "ResetDBParameterGroup":
		return h.handleResetDBParameterGroup(vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(vals)
	case "DescribeDBClusterSnapshotAttributes":
		return h.handleDescribeDBClusterSnapshotAttributes(vals)
	case "ModifyDBClusterSnapshotAttribute":
		return h.handleModifyDBClusterSnapshotAttribute(vals)
	case "ResetDBClusterParameterGroup":
		return h.handleResetDBClusterParameterGroup(vals)
	default:
		return h.dispatchNewOps3(action, vals)
	}
}

func (h *Handler) dispatchNewOps3(action string, vals url.Values) (any, error) {
	switch action {
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(vals)
	case "DescribeEventSubscriptions":
		return h.handleDescribeEventSubscriptions(vals)
	case "ModifyEventSubscription":
		return h.handleModifyEventSubscription(vals)
	case "RemoveSourceIdentifierFromSubscription":
		return h.handleRemoveSourceIdentifierFromSubscription(vals)
	case "DescribeEventCategories":
		return h.handleDescribeEventCategories(vals)
	case "DescribeEvents":
		return h.handleDescribeEvents(vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(vals)
	case "FailoverGlobalCluster":
		return h.handleFailoverGlobalCluster(vals)
	case "ModifyGlobalCluster":
		return h.handleModifyGlobalCluster(vals)
	case "RemoveFromGlobalCluster":
		return h.handleRemoveFromGlobalCluster(vals)
	default:
		return h.dispatchNewOps4(action, vals)
	}
}

func (h *Handler) dispatchNewOps4(action string, vals url.Values) (any, error) {
	switch action {
	case "SwitchoverGlobalCluster":
		return h.handleSwitchoverGlobalCluster(vals)
	case "RemoveRoleFromDBCluster":
		return h.handleRemoveRoleFromDBCluster(vals)
	case "DescribeEngineDefaultClusterParameters":
		return h.handleDescribeEngineDefaultClusterParameters(vals)
	case "DescribeEngineDefaultParameters":
		return h.handleDescribeEngineDefaultParameters(vals)
	case "DescribePendingMaintenanceActions":
		return h.handleDescribePendingMaintenanceActions(vals)
	case "DescribeValidDBInstanceModifications":
		return h.handleDescribeValidDBInstanceModifications(vals)
	case "PromoteReadReplicaDBCluster":
		return h.handlePromoteReadReplicaDBCluster(vals)
	case "RestoreDBClusterFromSnapshot":
		return h.handleRestoreDBClusterFromSnapshot(vals)
	case "RestoreDBClusterToPointInTime":
		return h.handleRestoreDBClusterToPointInTime(vals)
	case "ModifyDBSubnetGroup":
		return h.handleModifyDBSubnetGroup(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid Neptune action", ErrUnknownAction, action)
	}
}

func (h *Handler) handleCreateDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	port := 0
	if portStr := vals.Get("Port"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}
	sv2, sv2Err := parseServerlessV2ScalingConfig(vals)
	if sv2Err != nil && !errors.Is(sv2Err, errNoServerlessV2Config) {
		return nil, sv2Err
	}
	opts := DBClusterCreateOptions{
		EngineVersion:                   vals.Get("EngineVersion"),
		EngineMode:                      vals.Get("EngineMode"),
		KmsKeyID:                        vals.Get("KmsKeyId"),
		PreferredBackupWindow:           vals.Get("PreferredBackupWindow"),
		PreferredMaintenanceWindow:      vals.Get("PreferredMaintenanceWindow"),
		EnableIAMDatabaseAuthentication: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		ManageMasterUserPassword:        vals.Get("ManageMasterUserPassword") == formTrue,
		StorageEncrypted:                vals.Get("StorageEncrypted") == formTrue,
		DeletionProtection:              vals.Get("DeletionProtection") == formTrue,
		ServerlessV2ScalingConfig:       sv2,
	}
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	cluster, err := h.Backend.CreateDBCluster(id, paramGroupName, port, opts)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(h.clusterARN(cluster.DBClusterIdentifier), tags)
	}

	return &createDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeDBClusters(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBCluster, 0, len(clusters))
	for _, c := range clusters {
		cp := c
		members = append(members, toXMLCluster(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClustersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClustersResult{
			DBClusters: xmlDBClusterList{Members: members},
			Marker:     nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.DeleteDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	sv2, sv2Err := parseServerlessV2ScalingConfig(vals)
	if sv2Err != nil && !errors.Is(sv2Err, errNoServerlessV2Config) {
		return nil, sv2Err
	}
	rawIam := vals.Get("EnableIAMDatabaseAuthentication")
	rawDel := vals.Get("DeletionProtection")
	opts := DBClusterModifyOptions{
		EngineVersion:                   vals.Get("EngineVersion"),
		PreferredBackupWindow:           vals.Get("PreferredBackupWindow"),
		PreferredMaintenanceWindow:      vals.Get("PreferredMaintenanceWindow"),
		EnableIAMDatabaseAuthentication: rawIam == formTrue,
		IamAuthSet:                      rawIam != "",
		ManageMasterUserPassword:        vals.Get("ManageMasterUserPassword") == formTrue,
		DeletionProtection:              rawDel == formTrue,
		DeletionProtectionSet:           rawDel != "",
		ServerlessV2ScalingConfig:       sv2,
	}
	cluster, err := h.Backend.ModifyDBCluster(id, paramGroupName, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStopDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StopDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &stopDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStartDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StartDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &startDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleFailoverDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.FailoverDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &failoverDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleCreateDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required for Neptune instances", ErrInvalidParameter)
	}
	instanceClass := vals.Get("DBInstanceClass")
	promotionTier := 0
	if pt := vals.Get("PromotionTier"); pt != "" {
		v, err := strconv.Atoi(pt)
		if err != nil || v < 0 || v > maxPromotionTier {
			return nil, fmt.Errorf("%w: PromotionTier must be 0-%d", ErrInvalidParameter, maxPromotionTier)
		}
		promotionTier = v
	}
	opts := DBInstanceCreateOptions{
		DBParameterGroupName:            vals.Get("DBParameterGroupName"),
		PreferredMaintenanceWindow:      vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:           vals.Get("PreferredBackupWindow"),
		AvailabilityZone:                vals.Get("AvailabilityZone"),
		AutoMinorVersionUpgrade:         vals.Get("AutoMinorVersionUpgrade") == formTrue,
		CopyTagsToSnapshot:              vals.Get("CopyTagsToSnapshot") == formTrue,
		EnableIAMDatabaseAuthentication: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		StorageEncrypted:                vals.Get("StorageEncrypted") == formTrue,
		PromotionTier:                   promotionTier,
	}
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	inst, err := h.Backend.CreateDBInstance(id, clusterID, instanceClass, opts)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(inst.DBInstanceArn, tags)
	}

	return &createDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instances, err := h.Backend.DescribeDBInstances(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBInstance, 0, len(instances))
	for _, inst := range instances {
		cp := inst
		members = append(members, toXMLInstance(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBInstancesResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBInstancesResult{
			DBInstances: xmlDBInstanceList{Members: members},
			Marker:      nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.DeleteDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleModifyDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	rawAuto := vals.Get("AutoMinorVersionUpgrade")
	rawCopy := vals.Get("CopyTagsToSnapshot")
	rawIam := vals.Get("EnableIAMDatabaseAuthentication")
	promotionTier := 0
	promotionTierSet := false
	if pt := vals.Get("PromotionTier"); pt != "" {
		v, err := strconv.Atoi(pt)
		if err != nil || v < 0 || v > maxPromotionTier {
			return nil, fmt.Errorf("%w: PromotionTier must be 0-%d", ErrInvalidParameter, maxPromotionTier)
		}
		promotionTier = v
		promotionTierSet = true
	}
	opts := DBInstanceModifyOptions{
		DBParameterGroupName:            vals.Get("DBParameterGroupName"),
		PreferredMaintenanceWindow:      vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:           vals.Get("PreferredBackupWindow"),
		AutoMinorVersionUpgrade:         rawAuto == formTrue,
		AutoMinorVersionUpgradeSet:      rawAuto != "",
		CopyTagsToSnapshot:              rawCopy == formTrue,
		CopyTagsToSnapshotSet:           rawCopy != "",
		EnableIAMDatabaseAuthentication: rawIam == formTrue,
		IamAuthSet:                      rawIam != "",
		PromotionTier:                   promotionTier,
		PromotionTierSet:                promotionTierSet,
	}
	inst, err := h.Backend.ModifyDBInstance(id, instanceClass, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleRebootDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCreateDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")
	subnetIDs := parseSubnetIDMembers(vals)
	sg, err := h.Backend.CreateDBSubnetGroup(name, description, vpcID, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         neptuneXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleDescribeDBSubnetGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	sgs, err := h.Backend.DescribeDBSubnetGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBSubnetGroup, 0, len(sgs))
	for _, sg := range sgs {
		cp := sg
		members = append(members, toXMLSubnetGroup(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBSubnetGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBSubnetGroupsResult{
			DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
			Marker:         nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	if err := h.Backend.DeleteDBSubnetGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleCreateDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBClusterParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:                   neptuneXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if err := h.Backend.DeleteDBClusterParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBClusterParameterGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	pg, err := h.Backend.ModifyDBClusterParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       neptuneXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleCreateDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snap, err := h.Backend.CreateDBClusterSnapshot(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLClusterSnapshot(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterSnapshotsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterSnapshotsResult{
			DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
			Marker:             nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snap, err := h.Backend.DeleteDBClusterSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleListTagsForResource(vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	tags, err := h.Backend.ListTagsForResource(arnStr)
	if err != nil {
		return nil, err
	}
	members := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		members = append(members, svcTags.KV(t))
	}

	return &listTagsForResourceResponse{
		Xmlns:   neptuneXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	if err := h.Backend.AddTagsToResource(arnStr, tags); err != nil {
		return nil, err
	}

	return &addTagsToResourceResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	if err := h.Backend.RemoveTagsFromResource(arnStr, keys); err != nil {
		return nil, err
	}

	return &removeTagsFromResourceResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleDescribeDBEngineVersions(_ url.Values) (any, error) {
	members := []xmlDBEngineVersion{
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.2.0.0",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune12,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.2.0.1",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune12,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.2.0.2",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune12,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.2.1.0",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune12,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          defaultEngineVersion,
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune13,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.3.1.0",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune13,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.3.2.0",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: pgFamilyNeptune13,
		},
		{
			Engine:                 neptuneEngine,
			EngineVersion:          "1.4.0.0",
			DBEngineDescription:    engineDescriptionAmazonNeptune,
			DBParameterGroupFamily: "neptune1.4",
		},
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            neptuneXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(_ url.Values) (any, error) {
	engineVersions := []string{"1.2.0.0", "1.2.1.0", defaultEngineVersion, "1.3.1.0", "1.4.0.0"}
	instanceClasses := []string{
		"db.r5.large", "db.r5.xlarge", "db.r5.2xlarge", "db.r5.4xlarge", "db.r5.8xlarge",
		"db.r6g.large", "db.r6g.xlarge", "db.r6g.2xlarge", "db.r6g.4xlarge",
		"db.t3.medium",
	}
	members := make([]xmlOrderableDBInstanceOption, 0, len(instanceClasses)*len(engineVersions))
	for _, ev := range engineVersions {
		for _, ic := range instanceClasses {
			members = append(members, xmlOrderableDBInstanceOption{
				Engine:          neptuneEngine,
				EngineVersion:   ev,
				DBInstanceClass: ic,
			})
		}
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeGlobalClusters(_ url.Values) (any, error) {
	gcs := h.Backend.DescribeGlobalClusters()
	members := make([]xmlGlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeGlobalClustersResult{
			GlobalClusters: xmlGlobalClusterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleAddRoleToDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")
	if err := h.Backend.AddRoleToDBCluster(clusterID, roleARN); err != nil {
		return nil, err
	}

	return &addRoleToDBClusterResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleAddSourceIdentifierToSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.AddSourceIdentifierToSubscription(name, sourceID)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleApplyPendingMaintenanceAction(vals url.Values) (any, error) {
	resourceID := vals.Get("ResourceIdentifier")
	applyAction := vals.Get("ApplyAction")
	optInType := vals.Get("OptInType")
	if err := h.Backend.ApplyPendingMaintenanceAction(resourceID, applyAction, optInType); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(vals url.Values) (any, error) {
	sourceName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")
	pg, err := h.Backend.CopyDBClusterParameterGroup(sourceName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:                   neptuneXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCopyDBClusterSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBClusterSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBClusterSnapshotIdentifier")
	snap, err := h.Backend.CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterSnapshotResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleCopyDBParameterGroup(vals url.Values) (any, error) {
	sourceName := vals.Get("SourceDBParameterGroupIdentifier")
	targetName := vals.Get("TargetDBParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBParameterGroupDescription")
	pg, err := h.Backend.CopyDBParameterGroup(sourceName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBParameterGroupResponse{
		Xmlns:            neptuneXMLNS,
		DBParameterGroup: toXMLDBParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCreateDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.CreateDBClusterEndpoint(endpointID, clusterID, endpointType)
	if err != nil {
		return nil, err
	}

	return &createDBClusterEndpointResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterEndpoint: toXMLClusterEndpoint(ep),
	}, nil
}

func (h *Handler) handleCreateDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBParameterGroupResponse{
		Xmlns:            neptuneXMLNS,
		DBParameterGroup: toXMLDBParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceIDs := parseSourceIDMembers(vals)
	sub, err := h.Backend.CreateEventSubscription(name, snsTopicARN, sourceIDs)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleCreateGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	sourceDBClusterID := vals.Get("SourceDBClusterIdentifier")
	gc, err := h.Backend.CreateGlobalCluster(globalClusterID, sourceDBClusterID)
	if err != nil {
		return nil, err
	}

	return &createGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	if err := h.Backend.DeleteDBClusterEndpoint(endpointID); err != nil {
		return nil, err
	}

	return &deleteDBClusterEndpointResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleDescribeDBClusterEndpoints(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	endpoints, err := h.Backend.DescribeDBClusterEndpoints(endpointID, clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		cp := ep
		members = append(members, toXMLClusterEndpoint(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterEndpointsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterEndpointsResult{
			DBClusterEndpoints: xmlDBClusterEndpointList{Members: members},
			Marker:             nextMarker,
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.ModifyDBClusterEndpoint(endpointID, endpointType)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterEndpointResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterEndpoint: toXMLClusterEndpoint(ep),
	}, nil
}

func (h *Handler) handleDeleteDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if err := h.Backend.DeleteDBParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBParameterGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleDescribeDBParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	groups, err := h.Backend.DescribeDBParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLDBParameterGroup(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBParameterGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBParameterGroupsResult{
			DBParameterGroups: xmlDBParameterGroupList{Members: members},
			Marker:            nextMarker,
		},
	}, nil
}

func (h *Handler) handleDescribeDBParameters(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if name != "" {
		if _, err := h.Backend.DescribeDBParameterGroups(name); err != nil {
			return nil, err
		}
	}

	return &describeDBParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBParametersResult{
			Parameters: xmlParameterList{},
		},
	}, nil
}

func (h *Handler) handleModifyDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	pg, err := h.Backend.ModifyDBParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &modifyDBParameterGroupResponse{
		Xmlns:                neptuneXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleResetDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	pg, err := h.Backend.ResetDBParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &resetDBParameterGroupResponse{
		Xmlns:                neptuneXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if name != "" {
		if _, err := h.Backend.DescribeDBClusterParameterGroups(name); err != nil {
			return nil, err
		}
	}

	return &describeDBClusterParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterParametersResult{
			Parameters: xmlParameterList{},
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	if snapshotID != "" {
		if _, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, ""); err != nil {
			return nil, err
		}
	}

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterSnapshotAttributesResult{
			DBClusterSnapshotAttributesResult: xmlDBClusterSnapshotAttributesResult{
				DBClusterSnapshotIdentifier: snapshotID,
			},
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	if snapshotID != "" {
		if _, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, ""); err != nil {
			return nil, err
		}
	}

	return &modifyDBClusterSnapshotAttributeResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleResetDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	pg, err := h.Backend.ResetDBClusterParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &resetDBClusterParameterGroupResponse{
		Xmlns:                       neptuneXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	subs, err := h.Backend.DescribeEventSubscriptions(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlEventSubscription, 0, len(subs))
	for _, sub := range subs {
		cp := sub
		members = append(members, toXMLEventSubscription(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventSubscriptionsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEventSubscriptionsResult{
			EventSubscriptionsList: xmlEventSubscriptionList{Members: members},
			Marker:                 nextMarker,
		},
	}, nil
}

func (h *Handler) handleModifyEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sub, err := h.Backend.ModifyEventSubscription(name, snsTopicARN)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(name, sourceID)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             neptuneXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventCategories(_ url.Values) (any, error) {
	return &describeEventCategoriesResponse{
		Xmlns: neptuneXMLNS,
		EventCategoriesMapList: xmlEventCategoriesMapList{
			Members: []xmlEventCategoriesMap{
				{SourceType: "db-cluster", EventCategories: xmlEventCategoryList{Members: []string{
					"failover", "maintenance", sourceTypeNotification, "failure", "availability",
				}}},
				{SourceType: "db-instance", EventCategories: xmlEventCategoryList{Members: []string{
					"availability", "deletion", "failover", "failure", "maintenance",
					sourceTypeNotification, "recovery", "restoration",
				}}},
				{SourceType: "db-parameter-group", EventCategories: xmlEventCategoryList{Members: []string{
					"configuration change",
				}}},
				{SourceType: "db-cluster-snapshot", EventCategories: xmlEventCategoryList{Members: []string{
					"backup", sourceTypeNotification,
				}}},
			},
		},
	}, nil
}

func (h *Handler) handleDescribeEvents(_ url.Values) (any, error) {
	return &describeEventsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEventsResult{
			Events: xmlEventList{},
		},
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.DeleteGlobalCluster(globalClusterID)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.FailoverGlobalCluster(globalClusterID, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.ModifyGlobalCluster(globalClusterID)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleRemoveFromGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	dbClusterARN := vals.Get("DbClusterIdentifier")
	gc, err := h.Backend.RemoveFromGlobalCluster(globalClusterID, dbClusterARN)
	if err != nil {
		return nil, err
	}

	return &removeFromGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(globalClusterID, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         neptuneXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleRemoveRoleFromDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")
	if err := h.Backend.RemoveRoleFromDBCluster(clusterID, roleARN); err != nil {
		return nil, err
	}

	return &removeRoleFromDBClusterResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	if family == "" {
		family = pgFamilyNeptune13
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEngineDefaultClusterParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlParameterList{},
			},
		},
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	if family == "" {
		family = pgFamilyNeptune13
	}

	return &describeEngineDefaultParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEngineDefaultParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlParameterList{},
			},
		},
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(_ url.Values) (any, error) {
	return &describePendingMaintenanceActionsResponse{
		Xmlns: neptuneXMLNS,
		Result: describePendingMaintenanceActionsResult{
			PendingMaintenanceActions: xmlPendingMaintenanceActionsList{},
		},
	}, nil
}

func (h *Handler) handleDescribeValidDBInstanceModifications(_ url.Values) (any, error) {
	validClasses := []xmlValidStorageOption{
		{DBInstanceClass: "db.r5.large"},
		{DBInstanceClass: "db.r5.xlarge"},
		{DBInstanceClass: "db.r5.2xlarge"},
		{DBInstanceClass: "db.r5.4xlarge"},
		{DBInstanceClass: "db.r5.8xlarge"},
		{DBInstanceClass: "db.r6g.large"},
		{DBInstanceClass: "db.r6g.xlarge"},
		{DBInstanceClass: "db.r6g.2xlarge"},
		{DBInstanceClass: "db.r6g.4xlarge"},
		{DBInstanceClass: "db.t3.medium"},
	}

	return &describeValidDBInstanceModificationsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeValidDBInstanceModificationsResult{
			ValidDBInstanceModificationsMessage: xmlValidDBInstanceModificationsMessage{
				ValidProcessorFeatures: validClasses,
			},
		},
	}, nil
}

func (h *Handler) handlePromoteReadReplicaDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(id)
	if err != nil {
		return nil, err
	}
	if len(clusters) == 0 {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cluster := clusters[0]

	return &promoteReadReplicaDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(&cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterFromSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterFromSnapshot(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromSnapshotResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterToPointInTime(vals url.Values) (any, error) {
	srcClusterID := vals.Get("SourceDBClusterIdentifier")
	targetClusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterToPointInTime(srcClusterID, targetClusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterToPointInTimeResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	sg, err := h.Backend.ModifyDBSubnetGroup(name, description)
	if err != nil {
		return nil, err
	}

	return &modifyDBSubnetGroupResponse{
		Xmlns:         neptuneXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := neptuneErrorCode(opErr)
	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("Neptune internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func neptuneErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}
	mappings := []errorMapping{
		{ErrClusterNotFound, "DBClusterNotFoundFault"},
		{ErrClusterAlreadyExists, "DBClusterAlreadyExistsFault"},
		{ErrInstanceNotFound, "DBInstanceNotFound"},
		{ErrInstanceAlreadyExists, "DBInstanceAlreadyExists"},
		{ErrSubnetGroupNotFound, "DBSubnetGroupNotFoundFault"},
		{ErrSubnetGroupAlreadyExists, "DBSubnetGroupAlreadyExistsFault"},
		{ErrClusterParameterGroupNotFound, "DBClusterParameterGroupNotFoundFault"},
		{ErrClusterParameterGroupAlreadyExists, "DBClusterParameterGroupAlreadyExistsFault"},
		{ErrClusterSnapshotNotFound, "DBClusterSnapshotNotFoundFault"},
		{ErrClusterSnapshotAlreadyExists, "DBClusterSnapshotAlreadyExistsFault"},
		{ErrParameterGroupNotFound, "DBParameterGroupNotFoundFault"},
		{ErrParameterGroupAlreadyExists, "DBParameterGroupAlreadyExistsFault"},
		{ErrClusterEndpointNotFound, "DBClusterEndpointNotFoundFault"},
		{ErrClusterEndpointAlreadyExists, "DBClusterEndpointAlreadyExistsFault"},
		{ErrSubscriptionNotFound, "SubscriptionNotFoundFault"},
		{ErrSubscriptionAlreadyExists, "SubscriptionAlreadyExistFault"},
		{ErrGlobalClusterNotFound, "GlobalClusterNotFoundFault"},
		{ErrGlobalClusterAlreadyExists, "GlobalClusterAlreadyExistsFault"},
		{ErrInvalidParameter, "InvalidParameterValue"},
		{ErrUnknownAction, "InvalidAction"},
		{ErrInvalidDBClusterStateFault, "InvalidDBClusterStateFault"},
	}
	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &neptuneErrorResponse{
		Xmlns: neptuneXMLNS,
		Error: neptuneError{Code: code, Message: message, Type: "Sender"},
	}
	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// parseServerlessV2ScalingConfig parses ServerlessV2ScalingConfiguration from form values.
// Returns nil, errNoServerlessV2Config when neither field is present.
func parseServerlessV2ScalingConfig(vals url.Values) (*ServerlessV2ScalingConfiguration, error) {
	rawMin := vals.Get("ServerlessV2ScalingConfiguration.MinCapacity")
	rawMax := vals.Get("ServerlessV2ScalingConfiguration.MaxCapacity")
	if rawMin == "" && rawMax == "" {
		return nil, errNoServerlessV2Config
	}
	cfg := &ServerlessV2ScalingConfiguration{}
	if rawMin != "" {
		v, err := strconv.ParseFloat(rawMin, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid ServerlessV2ScalingConfiguration.MinCapacity %q",
				ErrInvalidParameter,
				rawMin,
			)
		}
		cfg.MinCapacity = v
	}
	if rawMax != "" {
		v, err := strconv.ParseFloat(rawMax, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid ServerlessV2ScalingConfiguration.MaxCapacity %q",
				ErrInvalidParameter,
				rawMax,
			)
		}
		cfg.MaxCapacity = v
	}

	return cfg, nil
}

func parseSubnetIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.member.%d", i))
		if sid == "" {
			return ids
		}
		ids = append(ids, sid)
	}
}

func parseTagEntries(vals url.Values) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			return tags
		}
		tags = append(tags, Tag{Key: key, Value: vals.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))})
	}
}

func validateTagEntries(tags []Tag) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrInvalidParameter, maxTagsPerResource)
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

func parseTagKeyMembers(vals url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

func toXMLCluster(c *DBCluster) xmlDBCluster {
	memberItems := make([]xmlDBClusterMember, 0, len(c.DBClusterMembers))
	for _, m := range c.DBClusterMembers {
		memberItems = append(memberItems, xmlDBClusterMember(m))
	}
	x := xmlDBCluster{
		DBClusterIdentifier:             c.DBClusterIdentifier,
		DBClusterArn:                    c.DBClusterArn,
		Engine:                          c.Engine,
		EngineVersion:                   c.EngineVersion,
		EngineMode:                      c.EngineMode,
		Status:                          c.Status,
		DBClusterParameterGroupName:     c.DBClusterParameterGroupName,
		DBSubnetGroupName:               c.DBSubnetGroupName,
		Endpoint:                        c.Endpoint,
		ReaderEndpoint:                  c.ReaderEndpoint,
		Port:                            c.Port,
		StorageEncrypted:                c.StorageEncrypted,
		MultiAZ:                         c.MultiAZ,
		BackupRetentionPeriod:           c.BackupRetentionPeriod,
		EnableIAMDatabaseAuthentication: c.EnableIAMDatabaseAuthentication,
		DeletionProtection:              c.DeletionProtection,
		PreferredBackupWindow:           c.PreferredBackupWindow,
		PreferredMaintenanceWindow:      c.PreferredMaintenanceWindow,
		KmsKeyID:                        c.KmsKeyID,
		DBClusterMembers:                xmlDBClusterMemberList{Members: memberItems},
	}
	if c.ServerlessV2ScalingConfig != nil {
		x.ServerlessV2ScalingConfiguration = &xmlServerlessV2ScalingConfiguration{
			MinCapacity: c.ServerlessV2ScalingConfig.MinCapacity,
			MaxCapacity: c.ServerlessV2ScalingConfig.MaxCapacity,
		}
	}
	if c.MasterUserManagedSecret != nil {
		x.MasterUserManagedSecret = &xmlMasterUserManagedSecret{
			SecretARN:    c.MasterUserManagedSecret.SecretARN,
			SecretStatus: c.MasterUserManagedSecret.SecretStatus,
		}
	}

	return x
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	return xmlDBInstance{
		DBInstanceIdentifier:            inst.DBInstanceIdentifier,
		DBInstanceArn:                   inst.DBInstanceArn,
		DBClusterIdentifier:             inst.DBClusterIdentifier,
		DBInstanceClass:                 inst.DBInstanceClass,
		Engine:                          inst.Engine,
		EngineVersion:                   inst.EngineVersion,
		DBInstanceStatus:                inst.DBInstanceStatus,
		Endpoint:                        inst.Endpoint,
		Port:                            inst.Port,
		StorageEncrypted:                inst.StorageEncrypted,
		AutoMinorVersionUpgrade:         inst.AutoMinorVersionUpgrade,
		PreferredMaintenanceWindow:      inst.PreferredMaintenanceWindow,
		PreferredBackupWindow:           inst.PreferredBackupWindow,
		AvailabilityZone:                inst.AvailabilityZone,
		DBParameterGroupName:            inst.DBParameterGroupName,
		CopyTagsToSnapshot:              inst.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: inst.EnableIAMDatabaseAuthentication,
		PromotionTier:                   inst.PromotionTier,
	}
}

func toXMLSubnetGroup(sg *DBSubnetGroup) xmlDBSubnetGroup {
	subnetMembers := make([]xmlSubnet, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnetMembers = append(subnetMembers, xmlSubnet{SubnetIdentifier: id})
	}

	return xmlDBSubnetGroup{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		VpcID:                    sg.VpcID,
		SubnetGroupStatus:        sg.Status,
		Subnets:                  xmlSubnetList{Members: subnetMembers},
	}
}

func toXMLParameterGroup(pg *DBClusterParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
	}
}

func toXMLClusterSnapshot(snap *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: snap.DBClusterSnapshotIdentifier,
		DBClusterSnapshotArn:        snap.DBClusterSnapshotArn,
		DBClusterIdentifier:         snap.DBClusterIdentifier,
		Engine:                      snap.Engine,
		EngineVersion:               snap.EngineVersion,
		Status:                      snap.Status,
		StorageEncrypted:            snap.StorageEncrypted,
		SnapshotType:                snap.SnapshotType,
	}
}

func toXMLDBParameterGroup(pg *DBParameterGroup) xmlDBParameterGroup {
	return xmlDBParameterGroup{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
	}
}

func toXMLClusterEndpoint(ep *DBClusterEndpoint) xmlDBClusterEndpoint {
	return xmlDBClusterEndpoint{
		DBClusterEndpointIdentifier: ep.DBClusterEndpointIdentifier,
		DBClusterIdentifier:         ep.DBClusterIdentifier,
		EndpointType:                ep.EndpointType,
		Status:                      ep.Status,
		Endpoint:                    ep.Endpoint,
	}
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]xmlSourceID, 0, len(sub.SourceIDs))
	for _, id := range sub.SourceIDs {
		ids = append(ids, xmlSourceID{Member: id})
	}

	return xmlEventSubscription{
		CustSubscriptionID: sub.CustSubscriptionID,
		SnsTopicARN:        sub.SnsTopicARN,
		Status:             sub.Status,
		SourceIDs:          xmlSourceIDList{Members: ids},
	}
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	members := make([]xmlGlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		members = append(members, xmlGlobalClusterMember(m))
	}

	return xmlGlobalCluster{
		GlobalClusterIdentifier: gc.GlobalClusterIdentifier,
		Status:                  gc.Status,
		GlobalClusterMembers:    xmlGlobalClusterMemberList{Members: members},
	}
}

func parseSourceIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("SourceIds.member.%d", i))
		if id == "" {
			return ids
		}
		ids = append(ids, id)
	}
}

type neptuneError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type neptuneErrorResponse struct {
	XMLName xml.Name     `xml:"ErrorResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Error   neptuneError `xml:"Error"`
}

type xmlDBClusterMember struct {
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	IsClusterWriter      bool   `xml:"IsClusterWriter"`
}

type xmlDBClusterMemberList struct {
	Members []xmlDBClusterMember `xml:"DBClusterMember"`
}

type xmlServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `xml:"MinCapacity"`
	MaxCapacity float64 `xml:"MaxCapacity"`
}

type xmlMasterUserManagedSecret struct {
	SecretARN    string `xml:"SecretArn,omitempty"`
	SecretStatus string `xml:"SecretStatus,omitempty"`
}

// xmlSV2Ref is a type alias to keep xmlDBCluster field definitions within line-length limits.
type xmlSV2Ref = xmlServerlessV2ScalingConfiguration

type xmlDBCluster struct {
	ServerlessV2ScalingConfiguration *xmlSV2Ref                  `xml:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret          *xmlMasterUserManagedSecret `xml:"MasterUserManagedSecret,omitempty"`
	DBClusterIdentifier              string                      `xml:"DBClusterIdentifier"`
	DBClusterArn                     string                      `xml:"DBClusterArn,omitempty"`
	Engine                           string                      `xml:"Engine"`
	EngineVersion                    string                      `xml:"EngineVersion,omitempty"`
	EngineMode                       string                      `xml:"EngineMode,omitempty"`
	Status                           string                      `xml:"Status"`
	DBClusterParameterGroupName      string                      `xml:"DBClusterParameterGroup,omitempty"`
	DBSubnetGroupName                string                      `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	Endpoint                         string                      `xml:"Endpoint,omitempty"`
	ReaderEndpoint                   string                      `xml:"ReaderEndpoint,omitempty"`
	PreferredBackupWindow            string                      `xml:"PreferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow       string                      `xml:"PreferredMaintenanceWindow,omitempty"`
	KmsKeyID                         string                      `xml:"KmsKeyId,omitempty"`
	DBClusterMembers                 xmlDBClusterMemberList      `xml:"DBClusterMembers"`
	Port                             int                         `xml:"Port"`
	BackupRetentionPeriod            int                         `xml:"BackupRetentionPeriod"`
	EnableIAMDatabaseAuthentication  bool                        `xml:"IAMDatabaseAuthenticationEnabled"`
	StorageEncrypted                 bool                        `xml:"StorageEncrypted"`
	MultiAZ                          bool                        `xml:"MultiAZ"`
	DeletionProtection               bool                        `xml:"DeletionProtection"`
}

type xmlDBClusterList struct {
	Members []xmlDBCluster `xml:"DBCluster"`
}

type createDBClusterResponse struct {
	XMLName   xml.Name     `xml:"CreateDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"CreateDBClusterResult>DBCluster"`
}

type describeDBClustersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	DBClusters xmlDBClusterList `xml:"DBClusters"`
}

type describeDBClustersResponse struct {
	XMLName xml.Name                 `xml:"DescribeDBClustersResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  describeDBClustersResult `xml:"DescribeDBClustersResult"`
}

type deleteDBClusterResponse struct {
	XMLName   xml.Name     `xml:"DeleteDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"DeleteDBClusterResult>DBCluster"`
}

type modifyDBClusterResponse struct {
	XMLName   xml.Name     `xml:"ModifyDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"ModifyDBClusterResult>DBCluster"`
}

type stopDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StopDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StopDBClusterResult>DBCluster"`
}

type startDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StartDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StartDBClusterResult>DBCluster"`
}

type failoverDBClusterResponse struct {
	XMLName   xml.Name     `xml:"FailoverDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"FailoverDBClusterResult>DBCluster"`
}

type xmlDBInstance struct {
	DBInstanceIdentifier            string `xml:"DBInstanceIdentifier"`
	DBInstanceArn                   string `xml:"DBInstanceArn,omitempty"`
	DBClusterIdentifier             string `xml:"DBClusterIdentifier,omitempty"`
	DBInstanceClass                 string `xml:"DBInstanceClass"`
	Engine                          string `xml:"Engine"`
	EngineVersion                   string `xml:"EngineVersion,omitempty"`
	DBInstanceStatus                string `xml:"DBInstanceStatus"`
	Endpoint                        string `xml:"Endpoint>Address,omitempty"`
	DBParameterGroupName            string `xml:"DBParameterGroups>DBParameterGroup>DBParameterGroupName,omitempty"`
	PreferredMaintenanceWindow      string `xml:"PreferredMaintenanceWindow,omitempty"`
	PreferredBackupWindow           string `xml:"PreferredBackupWindow,omitempty"`
	AvailabilityZone                string `xml:"AvailabilityZone,omitempty"`
	Port                            int    `xml:"Endpoint>Port"`
	PromotionTier                   int    `xml:"PromotionTier,omitempty"`
	StorageEncrypted                bool   `xml:"StorageEncrypted"`
	AutoMinorVersionUpgrade         bool   `xml:"AutoMinorVersionUpgrade"`
	CopyTagsToSnapshot              bool   `xml:"CopyTagsToSnapshot"`
	EnableIAMDatabaseAuthentication bool   `xml:"IAMDatabaseAuthenticationEnabled"`
}

type xmlDBInstanceList struct {
	Members []xmlDBInstance `xml:"DBInstance"`
}

type createDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceResult>DBInstance"`
}

type describeDBInstancesResult struct {
	Marker      string            `xml:"Marker,omitempty"`
	DBInstances xmlDBInstanceList `xml:"DBInstances"`
}

type describeDBInstancesResponse struct {
	XMLName xml.Name                  `xml:"DescribeDBInstancesResponse"`
	Xmlns   string                    `xml:"xmlns,attr"`
	Result  describeDBInstancesResult `xml:"DescribeDBInstancesResult"`
}

type deleteDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"DeleteDBInstanceResult>DBInstance"`
}

type modifyDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"ModifyDBInstanceResult>DBInstance"`
}

type rebootDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RebootDBInstanceResult>DBInstance"`
}

type xmlSubnet struct {
	SubnetIdentifier string `xml:"SubnetIdentifier"`
}

type xmlSubnetList struct {
	Members []xmlSubnet `xml:"Subnet"`
}

type xmlDBSubnetGroup struct {
	DBSubnetGroupName        string        `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string        `xml:"DBSubnetGroupDescription"`
	VpcID                    string        `xml:"VpcId,omitempty"`
	SubnetGroupStatus        string        `xml:"SubnetGroupStatus"`
	Subnets                  xmlSubnetList `xml:"Subnets"`
}

type xmlDBSubnetGroupList struct {
	Members []xmlDBSubnetGroup `xml:"DBSubnetGroup"`
}

type createDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"CreateDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
}

type describeDBSubnetGroupsResult struct {
	Marker         string               `xml:"Marker,omitempty"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DBSubnetGroups"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName xml.Name                     `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns   string                       `xml:"xmlns,attr"`
	Result  describeDBSubnetGroupsResult `xml:"DescribeDBSubnetGroupsResult"`
}

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
}

type xmlDBClusterParameterGroupList struct {
	Members []xmlDBClusterParameterGroup `xml:"DBClusterParameterGroup"`
}

type createDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type describeDBClusterParameterGroupsResult struct {
	DBClusterParameterGroups xmlDBClusterParameterGroupList `xml:"DBClusterParameterGroups"`
}

type describeDBClusterParameterGroupsResponse struct {
	XMLName xml.Name                               `xml:"DescribeDBClusterParameterGroupsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  describeDBClusterParameterGroupsResult `xml:"DescribeDBClusterParameterGroupsResult"`
}

type deleteDBClusterParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `xml:"DBClusterSnapshotArn,omitempty"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
	Status                      string `xml:"Status"`
	SnapshotType                string `xml:"SnapshotType,omitempty"`
	StorageEncrypted            bool   `xml:"StorageEncrypted"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResult struct {
	Marker             string                   `xml:"Marker,omitempty"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DBClusterSnapshots"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName xml.Name                         `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotsResult `xml:"DescribeDBClusterSnapshotsResult"`
}

type deleteDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"DeleteDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlDBEngineVersion struct {
	Engine                 string `xml:"Engine"`
	EngineVersion          string `xml:"EngineVersion"`
	DBEngineDescription    string `xml:"DBEngineDescription"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily,omitempty"`
}

type xmlDBEngineVersionList struct {
	Members []xmlDBEngineVersion `xml:"DBEngineVersion"`
}

type describeDBEngineVersionsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBEngineVersionsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	DBEngineVersions xmlDBEngineVersionList `xml:"DescribeDBEngineVersionsResult>DBEngineVersions"`
}

type xmlOrderableDBInstanceOption struct {
	Engine          string `xml:"Engine"`
	EngineVersion   string `xml:"EngineVersion"`
	DBInstanceClass string `xml:"DBInstanceClass"`
}

type xmlOrderableDBInstanceOptionList struct {
	Members []xmlOrderableDBInstanceOption `xml:"OrderableDBInstanceOption"`
}

type describeOrderableDBInstanceOptionsResult struct {
	OrderableDBInstanceOptions xmlOrderableDBInstanceOptionList `xml:"OrderableDBInstanceOptions"`
}

type describeOrderableDBInstanceOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableDBInstanceOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeOrderableDBInstanceOptionsResult `xml:"DescribeOrderableDBInstanceOptionsResult"`
}

type describeGlobalClustersResult struct {
	GlobalClusters xmlGlobalClusterList `xml:"GlobalClusters"`
}

type describeGlobalClustersResponse struct {
	XMLName xml.Name                     `xml:"DescribeGlobalClustersResponse"`
	Xmlns   string                       `xml:"xmlns,attr"`
	Result  describeGlobalClustersResult `xml:"DescribeGlobalClustersResult"`
}

type addRoleToDBClusterResponse struct {
	XMLName xml.Name `xml:"AddRoleToDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type applyPendingMaintenanceActionResponse struct {
	XMLName xml.Name `xml:"ApplyPendingMaintenanceActionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlDBParameterGroup struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
}

type copyDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CopyDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type copyDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CopyDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
}

type copyDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CopyDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CopyDBParameterGroupResult>DBParameterGroup"`
}

type xmlDBClusterEndpoint struct {
	DBClusterEndpointIdentifier string `xml:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	EndpointType                string `xml:"EndpointType"`
	Status                      string `xml:"Status"`
	Endpoint                    string `xml:"Endpoint,omitempty"`
}

type createDBClusterEndpointResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterEndpointResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterEndpoint xmlDBClusterEndpoint `xml:"CreateDBClusterEndpointResult"`
}

type createDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
}

type xmlSourceID struct {
	Member string `xml:",chardata"`
}

type xmlSourceIDList struct {
	Members []xmlSourceID `xml:"member"`
}

type xmlEventSubscription struct {
	CustSubscriptionID string          `xml:"CustSubscriptionId"`
	SnsTopicARN        string          `xml:"SnsTopicArn"`
	Status             string          `xml:"Status"`
	SourceIDs          xmlSourceIDList `xml:"SourceIdsList"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

type xmlGlobalClusterMember struct {
	DBClusterARN string `xml:"DBClusterArn"`
	IsWriter     bool   `xml:"IsWriter"`
}

type xmlGlobalClusterMemberList struct {
	Members []xmlGlobalClusterMember `xml:"GlobalClusterMember"`
}

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalCluster"`
}

type xmlGlobalCluster struct {
	GlobalClusterIdentifier string                     `xml:"GlobalClusterIdentifier"`
	Status                  string                     `xml:"Status"`
	GlobalClusterMembers    xmlGlobalClusterMemberList `xml:"GlobalClusterMembers"`
}

type createGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"CreateGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"CreateGlobalClusterResult>GlobalCluster"`
}

const defaultNeptuneMaxRecords = 100

// applyNeptuneMarker applies Marker/MaxRecords-based pagination to a slice.
func applyNeptuneMarker[T any](items []T, marker, maxRecordsStr string) ([]T, string) {
	start := 0
	if marker != "" {
		idx, err := strconv.Atoi(marker)
		if err == nil && idx > 0 {
			start = idx
		}
	}

	if start >= len(items) {
		return []T{}, ""
	}

	items = items[start:]

	limit := defaultNeptuneMaxRecords
	if maxRecordsStr != "" {
		if n, err := strconv.Atoi(maxRecordsStr); err == nil && n > 0 {
			limit = n
		}
	}

	if len(items) <= limit {
		return items, ""
	}

	return items[:limit], strconv.Itoa(start + limit)
}

// New XML types for the additional operations.

type xmlDBClusterEndpointList struct {
	Members []xmlDBClusterEndpoint `xml:"DBClusterEndpoint"`
}

type describeDBClusterEndpointsResult struct {
	Marker             string                   `xml:"Marker,omitempty"`
	DBClusterEndpoints xmlDBClusterEndpointList `xml:"DBClusterEndpoints"`
}

type describeDBClusterEndpointsResponse struct {
	XMLName xml.Name                         `xml:"DescribeDBClusterEndpointsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeDBClusterEndpointsResult `xml:"DescribeDBClusterEndpointsResult"`
}

type deleteDBClusterEndpointResponse struct {
	XMLName xml.Name `xml:"DeleteDBClusterEndpointResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBClusterEndpointResponse struct {
	XMLName           xml.Name             `xml:"ModifyDBClusterEndpointResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterEndpoint xmlDBClusterEndpoint `xml:"ModifyDBClusterEndpointResult"`
}

type xmlDBParameterGroupList struct {
	Members []xmlDBParameterGroup `xml:"DBParameterGroup"`
}

type describeDBParameterGroupsResult struct {
	Marker            string                  `xml:"Marker,omitempty"`
	DBParameterGroups xmlDBParameterGroupList `xml:"DBParameterGroups"`
}

type describeDBParameterGroupsResponse struct {
	XMLName xml.Name                        `xml:"DescribeDBParameterGroupsResponse"`
	Xmlns   string                          `xml:"xmlns,attr"`
	Result  describeDBParameterGroupsResult `xml:"DescribeDBParameterGroupsResult"`
}

type deleteDBParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ModifyDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ModifyDBParameterGroupResult>DBParameterGroupName"`
}

type resetDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ResetDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ResetDBParameterGroupResult>DBParameterGroupName"`
}

type xmlParameterList struct {
	Members []xmlParameter `xml:"Parameter"`
}

type xmlParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
}

type describeDBParametersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	Parameters xmlParameterList `xml:"Parameters"`
}

type describeDBParametersResponse struct {
	XMLName xml.Name                   `xml:"DescribeDBParametersResponse"`
	Xmlns   string                     `xml:"xmlns,attr"`
	Result  describeDBParametersResult `xml:"DescribeDBParametersResult"`
}

type describeDBClusterParametersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	Parameters xmlParameterList `xml:"Parameters"`
}

type describeDBClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDBClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  describeDBClusterParametersResult `xml:"DescribeDBClusterParametersResult"`
}

type xmlDBClusterSnapshotAttribute struct {
	AttributeName   string `xml:"AttributeName"`
	AttributeValues string `xml:"AttributeValues,omitempty"`
}

type xmlDBClusterSnapshotAttrList struct {
	Members []xmlDBClusterSnapshotAttribute `xml:"DBClusterSnapshotAttribute"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                       `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBClusterSnapshotAttrList `xml:"DBClusterSnapshotAttributes"`
}

type describeDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotAttributesResult xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type describeDBClusterSnapshotAttributesResponse struct {
	XMLName xml.Name                                  `xml:"DescribeDBClusterSnapshotAttributesResponse"`
	Xmlns   string                                    `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotAttributesResult `xml:"DescribeDBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResponse struct {
	XMLName xml.Name `xml:"ModifyDBClusterSnapshotAttributeResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type resetDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type xmlEventSubscriptionList struct {
	Members []xmlEventSubscription `xml:"EventSubscription"`
}

type describeEventSubscriptionsResult struct {
	Marker                 string                   `xml:"Marker,omitempty"`
	EventSubscriptionsList xmlEventSubscriptionList `xml:"EventSubscriptionsList"`
}

type describeEventSubscriptionsResponse struct {
	XMLName xml.Name                         `xml:"DescribeEventSubscriptionsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeEventSubscriptionsResult `xml:"DescribeEventSubscriptionsResult"`
}

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
}

type modifyEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"ModifyEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"ModifyEventSubscriptionResult>EventSubscription"`
}

type removeSourceIdentifierFromSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"RemoveSourceIdentifierFromSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"RemoveSourceIdentifierFromSubscriptionResult>EventSubscription"`
}

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventCategoriesMap struct {
	SourceType      string               `xml:"SourceType"`
	EventCategories xmlEventCategoryList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoriesMap `xml:"EventCategoriesMap"`
}

type describeEventCategoriesResponse struct {
	XMLName                xml.Name                  `xml:"DescribeEventCategoriesResponse"`
	Xmlns                  string                    `xml:"xmlns,attr"`
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"DescribeEventCategoriesResult>EventCategoriesMapList"`
}

type xmlEvent struct {
	SourceIdentifier string `xml:"SourceIdentifier,omitempty"`
	SourceType       string `xml:"SourceType,omitempty"`
	Message          string `xml:"Message,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResult struct {
	Marker string       `xml:"Marker,omitempty"`
	Events xmlEventList `xml:"Events"`
}

type describeEventsResponse struct {
	XMLName xml.Name             `xml:"DescribeEventsResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Result  describeEventsResult `xml:"DescribeEventsResult"`
}

type deleteGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"DeleteGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"DeleteGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
}

type modifyGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"ModifyGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"ModifyGlobalClusterResult>GlobalCluster"`
}

type removeFromGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"RemoveFromGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"RemoveFromGlobalClusterResult>GlobalCluster"`
}

type switchoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"SwitchoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"SwitchoverGlobalClusterResult>GlobalCluster"`
}

type removeRoleFromDBClusterResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlEngineDefaults struct {
	DBParameterGroupFamily string           `xml:"DBParameterGroupFamily"`
	Parameters             xmlParameterList `xml:"Parameters"`
}

type describeEngineDefaultClusterParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName xml.Name                                     `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns   string                                       `xml:"xmlns,attr"`
	Result  describeEngineDefaultClusterParametersResult `xml:"DescribeEngineDefaultClusterParametersResult"`
}

type describeEngineDefaultParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultParametersResponse struct {
	XMLName xml.Name                              `xml:"DescribeEngineDefaultParametersResponse"`
	Xmlns   string                                `xml:"xmlns,attr"`
	Result  describeEngineDefaultParametersResult `xml:"DescribeEngineDefaultParametersResult"`
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	Description string `xml:"Description,omitempty"`
}

type xmlPendingMaintenanceActionsList struct {
	Members []xmlPendingMaintenanceAction `xml:"ResourcePendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResult struct {
	PendingMaintenanceActions xmlPendingMaintenanceActionsList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                                `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describePendingMaintenanceActionsResult `xml:"DescribePendingMaintenanceActionsResult"`
}

type xmlValidStorageOption struct {
	DBInstanceClass string `xml:"DBInstanceClass"`
}

type xmlValidDBInstanceModificationsMessage struct {
	ValidProcessorFeatures []xmlValidStorageOption `xml:"ValidProcessorFeatures>AvailableProcessorFeature"`
}

type describeValidDBInstanceModificationsResult struct {
	ValidDBInstanceModificationsMessage xmlValidDBInstanceModificationsMessage `xml:"ValidDBInstanceModificationsMessage"`
}

type describeValidDBInstanceModificationsResponse struct {
	XMLName xml.Name                                   `xml:"DescribeValidDBInstanceModificationsResponse"`
	Xmlns   string                                     `xml:"xmlns,attr"`
	Result  describeValidDBInstanceModificationsResult `xml:"DescribeValidDBInstanceModificationsResult"`
}

type promoteReadReplicaDBClusterResponse struct {
	XMLName   xml.Name     `xml:"PromoteReadReplicaDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"PromoteReadReplicaDBClusterResult>DBCluster"`
}

type restoreDBClusterFromSnapshotResponse struct {
	XMLName   xml.Name     `xml:"RestoreDBClusterFromSnapshotResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"RestoreDBClusterFromSnapshotResult>DBCluster"`
}

type restoreDBClusterToPointInTimeResponse struct {
	XMLName   xml.Name     `xml:"RestoreDBClusterToPointInTimeResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"RestoreDBClusterToPointInTimeResult>DBCluster"`
}

type modifyDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"ModifyDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
}
