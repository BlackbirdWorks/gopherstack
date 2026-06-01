package docdb

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	docdbVersion = "2014-10-31"
	docdbXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
	stringTrue   = "true"
)

// Handler is the Echo HTTP handler for DocDB operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new DocDB handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "DocDB" }

// GetSupportedOperations returns supported DocDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDBCluster",
		"DescribeDBClusters",
		"DeleteDBCluster",
		"ModifyDBCluster",
		"StopDBCluster",
		"StartDBCluster",
		"FailoverDBCluster",
		"CreateDBInstance",
		"DescribeDBInstances",
		"DeleteDBInstance",
		"ModifyDBInstance",
		"RebootDBInstance",
		"CreateDBSubnetGroup",
		"DescribeDBSubnetGroups",
		"DeleteDBSubnetGroup",
		"CreateDBClusterParameterGroup",
		"DescribeDBClusterParameterGroups",
		"DeleteDBClusterParameterGroup",
		"ModifyDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"DescribeDBClusterSnapshots",
		"DeleteDBClusterSnapshot",
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"DescribeDBEngineVersions",
		"DescribeOrderableDBInstanceOptions",
		"DescribeGlobalClusters",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DescribeCertificates",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribePendingMaintenanceActions",
		"FailoverGlobalCluster",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"RemoveFromGlobalCluster",
		"RemoveSourceIdentifierFromSubscription",
		"ResetDBClusterParameterGroup",
		"RestoreDBClusterFromSnapshot",
		"RestoreDBClusterToPointInTime",
		"SwitchoverGlobalCluster",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "docdb" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DocDB instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches DocDB requests.
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
		if !strings.Contains(ua, "api/docdb") {
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

		return vals.Get("Version") == docdbVersion
	}
}

// MatchPriority returns the routing priority for DocDB (higher than RDS to intercept DocDB requests first).
func (h *Handler) MatchPriority() int { return service.PriorityFormDocDB }

// ExtractOperation extracts the DocDB action from the request.
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
		return h.dispatchExtended3(action, vals)
	}
}

func (h *Handler) dispatchExtended3(action string, vals url.Values) (any, error) {
	switch action {
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(vals)
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(vals)
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(vals)
	case "DescribeCertificates":
		return h.handleDescribeCertificates(vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(vals)
	default:
		return h.dispatchExtended4(action, vals)
	}
}

func (h *Handler) dispatchExtended4(action string, vals url.Values) (any, error) {
	switch action {
	case "DescribeDBClusterSnapshotAttributes":
		return h.handleDescribeDBClusterSnapshotAttributes(vals)
	case "DescribeEngineDefaultClusterParameters":
		return h.handleDescribeEngineDefaultClusterParameters(vals)
	case "DescribeEventCategories":
		return h.handleDescribeEventCategories(vals)
	case "DescribeEventSubscriptions":
		return h.handleDescribeEventSubscriptions(vals)
	case "DescribeEvents":
		return h.handleDescribeEvents(vals)
	case "DescribePendingMaintenanceActions":
		return h.handleDescribePendingMaintenanceActions(vals)
	case "FailoverGlobalCluster":
		return h.handleFailoverGlobalCluster(vals)
	case "ModifyDBClusterSnapshotAttribute":
		return h.handleModifyDBClusterSnapshotAttribute(vals)
	default:
		return h.dispatchExtended5(action, vals)
	}
}

func (h *Handler) dispatchExtended5(action string, vals url.Values) (any, error) {
	switch action {
	case "ModifyDBSubnetGroup":
		return h.handleModifyDBSubnetGroup(vals)
	case "ModifyEventSubscription":
		return h.handleModifyEventSubscription(vals)
	case "ModifyGlobalCluster":
		return h.handleModifyGlobalCluster(vals)
	case "RemoveFromGlobalCluster":
		return h.handleRemoveFromGlobalCluster(vals)
	case "RemoveSourceIdentifierFromSubscription":
		return h.handleRemoveSourceIdentifierFromSubscription(vals)
	case "ResetDBClusterParameterGroup":
		return h.handleResetDBClusterParameterGroup(vals)
	case "RestoreDBClusterFromSnapshot":
		return h.handleRestoreDBClusterFromSnapshot(vals)
	case "RestoreDBClusterToPointInTime":
		return h.handleRestoreDBClusterToPointInTime(vals)
	case "SwitchoverGlobalCluster":
		return h.handleSwitchoverGlobalCluster(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid DocDB action", ErrUnknownAction, action)
	}
}

func (h *Handler) handleCreateDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	masterUser := vals.Get("MasterUsername")
	masterUserPassword := vals.Get("MasterUserPassword")
	dbName := vals.Get("DatabaseName")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	subnetGroupName := vals.Get("DBSubnetGroupName")
	portStr := vals.Get("Port")
	port := 0
	if portStr != "" {
		port, _ = strconv.Atoi(portStr)
	}
	storageEncrypted := vals.Get("StorageEncrypted") == stringTrue
	deletionProtection := vals.Get("DeletionProtection") == stringTrue
	backupRetentionPeriodStr := vals.Get("BackupRetentionPeriod")
	backupRetentionPeriod := 0
	if backupRetentionPeriodStr != "" {
		backupRetentionPeriod, _ = strconv.Atoi(backupRetentionPeriodStr)
	}
	preferredBackupWindow := vals.Get("PreferredBackupWindow")
	preferredMaintenanceWindow := vals.Get("PreferredMaintenanceWindow")
	availabilityZones := parseAvailabilityZones(vals)
	tags := parseTags(vals)
	opts := &CreateDBClusterOptions{
		KmsKeyID:                         vals.Get("KmsKeyId"),
		VpcSecurityGroupIDs:              parseVpcSecurityGroupIDs(vals),
		EnabledCloudwatchLogsExports:     parseEnableLogTypes(vals),
		IAMDatabaseAuthenticationEnabled: vals.Get("EnableIAMDatabaseAuthentication") == stringTrue,
	}
	cluster, err := h.Backend.CreateDBCluster(
		id, engine, engineVersion, masterUser, masterUserPassword, dbName, paramGroupName, subnetGroupName,
		port, storageEncrypted, deletionProtection, backupRetentionPeriod,
		preferredBackupWindow, preferredMaintenanceWindow, availabilityZones, tags, opts,
	)
	if err != nil {
		return nil, err
	}

	return &createDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeDBClusters(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(id)
	if err != nil {
		return nil, err
	}
	xmlClusters := make([]xmlDBCluster, 0, len(clusters))
	for _, c := range clusters {
		cp := c
		clusterXML := toXMLCluster(&cp)
		instMembers := h.Backend.GetClusterMembers(cp.DBClusterIdentifier)
		xmlMembers := make([]xmlDBClusterMember, 0, len(instMembers))
		for _, m := range instMembers {
			xmlMembers = append(xmlMembers, xmlDBClusterMember{
				DBInstanceIdentifier:          m.DBInstanceIdentifier,
				DBClusterParameterGroupStatus: "in-sync",
				PromotionTier:                 m.PromotionTier,
				IsClusterWriter:               m.IsClusterWriter,
			})
		}
		clusterXML.DBClusterMembers = xmlDBClusterMemberList{Members: xmlMembers}
		xmlClusters = append(xmlClusters, clusterXML)
	}

	xmlClusters, nextMarker := applyDocDBMarker(xmlClusters, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClustersResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClustersResult{
			DBClusters: xmlDBClusterList{Members: xmlClusters},
			Marker:     nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	opts := &DeleteDBClusterOptions{
		SkipFinalSnapshot:                vals.Get("SkipFinalSnapshot") == stringTrue,
		FinalDBClusterSnapshotIdentifier: vals.Get("FinalDBClusterSnapshotIdentifier"),
	}
	cluster, err := h.Backend.DeleteDBCluster(id, opts)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	preferredBackupWindow := vals.Get("PreferredBackupWindow")
	preferredMaintenanceWindow := vals.Get("PreferredMaintenanceWindow")
	backupRetentionPeriodStr := vals.Get("BackupRetentionPeriod")
	backupRetentionPeriod := 0
	if backupRetentionPeriodStr != "" {
		backupRetentionPeriod, _ = strconv.Atoi(backupRetentionPeriodStr)
	}
	deletionProtection := parseBoolParam(vals, "DeletionProtection")

	portStr := vals.Get("Port")
	var port int
	if portStr != "" {
		port, _ = strconv.Atoi(portStr)
	}

	opts := &ModifyDBClusterOptions{
		EngineVersion:       vals.Get("EngineVersion"),
		VpcSecurityGroupIDs: parseVpcSecurityGroupIDs(vals),
		EnableLogsTypes:     parseCloudwatchEnableLogTypes(vals),
		DisableLogsTypes:    parseCloudwatchDisableLogTypes(vals),
		Port:                port,
	}

	cluster, err := h.Backend.ModifyDBCluster(
		id, paramGroupName, deletionProtection, backupRetentionPeriod,
		preferredBackupWindow, preferredMaintenanceWindow, opts,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterResponse{
		Xmlns:     docdbXMLNS,
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
		Xmlns:     docdbXMLNS,
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
		Xmlns:     docdbXMLNS,
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
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleCreateDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	engine := vals.Get("Engine")
	promotionTier := 1 // AWS default
	if ptStr := vals.Get("PromotionTier"); ptStr != "" {
		promotionTier, _ = strconv.Atoi(ptStr)
	}
	tags := parseTags(vals)
	opts := &CreateDBInstanceOptions{
		CACertificateIdentifier: vals.Get("CACertificateIdentifier"),
		CopyTagsToSnapshot:      vals.Get("CopyTagsToSnapshot") == stringTrue,
	}
	inst, err := h.Backend.CreateDBInstance(id, clusterID, instanceClass, engine, promotionTier, tags, opts)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	instances, err := h.Backend.DescribeDBInstances(id, clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBInstance, 0, len(instances))
	for _, inst := range instances {
		cp := inst
		members = append(members, toXMLInstance(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBInstancesResponse{
		Xmlns: docdbXMLNS,
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
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleModifyDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	autoMinorVersionUpgrade := parseBoolParam(vals, "AutoMinorVersionUpgrade")
	preferredMaintenanceWindow := vals.Get("PreferredMaintenanceWindow")

	opts := &ModifyDBInstanceOptions{
		CACertificateIdentifier: vals.Get("CACertificateIdentifier"),
		CopyTagsToSnapshot:      parseBoolParam(vals, "CopyTagsToSnapshot"),
	}
	if ptStr := vals.Get("PromotionTier"); ptStr != "" {
		pt, _ := strconv.Atoi(ptStr)
		opts.PromotionTier = &pt
	}

	inst, err := h.Backend.ModifyDBInstance(
		id, instanceClass, autoMinorVersionUpgrade, preferredMaintenanceWindow, opts,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      docdbXMLNS,
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
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCreateDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")
	subnetIDs := parseSubnetIDMembers(vals)
	tags := parseTags(vals)
	sg, err := h.Backend.CreateDBSubnetGroup(name, description, vpcID, subnetIDs, tags)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         docdbXMLNS,
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBSubnetGroupsResponse{
		Xmlns: docdbXMLNS,
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

	return &deleteDBSubnetGroupResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleCreateDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	tags := parseTags(vals)
	pg, err := h.Backend.CreateDBClusterParameterGroup(name, family, description, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
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
		Xmlns: docdbXMLNS,
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

	return &deleteDBClusterParameterGroupResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	pg, err := h.Backend.ModifyDBClusterParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       docdbXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleCreateDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	tags := parseTags(vals)
	snap, err := h.Backend.CreateDBClusterSnapshot(snapshotID, clusterID, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snapshotType := vals.Get("SnapshotType")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, clusterID, snapshotType)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLClusterSnapshot(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterSnapshotsResponse{
		Xmlns: docdbXMLNS,
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
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleListTagsForResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := h.Backend.ListTagsForResource(arn)
	members := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		members = append(members, svcTags.KV(t))
	}

	return &listTagsForResourceResponse{
		Xmlns:   docdbXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tagList := parseTagEntries(vals)
	if err := h.Backend.AddTagsToResource(arn, tagList); err != nil {
		return nil, err
	}

	return &addTagsToResourceResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	h.Backend.RemoveTagsFromResource(arn, keys)

	return &removeTagsFromResourceResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleDescribeDBEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	versions := h.Backend.DescribeDBEngineVersions(engine, engineVersion)
	members := make([]xmlDBEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBEngineVersion(v))
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            docdbXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(_ url.Values) (any, error) {
	members := []xmlOrderableDBInstanceOption{
		{Engine: docDBEngine, EngineVersion: defaultEngineVersion, DBInstanceClass: "db.t3.medium"},
		{Engine: docDBEngine, EngineVersion: defaultEngineVersion, DBInstanceClass: "db.r5.large"},
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion5, DBInstanceClass: "db.t3.medium"},
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion5, DBInstanceClass: "db.r5.large"},
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: docdbXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeGlobalClusters(vals url.Values) (any, error) {
	gcs := h.Backend.DescribeGlobalClusters(vals.Get("GlobalClusterIdentifier"))
	members := make([]xmlGlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns:          docdbXMLNS,
		GlobalClusters: xmlGlobalClusterList{Members: members},
	}, nil
}

func (h *Handler) handleAddSourceIdentifierToSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.AddSourceIdentifierToSubscription(subscriptionName, sourceID)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleApplyPendingMaintenanceAction(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	action := vals.Get("ApplyAction")
	optInType := vals.Get("OptInType")
	if err := h.Backend.ApplyPendingMaintenanceAction(resourceARN, action, optInType); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: docdbXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: xmlResourcePendingMaintenanceActions{
				ResourceIdentifier:              resourceARN,
				PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
			},
		},
	}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")
	pg, err := h.Backend.CopyDBClusterParameterGroup(sourceGroupName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
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
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	sourceIDs := parseSourceIDMembers(vals)
	eventCategories := parseEventCategoryMembers(vals)
	sub, err := h.Backend.CreateEventSubscription(name, snsTopicARN, sourceType, sourceIDs, eventCategories)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleCreateGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	sourceDBClusterID := vals.Get("SourceDBClusterIdentifier")
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	gc, err := h.Backend.CreateGlobalCluster(id, sourceDBClusterID, engine, engineVersion)
	if err != nil {
		return nil, err
	}

	return &createGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.DeleteGlobalCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDescribeCertificates(vals url.Values) (any, error) {
	certificateID := vals.Get("CertificateIdentifier")
	certs := h.Backend.DescribeCertificates(certificateID)
	members := make([]xmlCertificate, 0, len(certs))
	for _, c := range certs {
		cp := c
		members = append(members, toXMLCertificate(&cp))
	}

	return &describeCertificatesResponse{
		Xmlns: docdbXMLNS,
		Result: describeCertificatesResult{
			Certificates: xmlCertificateList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBClusterParameterGroupName")
	params, err := h.Backend.DescribeDBClusterParameters(groupName)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameter, 0, len(params))
	for _, p := range params {
		cp := p
		members = append(members, toXMLDBClusterParameter(&cp))
	}

	return &describeDBClusterParametersResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterParametersResult{
			Parameters: xmlDBClusterParameterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	result, err := h.Backend.DescribeDBClusterSnapshotAttributes(snapshotID)
	if err != nil {
		return nil, err
	}
	attrs := make([]xmlDBClusterSnapshotAttribute, 0, len(result.Attributes))
	for _, a := range result.Attributes {
		values := make([]string, len(a.AttributeValues))
		copy(values, a.AttributeValues)
		attrs = append(attrs, xmlDBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlAttributeValueList{Members: values},
		})
	}

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterSnapshotAttributesResult{
			DBClusterSnapshotAttributesResult: xmlDBClusterSnapshotAttributesResult{
				DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
				DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttributeList{Members: attrs},
			},
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	valuesToAdd := parseAttributeValueMembers(vals, "ValuesToAdd")
	valuesToRemove := parseAttributeValueMembers(vals, "ValuesToRemove")
	result, err := h.Backend.ModifyDBClusterSnapshotAttribute(snapshotID, attributeName, valuesToAdd, valuesToRemove)
	if err != nil {
		return nil, err
	}
	attrs := make([]xmlDBClusterSnapshotAttribute, 0, len(result.Attributes))
	for _, a := range result.Attributes {
		attrCopy := make([]string, len(a.AttributeValues))
		copy(attrCopy, a.AttributeValues)
		attrs = append(attrs, xmlDBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlAttributeValueList{Members: attrCopy},
		})
	}

	return &modifyDBClusterSnapshotAttributeResponse{
		Xmlns: docdbXMLNS,
		Result: modifyDBClusterSnapshotAttributeResult{
			DBClusterSnapshotAttributesResult: xmlDBClusterSnapshotAttributesResult{
				DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
				DBClusterSnapshotAttributes: xmlDBClusterSnapshotAttributeList{Members: attrs},
			},
		},
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultClusterParameters(family)
	members := make([]xmlDBClusterParameter, 0, len(params))
	for _, p := range params {
		cp := p
		members = append(members, toXMLDBClusterParameter(&cp))
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns: docdbXMLNS,
		Result: describeEngineDefaultClusterParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlDBClusterParameterList{Members: members},
			},
		},
	}, nil
}

func (h *Handler) handleResetDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	pg, err := h.Backend.ResetDBClusterParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &resetDBClusterParameterGroupResponse{
		Xmlns:                       docdbXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	subs := h.Backend.DescribeEventSubscriptions(name)
	members := make([]xmlEventSubscription, 0, len(subs))
	for _, sub := range subs {
		cp := sub
		members = append(members, toXMLEventSubscription(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeEventSubscriptionsResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventSubscriptionsResult{
			EventSubscriptionsList: xmlEventSubscriptionList{Members: members},
			Marker:                 nextMarker,
		},
	}, nil
}

func (h *Handler) handleModifyEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	eventCategories := parseEventCategoryMembers(vals)
	sub, err := h.Backend.ModifyEventSubscription(name, snsTopicARN, sourceType, eventCategories)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(subscriptionName, sourceID)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEvents(_ url.Values) (any, error) {
	return &describeEventsResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventsResult{
			Events: xmlEventList{},
		},
	}, nil
}

func (h *Handler) handleDescribeEventCategories(vals url.Values) (any, error) {
	sourceType := vals.Get("SourceType")
	cats := h.Backend.DescribeEventCategories(sourceType)
	members := make([]xmlEventCategoryMap, 0, len(cats))
	for _, cat := range cats {
		catCopy := make([]string, len(cat.EventCategories))
		copy(catCopy, cat.EventCategories)
		members = append(members, xmlEventCategoryMap{
			SourceType:      cat.SourceType,
			EventCategories: xmlEventCategoryList{Members: catCopy},
		})
	}

	return &describeEventCategoriesResponse{
		Xmlns: docdbXMLNS,
		Result: describeEventCategoriesResult{
			EventCategoriesMapList: xmlEventCategoriesMapList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	actions := h.Backend.DescribePendingMaintenanceActions(resourceARN)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		members = append(members, xmlResourcePendingMaintenanceActions{
			ResourceIdentifier:              a.ResourceIdentifier,
			PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
		})
	}

	return &describePendingMaintenanceActionsResponse{
		Xmlns: docdbXMLNS,
		Result: describePendingMaintenanceActionsResult{
			PendingMaintenanceActions: xmlResourcePendingMaintenanceActionsList{Members: members},
		},
	}, nil
}

func (h *Handler) handleModifyDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	subnetIDs := parseSubnetIDMembers(vals)
	sg, err := h.Backend.ModifyDBSubnetGroup(name, description, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &modifyDBSubnetGroupResponse{
		Xmlns:         docdbXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	newID := vals.Get("NewGlobalClusterIdentifier")
	deletionProtection := parseBoolParam(vals, "DeletionProtection")
	gc, err := h.Backend.ModifyGlobalCluster(id, newID, deletionProtection)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.FailoverGlobalCluster(id, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleRemoveFromGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	dbClusterID := vals.Get("DbClusterIdentifier")
	gc, err := h.Backend.RemoveFromGlobalCluster(globalClusterID, dbClusterID)
	if err != nil {
		return nil, err
	}

	return &removeFromGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	targetDBClusterID := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(id, targetDBClusterID)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleRestoreDBClusterFromSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	cluster, err := h.Backend.RestoreDBClusterFromSnapshot(snapshotID, clusterID, engine)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromSnapshotResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterToPointInTime(vals url.Values) (any, error) {
	sourceClusterID := vals.Get("SourceDBClusterIdentifier")
	targetClusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterToPointInTime(sourceClusterID, targetClusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterToPointInTimeResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := docdbErrorCode(opErr)
	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("DocDB internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func docdbErrorCode(opErr error) string {
	sentinels := []error{
		ErrClusterNotFound, ErrClusterAlreadyExists,
		ErrInstanceNotFound, ErrInstanceAlreadyExists,
		ErrSubnetGroupNotFound, ErrSubnetGroupAlreadyExists, ErrSubnetGroupInUse,
		ErrClusterParameterGroupNotFound, ErrClusterParameterGroupAlreadyExists, ErrParameterGroupInUse,
		ErrClusterSnapshotNotFound, ErrClusterSnapshotAlreadyExists,
		ErrEventSubscriptionNotFound, ErrEventSubscriptionAlreadyExists,
		ErrGlobalClusterNotFound, ErrGlobalClusterAlreadyExists,
		ErrInvalidParameter, ErrInvalidClusterState, ErrUnknownAction,
	}
	for _, s := range sentinels {
		if errors.Is(opErr, s) {
			return s.Error()
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &docdbErrorResponse{
		Xmlns: docdbXMLNS,
		Error: docdbError{Code: code, Message: message, Type: "Sender"},
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

// parseBoolParam parses an optional boolean query parameter.
// Returns nil if the key is absent, otherwise returns a pointer to the parsed value.
func parseBoolParam(vals url.Values, key string) *bool {
	s := vals.Get(key)
	if s == "" {
		return nil
	}
	v := s == stringTrue

	return &v
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
	vpcSGs := make([]xmlVpcSecurityGroupMembership, 0, len(c.VpcSecurityGroupIDs))
	for _, sgID := range c.VpcSecurityGroupIDs {
		vpcSGs = append(vpcSGs, xmlVpcSecurityGroupMembership{
			VpcSecurityGroupID: sgID,
			Status:             "active",
		})
	}
	logTypes := make([]string, len(c.EnabledCloudwatchLogsExports))
	copy(logTypes, c.EnabledCloudwatchLogsExports)

	return xmlDBCluster{
		DBClusterIdentifier:              c.DBClusterIdentifier,
		Engine:                           c.Engine,
		Status:                           c.Status,
		MasterUsername:                   c.MasterUsername,
		DatabaseName:                     c.DatabaseName,
		DBClusterParameterGroupName:      c.DBClusterParameterGroupName,
		Endpoint:                         c.Endpoint,
		ReaderEndpoint:                   c.ReaderEndpoint,
		DBSubnetGroupName:                c.DBSubnetGroupName,
		PreferredBackupWindow:            c.PreferredBackupWindow,
		PreferredMaintenanceWindow:       c.PreferredMaintenanceWindow,
		Port:                             c.Port,
		DBClusterArn:                     c.DBClusterArn,
		EngineVersion:                    c.EngineVersion,
		BackupRetentionPeriod:            c.BackupRetentionPeriod,
		StorageEncrypted:                 c.StorageEncrypted,
		MultiAZ:                          c.MultiAZ,
		DeletionProtection:               c.DeletionProtection,
		ClusterCreateTime:                c.ClusterCreateTime,
		HostedZoneID:                     c.HostedZoneID,
		KmsKeyID:                         c.KmsKeyID,
		ReplicationSourceIdentifier:      c.ReplicationSourceIdentifier,
		IAMDatabaseAuthenticationEnabled: c.IAMDatabaseAuthenticationEnabled,
		VpcSecurityGroups:                xmlVpcSecurityGroupMembershipList{Members: vpcSGs},
		EnabledCloudwatchLogsExports:     xmlLogTypeList{Members: logTypes},
		DBClusterMembers:                 xmlDBClusterMemberList{},
	}
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	logTypes := make([]string, len(inst.EnabledCloudwatchLogsExports))
	copy(logTypes, inst.EnabledCloudwatchLogsExports)

	return xmlDBInstance{
		DBInstanceIdentifier:         inst.DBInstanceIdentifier,
		DBClusterIdentifier:          inst.DBClusterIdentifier,
		DBInstanceClass:              inst.DBInstanceClass,
		Engine:                       inst.Engine,
		DBInstanceStatus:             inst.DBInstanceStatus,
		Endpoint:                     inst.Endpoint,
		Port:                         inst.Port,
		DBInstanceArn:                inst.DBInstanceArn,
		EngineVersion:                inst.EngineVersion,
		AvailabilityZone:             inst.AvailabilityZone,
		DBSubnetGroupName:            inst.DBSubnetGroupName,
		AutoMinorVersionUpgrade:      inst.AutoMinorVersionUpgrade,
		PubliclyAccessible:           inst.PubliclyAccessible,
		StorageEncrypted:             inst.StorageEncrypted,
		PromotionTier:                inst.PromotionTier,
		PreferredMaintenanceWindow:   inst.PreferredMaintenanceWindow,
		CACertificateIdentifier:      inst.CACertificateIdentifier,
		CopyTagsToSnapshot:           inst.CopyTagsToSnapshot,
		EnabledCloudwatchLogsExports: xmlLogTypeList{Members: logTypes},
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
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
	}
}

func toXMLParameterGroup(pg *DBClusterParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
		DBClusterParameterGroupArn:  pg.DBClusterParameterGroupArn,
	}
}

func toXMLClusterSnapshot(snap *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: snap.DBClusterSnapshotIdentifier,
		DBClusterIdentifier:         snap.DBClusterIdentifier,
		DBClusterArn:                snap.DBClusterArn,
		Engine:                      snap.Engine,
		Status:                      snap.Status,
		SnapshotType:                snap.SnapshotType,
		SnapshotCreateTime:          snap.SnapshotCreateTime,
		EngineVersion:               snap.EngineVersion,
		PercentProgress:             snap.PercentProgress,
		StorageEncrypted:            snap.StorageEncrypted,
	}
}

type docdbError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type docdbErrorResponse struct {
	XMLName xml.Name   `xml:"ErrorResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Error   docdbError `xml:"Error"`
}

type xmlVpcSecurityGroupMembership struct {
	VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
	Status             string `xml:"Status"`
}

type xmlVpcSecurityGroupMembershipList struct {
	Members []xmlVpcSecurityGroupMembership `xml:"VpcSecurityGroupMembership"`
}

type xmlLogTypeList struct {
	Members []string `xml:"member"`
}

type xmlDBClusterMember struct {
	DBInstanceIdentifier          string `xml:"DBInstanceIdentifier"`
	DBClusterParameterGroupStatus string `xml:"DBClusterParameterGroupStatus"`
	PromotionTier                 int    `xml:"PromotionTier"`
	IsClusterWriter               bool   `xml:"IsClusterWriter"`
}

type xmlDBClusterMemberList struct {
	Members []xmlDBClusterMember `xml:"DBClusterMember"`
}

type xmlDBCluster struct {
	DBClusterIdentifier              string                            `xml:"DBClusterIdentifier"`
	Engine                           string                            `xml:"Engine"`
	Status                           string                            `xml:"Status"`
	MasterUsername                   string                            `xml:"MasterUsername,omitempty"`
	DatabaseName                     string                            `xml:"DatabaseName,omitempty"`
	DBClusterParameterGroupName      string                            `xml:"DBClusterParameterGroup,omitempty"`
	Endpoint                         string                            `xml:"Endpoint,omitempty"`
	ReaderEndpoint                   string                            `xml:"ReaderEndpoint,omitempty"`
	DBSubnetGroupName                string                            `xml:"DBSubnetGroup,omitempty"`
	PreferredBackupWindow            string                            `xml:"PreferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow       string                            `xml:"PreferredMaintenanceWindow,omitempty"`
	DBClusterArn                     string                            `xml:"DBClusterArn,omitempty"`
	EngineVersion                    string                            `xml:"EngineVersion,omitempty"`
	ClusterCreateTime                string                            `xml:"ClusterCreateTime,omitempty"`
	HostedZoneID                     string                            `xml:"HostedZoneId,omitempty"`
	KmsKeyID                         string                            `xml:"KmsKeyId,omitempty"`
	ReplicationSourceIdentifier      string                            `xml:"ReplicationSourceIdentifier,omitempty"`
	VpcSecurityGroups                xmlVpcSecurityGroupMembershipList `xml:"VpcSecurityGroups"`
	EnabledCloudwatchLogsExports     xmlLogTypeList                    `xml:"EnabledCloudwatchLogsExports"`
	DBClusterMembers                 xmlDBClusterMemberList            `xml:"DBClusterMembers"`
	Port                             int                               `xml:"Port"`
	BackupRetentionPeriod            int                               `xml:"BackupRetentionPeriod,omitempty"`
	StorageEncrypted                 bool                              `xml:"StorageEncrypted"`
	MultiAZ                          bool                              `xml:"MultiAZ"`
	DeletionProtection               bool                              `xml:"DeletionProtection"`
	IAMDatabaseAuthenticationEnabled bool                              `xml:"IAMDatabaseAuthenticationEnabled"`
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
	DBInstanceIdentifier         string         `xml:"DBInstanceIdentifier"`
	DBClusterIdentifier          string         `xml:"DBClusterIdentifier,omitempty"`
	DBInstanceClass              string         `xml:"DBInstanceClass"`
	Engine                       string         `xml:"Engine"`
	DBInstanceStatus             string         `xml:"DBInstanceStatus"`
	Endpoint                     string         `xml:"Endpoint>Address,omitempty"`
	DBInstanceArn                string         `xml:"DBInstanceArn,omitempty"`
	EngineVersion                string         `xml:"EngineVersion,omitempty"`
	AvailabilityZone             string         `xml:"AvailabilityZone,omitempty"`
	DBSubnetGroupName            string         `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	PreferredMaintenanceWindow   string         `xml:"PreferredMaintenanceWindow,omitempty"`
	CACertificateIdentifier      string         `xml:"CACertificateIdentifier,omitempty"`
	EnabledCloudwatchLogsExports xmlLogTypeList `xml:"EnabledCloudwatchLogsExports"`
	StorageEncrypted             bool           `xml:"StorageEncrypted"`
	AutoMinorVersionUpgrade      bool           `xml:"AutoMinorVersionUpgrade"`
	PubliclyAccessible           bool           `xml:"PubliclyAccessible"`
	CopyTagsToSnapshot           bool           `xml:"CopyTagsToSnapshot"`
	Port                         int            `xml:"Endpoint>Port"`
	PromotionTier                int            `xml:"PromotionTier"`
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
	DBSubnetGroupArn         string        `xml:"DBSubnetGroupArn,omitempty"`
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
	DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn,omitempty"`
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
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	DBClusterArn                string `xml:"DBClusterArn,omitempty"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
	SnapshotType                string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime          string `xml:"SnapshotCreateTime,omitempty"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
	PercentProgress             int    `xml:"PercentProgress"`
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
	Engine              string `xml:"Engine"`
	EngineVersion       string `xml:"EngineVersion"`
	DBEngineDescription string `xml:"DBEngineDescription"`
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

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalCluster"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
}

type xmlSourceIDList struct {
	Members []string `xml:"SourceId"`
}

type xmlEventSubscription struct {
	SubscriptionName string          `xml:"CustSubscriptionId"`
	SnsTopicARN      string          `xml:"SnsTopicArn,omitempty"`
	SourceType       string          `xml:"SourceType,omitempty"`
	Status           string          `xml:"Status"`
	SourceIDsList    xmlSourceIDList `xml:"SourceIdsList"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	OptInStatus string `xml:"OptInStatus"`
}

type xmlPendingMaintenanceActionList struct {
	Members []xmlPendingMaintenanceAction `xml:"PendingMaintenanceAction"`
}

type xmlResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                          `xml:"ResourceIdentifier"`
	PendingMaintenanceActionDetails xmlPendingMaintenanceActionList `xml:"PendingMaintenanceActionDetails"`
}

type applyPendingMaintenanceActionResult struct {
	ResourcePendingMaintenanceActions xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type applyPendingMaintenanceActionResponse struct {
	XMLName xml.Name                            `xml:"ApplyPendingMaintenanceActionResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  applyPendingMaintenanceActionResult `xml:"ApplyPendingMaintenanceActionResult"`
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

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

type xmlGlobalCluster struct {
	GlobalClusterIdentifier   string `xml:"GlobalClusterIdentifier"`
	SourceDBClusterIdentifier string `xml:"SourceDBClusterIdentifier,omitempty"`
	Engine                    string `xml:"Engine,omitempty"`
	EngineVersion             string `xml:"EngineVersion,omitempty"`
	GlobalClusterArn          string `xml:"GlobalClusterArn,omitempty"`
	Status                    string `xml:"Status"`
	StorageEncrypted          bool   `xml:"StorageEncrypted"`
	DeletionProtection        bool   `xml:"DeletionProtection"`
}

type createGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"CreateGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"CreateGlobalClusterResult>GlobalCluster"`
}

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
}

type deleteGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"DeleteGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"DeleteGlobalClusterResult>GlobalCluster"`
}

type xmlCertificate struct {
	CertificateIdentifier string `xml:"CertificateIdentifier"`
	CertificateType       string `xml:"CertificateType"`
	Thumbprint            string `xml:"Thumbprint,omitempty"`
	ValidFrom             string `xml:"ValidFrom,omitempty"`
	ValidTill             string `xml:"ValidTill,omitempty"`
}

type xmlCertificateList struct {
	Members []xmlCertificate `xml:"Certificate"`
}

type describeCertificatesResult struct {
	Certificates xmlCertificateList `xml:"Certificates"`
}

type describeCertificatesResponse struct {
	XMLName xml.Name                   `xml:"DescribeCertificatesResponse"`
	Xmlns   string                     `xml:"xmlns,attr"`
	Result  describeCertificatesResult `xml:"DescribeCertificatesResult"`
}

type xmlDBClusterParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	Source         string `xml:"Source,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBClusterParameterList struct {
	Members []xmlDBClusterParameter `xml:"Parameter"`
}

type describeDBClusterParametersResult struct {
	Parameters xmlDBClusterParameterList `xml:"Parameters"`
}

type describeDBClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDBClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  describeDBClusterParametersResult `xml:"DescribeDBClusterParametersResult"`
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]string, len(sub.SourceIDs))
	copy(ids, sub.SourceIDs)

	return xmlEventSubscription{
		SubscriptionName: sub.SubscriptionName,
		SnsTopicARN:      sub.SnsTopicARN,
		SourceType:       sub.SourceType,
		Status:           sub.Status,
		SourceIDsList:    xmlSourceIDList{Members: ids},
	}
}

// XML types for the new operations.

type xmlAttributeValueList struct {
	Members []string `xml:"AttributeValue"`
}

type xmlDBClusterSnapshotAttribute struct {
	AttributeName   string                `xml:"AttributeName"`
	AttributeValues xmlAttributeValueList `xml:"AttributeValues"`
}

type xmlDBClusterSnapshotAttributeList struct {
	Members []xmlDBClusterSnapshotAttribute `xml:"DBClusterSnapshotAttribute"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                            `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBClusterSnapshotAttributeList `xml:"DBClusterSnapshotAttributes"`
}

type describeDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotAttributesResult xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type describeDBClusterSnapshotAttributesResponse struct {
	XMLName xml.Name                                  `xml:"DescribeDBClusterSnapshotAttributesResponse"`
	Xmlns   string                                    `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotAttributesResult `xml:"DescribeDBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResult struct {
	DBClusterSnapshotAttributesResult xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResponse struct {
	XMLName xml.Name                               `xml:"ModifyDBClusterSnapshotAttributeResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  modifyDBClusterSnapshotAttributeResult `xml:"ModifyDBClusterSnapshotAttributeResult"`
}

type xmlEngineDefaults struct {
	DBParameterGroupFamily string                    `xml:"DBParameterGroupFamily"`
	Parameters             xmlDBClusterParameterList `xml:"Parameters"`
}

type describeEngineDefaultClusterParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName xml.Name                                     `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns   string                                       `xml:"xmlns,attr"`
	Result  describeEngineDefaultClusterParametersResult `xml:"DescribeEngineDefaultClusterParametersResult"`
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

type xmlEvent struct {
	Message    string `xml:"Message,omitempty"`
	SourceType string `xml:"SourceType,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResult struct {
	Events xmlEventList `xml:"Events"`
}

type describeEventsResponse struct {
	XMLName xml.Name             `xml:"DescribeEventsResponse"`
	Xmlns   string               `xml:"xmlns,attr"`
	Result  describeEventsResult `xml:"DescribeEventsResult"`
}

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventCategoryMap struct {
	SourceType      string               `xml:"SourceType"`
	EventCategories xmlEventCategoryList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoryMap `xml:"EventCategoryMap"`
}

type describeEventCategoriesResult struct {
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"EventCategoriesMapList"`
}

type describeEventCategoriesResponse struct {
	XMLName xml.Name                      `xml:"DescribeEventCategoriesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  describeEventCategoriesResult `xml:"DescribeEventCategoriesResult"`
}

type xmlResourcePendingMaintenanceActionsList struct {
	Members []xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResult struct {
	PendingMaintenanceActions xmlResourcePendingMaintenanceActionsList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                                `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describePendingMaintenanceActionsResult `xml:"DescribePendingMaintenanceActionsResult"`
}

type modifyDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"ModifyDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
}

type modifyGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"ModifyGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"ModifyGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
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

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	return xmlGlobalCluster{
		GlobalClusterIdentifier:   gc.GlobalClusterIdentifier,
		SourceDBClusterIdentifier: gc.SourceDBClusterID,
		Engine:                    gc.Engine,
		EngineVersion:             gc.EngineVersion,
		GlobalClusterArn:          gc.GlobalClusterArn,
		Status:                    gc.Status,
		StorageEncrypted:          gc.StorageEncrypted,
		DeletionProtection:        gc.DeletionProtection,
	}
}

func toXMLCertificate(c *Certificate) xmlCertificate {
	return xmlCertificate{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateType:       c.CertificateType,
		Thumbprint:            c.Thumbprint,
		ValidFrom:             c.ValidFrom,
		ValidTill:             c.ValidTill,
	}
}

func toXMLDBClusterParameter(p *DBClusterParameter) xmlDBClusterParameter {
	return xmlDBClusterParameter{
		ParameterName:  p.ParameterName,
		ParameterValue: p.ParameterValue,
		Description:    p.Description,
		Source:         p.Source,
		ApplyType:      p.ApplyType,
		DataType:       p.DataType,
		IsModifiable:   p.IsModifiable,
	}
}

func parseSourceIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("SourceIds.SourceId.%d", i))
		if id == "" {
			return ids
		}
		ids = append(ids, id)
	}
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// parseTags parses Tags.Tag.N.Key/Value form values and returns a map.
func parseTags(vals url.Values) map[string]string {
	return tagsToMap(parseTagEntries(vals))
}

const defaultDocDBMaxRecords = 100

// applyDocDBMarker applies Marker/MaxRecords-based pagination to a slice.
// marker is the starting index as a string, maxRecordsStr is the limit as a string.
func applyDocDBMarker[T any](items []T, marker, maxRecordsStr string) ([]T, string) {
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

	limit := defaultDocDBMaxRecords
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

func parseAvailabilityZones(vals url.Values) []string {
	var azs []string
	for i := 1; ; i++ {
		az := vals.Get(fmt.Sprintf("AvailabilityZones.AvailabilityZone.%d", i))
		if az == "" {
			return azs
		}
		azs = append(azs, az)
	}
}

func parseEventCategoryMembers(vals url.Values) []string {
	var cats []string
	for i := 1; ; i++ {
		cat := vals.Get(fmt.Sprintf("EventCategories.EventCategory.%d", i))
		if cat == "" {
			return cats
		}
		cats = append(cats, cat)
	}
}

func parseAttributeValueMembers(vals url.Values, prefix string) []string {
	var values []string
	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.AttributeValue.%d", prefix, i))
		if v == "" {
			return values
		}
		values = append(values, v)
	}
}

func parseVpcSecurityGroupIDs(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("VpcSecurityGroupIds.member.%d", i))
		if id == "" {
			return ids
		}
		ids = append(ids, id)
	}
}

func parseEnableLogTypes(vals url.Values) []string {
	var types []string
	for i := 1; ; i++ {
		t := vals.Get(fmt.Sprintf("EnableCloudwatchLogsExports.member.%d", i))
		if t == "" {
			return types
		}
		types = append(types, t)
	}
}

func parseCloudwatchEnableLogTypes(vals url.Values) []string {
	var types []string
	for i := 1; ; i++ {
		t := vals.Get(fmt.Sprintf("CloudwatchLogsExportConfiguration.EnableLogTypes.member.%d", i))
		if t == "" {
			return types
		}
		types = append(types, t)
	}
}

func parseCloudwatchDisableLogTypes(vals url.Values) []string {
	var types []string
	for i := 1; ; i++ {
		t := vals.Get(fmt.Sprintf("CloudwatchLogsExportConfiguration.DisableLogTypes.member.%d", i))
		if t == "" {
			return types
		}
		types = append(types, t)
	}
}
