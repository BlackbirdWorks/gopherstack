package rds

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	rdsVersion = "2014-10-31"
	rdsXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
	formTrue   = "true"

	rdsDescribeDefaultPageSize = 100

	monitoringInterval5  = 5
	monitoringInterval10 = 10
	monitoringInterval15 = 15
	monitoringInterval30 = 30
	monitoringInterval60 = 60
)

// Handler is the Echo HTTP handler for RDS operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new RDS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state. Useful for test isolation.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "RDS" }

// GetSupportedOperations returns supported RDS operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(supportedOpsBase())+len(supportedOpsExtended()))
	ops = append(ops, supportedOpsBase()...)
	ops = append(ops, supportedOpsExtended()...)
	sort.Strings(ops)

	return ops
}

func supportedOpsBase() []string {
	return []string{
		"AddRoleToDBCluster",
		"AddRoleToDBInstance",
		"AddSourceIdentifierToSubscription",
		"AddTagsToResource",
		"ApplyPendingMaintenanceAction",
		"AuthorizeDBSecurityGroupIngress",
		"BacktrackDBCluster",
		"CancelExportTask",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CopyDBSnapshot",
		"CopyOptionGroup",
		"CreateBlueGreenDeployment",
		"CreateDBCluster",
		"CreateDBClusterEndpoint",
		"CreateDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"CreateDBInstance",
		"CreateDBInstanceReadReplica",
		"CreateDBParameterGroup",
		"CreateDBProxy",
		"CreateDBProxyEndpoint",
		"CreateDBSecurityGroup",
		"CreateDBSnapshot",
		"CreateDBSubnetGroup",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"CreateOptionGroup",
		"DeleteBlueGreenDeployment",
		"DeleteDBCluster",
		"DeleteDBClusterEndpoint",
		"DeleteDBClusterParameterGroup",
		"DeleteDBClusterSnapshot",
		"DeleteDBInstance",
		"DeleteDBParameterGroup",
		"DeleteDBProxy",
		"DeleteDBProxyEndpoint",
		"DeleteDBSecurityGroup",
		"DeleteDBSnapshot",
		"DeleteDBSubnetGroup",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DeleteOptionGroup",
		"DeregisterDBProxyTargets",
		"DescribeAccountAttributes",
		"DescribeBlueGreenDeployments",
		"DescribeCertificates",
		"DescribeDBClusterBacktracks",
		"DescribeDBClusterEndpoints",
		"DescribeDBClusterParameterGroups",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBClusterSnapshots",
		"DescribeCustomDBEngineVersions",
		"DescribeDBClusters",
		"DescribeDBEngineVersions",
		"DescribeDBInstances",
		"DescribeDBLogFiles",
		"DescribeDBMajorEngineVersions",
		"DescribeDBParameterGroups",
		"DescribeDBParameters",
		"DescribeDBProxies",
		"DescribeDBProxyEndpoints",
		"DescribeDBProxyTargetGroups",
		"DescribeDBProxyTargets",
		"DescribeDBRecommendations",
		"DescribeDBSecurityGroups",
		"DescribeDBSnapshotAttributes",
		"DescribeDBSnapshots",
		"DescribeDBSubnetGroups",
	}
}

func supportedOpsExtended() []string {
	return []string{
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeExportTasks",
		opDescribeGlobalClusters,
		"DescribeOptionGroupOptions",
		"DescribeOptionGroups",
		"DescribeOrderableDBInstanceOptions",
		"DescribePendingMaintenanceActions",
		"DescribeReservedDBInstances",
		"DescribeReservedDBInstancesOfferings",
		"DescribeSourceRegions",
		"DescribeValidDBInstanceModifications",
		"DisableHttpEndpoint",
		"DownloadDBLogFilePortion",
		"EnableHttpEndpoint",
		"FailoverDBCluster",
		"FailoverGlobalCluster",
		"ListTagsForResource",
		"ModifyActivityStream",
		"ModifyCertificates",
		"ModifyCurrentDBClusterCapacity",
		"ModifyDBCluster",
		"ModifyDBClusterEndpoint",
		"ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBInstance",
		"ModifyDBParameterGroup",
		"ModifyDBProxy",
		"ModifyDBProxyEndpoint",
		"ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation",
		"ModifyDBSnapshot",
		"ModifyDBSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"ModifyOptionGroup",
		"PromoteReadReplica",
		"PromoteReadReplicaDBCluster",
		"PurchaseReservedDBInstancesOffering",
		"RebootDBCluster",
		"RebootDBInstance",
		"RegisterDBProxyTargets",
		"RemoveFromGlobalCluster",
		"RemoveRoleFromDBCluster",
		"RemoveRoleFromDBInstance",
		"RemoveSourceIdentifierFromSubscription",
		"RemoveTagsFromResource",
		"ResetDBClusterParameterGroup",
		"ResetDBParameterGroup",
		"RestoreDBClusterFromS3",
		"RestoreDBClusterFromSnapshot",
		"RestoreDBClusterToPointInTime",
		"RestoreDBInstanceFromDBSnapshot",
		"RestoreDBInstanceFromS3",
		"RestoreDBInstanceToPointInTime",
		"RevokeDBSecurityGroupIngress",
		"StartActivityStream",
		"StartDBCluster",
		"StartDBInstance",
		"StartExportTask",
		"StopActivityStream",
		"StopDBCluster",
		"StopDBInstance",
		"SwitchoverBlueGreenDeployment",
		"SwitchoverGlobalCluster",
		"SwitchoverReadReplica",
		// Custom DB engine versions.
		"CreateCustomDBEngineVersion",
		"DeleteCustomDBEngineVersion",
		"ModifyCustomDBEngineVersion",
		// DB shard groups.
		"CreateDBShardGroup",
		"DeleteDBShardGroup",
		"DescribeDBShardGroups",
		"ModifyDBShardGroup",
		"RebootDBShardGroup",
		// Integrations.
		"CreateIntegration",
		"DeleteIntegration",
		"DescribeIntegrations",
		"ModifyIntegration",
		// Tenant databases.
		"CreateTenantDatabase",
		"DeleteTenantDatabase",
		"DescribeTenantDatabases",
		"ModifyTenantDatabase",
		// Automated backups.
		"DeleteDBClusterAutomatedBackup",
		"DeleteDBInstanceAutomatedBackup",
		"DescribeDBClusterAutomatedBackups",
		"DescribeDBInstanceAutomatedBackups",
		"StartDBInstanceAutomatedBackupsReplication",
		"StopDBInstanceAutomatedBackupsReplication",
		// Snapshot tenant databases.
		"DescribeDBSnapshotTenantDatabases",
		// Performance Insights.
		"GetPerformanceInsightsMetrics",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "rds" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this RDS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches RDS requests.
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

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == rdsVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityFormRDS }

// ExtractOperation extracts the RDS action from the request.
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

// ExtractResource returns the DB instance identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("DBInstanceIdentifier")
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

// dispatch routes the RDS action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBInstance":
		return h.handleCreateDBInstance(vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(vals)
	case "CreateDBSnapshot":
		return h.handleCreateDBSnapshot(vals)
	case "DescribeDBSnapshots":
		return h.handleDescribeDBSnapshots(vals)
	case "DeleteDBSnapshot":
		return h.handleDeleteDBSnapshot(vals)
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(vals)
	case "ListTagsForResource":
		return h.handleListTagsForResource(vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(vals)
	default:
		return h.dispatchExtended(action, vals)
	}
}

// dispatchExtended routes the first subset of extended RDS actions (parameter groups, option groups,
// and cluster CRUD) to the appropriate handler. See dispatchExtended2 for the remaining actions.
func (h *Handler) dispatchExtended(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBParameterGroup":
		return h.handleCreateDBParameterGroup(vals)
	case "DescribeDBParameterGroups":
		return h.handleDescribeDBParameterGroups(vals)
	case "DeleteDBParameterGroup":
		return h.handleDeleteDBParameterGroup(vals)
	case "ModifyDBParameterGroup":
		return h.handleModifyDBParameterGroup(vals)
	case "DescribeDBParameters":
		return h.handleDescribeDBParameters(vals)
	case "ResetDBParameterGroup":
		return h.handleResetDBParameterGroup(vals)
	case "CreateOptionGroup":
		return h.handleCreateOptionGroup(vals)
	case "DescribeOptionGroups":
		return h.handleDescribeOptionGroups(vals)
	case "DeleteOptionGroup":
		return h.handleDeleteOptionGroup(vals)
	case "ModifyOptionGroup":
		return h.handleModifyOptionGroup(vals)
	case "DescribeOptionGroupOptions":
		return h.handleDescribeOptionGroupOptions(vals)
	case "CreateDBCluster":
		return h.handleCreateDBCluster(vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(vals)
	default:
		return h.dispatchExtended2(action, vals)
	}
}

// dispatchExtended2 routes the remaining extended RDS actions (cluster modifications, snapshots,
// replicas, and misc operations) to the appropriate handler. Split from dispatchExtended to keep
// cyclomatic complexity within limits.
func (h *Handler) dispatchExtended2(action string, vals url.Values) (any, error) {
	switch action {
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(vals)
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(vals)
	case "CreateDBInstanceReadReplica":
		return h.handleCreateDBInstanceReadReplica(vals)
	case "PromoteReadReplica":
		return h.handlePromoteReadReplica(vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(vals)
	case "DescribeDBLogFiles":
		return h.handleDescribeDBLogFiles(vals)
	case "DownloadDBLogFilePortion":
		return h.handleDownloadDBLogFilePortion(vals)
	default:
		return h.dispatchExtended3(action, vals)
	}
}

// dispatchExtended3 routes the next set of extended RDS actions (cluster start/stop, snapshot
// management, and restore operations). Split from dispatchExtended2 to keep cyclomatic complexity
// within limits.
func (h *Handler) dispatchExtended3(action string, vals url.Values) (any, error) {
	switch action {
	case opDescribeGlobalClusters:
		return h.handleDescribeGlobalClusters(vals)
	case "StartDBCluster":
		return h.handleStartDBCluster(vals)
	case "StopDBCluster":
		return h.handleStopDBCluster(vals)
	case "DeleteDBClusterSnapshot":
		return h.handleDeleteDBClusterSnapshot(vals)
	case "RestoreDBClusterFromSnapshot":
		return h.handleRestoreDBClusterFromSnapshot(vals)
	case "RestoreDBClusterToPointInTime":
		return h.handleRestoreDBClusterToPointInTime(vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(vals)
	default:
		return h.dispatchExtended4(action, vals)
	}
}

// dispatchExtended4 routes the final set of extended RDS actions (cluster endpoints, export tasks,
// and instance modification validation). Split from dispatchExtended3 to keep cyclomatic complexity
// within limits.
func (h *Handler) dispatchExtended4(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBClusterEndpoint":
		return h.handleCreateDBClusterEndpoint(vals)
	case "DescribeDBClusterEndpoints":
		return h.handleDescribeDBClusterEndpoints(vals)
	case "DeleteDBClusterEndpoint":
		return h.handleDeleteDBClusterEndpoint(vals)
	case "DescribeValidDBInstanceModifications":
		return h.handleDescribeValidDBInstanceModifications(vals)
	case "StartExportTask":
		return h.handleStartExportTask(vals)
	case "DescribeExportTasks":
		return h.handleDescribeExportTasks(vals)
	case "CancelExportTask":
		return h.handleCancelExportTask(vals)
	default:
		return h.dispatchExtended5(action, vals)
	}
}

// dispatchExtended5 routes instance restore, snapshot copy, start/stop, and global cluster operations.
// Split from dispatchExtended4 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended5(action string, vals url.Values) (any, error) {
	switch action {
	case "RestoreDBInstanceFromDBSnapshot":
		return h.handleRestoreDBInstanceFromDBSnapshot(vals)
	case "RestoreDBInstanceToPointInTime":
		return h.handleRestoreDBInstanceToPointInTime(vals)
	case "CopyDBSnapshot":
		return h.handleCopyDBSnapshot(vals)
	case "StartDBInstance":
		return h.handleStartDBInstance(vals)
	case "StopDBInstance":
		return h.handleStopDBInstance(vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(vals)
	case opDescribeGlobalClusters:
		return h.handleDescribeGlobalClusters(vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(vals)
	case "ModifyGlobalCluster":
		return h.handleModifyGlobalCluster(vals)
	default:
		return h.dispatchExtended6(action, vals)
	}
}

// dispatchExtended6 routes the new RDS operations added in this iteration.
// Split from dispatchExtended5 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended6(action string, vals url.Values) (any, error) {
	switch action {
	case "AddRoleToDBCluster":
		return h.handleAddRoleToDBCluster(vals)
	case "AddRoleToDBInstance":
		return h.handleAddRoleToDBInstance(vals)
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(vals)
	case "AuthorizeDBSecurityGroupIngress":
		return h.handleAuthorizeDBSecurityGroupIngress(vals)
	case "BacktrackDBCluster":
		return h.handleBacktrackDBCluster(vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(vals)
	case "CopyDBParameterGroup":
		return h.handleCopyDBParameterGroup(vals)
	case "CopyOptionGroup":
		return h.handleCopyOptionGroup(vals)
	case "CreateBlueGreenDeployment":
		return h.handleCreateBlueGreenDeployment(vals)
	case "RemoveRoleFromDBCluster":
		return h.handleRemoveRoleFromDBCluster(vals)
	case "RemoveRoleFromDBInstance":
		return h.handleRemoveRoleFromDBInstance(vals)
	case "RemoveSourceIdentifierFromSubscription":
		return h.handleRemoveSourceIdentifierFromSubscription(vals)
	default:
		return h.dispatchExtended7(action, vals)
	}
}

// parseDBInstanceIntParams parses numeric form parameters for CreateDBInstance/ModifyDBInstance.
func parseDBInstanceIntParams(
	vals url.Values,
) (int, int, int, int, int, error) {
	var allocatedStorage, iops, storageThroughput, monitoringInterval int
	backupRetention := 1

	if raw := vals.Get("AllocatedStorage"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			return 0, 0, 0, 0, 0, fmt.Errorf("%w: invalid AllocatedStorage %q", ErrInvalidParameter, raw)
		}
		allocatedStorage = v
	}

	if raw := vals.Get("BackupRetentionPeriod"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			return 0, 0, 0, 0, 0, fmt.Errorf("%w: invalid BackupRetentionPeriod %q", ErrInvalidParameter, raw)
		}

		backupRetention = v
	}

	if raw := vals.Get("Iops"); raw != "" {
		if v, e := strconv.Atoi(raw); e == nil {
			iops = v
		}
	}

	if raw := vals.Get("StorageThroughput"); raw != "" {
		if v, e := strconv.Atoi(raw); e == nil {
			storageThroughput = v
		}
	}

	if raw := vals.Get("MonitoringInterval"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			return 0, 0, 0, 0, 0, fmt.Errorf("%w: invalid MonitoringInterval %q", ErrInvalidParameter, raw)
		}

		if !validMonitoringInterval(v) {
			return 0, 0, 0, 0, 0, fmt.Errorf(
				"%w: MonitoringInterval must be one of 0, 1, 5, 10, 15, 30, 60; got %d",
				ErrInvalidParameter, v,
			)
		}

		monitoringInterval = v
	}

	return allocatedStorage, backupRetention, iops, storageThroughput, monitoringInterval, nil
}

func (h *Handler) handleCreateDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	engine := vals.Get("Engine")
	instanceClass := vals.Get("DBInstanceClass")
	dbName := vals.Get("DBName")
	masterUser := vals.Get("MasterUsername")
	paramGroupName := vals.Get("DBParameterGroupName")

	allocatedStorage, backupRetention, iops, storageThroughput, monitoringInterval, err := parseDBInstanceIntParams(
		vals,
	)
	if err != nil {
		return nil, err
	}

	if backupRetention < 0 || backupRetention > 35 {
		return nil, fmt.Errorf(
			"%w: BackupRetentionPeriod must be between 0 and 35; got %d",
			ErrInvalidParameter, backupRetention,
		)
	}

	vpcSGIds := parseMultiValueParam(vals, "VpcSecurityGroupIds.VpcSecurityGroupID")
	logExports := parseMultiValueParam(vals, "EnableCloudwatchLogsExports.member")

	opts := DBInstanceOptions{
		EngineVersion:                    vals.Get("EngineVersion"),
		StorageType:                      vals.Get("StorageType"),
		AvailabilityZone:                 vals.Get("AvailabilityZone"),
		OptionGroupName:                  vals.Get("OptionGroupName"),
		LicenseModel:                     vals.Get("LicenseModel"),
		MonitoringRoleArn:                vals.Get("MonitoringRoleArn"),
		PreferredMaintenanceWindow:       vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:            vals.Get("PreferredBackupWindow"),
		KmsKeyID:                         vals.Get("KmsKeyId"),
		DBClusterIdentifier:              vals.Get("DBClusterIdentifier"),
		BackupRetentionPeriod:            backupRetention,
		Iops:                             iops,
		StorageThroughput:                storageThroughput,
		MonitoringInterval:               monitoringInterval,
		MultiAZ:                          vals.Get("MultiAZ") == formTrue,
		StorageEncrypted:                 vals.Get("StorageEncrypted") == formTrue,
		IAMDatabaseAuthenticationEnabled: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		DeletionProtection:               vals.Get("DeletionProtection") == formTrue,
		CopyTagsToSnapshot:               vals.Get("CopyTagsToSnapshot") == formTrue,
		PubliclyAccessible:               vals.Get("PubliclyAccessible") == formTrue,
		PerformanceInsightsEnabled:       vals.Get("EnablePerformanceInsights") == formTrue,
		StorageOptimized:                 vals.Get("StorageOptimized") == formTrue,
		OptimizedWrites:                  vals.Get("EnableOptimizedWrites") == formTrue,
		EngineLifecycleSupport:           vals.Get("EngineLifecycleSupport"),
		VpcSecurityGroupIDs:              vpcSGIds,
		EnabledCloudwatchLogsExports:     logExports,
	}

	inst, err := h.Backend.CreateDBInstance(
		id,
		engine,
		instanceClass,
		dbName,
		masterUser,
		paramGroupName,
		allocatedStorage,
		opts,
	)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDeleteDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")

	inst, err := h.Backend.DeleteDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instances, err := h.Backend.DescribeDBInstances(id)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, instances, func(a, b DBInstance) bool {
		return a.DBInstanceIdentifier < b.DBInstanceIdentifier
	}, func(item DBInstance) xmlDBInstance {
		cp := item

		return toXMLInstance(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBInstancesResponse{
		Xmlns:       rdsXMLNS,
		DBInstances: xmlDBInstanceList{Members: members},
		Marker:      marker,
	}, nil
}

func (h *Handler) handleModifyDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")

	allocatedStorage, backupRetention, iops, storageThroughput, monitoringInterval, err := parseDBInstanceIntParams(
		vals,
	)
	if err != nil {
		return nil, err
	}
	// Modify uses -1 as sentinel for "not provided"
	if vals.Get("BackupRetentionPeriod") == "" {
		backupRetention = -1
	}
	if vals.Get("MonitoringInterval") == "" {
		monitoringInterval = -1
	}

	if backupRetention >= 0 && backupRetention > 35 {
		return nil, fmt.Errorf(
			"%w: BackupRetentionPeriod must be between 0 and 35; got %d",
			ErrInvalidParameter, backupRetention,
		)
	}

	vpcSGIds := parseMultiValueParam(vals, "VpcSecurityGroupIds.VpcSecurityGroupID")
	logExports := parseMultiValueParam(vals, "CloudwatchLogsExportConfiguration.EnableLogTypes.member")

	opts := DBInstanceOptions{
		EngineVersion:                    vals.Get("EngineVersion"),
		StorageType:                      vals.Get("StorageType"),
		OptionGroupName:                  vals.Get("OptionGroupName"),
		LicenseModel:                     vals.Get("LicenseModel"),
		MonitoringRoleArn:                vals.Get("MonitoringRoleArn"),
		PreferredMaintenanceWindow:       vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:            vals.Get("PreferredBackupWindow"),
		DBParameterGroupName:             vals.Get("DBParameterGroupName"),
		BackupRetentionPeriod:            backupRetention,
		Iops:                             iops,
		StorageThroughput:                storageThroughput,
		MonitoringInterval:               monitoringInterval,
		MultiAZ:                          vals.Get("MultiAZ") == formTrue,
		MultiAZSet:                       vals.Get("MultiAZ") != "",
		IAMDatabaseAuthenticationEnabled: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		IAMDatabaseAuthSet:               vals.Get("EnableIAMDatabaseAuthentication") != "",
		DeletionProtection:               vals.Get("DeletionProtection") == formTrue,
		DeletionProtectionSet:            vals.Get("DeletionProtection") != "",
		CopyTagsToSnapshot:               vals.Get("CopyTagsToSnapshot") == formTrue,
		AllowMajorVersionUpgrade:         vals.Get("AllowMajorVersionUpgrade") == formTrue,
		ApplyImmediately:                 vals.Get("ApplyImmediately") == formTrue,
		PubliclyAccessible:               vals.Get("PubliclyAccessible") == formTrue,
		PerformanceInsightsEnabled:       vals.Get("EnablePerformanceInsights") == formTrue,
		StorageOptimized:                 vals.Get("StorageOptimized") == formTrue,
		OptimizedWrites:                  vals.Get("EnableOptimizedWrites") == formTrue,
		EngineLifecycleSupport:           vals.Get("EngineLifecycleSupport"),
		VpcSecurityGroupIDs:              vpcSGIds,
		EnabledCloudwatchLogsExports:     logExports,
	}

	inst, err := h.Backend.ModifyDBInstance(id, instanceClass, allocatedStorage, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCreateDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	snap, err := h.Backend.CreateDBSnapshot(snapshotID, instanceID)
	if err != nil {
		return nil, err
	}

	return &createDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	snaps, err := h.Backend.DescribeDBSnapshots(snapshotID, instanceID)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, snaps, func(a, b DBSnapshot) bool {
		return a.DBSnapshotIdentifier < b.DBSnapshotIdentifier
	}, func(item DBSnapshot) xmlDBSnapshot {
		cp := item

		return toXMLSnapshot(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBSnapshotsResponse{
		Xmlns:       rdsXMLNS,
		DBSnapshots: xmlDBSnapshotList{Members: members},
		Marker:      marker,
	}, nil
}

func (h *Handler) handleDeleteDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")

	snap, err := h.Backend.DeleteDBSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
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
		Xmlns:         rdsXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleDescribeDBSubnetGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	sgs, err := h.Backend.DescribeDBSubnetGroups(name)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, sgs, func(a, b DBSubnetGroup) bool {
		return a.DBSubnetGroupName < b.DBSubnetGroupName
	}, func(item DBSubnetGroup) xmlDBSubnetGroup {
		cp := item

		return toXMLSubnetGroup(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBSubnetGroupsResponse{
		Xmlns:          rdsXMLNS,
		DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
		Marker:         marker,
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	if err := h.Backend.DeleteDBSubnetGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{
		Xmlns: rdsXMLNS,
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
		Xmlns:   rdsXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	h.Backend.AddTagsToResource(arn, tags)

	return &addTagsToResourceResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	h.Backend.RemoveTagsFromResource(arn, keys)

	return &removeTagsFromResourceResponse{Xmlns: rdsXMLNS}, nil
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	var instanceCreateTime string
	if !inst.InstanceCreateTime.IsZero() {
		instanceCreateTime = inst.InstanceCreateTime.UTC().Format(time.RFC3339)
	}
	result := xmlDBInstance{
		DBInstanceIdentifier:              inst.DBInstanceIdentifier,
		DbiResourceID:                     inst.DbiResourceID,
		DBInstanceClass:                   inst.DBInstanceClass,
		DBClusterIdentifier:               inst.DBClusterIdentifier,
		Engine:                            inst.Engine,
		EngineVersion:                     inst.EngineVersion,
		DBInstanceStatus:                  inst.DBInstanceStatus,
		MasterUsername:                    inst.MasterUsername,
		DBName:                            inst.DBName,
		Endpoint:                          inst.Endpoint,
		Port:                              inst.Port,
		AllocatedStorage:                  inst.AllocatedStorage,
		Iops:                              inst.Iops,
		StorageThroughput:                 inst.StorageThroughput,
		VpcID:                             inst.VpcID,
		DBSubnetGroupName:                 inst.DBSubnetGroupName,
		ReplicaSourceDBInstanceIdentifier: inst.ReplicaSourceDBInstanceIdentifier,
		StorageType:                       inst.StorageType,
		StorageEncrypted:                  inst.StorageEncrypted,
		MultiAZ:                           inst.MultiAZ,
		AvailabilityZone:                  inst.AvailabilityZone,
		BackupRetentionPeriod:             inst.BackupRetentionPeriod,
		IAMDatabaseAuthenticationEnabled:  inst.IAMDatabaseAuthenticationEnabled,
		DeletionProtection:                inst.DeletionProtection,
		LicenseModel:                      inst.LicenseModel,
		MonitoringInterval:                inst.MonitoringInterval,
		MonitoringRoleArn:                 inst.MonitoringRoleArn,
		EnhancedMonitoringResourceArn:     inst.EnhancedMonitoringResourceArn,
		PreferredMaintenanceWindow:        inst.PreferredMaintenanceWindow,
		PreferredBackupWindow:             inst.PreferredBackupWindow,
		KmsKeyID:                          inst.KmsKeyID,
		CopyTagsToSnapshot:                inst.CopyTagsToSnapshot,
		PubliclyAccessible:                inst.PubliclyAccessible,
		PerformanceInsightsEnabled:        inst.PerformanceInsightsEnabled,
		StorageOptimized:                  inst.StorageOptimized,
		OptimizedWrites:                   inst.OptimizedWrites,
		EngineLifecycleSupport:            inst.EngineLifecycleSupport,
		InstanceCreateTime:                instanceCreateTime,
	}

	if inst.DBInstanceStatus == instanceStatusModifying {
		result.PendingModifiedValues = &xmlPendingModifiedValues{}
	}

	if inst.DBParameterGroupName != "" {
		result.DBParameterGroups = &xmlDBParamGroupsWrapper{
			Status: &xmlDBParamGroupStatus{DBParameterGroupName: inst.DBParameterGroupName},
		}
	}

	if len(inst.VpcSecurityGroups) > 0 {
		members := make([]xmlVpcSecurityGroupMembership, 0, len(inst.VpcSecurityGroups))
		for _, sg := range inst.VpcSecurityGroups {
			members = append(members, xmlVpcSecurityGroupMembership(sg))
		}

		result.VpcSecurityGroups = &xmlVpcSecurityGroupList{Members: members}
	}

	if len(inst.ReadReplicaIdentifiers) > 0 {
		members := make([]xmlReadReplicaIdentifier, 0, len(inst.ReadReplicaIdentifiers))
		for _, rid := range inst.ReadReplicaIdentifiers {
			members = append(members, xmlReadReplicaIdentifier{Value: rid})
		}

		result.ReadReplicaDBInstanceIdentifiers = &xmlReadReplicaIdentifierList{Members: members}
	}

	if len(inst.EnabledCloudwatchLogsExports) > 0 {
		members := make([]xmlLogTypeMember, 0, len(inst.EnabledCloudwatchLogsExports))
		for _, lt := range inst.EnabledCloudwatchLogsExports {
			members = append(members, xmlLogTypeMember{Value: lt})
		}

		result.EnabledCloudwatchLogsExports = &xmlLogTypeList{Members: members}
	}

	return result
}

func toXMLSnapshot(snap *DBSnapshot) xmlDBSnapshot {
	var snapshotCreateTime string
	if !snap.SnapshotCreateTime.IsZero() {
		snapshotCreateTime = snap.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return xmlDBSnapshot{
		DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
		DBInstanceIdentifier: snap.DBInstanceIdentifier,
		Engine:               snap.Engine,
		EngineVersion:        snap.EngineVersion,
		Status:               snap.Status,
		AllocatedStorage:     snap.AllocatedStorage,
		Port:                 snap.Port,
		StorageType:          snap.StorageType,
		OptionGroupName:      snap.OptionGroupName,
		KmsKeyID:             snap.KmsKeyID,
		SourceRegion:         snap.SourceRegion,
		SnapshotType:         snap.SnapshotType,
		SnapshotCreateTime:   snapshotCreateTime,
		PercentProgress:      snap.PercentProgress,
		CopyTagsToSnapshot:   snap.CopyTagsToSnapshot,
		Encrypted:            snap.StorageEncrypted,
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

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := rdsErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("RDS internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func rdsErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrInstanceNotFound, "DBInstanceNotFound"},
		{ErrInstanceAlreadyExists, "DBInstanceAlreadyExists"},
		{ErrSnapshotNotFound, "DBSnapshotNotFound"},
		{ErrSnapshotAlreadyExists, "DBSnapshotAlreadyExists"},
		{ErrSubnetGroupNotFound, "DBSubnetGroupNotFoundFault"},
		{ErrSubnetGroupAlreadyExists, "DBSubnetGroupAlreadyExists"},
		{ErrInvalidParameter, "InvalidParameterValue"},
		{ErrUnknownAction, "InvalidAction"},
		{ErrInvalidDBInstanceState, "InvalidDBInstanceState"},
		{ErrParameterGroupNotFound, "DBParameterGroupNotFound"},
		{ErrParameterGroupAlreadyExists, "DBParameterGroupAlreadyExists"},
		{ErrOptionGroupNotFound, "OptionGroupNotFound"},
		{ErrOptionGroupAlreadyExists, "OptionGroupAlreadyExists"},
		{ErrClusterNotFound, "DBClusterNotFound"},
		{ErrClusterAlreadyExists, "DBClusterAlreadyExists"},
		{ErrClusterSnapshotNotFound, "DBClusterSnapshotNotFound"},
		{ErrClusterSnapshotAlreadyExists, "DBClusterSnapshotAlreadyExists"},
		{ErrClusterEndpointNotFound, "DBClusterEndpointNotFound"},
		{ErrClusterEndpointAlreadyExists, "DBClusterEndpointAlreadyExists"},
		{ErrExportTaskNotFound, "ExportTaskNotFound"},
		{ErrExportTaskAlreadyExists, "ExportTaskAlreadyExists"},
		{ErrGlobalClusterNotFound, "GlobalClusterNotFound"},
		{ErrGlobalClusterAlreadyExists, "GlobalClusterAlreadyExists"},
		{ErrInvalidDBClusterStateFault, "InvalidDBClusterStateFault"},
		{ErrInvalidGlobalClusterState, "InvalidGlobalClusterStateFault"},
		{ErrEventSubscriptionNotFound, "SubscriptionNotFound"},
		{ErrEventSubscriptionAlreadyExists, "SubscriptionAlreadyExist"},
		{ErrDBSecurityGroupNotFound, "DBSecurityGroupNotFound"},
		{ErrDBSecurityGroupAlreadyExists, "DBSecurityGroupAlreadyExists"},
		{ErrBlueGreenDeploymentNotFound, "BlueGreenDeploymentNotFound"},
		{ErrBlueGreenDeploymentAlreadyExists, "BlueGreenDeploymentAlreadyExists"},
		{ErrDBShardGroupNotFound, "DBShardGroupNotFound"},
		{ErrDBShardGroupAlreadyExists, "DBShardGroupAlreadyExists"},
		{ErrIntegrationNotFound, "IntegrationNotFound"},
		{ErrIntegrationAlreadyExists, "IntegrationAlreadyExists"},
		{ErrTenantDatabaseNotFound, "TenantDatabaseNotFound"},
		{ErrTenantDatabaseAlreadyExists, "TenantDatabaseAlreadyExists"},
		{ErrDBClusterAutomatedBackupNotFound, "DBClusterAutomatedBackupNotFound"},
		{ErrDBInstanceAutomatedBackupNotFound, "DBInstanceAutomatedBackupNotFound"},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &rdsErrorResponse{
		Xmlns: rdsXMLNS,
		Error: rdsError{Code: code, Message: message, Type: "Sender"},
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

// ---- XML response types ----

type rdsError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type rdsErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Error   rdsError `xml:"Error"`
}

type xmlDBParamGroupStatus struct {
	DBParameterGroupName string `xml:"DBParameterGroupName,omitempty"`
}

type xmlDBParamGroupsWrapper struct {
	Status *xmlDBParamGroupStatus `xml:"DBParameterGroupStatus,omitempty"`
}

type xmlVpcSecurityGroupMembership struct {
	VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
	Status             string `xml:"Status"`
}

type xmlVpcSecurityGroupList struct {
	Members []xmlVpcSecurityGroupMembership `xml:"VpcSecurityGroupMembership"`
}

type xmlReadReplicaIdentifier struct {
	Value string `xml:",chardata"`
}

type xmlReadReplicaIdentifierList struct {
	Members []xmlReadReplicaIdentifier `xml:"ReadReplicaDBInstanceIdentifier"`
}

type xmlLogTypeMember struct {
	Value string `xml:",chardata"`
}

type xmlLogTypeList struct {
	Members []xmlLogTypeMember `xml:"member"`
}

type xmlPendingModifiedValues struct {
	DBInstanceClass  string `xml:"DBInstanceClass,omitempty"`
	EngineVersion    string `xml:"EngineVersion,omitempty"`
	AllocatedStorage int    `xml:"AllocatedStorage,omitempty"`
	Iops             int    `xml:"Iops,omitempty"`
	MultiAZ          bool   `xml:"MultiAZ,omitempty"`
}

type xmlDBInstance struct {
	DBParameterGroups                 *xmlDBParamGroupsWrapper      `xml:"DBParameterGroups,omitempty"`
	VpcSecurityGroups                 *xmlVpcSecurityGroupList      `xml:"VpcSecurityGroups,omitempty"`
	ReadReplicaDBInstanceIdentifiers  *xmlReadReplicaIdentifierList `xml:"ReadReplicaDBInstanceIdentifiers,omitempty"`
	EnabledCloudwatchLogsExports      *xmlLogTypeList               `xml:"EnabledCloudwatchLogsExports,omitempty"`
	PendingModifiedValues             *xmlPendingModifiedValues     `xml:"PendingModifiedValues,omitempty"`
	LicenseModel                      string                        `xml:"LicenseModel,omitempty"`
	PreferredBackupWindow             string                        `xml:"PreferredBackupWindow,omitempty"`
	DBInstanceClass                   string                        `xml:"DBInstanceClass"`
	DBClusterIdentifier               string                        `xml:"DBClusterIdentifier,omitempty"`
	Engine                            string                        `xml:"Engine"`
	EngineVersion                     string                        `xml:"EngineVersion,omitempty"`
	DBInstanceStatus                  string                        `xml:"DBInstanceStatus"`
	MasterUsername                    string                        `xml:"MasterUsername"`
	DBName                            string                        `xml:"DBName,omitempty"`
	Endpoint                          string                        `xml:"Endpoint>Address"`
	VpcID                             string                        `xml:"DBSubnetGroup>VpcId,omitempty"`
	DBSubnetGroupName                 string                        `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	ReplicaSourceDBInstanceIdentifier string                        `xml:"ReadReplicaSourceDBInstanceIdentifier,omitempty"`
	StorageType                       string                        `xml:"StorageType,omitempty"`
	AvailabilityZone                  string                        `xml:"AvailabilityZone,omitempty"`
	DBInstanceIdentifier              string                        `xml:"DBInstanceIdentifier"`
	MonitoringRoleArn                 string                        `xml:"MonitoringRoleArn,omitempty"`
	EnhancedMonitoringResourceArn     string                        `xml:"EnhancedMonitoringResourceArn,omitempty"`
	PreferredMaintenanceWindow        string                        `xml:"PreferredMaintenanceWindow,omitempty"`
	DbiResourceID                     string                        `xml:"DbiResourceId,omitempty"`
	KmsKeyID                          string                        `xml:"KmsKeyId,omitempty"`
	InstanceCreateTime                string                        `xml:"InstanceCreateTime,omitempty"`
	EngineLifecycleSupport            string                        `xml:"EngineLifecycleSupport,omitempty"`
	AllocatedStorage                  int                           `xml:"AllocatedStorage"`
	Iops                              int                           `xml:"Iops,omitempty"`
	StorageThroughput                 int                           `xml:"StorageThroughput,omitempty"`
	BackupRetentionPeriod             int                           `xml:"BackupRetentionPeriod,omitempty"`
	MonitoringInterval                int                           `xml:"MonitoringInterval,omitempty"`
	Port                              int                           `xml:"Endpoint>Port"`
	StorageEncrypted                  bool                          `xml:"StorageEncrypted"`
	IAMDatabaseAuthenticationEnabled  bool                          `xml:"IAMDatabaseAuthenticationEnabled,omitempty"`
	DeletionProtection                bool                          `xml:"DeletionProtection,omitempty"`
	CopyTagsToSnapshot                bool                          `xml:"CopyTagsToSnapshot,omitempty"`
	PubliclyAccessible                bool                          `xml:"PubliclyAccessible,omitempty"`
	PerformanceInsightsEnabled        bool                          `xml:"PerformanceInsightsEnabled,omitempty"`
	StorageOptimized                  bool                          `xml:"StorageOptimized,omitempty"`
	OptimizedWrites                   bool                          `xml:"OptimizedWritesEnabled,omitempty"`
	MultiAZ                           bool                          `xml:"MultiAZ"`
}

type xmlDBInstanceList struct {
	Members []xmlDBInstance `xml:"DBInstance"`
}

type createDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceResult>DBInstance"`
}

type deleteDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"DeleteDBInstanceResult>DBInstance"`
}

type describeDBInstancesResponse struct {
	XMLName     xml.Name          `xml:"DescribeDBInstancesResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	Marker      string            `xml:"DescribeDBInstancesResult>Marker,omitempty"`
	DBInstances xmlDBInstanceList `xml:"DescribeDBInstancesResult>DBInstances"`
}

type modifyDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"ModifyDBInstanceResult>DBInstance"`
}

type xmlDBSnapshot struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	Engine               string `xml:"Engine"`
	EngineVersion        string `xml:"EngineVersion,omitempty"`
	Status               string `xml:"Status"`
	StorageType          string `xml:"StorageType,omitempty"`
	OptionGroupName      string `xml:"OptionGroupName,omitempty"`
	KmsKeyID             string `xml:"KmsKeyId,omitempty"`
	SourceRegion         string `xml:"SourceRegion,omitempty"`
	SnapshotType         string `xml:"SnapshotType,omitempty"`
	SnapshotCreateTime   string `xml:"SnapshotCreateTime,omitempty"`
	AllocatedStorage     int    `xml:"AllocatedStorage,omitempty"`
	Port                 int    `xml:"Port,omitempty"`
	PercentProgress      int    `xml:"PercentProgress,omitempty"`
	Encrypted            bool   `xml:"Encrypted,omitempty"`
	CopyTagsToSnapshot   bool   `xml:"CopyTagsToSnapshot,omitempty"`
}

type xmlDBSnapshotList struct {
	Members []xmlDBSnapshot `xml:"DBSnapshot"`
}

type createDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"CreateDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"CreateDBSnapshotResult>DBSnapshot"`
}

type deleteDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"DeleteDBSnapshotResult>DBSnapshot"`
}

type describeDBSnapshotsResponse struct {
	XMLName     xml.Name          `xml:"DescribeDBSnapshotsResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	Marker      string            `xml:"DescribeDBSnapshotsResult>Marker,omitempty"`
	DBSnapshots xmlDBSnapshotList `xml:"DescribeDBSnapshotsResult>DBSnapshots"`
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
	VpcID                    string        `xml:"VpcId"`
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

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName        xml.Name             `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	Marker         string               `xml:"DescribeDBSubnetGroupsResult>Marker,omitempty"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups"`
}

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// parseSubnetIDMembers parses SubnetIds.SubnetIdentifier.N form values (AWS query protocol encoding).
func parseSubnetIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i))
		if sid == "" {
			return ids
		}
		ids = append(ids, sid)
	}
}

// parseTagEntries parses Tags.Tag.N.Key/Value form values.
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

// parseTagKeyMembers parses TagKeys.member.N form values.
// parseMultiValueParam extracts indexed values from a form param prefix
// (e.g. "VpcSecurityGroupIds.VpcSecurityGroupId.N").
// validMonitoringInterval returns true if v is one of the AWS-allowed monitoring intervals.
func validMonitoringInterval(v int) bool {
	switch v {
	case 0,
		1,
		monitoringInterval5,
		monitoringInterval10,
		monitoringInterval15,
		monitoringInterval30,
		monitoringInterval60:
		return true
	}

	return false
}

func parseMultiValueParam(vals url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
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

func parseDescribePagination(vals url.Values) (string, int, error) {
	marker := vals.Get("Marker")
	maxRecords := 0
	rawMaxRecords := vals.Get("MaxRecords")
	if rawMaxRecords == "" {
		return marker, maxRecords, nil
	}

	maxRecords, err := strconv.Atoi(rawMaxRecords)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid MaxRecords %q", ErrInvalidParameter, rawMaxRecords)
	}

	if maxRecords <= 0 {
		return "", 0, fmt.Errorf("%w: MaxRecords must be greater than 0", ErrInvalidParameter)
	}

	return marker, maxRecords, nil
}

func paginateDescribe[TData any, TXMLOutput any](
	vals url.Values,
	data []TData,
	less func(a, b TData) bool,
	convert func(TData) TXMLOutput,
) ([]TXMLOutput, string, error) {
	marker, maxRecords, err := parseDescribePagination(vals)
	if err != nil {
		return nil, "", err
	}

	sort.Slice(data, func(i, j int) bool {
		return less(data[i], data[j])
	})

	p := page.New(data, marker, maxRecords, rdsDescribeDefaultPageSize)
	members := make([]TXMLOutput, 0, len(p.Data))
	for _, item := range p.Data {
		members = append(members, convert(item))
	}

	return members, p.Next, nil
}

// parseParameterMembers parses Parameters.Parameter.N.ParameterName/ParameterValue form values.
func parseParameterMembers(vals url.Values) []DBParameter {
	var params []DBParameter
	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if name == "" {
			return params
		}
		params = append(params, DBParameter{
			ParameterName:  name,
			ParameterValue: vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterValue", i)),
		})
	}
}

// ---- Parameter Group handlers ----

func (h *Handler) handleCreateDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
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
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBParameterGroupsResponse{
		Xmlns:             rdsXMLNS,
		DBParameterGroups: xmlDBParameterGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if err := h.Backend.DeleteDBParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBParameterGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	params := parseParameterMembers(vals)
	pg, err := h.Backend.ModifyDBParameterGroup(name, params)
	if err != nil {
		return nil, err
	}

	return &modifyDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeDBParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBParameterGroupName")
	params, err := h.Backend.DescribeDBParameters(groupName)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter(p))
	}

	return &describeDBParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}

func (h *Handler) handleResetDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == formTrue
	var paramNames []string
	for i := 1; ; i++ {
		pName := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if pName == "" {
			break
		}
		paramNames = append(paramNames, pName)
	}
	pg, err := h.Backend.ResetDBParameterGroup(name, resetAll, paramNames)
	if err != nil {
		return nil, err
	}

	return &resetDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

// ---- Option Group handlers ----

func (h *Handler) handleCreateOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	engine := vals.Get("EngineName")
	majorVersion := vals.Get("MajorEngineVersion")
	description := vals.Get("OptionGroupDescription")
	og, err := h.Backend.CreateOptionGroup(name, engine, majorVersion, description)
	if err != nil {
		return nil, err
	}

	return &createOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroups(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	groups, err := h.Backend.DescribeOptionGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlOptionGroup, 0, len(groups))
	for _, og := range groups {
		cp := og
		members = append(members, toXMLOptionGroup(&cp))
	}

	return &describeOptionGroupsResponse{
		Xmlns:            rdsXMLNS,
		OptionGroupsList: xmlOptionGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	if err := h.Backend.DeleteOptionGroup(name); err != nil {
		return nil, err
	}

	return &deleteOptionGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	var optionsToAdd []OptionGroupOption
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionName", i))
		if optName == "" {
			break
		}
		optionsToAdd = append(optionsToAdd, OptionGroupOption{
			OptionName:    optName,
			OptionVersion: vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionVersion", i)),
		})
	}
	var optionsToRemove []string
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToRemove.member.%d", i))
		if optName == "" {
			break
		}
		optionsToRemove = append(optionsToRemove, optName)
	}
	og, err := h.Backend.ModifyOptionGroup(name, optionsToAdd, optionsToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroupOptions(_ url.Values) (any, error) {
	return &describeOptionGroupOptionsResponse{Xmlns: rdsXMLNS}, nil
}

// ---- Cluster handlers ----

func (h *Handler) handleCreateDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	masterUser := vals.Get("MasterUsername")
	dbName := vals.Get("DatabaseName")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	rawPort := vals.Get("Port")
	port := 0
	if rawPort != "" {
		var err error
		port, err = strconv.Atoi(rawPort)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Port %q", ErrInvalidParameter, rawPort)
		}
	}

	serverlessV2Cfg, parseErr := parseServerlessV2ScalingConfig(vals)
	if parseErr != nil && !errors.Is(parseErr, ErrNoServerlessV2Config) {
		return nil, parseErr
	}

	backtrackWindow := int64(0)
	if rawBW := vals.Get("BacktrackWindow"); rawBW != "" {
		if v, err := strconv.ParseInt(rawBW, 10, 64); err == nil {
			backtrackWindow = v
		}
	}

	monitoringInterval := 0
	if rawMI := vals.Get("MonitoringInterval"); rawMI != "" {
		if v, err := strconv.Atoi(rawMI); err == nil {
			monitoringInterval = v
		}
	}

	clusterOpts := DBClusterOptions{
		EngineVersion:                vals.Get("EngineVersion"),
		KmsKeyID:                     vals.Get("KmsKeyId"),
		PreferredBackupWindow:        vals.Get("PreferredBackupWindow"),
		PreferredMaintenanceWindow:   vals.Get("PreferredMaintenanceWindow"),
		MonitoringRoleArn:            vals.Get("MonitoringRoleArn"),
		StorageType:                  vals.Get("StorageType"),
		NetworkType:                  vals.Get("NetworkType"),
		EngineLifecycleSupport:       vals.Get("EngineLifecycleSupport"),
		EnabledCloudwatchLogsExports: parseMultiValueParam(vals, "EnableCloudwatchLogsExports.member"),
		AvailabilityZones:            parseMultiValueParam(vals, "AvailabilityZones.AvailabilityZone"),
		BacktrackWindow:              backtrackWindow,
		MonitoringInterval:           monitoringInterval,
		MultiAZ:                      vals.Get("MultiAZ") == formTrue,
		StorageEncrypted:             vals.Get("StorageEncrypted") == formTrue,
		CopyTagsToSnapshot:           vals.Get("CopyTagsToSnapshot") == formTrue,
		DeletionProtection:           vals.Get("DeletionProtection") == formTrue,
		OptimizedWrites:              vals.Get("EnableOptimizedWrites") == formTrue,
	}

	cluster, err := h.Backend.CreateDBCluster(
		id,
		engine,
		masterUser,
		dbName,
		paramGroupName,
		port,
		serverlessV2Cfg,
		clusterOpts,
	)
	if err != nil {
		return nil, err
	}

	return &createDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeDBClusters(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(id)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, clusters, func(a, b DBCluster) bool {
		return a.DBClusterIdentifier < b.DBClusterIdentifier
	}, func(item DBCluster) xmlDBCluster {
		cp := item

		return toXMLCluster(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBClustersResponse{
		Xmlns:      rdsXMLNS,
		DBClusters: xmlDBClusterList{Members: members},
		Marker:     marker,
	}, nil
}

func (h *Handler) handleDeleteDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.DeleteDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")

	backtrackWindow := int64(0)
	if rawBW := vals.Get("BacktrackWindow"); rawBW != "" {
		if v, err := strconv.ParseInt(rawBW, 10, 64); err == nil {
			backtrackWindow = v
		}
	}

	monitoringInterval := -1
	if rawMI := vals.Get("MonitoringInterval"); rawMI != "" {
		if v, err := strconv.Atoi(rawMI); err == nil {
			monitoringInterval = v
		}
	}

	storageEncryptedRaw := vals.Get("StorageEncrypted")
	opts := DBClusterOptions{
		EngineVersion:              vals.Get("EngineVersion"),
		KmsKeyID:                   vals.Get("KmsKeyId"),
		PreferredBackupWindow:      vals.Get("PreferredBackupWindow"),
		PreferredMaintenanceWindow: vals.Get("PreferredMaintenanceWindow"),
		MonitoringRoleArn:          vals.Get("MonitoringRoleArn"),
		StorageType:                vals.Get("StorageType"),
		NetworkType:                vals.Get("NetworkType"),
		EngineLifecycleSupport:     vals.Get("EngineLifecycleSupport"),
		EnabledCloudwatchLogsExports: parseMultiValueParam(
			vals,
			"CloudwatchLogsExportConfiguration.EnableLogTypes.member",
		),
		BacktrackWindow:         backtrackWindow,
		MonitoringInterval:      monitoringInterval,
		MultiAZ:                 vals.Get("MultiAZ") == formTrue,
		CopyTagsToSnapshot:      vals.Get("CopyTagsToSnapshot") == formTrue,
		DeletionProtection:      vals.Get("DeletionProtection") == formTrue,
		DeletionProtectionSet:   vals.Get("DeletionProtection") != "",
		StorageEncrypted:        storageEncryptedRaw == formTrue,
		StorageEncryptedChanged: storageEncryptedRaw != "",
		OptimizedWrites:         vals.Get("EnableOptimizedWrites") == formTrue,
	}

	cluster, err := h.Backend.ModifyDBCluster(id, paramGroupName, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
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
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLClusterParameterGroup(pg),
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
		members = append(members, toXMLClusterParameterGroup(&cp))
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
		},
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
		Xmlns:             rdsXMLNS,
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

	return &describeDBClusterSnapshotsResponse{
		Xmlns:              rdsXMLNS,
		DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
	}, nil
}

// ---- Read Replica handlers ----

func (h *Handler) handleCreateDBInstanceReadReplica(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	sourceID := vals.Get("SourceDBInstanceIdentifier")
	sourceRegion := vals.Get("SourceRegion")
	inst, err := h.Backend.CreateDBInstanceReadReplica(id, sourceID, sourceRegion)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceReadReplicaResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handlePromoteReadReplica(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.PromoteReadReplica(id)
	if err != nil {
		return nil, err
	}

	return &promoteReadReplicaResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

// ---- Misc handlers ----

func (h *Handler) handleRebootDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
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
		Xmlns:            rdsXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	options := h.Backend.DescribeOrderableDBInstanceOptions(engine, engineVersion)
	members := make([]xmlOrderableDBInstanceOption, 0, len(options))
	for _, o := range options {
		members = append(members, xmlOrderableDBInstanceOption(o))
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: rdsXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeDBLogFiles(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	files, err := h.Backend.DescribeDBLogFiles(instanceID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBLogFile, 0, len(files))
	for _, f := range files {
		members = append(members, xmlDBLogFile(f))
	}

	return &describeDBLogFilesResponse{
		Xmlns:              rdsXMLNS,
		DescribeDBLogFiles: xmlDBLogFileList{Members: members},
	}, nil
}

func (h *Handler) handleDownloadDBLogFilePortion(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	logFileName := vals.Get("LogFileName")
	data, err := h.Backend.DownloadDBLogFilePortion(instanceID, logFileName)
	if err != nil {
		return nil, err
	}

	return &downloadDBLogFilePortionResponse{
		Xmlns:                 rdsXMLNS,
		LogFileData:           data,
		AdditionalDataPending: false,
		Marker:                "",
	}, nil
}

func (h *Handler) handleDescribeGlobalClusters(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	clusters, err := h.Backend.DescribeGlobalClusters(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlGlobalCluster, 0, len(clusters))
	for _, gc := range clusters {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns:          rdsXMLNS,
		GlobalClusters: xmlGlobalClusterList{Members: members},
	}, nil
}

// ---- Cluster start/stop handlers ----

func (h *Handler) handleStartDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StartDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &startDBClusterResponse{
		Xmlns:     rdsXMLNS,
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
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

// ---- Cluster snapshot management handlers ----

func (h *Handler) handleDeleteDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snap, err := h.Backend.DeleteDBClusterSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterSnapshotResponse{
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleRestoreDBClusterFromSnapshot(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	snapshotID := vals.Get("SnapshotIdentifier")
	engine := vals.Get("Engine")
	cluster, err := h.Backend.RestoreDBClusterFromSnapshot(clusterID, snapshotID, engine)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromSnapshotResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterToPointInTime(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	sourceClusterID := vals.Get("SourceDBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterToPointInTime(clusterID, sourceClusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterToPointInTimeResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
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
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
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
		Xmlns:  rdsXMLNS,
		Result: createDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

func (h *Handler) handleDescribeDBClusterEndpoints(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpoints, err := h.Backend.DescribeDBClusterEndpoints(clusterID, endpointID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterEndpointFields, 0, len(endpoints))
	for _, ep := range endpoints {
		cp := ep
		members = append(members, toXMLClusterEndpointFields(&cp))
	}

	return &describeDBClusterEndpointsResponse{
		Xmlns:              rdsXMLNS,
		DBClusterEndpoints: xmlDBClusterEndpointList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	ep, err := h.Backend.DeleteDBClusterEndpoint(endpointID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterEndpointResponse{
		Xmlns:  rdsXMLNS,
		Result: deleteDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

// ---- Misc handlers ----

func (h *Handler) handleDescribeValidDBInstanceModifications(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	if _, err := h.Backend.DescribeValidDBInstanceModifications(id); err != nil {
		return nil, err
	}

	features := []xmlAvailableProcessorFeature{
		{Name: "coreCount", DefaultValue: "2", AllowedValues: "1,2,4,8"},
		{Name: "threadsPerCore", DefaultValue: "2", AllowedValues: "1,2"},
	}

	return &describeValidDBInstanceModificationsResponse{
		Xmlns: rdsXMLNS,
		Result: xmlValidDBInstanceModificationsWrapper{
			Message: xmlValidDBInstanceModificationsMessage{
				ValidProcessorFeatures: xmlAvailableProcessorFeatureList{Members: features},
			},
		},
	}, nil
}

func (h *Handler) handleStartExportTask(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	sourceARN := vals.Get("SourceArn")
	s3Bucket := vals.Get("S3BucketName")
	task, err := h.Backend.StartExportTask(taskID, sourceARN, s3Bucket)
	if err != nil {
		return nil, err
	}

	return &startExportTaskResponse{
		Xmlns:  rdsXMLNS,
		Result: startExportTaskResult{toXMLExportTask(task)},
	}, nil
}

func (h *Handler) handleDescribeExportTasks(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	tasks, err := h.Backend.DescribeExportTasks(taskID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlExportTask, 0, len(tasks))
	for _, task := range tasks {
		cp := task
		members = append(members, toXMLExportTask(&cp))
	}

	return &describeExportTasksResponse{
		Xmlns:       rdsXMLNS,
		ExportTasks: xmlExportTaskList{Members: members},
	}, nil
}

func (h *Handler) handleCancelExportTask(vals url.Values) (any, error) {
	taskID := vals.Get("ExportTaskIdentifier")
	task, err := h.Backend.CancelExportTask(taskID)
	if err != nil {
		return nil, err
	}

	return &cancelExportTaskResponse{
		Xmlns:  rdsXMLNS,
		Result: cancelExportTaskResult{toXMLExportTask(task)},
	}, nil
}

func toXMLClusterEndpointFields(ep *DBClusterEndpoint) xmlDBClusterEndpointFields {
	return xmlDBClusterEndpointFields{
		DBClusterEndpointIdentifier: ep.DBClusterEndpointIdentifier,
		DBClusterIdentifier:         ep.DBClusterIdentifier,
		EndpointType:                ep.EndpointType,
		Status:                      ep.Status,
		Endpoint:                    ep.Endpoint,
	}
}

func toXMLExportTask(task *ExportTask) xmlExportTask {
	return xmlExportTask{
		ExportTaskIdentifier: task.ExportTaskIdentifier,
		SourceArn:            task.SourceArn,
		Status:               task.Status,
		S3Bucket:             task.S3Bucket,
	}
}

func toXMLParameterGroup(pg *DBParameterGroup) xmlDBParameterGroup {
	return xmlDBParameterGroup{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
	}
}

func toXMLClusterParameterGroup(pg *DBParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
	}
}

func toXMLOptionGroup(og *OptionGroup) xmlOptionGroup {
	opts := make([]xmlOptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		opts = append(opts, xmlOptionGroupOption(o))
	}

	return xmlOptionGroup{
		OptionGroupName:        og.OptionGroupName,
		OptionGroupDescription: og.OptionGroupDescription,
		EngineName:             og.EngineName,
		MajorEngineVersion:     og.MajorEngineVersion,
		Options:                xmlOptionGroupOptionList{Members: opts},
	}
}

func toXMLCluster(c *DBCluster) xmlDBCluster {
	var clusterCreateTime string
	if !c.ClusterCreateTime.IsZero() {
		clusterCreateTime = c.ClusterCreateTime.UTC().Format(time.RFC3339)
	}
	x := xmlDBCluster{
		DBClusterIdentifier:             c.DBClusterIdentifier,
		Engine:                          c.Engine,
		EngineVersion:                   c.EngineVersion,
		Status:                          c.Status,
		MasterUsername:                  c.MasterUsername,
		DatabaseName:                    c.DatabaseName,
		DBClusterParameterGroupName:     c.DBClusterParameterGroupName,
		Endpoint:                        c.Endpoint,
		ReaderEndpoint:                  c.ReaderEndpoint,
		NetworkType:                     c.NetworkType,
		StorageType:                     c.StorageType,
		EngineLifecycleSupport:          c.EngineLifecycleSupport,
		Port:                            c.Port,
		ActivityStreamStatus:            c.ActivityStreamStatus,
		ActivityStreamMode:              c.ActivityStreamMode,
		ActivityStreamKMSKeyID:          c.ActivityStreamKMSKeyID,
		ActivityStreamKinesisStreamName: c.ActivityStreamKinesisStreamName,
		PreferredBackupWindow:           c.PreferredBackupWindow,
		PreferredMaintenanceWindow:      c.PreferredMaintenanceWindow,
		KmsKeyID:                        c.KmsKeyID,
		MonitoringRoleArn:               c.MonitoringRoleArn,
		ClusterCreateTime:               clusterCreateTime,
		BacktrackWindow:                 c.BacktrackWindow,
		MonitoringInterval:              c.MonitoringInterval,
		MultiAZ:                         c.MultiAZ,
		StorageEncrypted:                c.StorageEncrypted,
		CopyTagsToSnapshot:              c.CopyTagsToSnapshot,
		DeletionProtection:              c.DeletionProtection,
		OptimizedWrites:                 c.OptimizedWrites,
	}

	if c.ServerlessV2ScalingConfig != nil {
		x.ServerlessV2ScalingConfiguration = &xmlServerlessV2ScalingConfiguration{
			MinCapacity: c.ServerlessV2ScalingConfig.MinCapacity,
			MaxCapacity: c.ServerlessV2ScalingConfig.MaxCapacity,
		}
	}

	if len(c.DBClusterMembers) > 0 {
		members := make([]xmlDBClusterMember, 0, len(c.DBClusterMembers))
		for _, m := range c.DBClusterMembers {
			members = append(members, xmlDBClusterMember(m))
		}

		x.DBClusterMembers = &xmlDBClusterMemberList{Members: members}
	}

	if len(c.EnabledCloudwatchLogsExports) > 0 {
		members := make([]xmlLogTypeMember, 0, len(c.EnabledCloudwatchLogsExports))
		for _, lt := range c.EnabledCloudwatchLogsExports {
			members = append(members, xmlLogTypeMember{Value: lt})
		}

		x.EnabledCloudwatchLogsExports = &xmlLogTypeList{Members: members}
	}

	if len(c.AvailabilityZones) > 0 {
		x.AvailabilityZones = &xmlAvailabilityZoneList{Members: c.AvailabilityZones}
	}

	return x
}

// parseServerlessV2ScalingConfig parses ServerlessV2ScalingConfiguration from request form values.
// Returns nil when neither field is present in the request.
func parseServerlessV2ScalingConfig(vals url.Values) (*ServerlessV2ScalingConfiguration, error) {
	rawMin := vals.Get("ServerlessV2ScalingConfiguration.MinCapacity")
	rawMax := vals.Get("ServerlessV2ScalingConfiguration.MaxCapacity")
	if rawMin == "" && rawMax == "" {
		return nil, ErrNoServerlessV2Config
	}
	cfg := &ServerlessV2ScalingConfiguration{}
	if rawMin != "" {
		v, err := strconv.ParseFloat(rawMin, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid ServerlessV2ScalingConfiguration.MinCapacity %q",
				ErrInvalidParameter, rawMin,
			)
		}
		cfg.MinCapacity = v
	}
	if rawMax != "" {
		v, err := strconv.ParseFloat(rawMax, 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid ServerlessV2ScalingConfiguration.MaxCapacity %q",
				ErrInvalidParameter, rawMax,
			)
		}
		cfg.MaxCapacity = v
	}

	return cfg, nil
}

func toXMLClusterSnapshot(s *DBClusterSnapshot) xmlDBClusterSnapshot {
	var snapshotCreateTime string
	if !s.SnapshotCreateTime.IsZero() {
		snapshotCreateTime = s.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
		DBClusterIdentifier:         s.DBClusterIdentifier,
		Engine:                      s.Engine,
		EngineVersion:               s.EngineVersion,
		Status:                      s.Status,
		SnapshotCreateTime:          snapshotCreateTime,
		PercentProgress:             s.PercentProgress,
		StorageEncrypted:            s.StorageEncrypted,
	}
}

// ---- Parameter Group XML types ----

type xmlDBParameterGroup struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
}

type xmlDBParameterGroupList struct {
	Members []xmlDBParameterGroup `xml:"DBParameterGroup"`
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
}

type xmlDBClusterParameterGroupList struct {
	Members []xmlDBClusterParameterGroup `xml:"DBClusterParameterGroup"`
}

type xmlDBParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	Source         string `xml:"Source,omitempty"`
	ApplyMethod    string `xml:"ApplyMethod,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBParameterList struct {
	Members []xmlDBParameter `xml:"Parameter"`
}

type createDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
}

type describeDBParameterGroupsResponse struct {
	XMLName           xml.Name                `xml:"DescribeDBParameterGroupsResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	DBParameterGroups xmlDBParameterGroupList `xml:"DescribeDBParameterGroupsResult>DBParameterGroups"`
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

type describeDBParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeDBParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeDBParametersResult>Parameters"`
}

// ---- Option Group XML types ----

type xmlOptionGroupOption struct {
	OptionName    string `xml:"OptionName"`
	OptionVersion string `xml:"OptionVersion,omitempty"`
}

type xmlOptionGroupOptionList struct {
	Members []xmlOptionGroupOption `xml:"Option"`
}

type xmlOptionGroup struct {
	OptionGroupName        string                   `xml:"OptionGroupName"`
	OptionGroupDescription string                   `xml:"OptionGroupDescription"`
	EngineName             string                   `xml:"EngineName"`
	MajorEngineVersion     string                   `xml:"MajorEngineVersion"`
	Options                xmlOptionGroupOptionList `xml:"Options"`
}

type xmlOptionGroupList struct {
	Members []xmlOptionGroup `xml:"OptionGroup"`
}

type createOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"CreateOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"CreateOptionGroupResult>OptionGroup"`
}

type describeOptionGroupsResponse struct {
	XMLName          xml.Name           `xml:"DescribeOptionGroupsResponse"`
	Xmlns            string             `xml:"xmlns,attr"`
	OptionGroupsList xmlOptionGroupList `xml:"DescribeOptionGroupsResult>OptionGroupsList"`
}

type deleteOptionGroupResponse struct {
	XMLName xml.Name `xml:"DeleteOptionGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"ModifyOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"ModifyOptionGroupResult>OptionGroup"`
}

type describeOptionGroupOptionsResponse struct {
	XMLName xml.Name `xml:"DescribeOptionGroupOptionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// ---- Cluster XML types ----

type xmlServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `xml:"MinCapacity"`
	MaxCapacity float64 `xml:"MaxCapacity"`
}

// xmlServerlessV2Ref is a type alias to keep struct field definitions within line-length limits.
type xmlServerlessV2Ref = xmlServerlessV2ScalingConfiguration

type xmlDBClusterMember struct {
	DBInstanceIdentifier        string `xml:"DBInstanceIdentifier"`
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName,omitempty"`
	PromotionTier               int    `xml:"PromotionTier,omitempty"`
	IsClusterWriter             bool   `xml:"IsClusterWriter"`
}

type xmlDBClusterMemberList struct {
	Members []xmlDBClusterMember `xml:"DBClusterMember"`
}

type xmlAvailabilityZoneList struct {
	Members []string `xml:"AvailabilityZone"`
}

type xmlDBCluster struct {
	ServerlessV2ScalingConfiguration *xmlServerlessV2Ref      `xml:"ServerlessV2ScalingConfiguration,omitempty"`
	DBClusterMembers                 *xmlDBClusterMemberList  `xml:"DBClusterMembers,omitempty"`
	EnabledCloudwatchLogsExports     *xmlLogTypeList          `xml:"EnabledCloudwatchLogsExports,omitempty"`
	AvailabilityZones                *xmlAvailabilityZoneList `xml:"AvailabilityZones,omitempty"`
	DBClusterIdentifier              string                   `xml:"DBClusterIdentifier"`
	Engine                           string                   `xml:"Engine"`
	EngineVersion                    string                   `xml:"EngineVersion,omitempty"`
	Status                           string                   `xml:"Status"`
	MasterUsername                   string                   `xml:"MasterUsername"`
	DatabaseName                     string                   `xml:"DatabaseName,omitempty"`
	DBClusterParameterGroupName      string                   `xml:"DBClusterParameterGroup"`
	Endpoint                         string                   `xml:"Endpoint,omitempty"`
	ReaderEndpoint                   string                   `xml:"ReaderEndpoint,omitempty"`
	NetworkType                      string                   `xml:"NetworkType,omitempty"`
	StorageType                      string                   `xml:"StorageType,omitempty"`
	EngineLifecycleSupport           string                   `xml:"EngineLifecycleSupport,omitempty"`
	ActivityStreamStatus             string                   `xml:"ActivityStreamStatus,omitempty"`
	ActivityStreamMode               string                   `xml:"ActivityStreamMode,omitempty"`
	ActivityStreamKMSKeyID           string                   `xml:"ActivityStreamKmsKeyId,omitempty"`
	ActivityStreamKinesisStreamName  string                   `xml:"ActivityStreamKinesisStreamName,omitempty"`
	PreferredBackupWindow            string                   `xml:"PreferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow       string                   `xml:"PreferredMaintenanceWindow,omitempty"`
	KmsKeyID                         string                   `xml:"KmsKeyId,omitempty"`
	MonitoringRoleArn                string                   `xml:"MonitoringRoleArn,omitempty"`
	ClusterCreateTime                string                   `xml:"ClusterCreateTime,omitempty"`
	Port                             int                      `xml:"Port"`
	BacktrackWindow                  int64                    `xml:"BacktrackWindow,omitempty"`
	MonitoringInterval               int                      `xml:"MonitoringInterval,omitempty"`
	MultiAZ                          bool                     `xml:"MultiAZ,omitempty"`
	StorageEncrypted                 bool                     `xml:"StorageEncrypted,omitempty"`
	CopyTagsToSnapshot               bool                     `xml:"CopyTagsToSnapshot,omitempty"`
	DeletionProtection               bool                     `xml:"DeletionProtection,omitempty"`
	OptimizedWrites                  bool                     `xml:"OptimizedWritesEnabled,omitempty"`
}

type xmlDBClusterList struct {
	Members []xmlDBCluster `xml:"DBCluster"`
}

type createDBClusterResponse struct {
	XMLName   xml.Name     `xml:"CreateDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"CreateDBClusterResult>DBCluster"`
}

type describeDBClustersResponse struct {
	XMLName    xml.Name         `xml:"DescribeDBClustersResponse"`
	Xmlns      string           `xml:"xmlns,attr"`
	Marker     string           `xml:"DescribeDBClustersResult>Marker,omitempty"`
	DBClusters xmlDBClusterList `xml:"DescribeDBClustersResult>DBClusters"`
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

type createDBClusterParameterGroupResponse struct {
	XMLName          xml.Name                   `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	DBParameterGroup xmlDBClusterParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type describeDBClusterParameterGroupsResult struct {
	DBClusterParameterGroups xmlDBClusterParameterGroupList `xml:"DBClusterParameterGroups"`
}

type describeDBClusterParameterGroupsResponse struct {
	XMLName xml.Name                               `xml:"DescribeDBClusterParameterGroupsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  describeDBClusterParameterGroupsResult `xml:"DescribeDBClusterParameterGroupsResult"`
}

// ---- Cluster Snapshot XML types ----

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
	Status                      string `xml:"Status"`
	SnapshotCreateTime          string `xml:"SnapshotCreateTime,omitempty"`
	PercentProgress             int    `xml:"PercentProgress,omitempty"`
	StorageEncrypted            bool   `xml:"StorageEncrypted,omitempty"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots"`
}

// ---- Read Replica XML types ----

type createDBInstanceReadReplicaResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceReadReplicaResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceReadReplicaResult>DBInstance"`
}

type promoteReadReplicaResponse struct {
	XMLName    xml.Name      `xml:"PromoteReadReplicaResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"PromoteReadReplicaResult>DBInstance"`
}

// ---- Misc XML types ----

type rebootDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RebootDBInstanceResult>DBInstance"`
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
	MultiAZCapable  bool   `xml:"MultiAZCapable"`
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

type xmlDBLogFile struct {
	LogFileName string `xml:"LogFileName"`
	Size        int64  `xml:"Size"`
}

type xmlDBLogFileList struct {
	Members []xmlDBLogFile `xml:"DescribeDBLogFilesDetails"`
}

type describeDBLogFilesResponse struct {
	XMLName            xml.Name         `xml:"DescribeDBLogFilesResponse"`
	Xmlns              string           `xml:"xmlns,attr"`
	DescribeDBLogFiles xmlDBLogFileList `xml:"DescribeDBLogFilesResult>DescribeDBLogFiles"`
}

type downloadDBLogFilePortionResponse struct {
	XMLName               xml.Name `xml:"DownloadDBLogFilePortionResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	LogFileData           string   `xml:"DownloadDBLogFilePortionResult>LogFileData"`
	Marker                string   `xml:"DownloadDBLogFilePortionResult>Marker,omitempty"`
	AdditionalDataPending bool     `xml:"DownloadDBLogFilePortionResult>AdditionalDataPending"`
}

type xmlGlobalClusterMember struct {
	DBClusterArn          string `xml:"DBClusterArn"`
	GlobalWriteForwarding bool   `xml:"GlobalWriteForwarding,omitempty"`
	IsWriter              bool   `xml:"IsWriter"`
}

type xmlGlobalClusterMemberList struct {
	Members []xmlGlobalClusterMember `xml:"GlobalClusterMember"`
}

type xmlGlobalCluster struct {
	GlobalClusterMembers    *xmlGlobalClusterMemberList `xml:"GlobalClusterMembers,omitempty"`
	GlobalClusterIdentifier string                      `xml:"GlobalClusterIdentifier"`
	Engine                  string                      `xml:"Engine,omitempty"`
	EngineVersion           string                      `xml:"EngineVersion,omitempty"`
	Status                  string                      `xml:"Status,omitempty"`
	PrimaryRegion           string                      `xml:"PrimaryRegion,omitempty"`
	StorageEncrypted        bool                        `xml:"StorageEncrypted,omitempty"`
	DeletionProtection      bool                        `xml:"DeletionProtection,omitempty"`
}

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalCluster"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
}

type createGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"CreateGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"CreateGlobalClusterResult>GlobalCluster"`
}

type deleteGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"DeleteGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"DeleteGlobalClusterResult>GlobalCluster"`
}

type modifyGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"ModifyGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"ModifyGlobalClusterResult>GlobalCluster"`
}

// ---- Cluster start/stop XML types ----

type startDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StartDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StartDBClusterResult>DBCluster"`
}

type stopDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StopDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StopDBClusterResult>DBCluster"`
}

// ---- Cluster snapshot management XML types ----

type deleteDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"DeleteDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
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

type copyDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CopyDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
}

// ---- Cluster endpoint XML types ----

// xmlDBClusterEndpointFields contains the individual fields of a cluster endpoint.
// It is used both standalone (for Create/Delete, whose results inline all fields) and
// inside the DescribeDBClusterEndpoints list (as DBClusterEndpointList items).
type xmlDBClusterEndpointFields struct {
	DBClusterEndpointIdentifier string `xml:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	EndpointType                string `xml:"EndpointType"`
	Status                      string `xml:"Status"`
	Endpoint                    string `xml:"Endpoint,omitempty"`
}

type xmlDBClusterEndpointList struct {
	Members []xmlDBClusterEndpointFields `xml:"DBClusterEndpointList"`
}

// createDBClusterEndpointResult wraps fields directly inside CreateDBClusterEndpointResult.
// The SDK deserializes these fields directly (no inner DBClusterEndpoint element).
type createDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type createDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"CreateDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  createDBClusterEndpointResult `xml:"CreateDBClusterEndpointResult"`
}

type describeDBClusterEndpointsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeDBClusterEndpointsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	DBClusterEndpoints xmlDBClusterEndpointList `xml:"DescribeDBClusterEndpointsResult>DBClusterEndpoints"`
}

// deleteDBClusterEndpointResult wraps fields directly inside DeleteDBClusterEndpointResult.
type deleteDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type deleteDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"DeleteDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  deleteDBClusterEndpointResult `xml:"DeleteDBClusterEndpointResult"`
}

// ---- Valid DB instance modifications XML types ----

type xmlAvailableProcessorFeature struct {
	Name          string `xml:"Name"`
	DefaultValue  string `xml:"DefaultValue"`
	AllowedValues string `xml:"AllowedValues"`
}

type xmlAvailableProcessorFeatureList struct {
	Members []xmlAvailableProcessorFeature `xml:"AvailableProcessorFeature"`
}

type xmlValidDBInstanceModificationsMessage struct {
	ValidProcessorFeatures xmlAvailableProcessorFeatureList `xml:"ValidProcessorFeatures"`
}

type xmlValidDBInstanceModificationsWrapper struct {
	Message xmlValidDBInstanceModificationsMessage `xml:"ValidDBInstanceModificationsMessage"`
}

type describeValidDBInstanceModificationsResponse struct {
	XMLName xml.Name                               `xml:"DescribeValidDBInstanceModificationsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  xmlValidDBInstanceModificationsWrapper `xml:"DescribeValidDBInstanceModificationsResult"`
}

// ---- Export task XML types ----

type xmlExportTask struct {
	ExportTaskIdentifier string `xml:"ExportTaskIdentifier"`
	SourceArn            string `xml:"SourceArn"`
	Status               string `xml:"Status"`
	S3Bucket             string `xml:"S3Bucket,omitempty"`
}

type xmlExportTaskList struct {
	Members []xmlExportTask `xml:"ExportTask"`
}

// startExportTaskResult inlines export task fields directly inside StartExportTaskResult.
// The SDK deserializes fields directly (no nested ExportTask element).
type startExportTaskResult struct {
	xmlExportTask
}

type startExportTaskResponse struct {
	XMLName xml.Name              `xml:"StartExportTaskResponse"`
	Xmlns   string                `xml:"xmlns,attr"`
	Result  startExportTaskResult `xml:"StartExportTaskResult"`
}

type describeExportTasksResponse struct {
	XMLName     xml.Name          `xml:"DescribeExportTasksResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	ExportTasks xmlExportTaskList `xml:"DescribeExportTasksResult>ExportTasks"`
}

// cancelExportTaskResult inlines export task fields directly inside CancelExportTaskResult.
type cancelExportTaskResult struct {
	xmlExportTask
}

type cancelExportTaskResponse struct {
	XMLName xml.Name               `xml:"CancelExportTaskResponse"`
	Xmlns   string                 `xml:"xmlns,attr"`
	Result  cancelExportTaskResult `xml:"CancelExportTaskResult"`
}

// ---- New instance-level operations XML types ----

type restoreDBInstanceFromDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"RestoreDBInstanceFromDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RestoreDBInstanceFromDBSnapshotResult>DBInstance"`
}

type restoreDBInstanceToPointInTimeResponse struct {
	XMLName    xml.Name      `xml:"RestoreDBInstanceToPointInTimeResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RestoreDBInstanceToPointInTimeResult>DBInstance"`
}

type copyDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"CopyDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"CopyDBSnapshotResult>DBSnapshot"`
}

type startDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"StartDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"StartDBInstanceResult>DBInstance"`
}

type stopDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"StopDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"StopDBInstanceResult>DBInstance"`
}

// ---- New instance-level operation handlers ----

func (h *Handler) handleRestoreDBInstanceFromDBSnapshot(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	snapshotID := vals.Get("DBSnapshotIdentifier")
	opts := DBInstanceOptions{
		MultiAZ:            vals.Get("MultiAZ") == formTrue,
		DeletionProtection: vals.Get("DeletionProtection") == formTrue,
		StorageType:        vals.Get("StorageType"),
		AvailabilityZone:   vals.Get("AvailabilityZone"),
	}

	inst, err := h.Backend.RestoreDBInstanceFromDBSnapshot(id, snapshotID, opts)
	if err != nil {
		return nil, err
	}

	return &restoreDBInstanceFromDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleRestoreDBInstanceToPointInTime(vals url.Values) (any, error) {
	id := vals.Get("TargetDBInstanceIdentifier")
	sourceID := vals.Get("SourceDBInstanceIdentifier")
	opts := DBInstanceOptions{
		MultiAZ:            vals.Get("MultiAZ") == formTrue,
		DeletionProtection: vals.Get("DeletionProtection") == formTrue,
		StorageType:        vals.Get("StorageType"),
		AvailabilityZone:   vals.Get("AvailabilityZone"),
	}

	inst, err := h.Backend.RestoreDBInstanceToPointInTime(id, sourceID, opts)
	if err != nil {
		return nil, err
	}

	return &restoreDBInstanceToPointInTimeResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCopyDBSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBSnapshotIdentifier")

	opts := CopyDBSnapshotOptions{
		KmsKeyID:     vals.Get("KmsKeyId"),
		SourceRegion: vals.Get("SourceRegion"),
		CopyTags:     vals.Get("CopyTags") == formTrue,
	}

	snap, err := h.Backend.CopyDBSnapshot(sourceSnapshotID, targetSnapshotID, opts)
	if err != nil {
		return nil, err
	}

	return &copyDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleStartDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")

	inst, err := h.Backend.StartDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &startDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleStopDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")

	inst, err := h.Backend.StopDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &stopDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

// ---- Global cluster handlers ----

func (h *Handler) handleCreateGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	storageEncrypted := vals.Get("StorageEncrypted") == formTrue
	deletionProtection := vals.Get("DeletionProtection") == formTrue

	gc, err := h.Backend.CreateGlobalCluster(id, engine, engineVersion, storageEncrypted, deletionProtection)
	if err != nil {
		return nil, err
	}

	return &createGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")

	gc, err := h.Backend.DeleteGlobalCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleModifyGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	newID := vals.Get("NewGlobalClusterIdentifier")
	engineVersion := vals.Get("EngineVersion")

	var deletionProtection *bool
	if dp := vals.Get("DeletionProtection"); dp != "" {
		v := dp == formTrue
		deletionProtection = &v
	}

	gc, err := h.Backend.ModifyGlobalCluster(id, newID, engineVersion, deletionProtection)
	if err != nil {
		return nil, err
	}

	return &modifyGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	x := xmlGlobalCluster{
		GlobalClusterIdentifier: gc.GlobalClusterIdentifier,
		Engine:                  gc.Engine,
		EngineVersion:           gc.EngineVersion,
		Status:                  gc.Status,
		PrimaryRegion:           gc.PrimaryRegion,
		StorageEncrypted:        gc.StorageEncrypted,
		DeletionProtection:      gc.DeletionProtection,
	}

	if len(gc.GlobalClusterMembers) > 0 {
		members := make([]xmlGlobalClusterMember, 0, len(gc.GlobalClusterMembers))
		for _, m := range gc.GlobalClusterMembers {
			members = append(members, xmlGlobalClusterMember(m))
		}

		x.GlobalClusterMembers = &xmlGlobalClusterMemberList{Members: members}
	}

	return x
}

// ---- New operations: handlers ----

func (h *Handler) handleAddRoleToDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.AddRoleToDBCluster(clusterID, roleARN); err != nil {
		return nil, err
	}

	return &addRoleToDBClusterResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleAddRoleToDBInstance(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.AddRoleToDBInstance(instanceID, roleARN); err != nil {
		return nil, err
	}

	return &addRoleToDBInstanceResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleAddSourceIdentifierToSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceIdentifier := vals.Get("SourceIdentifier")

	sub, err := h.Backend.AddSourceIdentifierToSubscription(subscriptionName, sourceIdentifier)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleApplyPendingMaintenanceAction(vals url.Values) (any, error) {
	resourceID := vals.Get("ResourceIdentifier")
	applyAction := vals.Get("ApplyAction")

	if _, err := h.Backend.ApplyPendingMaintenanceAction(resourceID, applyAction); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: rdsXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: xmlResourcePendingMaintenanceActions{
				ResourceIdentifier:              resourceID,
				PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
			},
		},
	}, nil
}

func (h *Handler) handleAuthorizeDBSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("DBSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")

	sg, err := h.Backend.AuthorizeDBSecurityGroupIngress(groupName, cidrIP)
	if err != nil {
		return nil, err
	}

	return &authorizeDBSecurityGroupIngressResponse{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
	}, nil
}

func (h *Handler) handleBacktrackDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	backtrackTo := vals.Get("BacktrackTo")

	bt, err := h.Backend.BacktrackDBCluster(clusterID, backtrackTo)
	if err != nil {
		return nil, err
	}

	return &backtrackDBClusterResponse{
		Xmlns:              rdsXMLNS,
		DBClusterBacktrack: toXMLDBClusterBacktrack(bt),
	}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetGroupName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")

	pg, err := h.Backend.CopyDBClusterParameterGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLClusterParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCopyDBParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBParameterGroupIdentifier")
	targetGroupName := vals.Get("TargetDBParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBParameterGroupDescription")

	pg, err := h.Backend.CopyDBParameterGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCopyOptionGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceOptionGroupIdentifier")
	targetGroupName := vals.Get("TargetOptionGroupIdentifier")
	targetDescription := vals.Get("TargetOptionGroupDescription")

	og, err := h.Backend.CopyOptionGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleCreateBlueGreenDeployment(vals url.Values) (any, error) {
	name := vals.Get("BlueGreenDeploymentName")
	source := vals.Get("Source")

	deployment, err := h.Backend.CreateBlueGreenDeployment(name, source)
	if err != nil {
		return nil, err
	}

	return &createBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

// ---- helper converters ----

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]string, len(sub.SourceIDs))
	copy(ids, sub.SourceIDs)
	cats := make([]string, len(sub.EventCategories))
	copy(cats, sub.EventCategories)

	return xmlEventSubscription{
		CustSubscriptionID:  sub.SubscriptionName,
		SnsTopicArn:         sub.SnsTopicArn,
		Status:              sub.Status,
		SourceType:          sub.SourceType,
		Enabled:             sub.Enabled,
		SourceIDsList:       xmlSourceIDList{Members: ids},
		EventCategoriesList: xmlEventCategoryList{Members: cats},
	}
}

func toXMLDBSecurityGroup(sg *DBSecurityGroup) xmlDBSecurityGroup {
	ipRanges := make([]xmlIPRange, 0, len(sg.IPRanges))
	for _, r := range sg.IPRanges {
		ipRanges = append(ipRanges, xmlIPRange(r))
	}

	return xmlDBSecurityGroup{
		DBSecurityGroupName:        sg.DBSecurityGroupName,
		DBSecurityGroupDescription: sg.DBSecurityGroupDescription,
		IPRanges:                   xmlIPRangeList{Members: ipRanges},
	}
}

func toXMLDBClusterBacktrack(bt *DBClusterBacktrack) xmlDBClusterBacktrack {
	return xmlDBClusterBacktrack{
		DBClusterIdentifier: bt.DBClusterIdentifier,
		BacktrackIdentifier: bt.BacktrackIdentifier,
		BacktrackTo:         bt.BacktrackTo,
		Status:              bt.Status,
	}
}

func toXMLBlueGreenDeployment(d *BlueGreenDeployment) xmlBlueGreenDeployment {
	return xmlBlueGreenDeployment{
		BlueGreenDeploymentIdentifier: d.BlueGreenDeploymentIdentifier,
		BlueGreenDeploymentName:       d.BlueGreenDeploymentName,
		Target:                        d.Target,
		Source:                        d.Source,
		Status:                        d.Status,
	}
}

// ---- New operations: XML response types ----

type addRoleToDBClusterResponse struct {
	XMLName xml.Name `xml:"AddRoleToDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type addRoleToDBInstanceResponse struct {
	XMLName xml.Name `xml:"AddRoleToDBInstanceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlSourceIDList struct {
	Members []string `xml:"member"`
}

type xmlEventCategoryList struct {
	Members []string `xml:"EventCategory"`
}

type xmlEventSubscription struct {
	CustSubscriptionID  string               `xml:"CustSubscriptionId"`
	SnsTopicArn         string               `xml:"SnsTopicArn,omitempty"`
	Status              string               `xml:"Status"`
	SourceType          string               `xml:"SourceType,omitempty"`
	SourceIDsList       xmlSourceIDList      `xml:"SourceIdsList"`
	EventCategoriesList xmlEventCategoryList `xml:"EventCategoriesList,omitempty"`
	Enabled             bool                 `xml:"Enabled,omitempty"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	Description string `xml:"Description,omitempty"`
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

type xmlIPRange struct {
	CIDRIP string `xml:"CIDRIP"`
	Status string `xml:"Status"`
}

type xmlIPRangeList struct {
	Members []xmlIPRange `xml:"IPRange"`
}

type xmlDBSecurityGroup struct {
	DBSecurityGroupName        string         `xml:"DBSecurityGroupName"`
	DBSecurityGroupDescription string         `xml:"DBSecurityGroupDescription,omitempty"`
	IPRanges                   xmlIPRangeList `xml:"IPRanges"`
}

type authorizeDBSecurityGroupIngressResponse struct {
	XMLName         xml.Name           `xml:"AuthorizeDBSecurityGroupIngressResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"AuthorizeDBSecurityGroupIngressResult>DBSecurityGroup"`
}

type xmlDBClusterBacktrack struct {
	DBClusterIdentifier string `xml:"DBClusterIdentifier"`
	BacktrackIdentifier string `xml:"BacktrackIdentifier"`
	BacktrackTo         string `xml:"BacktrackTo,omitempty"`
	Status              string `xml:"Status"`
}

type backtrackDBClusterResponse struct {
	XMLName            xml.Name              `xml:"BacktrackDBClusterResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	DBClusterBacktrack xmlDBClusterBacktrack `xml:"BacktrackDBClusterResult>DBClusterBacktrack"`
}

type copyDBClusterParameterGroupResponse struct {
	XMLName          xml.Name                   `xml:"CopyDBClusterParameterGroupResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	DBParameterGroup xmlDBClusterParameterGroup `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type copyDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CopyDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CopyDBParameterGroupResult>DBParameterGroup"`
}

type copyOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"CopyOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"CopyOptionGroupResult>OptionGroup"`
}

type xmlBlueGreenDeployment struct {
	BlueGreenDeploymentIdentifier string `xml:"BlueGreenDeploymentIdentifier"`
	BlueGreenDeploymentName       string `xml:"BlueGreenDeploymentName"`
	Source                        string `xml:"Source,omitempty"`
	Target                        string `xml:"Target,omitempty"`
	Status                        string `xml:"Status"`
}

type createBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"CreateBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"CreateBlueGreenDeploymentResult>BlueGreenDeployment"`
}

func (h *Handler) handleRemoveRoleFromDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.RemoveRoleFromDBCluster(clusterID, roleARN); err != nil {
		return nil, err
	}

	return &removeRoleFromDBClusterResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRemoveRoleFromDBInstance(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	roleARN := vals.Get("RoleArn")

	if err := h.Backend.RemoveRoleFromDBInstance(instanceID, roleARN); err != nil {
		return nil, err
	}

	return &removeRoleFromDBInstanceResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRemoveSourceIdentifierFromSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceIdentifier := vals.Get("SourceIdentifier")

	sub, err := h.Backend.RemoveSourceIdentifierFromSubscription(subscriptionName, sourceIdentifier)
	if err != nil {
		return nil, err
	}

	return &removeSourceIdentifierFromSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

type removeRoleFromDBClusterResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeRoleFromDBInstanceResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromDBInstanceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeSourceIdentifierFromSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"RemoveSourceIdentifierFromSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"RemoveSourceIdentifierFromSubscriptionResult>EventSubscription"`
}
