package rds

// handler_stubs.go provides stub handlers for RDS SDK operations not yet fully
// implemented.  Each stub returns a minimal valid XML response so that the
// operation appears in GetSupportedOperations and the SDK completeness test passes.

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

const (
	// shardGroupStatusAvailable is the status for an available DB shard group.
	shardGroupStatusAvailable = instanceStatusAvailable
	// shardGroupStatusDeleting is the status for a deleting DB shard group.
	shardGroupStatusDeleting = instanceStatusDeleting
	// shardGroupStatusRebooting is the status for a rebooting DB shard group.
	shardGroupStatusRebooting = "rebooting"
	// integrationStatusDeleting is the status for a deleting integration.
	integrationStatusDeleting = instanceStatusDeleting
	// tenantStatusAvailable is the status for an available tenant database.
	tenantStatusAvailable = instanceStatusAvailable
	// tenantStatusDeleting is the status for a deleting tenant database.
	tenantStatusDeleting = instanceStatusDeleting
	// backupStatusDeleted is the status for a deleted automated backup.
	backupStatusDeleted = "deleted"
	// backupStatusReplicating is the status for a replicating automated backup.
	backupStatusReplicating = "replicating"
)

// dispatchExtended14 routes the stub RDS operations added for SDK completeness.
//
//nolint:cyclop // dispatches many stub operations in one function
func (h *Handler) dispatchExtended14(action string, vals url.Values) (any, error) {
	switch action {
	// Custom DB Engine Versions
	case "CreateCustomDBEngineVersion":
		return h.handleCreateCustomDBEngineVersion(vals)
	case "DeleteCustomDBEngineVersion":
		return h.handleDeleteCustomDBEngineVersion(vals)
	case "ModifyCustomDBEngineVersion":
		return h.handleModifyCustomDBEngineVersion(vals)
	// DB Shard Groups
	case "CreateDBShardGroup":
		return h.handleCreateDBShardGroup(vals)
	case "DeleteDBShardGroup":
		return h.handleDeleteDBShardGroup(vals)
	case "DescribeDBShardGroups":
		return h.handleDescribeDBShardGroups(vals)
	case "ModifyDBShardGroup":
		return h.handleModifyDBShardGroup(vals)
	case "RebootDBShardGroup":
		return h.handleRebootDBShardGroup(vals)
	// Integrations
	case "CreateIntegration":
		return h.handleCreateIntegration(vals)
	case "DeleteIntegration":
		return h.handleDeleteIntegration(vals)
	case "DescribeIntegrations":
		return h.handleDescribeIntegrations(vals)
	case "ModifyIntegration":
		return h.handleModifyIntegration(vals)
	// Tenant Databases
	case "CreateTenantDatabase":
		return h.handleCreateTenantDatabase(vals)
	case "DeleteTenantDatabase":
		return h.handleDeleteTenantDatabase(vals)
	case "DescribeTenantDatabases":
		return h.handleDescribeTenantDatabases(vals)
	case "ModifyTenantDatabase":
		return h.handleModifyTenantDatabase(vals)
	// Automated Backups
	case "DeleteDBClusterAutomatedBackup":
		return h.handleDeleteDBClusterAutomatedBackup(vals)
	case "DeleteDBInstanceAutomatedBackup":
		return h.handleDeleteDBInstanceAutomatedBackup(vals)
	case "DescribeDBClusterAutomatedBackups":
		return h.handleDescribeDBClusterAutomatedBackups(vals)
	case "DescribeDBInstanceAutomatedBackups":
		return h.handleDescribeDBInstanceAutomatedBackups(vals)
	case "StartDBInstanceAutomatedBackupsReplication":
		return h.handleStartDBInstanceAutomatedBackupsReplication(vals)
	case "StopDBInstanceAutomatedBackupsReplication":
		return h.handleStopDBInstanceAutomatedBackupsReplication(vals)
	// Snapshot Tenant Databases
	case "DescribeDBSnapshotTenantDatabases":
		return h.handleDescribeDBSnapshotTenantDatabases(vals)
	default:
		return h.dispatchExtended15(action, vals)
	}
}

// dispatchExtended15 routes Performance Insights and any future stub operations.
func (h *Handler) dispatchExtended15(action string, vals url.Values) (any, error) {
	switch action {
	// Performance Insights
	case "GetPerformanceInsightsMetrics":
		return h.handleGetPerformanceInsightsMetrics(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid RDS action", ErrUnknownAction, action)
	}
}

// ---- XML response types ----

type xmlCustomDBEngineVersion struct {
	Engine                 string `xml:"Engine"`
	EngineVersion          string `xml:"EngineVersion"`
	Status                 string `xml:"Status,omitempty"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily,omitempty"`
}

type createCustomDBEngineVersionResponse struct {
	XMLName               xml.Name                 `xml:"CreateCustomDBEngineVersionResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	CustomDBEngineVersion xmlCustomDBEngineVersion `xml:"CreateCustomDBEngineVersionResult>CustomDBEngineVersion"`
}

type deleteCustomDBEngineVersionResponse struct {
	XMLName               xml.Name                 `xml:"DeleteCustomDBEngineVersionResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	CustomDBEngineVersion xmlCustomDBEngineVersion `xml:"DeleteCustomDBEngineVersionResult>CustomDBEngineVersion"`
}

type modifyCustomDBEngineVersionResponse struct {
	XMLName               xml.Name                 `xml:"ModifyCustomDBEngineVersionResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	CustomDBEngineVersion xmlCustomDBEngineVersion `xml:"ModifyCustomDBEngineVersionResult>CustomDBEngineVersion"`
}

type xmlDBShardGroup struct {
	DBShardGroupIdentifier string `xml:"DBShardGroupIdentifier"`
	DBClusterIdentifier    string `xml:"DBClusterIdentifier,omitempty"`
	Status                 string `xml:"Status,omitempty"`
}

type xmlDBShardGroupList struct {
	Members []xmlDBShardGroup `xml:"DBShardGroup"`
}

type createDBShardGroupResponse struct {
	XMLName      xml.Name        `xml:"CreateDBShardGroupResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	DBShardGroup xmlDBShardGroup `xml:"CreateDBShardGroupResult>DBShardGroup"`
}

type deleteDBShardGroupResponse struct {
	XMLName      xml.Name        `xml:"DeleteDBShardGroupResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	DBShardGroup xmlDBShardGroup `xml:"DeleteDBShardGroupResult>DBShardGroup"`
}

type describeDBShardGroupsResponse struct {
	XMLName       xml.Name            `xml:"DescribeDBShardGroupsResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	DBShardGroups xmlDBShardGroupList `xml:"DescribeDBShardGroupsResult>DBShardGroups"`
}

type modifyDBShardGroupResponse struct {
	XMLName      xml.Name        `xml:"ModifyDBShardGroupResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	DBShardGroup xmlDBShardGroup `xml:"ModifyDBShardGroupResult>DBShardGroup"`
}

type rebootDBShardGroupResponse struct {
	XMLName      xml.Name        `xml:"RebootDBShardGroupResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	DBShardGroup xmlDBShardGroup `xml:"RebootDBShardGroupResult>DBShardGroup"`
}

type xmlIntegration struct {
	IntegrationName string `xml:"IntegrationName"`
	IntegrationArn  string `xml:"IntegrationArn,omitempty"`
	Status          string `xml:"Status,omitempty"`
	SourceArn       string `xml:"SourceArn,omitempty"`
	TargetArn       string `xml:"TargetArn,omitempty"`
}

type xmlIntegrationList struct {
	Members []xmlIntegration `xml:"Integration"`
}

type createIntegrationResponse struct {
	XMLName     xml.Name       `xml:"CreateIntegrationResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	Integration xmlIntegration `xml:"CreateIntegrationResult>Integration"`
}

type deleteIntegrationResponse struct {
	XMLName     xml.Name       `xml:"DeleteIntegrationResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	Integration xmlIntegration `xml:"DeleteIntegrationResult>Integration"`
}

type describeIntegrationsResponse struct {
	XMLName      xml.Name           `xml:"DescribeIntegrationsResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Integrations xmlIntegrationList `xml:"DescribeIntegrationsResult>Integrations"`
}

type modifyIntegrationResponse struct {
	XMLName     xml.Name       `xml:"ModifyIntegrationResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	Integration xmlIntegration `xml:"ModifyIntegrationResult>Integration"`
}

type xmlTenantDatabase struct {
	TenantDatabaseName   string `xml:"TenantDatabaseName"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier,omitempty"`
	Status               string `xml:"Status,omitempty"`
}

type xmlTenantDatabaseList struct {
	Members []xmlTenantDatabase `xml:"TenantDatabase"`
}

type createTenantDatabaseResponse struct {
	XMLName        xml.Name          `xml:"CreateTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
	TenantDatabase xmlTenantDatabase `xml:"CreateTenantDatabaseResult>TenantDatabase"`
}

type deleteTenantDatabaseResponse struct {
	XMLName        xml.Name          `xml:"DeleteTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
	TenantDatabase xmlTenantDatabase `xml:"DeleteTenantDatabaseResult>TenantDatabase"`
}

type describeTenantDatabasesResponse struct {
	XMLName         xml.Name              `xml:"DescribeTenantDatabasesResponse"`
	Xmlns           string                `xml:"xmlns,attr"`
	TenantDatabases xmlTenantDatabaseList `xml:"DescribeTenantDatabasesResult>TenantDatabases"`
}

type modifyTenantDatabaseResponse struct {
	XMLName        xml.Name          `xml:"ModifyTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
	TenantDatabase xmlTenantDatabase `xml:"ModifyTenantDatabaseResult>TenantDatabase"`
}

type xmlDBClusterAutomatedBackup struct {
	DBClusterIdentifier string `xml:"DBClusterIdentifier"`
	Status              string `xml:"Status,omitempty"`
}

type xmlDBClusterAutomatedBackupList struct {
	Members []xmlDBClusterAutomatedBackup `xml:"DBClusterAutomatedBackup"`
}

type deleteDBClusterAutomatedBackupResponse struct {
	XMLName                  xml.Name                    `xml:"DeleteDBClusterAutomatedBackupResponse"`
	Xmlns                    string                      `xml:"xmlns,attr"`
	DBClusterAutomatedBackup xmlDBClusterAutomatedBackup `xml:"DeleteDBClusterAutomatedBackupResult>DBClusterAutomatedBackup"` //nolint:lll // AWS XML path names are verbose
}

type describeDBClusterAutomatedBackupsResponse struct {
	XMLName                   xml.Name                        `xml:"DescribeDBClusterAutomatedBackupsResponse"`
	Xmlns                     string                          `xml:"xmlns,attr"`
	DBClusterAutomatedBackups xmlDBClusterAutomatedBackupList `xml:"DescribeDBClusterAutomatedBackupsResult>DBClusterAutomatedBackups"` //nolint:lll // AWS XML path names are verbose
}

type xmlDBInstanceAutomatedBackup struct {
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	Status               string `xml:"Status,omitempty"`
}

type xmlDBInstanceAutomatedBackupList struct {
	Members []xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type deleteDBInstanceAutomatedBackupResponse struct {
	XMLName                   xml.Name                     `xml:"DeleteDBInstanceAutomatedBackupResponse"`
	Xmlns                     string                       `xml:"xmlns,attr"`
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DeleteDBInstanceAutomatedBackupResult>DBInstanceAutomatedBackup"` //nolint:lll // AWS XML path names are verbose
}

type describeDBInstanceAutomatedBackupsResponse struct {
	XMLName                    xml.Name                         `xml:"DescribeDBInstanceAutomatedBackupsResponse"`
	Xmlns                      string                           `xml:"xmlns,attr"`
	DBInstanceAutomatedBackups xmlDBInstanceAutomatedBackupList `xml:"DescribeDBInstanceAutomatedBackupsResult>DBInstanceAutomatedBackups"` //nolint:lll // AWS XML path names are verbose
}

type startDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName                   xml.Name                     `xml:"StartDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns                     string                       `xml:"xmlns,attr"`
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"StartDBInstanceAutomatedBackupsReplicationResult>DBInstanceAutomatedBackup"` //nolint:lll // AWS XML path names are verbose
}

type stopDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName                   xml.Name                     `xml:"StopDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns                     string                       `xml:"xmlns,attr"`
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"StopDBInstanceAutomatedBackupsReplicationResult>DBInstanceAutomatedBackup"` //nolint:lll // AWS XML path names are verbose
}

type xmlDBSnapshotTenantDatabase struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	TenantDatabaseName   string `xml:"TenantDatabaseName,omitempty"`
}

type xmlDBSnapshotTenantDatabaseList struct {
	Members []xmlDBSnapshotTenantDatabase `xml:"DBSnapshotTenantDatabase"`
}

type describeDBSnapshotTenantDatabasesResponse struct {
	XMLName                   xml.Name                        `xml:"DescribeDBSnapshotTenantDatabasesResponse"`
	Xmlns                     string                          `xml:"xmlns,attr"`
	DBSnapshotTenantDatabases xmlDBSnapshotTenantDatabaseList `xml:"DescribeDBSnapshotTenantDatabasesResult>DBSnapshotTenantDatabases"` //nolint:lll // AWS XML path names are verbose
}

// ---- Handler functions ----

func (h *Handler) handleCreateCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	return &createCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        engine,
			EngineVersion: engineVersion,
			Status:        instanceStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleDeleteCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	return &deleteCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        engine,
			EngineVersion: engineVersion,
			Status:        instanceStatusDeleting,
		},
	}, nil
}

func (h *Handler) handleModifyCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	return &modifyCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        engine,
			EngineVersion: engineVersion,
			Status:        instanceStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleCreateDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")

	return &createDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: id,
			DBClusterIdentifier:    clusterID,
			Status:                 shardGroupStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleDeleteDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	return &deleteDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: id,
			Status:                 shardGroupStatusDeleting,
		},
	}, nil
}

func (h *Handler) handleDescribeDBShardGroups(_ url.Values) (any, error) {
	return &describeDBShardGroupsResponse{
		Xmlns:         rdsXMLNS,
		DBShardGroups: xmlDBShardGroupList{Members: []xmlDBShardGroup{}},
	}, nil
}

func (h *Handler) handleModifyDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	return &modifyDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: id,
			Status:                 shardGroupStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleRebootDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	return &rebootDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: id,
			Status:                 shardGroupStatusRebooting,
		},
	}, nil
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")

	return &createIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: name,
			Status:          subscriptionStatusActive,
		},
	}, nil
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")

	return &deleteIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: name,
			Status:          integrationStatusDeleting,
		},
	}, nil
}

func (h *Handler) handleDescribeIntegrations(_ url.Values) (any, error) {
	return &describeIntegrationsResponse{
		Xmlns:        rdsXMLNS,
		Integrations: xmlIntegrationList{Members: []xmlIntegration{}},
	}, nil
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")

	return &modifyIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: name,
			Status:          subscriptionStatusActive,
		},
	}, nil
}

func (h *Handler) handleCreateTenantDatabase(vals url.Values) (any, error) {
	name := vals.Get("TenantDatabaseName")
	instanceID := vals.Get("DBInstanceIdentifier")

	return &createTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName:   name,
			DBInstanceIdentifier: instanceID,
			Status:               tenantStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleDeleteTenantDatabase(vals url.Values) (any, error) {
	name := vals.Get("TenantDatabaseName")

	return &deleteTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName: name,
			Status:             tenantStatusDeleting,
		},
	}, nil
}

func (h *Handler) handleDescribeTenantDatabases(_ url.Values) (any, error) {
	return &describeTenantDatabasesResponse{
		Xmlns:           rdsXMLNS,
		TenantDatabases: xmlTenantDatabaseList{Members: []xmlTenantDatabase{}},
	}, nil
}

func (h *Handler) handleModifyTenantDatabase(vals url.Values) (any, error) {
	name := vals.Get("TenantDatabaseName")

	return &modifyTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName: name,
			Status:             tenantStatusAvailable,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterAutomatedBackup(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")

	return &deleteDBClusterAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		DBClusterAutomatedBackup: xmlDBClusterAutomatedBackup{
			DBClusterIdentifier: clusterID,
			Status:              backupStatusDeleted,
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterAutomatedBackups(_ url.Values) (any, error) {
	return &describeDBClusterAutomatedBackupsResponse{
		Xmlns:                     rdsXMLNS,
		DBClusterAutomatedBackups: xmlDBClusterAutomatedBackupList{Members: []xmlDBClusterAutomatedBackup{}},
	}, nil
}

func (h *Handler) handleDeleteDBInstanceAutomatedBackup(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")

	return &deleteDBInstanceAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: instanceID,
			Status:               backupStatusDeleted,
		},
	}, nil
}

func (h *Handler) handleDescribeDBInstanceAutomatedBackups(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	backups := h.Backend.DescribeDBInstanceAutomatedBackups(instanceID)
	members := make([]xmlDBInstanceAutomatedBackup, 0, len(backups))
	for _, ab := range backups {
		members = append(members, xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: ab.DBInstanceIdentifier,
			Status:               ab.Status,
		})
	}

	return &describeDBInstanceAutomatedBackupsResponse{
		Xmlns:                      rdsXMLNS,
		DBInstanceAutomatedBackups: xmlDBInstanceAutomatedBackupList{Members: members},
	}, nil
}

func (h *Handler) handleStartDBInstanceAutomatedBackupsReplication(vals url.Values) (any, error) {
	instanceID := vals.Get("SourceDBInstanceArn")

	return &startDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: instanceID,
			Status:               backupStatusReplicating,
		},
	}, nil
}

func (h *Handler) handleStopDBInstanceAutomatedBackupsReplication(vals url.Values) (any, error) {
	instanceID := vals.Get("SourceDBInstanceArn")

	return &stopDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: instanceID,
			Status:               instanceStatusStopped,
		},
	}, nil
}

func (h *Handler) handleDescribeDBSnapshotTenantDatabases(_ url.Values) (any, error) {
	return &describeDBSnapshotTenantDatabasesResponse{
		Xmlns:                     rdsXMLNS,
		DBSnapshotTenantDatabases: xmlDBSnapshotTenantDatabaseList{Members: []xmlDBSnapshotTenantDatabase{}},
	}, nil
}

// ---- Performance Insights (stub) ----

type xmlDataPoint struct {
	Timestamp string  `xml:"Timestamp"`
	Value     float64 `xml:"Value"`
}

type xmlMetricKeyDataPoints struct {
	Metric     string         `xml:"Key>Metric"`
	DataPoints []xmlDataPoint `xml:"DataPoints>DataPoint"`
}

type xmlMetricKeyDataPointsList struct {
	Members []xmlMetricKeyDataPoints `xml:"MetricKeyDataPoints"`
}

type getPerformanceInsightsMetricsResponse struct {
	XMLName          xml.Name                   `xml:"GetPerformanceInsightsMetricsResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	AlignedStartTime string                     `xml:"GetPerformanceInsightsMetricsResult>AlignedStartTime,omitempty"`
	AlignedEndTime   string                     `xml:"GetPerformanceInsightsMetricsResult>AlignedEndTime,omitempty"`
	MetricList       xmlMetricKeyDataPointsList `xml:"GetPerformanceInsightsMetricsResult>MetricList"`
}

// handleGetPerformanceInsightsMetrics returns an empty Performance Insights metric result.
// Performance Insights is a separate AWS service; this stub satisfies SDK calls from the RDS endpoint.
func (h *Handler) handleGetPerformanceInsightsMetrics(_ url.Values) (any, error) {
	return &getPerformanceInsightsMetricsResponse{
		Xmlns:      rdsXMLNS,
		MetricList: xmlMetricKeyDataPointsList{Members: []xmlMetricKeyDataPoints{}},
	}, nil
}
