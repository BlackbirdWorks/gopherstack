package rds

// handler_stubs.go provides stub handlers for RDS SDK operations not yet fully
// implemented.  Each stub returns a minimal valid XML response so that the
// operation appears in GetSupportedOperations and the SDK completeness test passes.

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strconv"
)

// parseFloat parses a string as float64, returning 0 on error.
func parseFloat(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}

	return v
}

// parseInt parses a string as int, returning 0 on error.
func parseInt(s string) int {
	if s == "" {
		return 0
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}

	return v
}

// errRDSStubNotHandled is a sentinel used internally to signal that a dispatch
// helper did not recognise the action. It is never returned to callers.
var errRDSStubNotHandled = errors.New("rds: action not handled by this dispatcher")

// dispatchExtended14 routes the stub RDS operations added for SDK completeness.
func (h *Handler) dispatchExtended14(action string, vals url.Values) (any, error) {
	if r, err := h.dispatchEngineShardOps(action, vals); !errors.Is(err, errRDSStubNotHandled) {
		return r, err
	}

	if r, err := h.dispatchIntegrationTenantOps(action, vals); !errors.Is(err, errRDSStubNotHandled) {
		return r, err
	}

	if r, err := h.dispatchAutomatedBackupOps(action, vals); !errors.Is(err, errRDSStubNotHandled) {
		return r, err
	}

	return nil, fmt.Errorf("%w: %s is not a valid RDS action", ErrUnknownAction, action)
}

// dispatchEngineShardOps handles custom engine version and shard group operations.
func (h *Handler) dispatchEngineShardOps(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateCustomDBEngineVersion":

		return h.handleCreateCustomDBEngineVersion(vals)
	case "DeleteCustomDBEngineVersion":

		return h.handleDeleteCustomDBEngineVersion(vals)
	case "ModifyCustomDBEngineVersion":

		return h.handleModifyCustomDBEngineVersion(vals)
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
	}

	return nil, errRDSStubNotHandled
}

// dispatchIntegrationTenantOps handles integration and tenant database operations.
func (h *Handler) dispatchIntegrationTenantOps(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateIntegration":

		return h.handleCreateIntegration(vals)
	case "DeleteIntegration":

		return h.handleDeleteIntegration(vals)
	case "DescribeIntegrations":

		return h.handleDescribeIntegrations(vals)
	case "ModifyIntegration":

		return h.handleModifyIntegration(vals)
	case "CreateTenantDatabase":

		return h.handleCreateTenantDatabase(vals)
	case "DeleteTenantDatabase":

		return h.handleDeleteTenantDatabase(vals)
	case "DescribeTenantDatabases":

		return h.handleDescribeTenantDatabases(vals)
	case "ModifyTenantDatabase":

		return h.handleModifyTenantDatabase(vals)
	}

	return nil, errRDSStubNotHandled
}

// dispatchAutomatedBackupOps handles automated backup and snapshot tenant database operations.
func (h *Handler) dispatchAutomatedBackupOps(action string, vals url.Values) (any, error) {
	switch action {
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
	Description            string `xml:"DatabaseInstallationFilesS3BucketName,omitempty"`
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
	description := vals.Get("Description")

	cev, err := h.Backend.CreateCustomDBEngineVersion(engine, engineVersion, description)
	if err != nil {
		return nil, err
	}

	return &createCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        cev.Engine,
			EngineVersion: cev.EngineVersion,
			Status:        cev.Status,
			Description:   cev.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	cev, err := h.Backend.DeleteCustomDBEngineVersion(engine, engineVersion)
	if err != nil {
		return nil, err
	}

	return &deleteCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        cev.Engine,
			EngineVersion: cev.EngineVersion,
			Status:        cev.Status,
		},
	}, nil
}

func (h *Handler) handleModifyCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	description := vals.Get("Description")
	status := vals.Get("Status")

	cev, err := h.Backend.ModifyCustomDBEngineVersion(engine, engineVersion, description, status)
	if err != nil {
		return nil, err
	}

	return &modifyCustomDBEngineVersionResponse{
		Xmlns: rdsXMLNS,
		CustomDBEngineVersion: xmlCustomDBEngineVersion{
			Engine:        cev.Engine,
			EngineVersion: cev.EngineVersion,
			Status:        cev.Status,
			Description:   cev.Description,
		},
	}, nil
}

func (h *Handler) handleCreateDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	maxACU := parseFloat(vals.Get("MaxACU"))
	minACU := parseFloat(vals.Get("MinACU"))
	computeRedundancy := parseInt(vals.Get("ComputeRedundancy"))
	publiclyAccessible := vals.Get("PubliclyAccessible") == "true"

	sg, err := h.Backend.CreateDBShardGroup(id, clusterID, maxACU, minACU, computeRedundancy, publiclyAccessible)
	if err != nil {
		return nil, err
	}

	return &createDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
			DBClusterIdentifier:    sg.DBClusterIdentifier,
			Status:                 sg.Status,
		},
	}, nil
}

func (h *Handler) handleDeleteDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.DeleteDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
			Status:                 sg.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeDBShardGroups(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	groups, err := h.Backend.DescribeDBShardGroups(id)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDBShardGroup, 0, len(groups))
	for _, sg := range groups {
		members = append(members, xmlDBShardGroup{
			DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
			DBClusterIdentifier:    sg.DBClusterIdentifier,
			Status:                 sg.Status,
		})
	}

	return &describeDBShardGroupsResponse{
		Xmlns:         rdsXMLNS,
		DBShardGroups: xmlDBShardGroupList{Members: members},
	}, nil
}

func (h *Handler) handleModifyDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")
	maxACU := parseFloat(vals.Get("MaxACU"))
	computeRedundancy := parseInt(vals.Get("ComputeRedundancy"))

	sg, err := h.Backend.ModifyDBShardGroup(id, maxACU, computeRedundancy)
	if err != nil {
		return nil, err
	}

	return &modifyDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
			DBClusterIdentifier:    sg.DBClusterIdentifier,
			Status:                 sg.Status,
		},
	}, nil
}

func (h *Handler) handleRebootDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.RebootDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBShardGroupResponse{
		Xmlns: rdsXMLNS,
		DBShardGroup: xmlDBShardGroup{
			DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
			Status:                 sg.Status,
		},
	}, nil
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")
	sourceARN := vals.Get("SourceArn")
	targetARN := vals.Get("TargetArn")
	kmsKeyID := vals.Get("KMSKeyId")

	intg, err := h.Backend.CreateIntegration(name, sourceARN, targetARN, kmsKeyID)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: intg.IntegrationName,
			IntegrationArn:  intg.IntegrationArn,
			SourceArn:       intg.SourceArn,
			TargetArn:       intg.TargetArn,
			Status:          intg.Status,
		},
	}, nil
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	intg, err := h.Backend.DeleteIntegration(identifier)
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: intg.IntegrationName,
			IntegrationArn:  intg.IntegrationArn,
			Status:          intg.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	integrations, err := h.Backend.DescribeIntegrations(identifier)
	if err != nil {
		return nil, err
	}

	members := make([]xmlIntegration, 0, len(integrations))
	for _, intg := range integrations {
		members = append(members, xmlIntegration{
			IntegrationName: intg.IntegrationName,
			IntegrationArn:  intg.IntegrationArn,
			SourceArn:       intg.SourceArn,
			TargetArn:       intg.TargetArn,
			Status:          intg.Status,
		})
	}

	return &describeIntegrationsResponse{
		Xmlns:        rdsXMLNS,
		Integrations: xmlIntegrationList{Members: members},
	}, nil
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	intg, err := h.Backend.ModifyIntegration(identifier)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns: rdsXMLNS,
		Integration: xmlIntegration{
			IntegrationName: intg.IntegrationName,
			IntegrationArn:  intg.IntegrationArn,
			Status:          intg.Status,
		},
	}, nil
}

func (h *Handler) handleCreateTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")
	masterUsername := vals.Get("MasterUsername")

	tdb, err := h.Backend.CreateTenantDatabase(instanceID, tenantDBName, masterUsername)
	if err != nil {
		return nil, err
	}

	return &createTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName:   tdb.TenantDBName,
			DBInstanceIdentifier: tdb.DBInstanceIdentifier,
			Status:               tdb.Status,
		},
	}, nil
}

func (h *Handler) handleDeleteTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")

	tdb, err := h.Backend.DeleteTenantDatabase(instanceID, tenantDBName)
	if err != nil {
		return nil, err
	}

	return &deleteTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName:   tdb.TenantDBName,
			DBInstanceIdentifier: tdb.DBInstanceIdentifier,
			Status:               tdb.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeTenantDatabases(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")

	tdbs, err := h.Backend.DescribeTenantDatabases(instanceID, tenantDBName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTenantDatabase, 0, len(tdbs))
	for _, tdb := range tdbs {
		members = append(members, xmlTenantDatabase{
			TenantDatabaseName:   tdb.TenantDBName,
			DBInstanceIdentifier: tdb.DBInstanceIdentifier,
			Status:               tdb.Status,
		})
	}

	return &describeTenantDatabasesResponse{
		Xmlns:           rdsXMLNS,
		TenantDatabases: xmlTenantDatabaseList{Members: members},
	}, nil
}

func (h *Handler) handleModifyTenantDatabase(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	tenantDBName := vals.Get("TenantDBName")

	tdb, err := h.Backend.ModifyTenantDatabase(instanceID, tenantDBName)
	if err != nil {
		return nil, err
	}

	return &modifyTenantDatabaseResponse{
		Xmlns: rdsXMLNS,
		TenantDatabase: xmlTenantDatabase{
			TenantDatabaseName:   tdb.TenantDBName,
			DBInstanceIdentifier: tdb.DBInstanceIdentifier,
			Status:               tdb.Status,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterAutomatedBackup(vals url.Values) (any, error) {
	resourceID := vals.Get("DbClusterResourceId")
	if resourceID == "" {
		resourceID = vals.Get("DBClusterIdentifier")
	}

	backup, err := h.Backend.DeleteDBClusterAutomatedBackup(resourceID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		DBClusterAutomatedBackup: xmlDBClusterAutomatedBackup{
			DBClusterIdentifier: backup.DBClusterIdentifier,
			Status:              backup.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterAutomatedBackups(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	backups := h.Backend.DescribeDBClusterAutomatedBackups(clusterID)

	members := make([]xmlDBClusterAutomatedBackup, 0, len(backups))
	for _, b := range backups {
		members = append(members, xmlDBClusterAutomatedBackup{
			DBClusterIdentifier: b.DBClusterIdentifier,
			Status:              b.Status,
		})
	}

	return &describeDBClusterAutomatedBackupsResponse{
		Xmlns:                     rdsXMLNS,
		DBClusterAutomatedBackups: xmlDBClusterAutomatedBackupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBInstanceAutomatedBackup(vals url.Values) (any, error) {
	resourceID := vals.Get("DbiResourceId")
	if resourceID == "" {
		resourceID = vals.Get("DBInstanceIdentifier")
	}

	backup, err := h.Backend.DeleteDBInstanceAutomatedBackup(resourceID)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceAutomatedBackupResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: backup.DBInstanceIdentifier,
			Status:               backup.Status,
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
	sourceARN := vals.Get("SourceDBInstanceArn")
	retentionPeriod := parseInt(vals.Get("BackupRetentionPeriod"))

	backup, err := h.Backend.StartDBInstanceAutomatedBackupsReplication(sourceARN, retentionPeriod)
	if err != nil {
		return nil, err
	}

	return &startDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: backup.DBInstanceIdentifier,
			Status:               backup.Status,
		},
	}, nil
}

func (h *Handler) handleStopDBInstanceAutomatedBackupsReplication(vals url.Values) (any, error) {
	sourceARN := vals.Get("SourceDBInstanceArn")

	backup, err := h.Backend.StopDBInstanceAutomatedBackupsReplication(sourceARN)
	if err != nil {
		return nil, err
	}

	return &stopDBInstanceAutomatedBackupsReplicationResponse{
		Xmlns: rdsXMLNS,
		DBInstanceAutomatedBackup: xmlDBInstanceAutomatedBackup{
			DBInstanceIdentifier: backup.DBInstanceIdentifier,
			Status:               backup.Status,
		},
	}, nil
}

func (h *Handler) handleDescribeDBSnapshotTenantDatabases(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	entries := h.Backend.DescribeDBSnapshotTenantDatabases(snapshotID, instanceID)

	members := make([]xmlDBSnapshotTenantDatabase, 0, len(entries))
	for _, e := range entries {
		members = append(members, xmlDBSnapshotTenantDatabase{
			DBSnapshotIdentifier: e.DBSnapshotIdentifier,
			TenantDatabaseName:   e.TenantDatabaseName,
		})
	}

	return &describeDBSnapshotTenantDatabasesResponse{
		Xmlns:                     rdsXMLNS,
		DBSnapshotTenantDatabases: xmlDBSnapshotTenantDatabaseList{Members: members},
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
