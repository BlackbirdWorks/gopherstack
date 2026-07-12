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
	case "GetPerformanceInsightsMetrics":
		return h.handleGetPerformanceInsightsMetricsReal(vals)
	default:
		return h.dispatchExtended16(action, vals)
	}
}

// ---- XML response types ----

// CreateCustomDBEngineVersionOutput, DeleteCustomDBEngineVersionOutput, and
// ModifyCustomDBEngineVersionOutput are all flat shapes in the real RDS API — the
// Engine/EngineVersion/Status/DBEngineVersionDescription members sit directly under
// the <XxxResult> element, there is no nested <CustomDBEngineVersion> wrapper (unlike
// e.g. CreateDBInstanceOutput, which does nest under <DBInstance>). Each field below
// therefore carries the full result-element chain individually instead of nesting
// through a shared struct, matching the pattern already used for e.g.
// ModifyCurrentDBClusterCapacityResult below.
type createCustomDBEngineVersionResponse struct {
	XMLName                    xml.Name `xml:"CreateCustomDBEngineVersionResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	Engine                     string   `xml:"CreateCustomDBEngineVersionResult>Engine"`
	EngineVersion              string   `xml:"CreateCustomDBEngineVersionResult>EngineVersion"`
	Status                     string   `xml:"CreateCustomDBEngineVersionResult>Status,omitempty"`
	DBEngineVersionDescription string   `xml:"CreateCustomDBEngineVersionResult>DBEngineVersionDescription,omitempty"`
}

type deleteCustomDBEngineVersionResponse struct {
	XMLName       xml.Name `xml:"DeleteCustomDBEngineVersionResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	Engine        string   `xml:"DeleteCustomDBEngineVersionResult>Engine"`
	EngineVersion string   `xml:"DeleteCustomDBEngineVersionResult>EngineVersion"`
	Status        string   `xml:"DeleteCustomDBEngineVersionResult>Status,omitempty"`
}

type modifyCustomDBEngineVersionResponse struct {
	XMLName                    xml.Name `xml:"ModifyCustomDBEngineVersionResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	Engine                     string   `xml:"ModifyCustomDBEngineVersionResult>Engine"`
	EngineVersion              string   `xml:"ModifyCustomDBEngineVersionResult>EngineVersion"`
	Status                     string   `xml:"ModifyCustomDBEngineVersionResult>Status,omitempty"`
	DBEngineVersionDescription string   `xml:"ModifyCustomDBEngineVersionResult>DBEngineVersionDescription,omitempty"`
}

type xmlDBShardGroup struct {
	DBShardGroupIdentifier string  `xml:"DBShardGroupIdentifier"`
	DBClusterIdentifier    string  `xml:"DBClusterIdentifier,omitempty"`
	Status                 string  `xml:"Status,omitempty"`
	Endpoint               string  `xml:"Endpoint,omitempty"`
	MaxACU                 float64 `xml:"MaxACU,omitempty"`
	MinACU                 float64 `xml:"MinACU,omitempty"`
	ComputeRedundancy      int     `xml:"ComputeRedundancy,omitempty"`
}

type xmlDBShardGroupList struct {
	Members []xmlDBShardGroup `xml:"DBShardGroup"`
}

// CreateDBShardGroupOutput, DeleteDBShardGroupOutput, ModifyDBShardGroupOutput, and
// RebootDBShardGroupOutput are flat shapes in the real RDS API (no nested
// <DBShardGroup> wrapper — see the comment on createCustomDBEngineVersionResponse for
// why each field below repeats the full result-element chain). DescribeDBShardGroups
// is different: it returns a real list (DBShardGroups []types.DBShardGroup), so
// describeDBShardGroupsResponse below correctly keeps the xmlDBShardGroupList nesting.
type createDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"CreateDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"CreateDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"CreateDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"CreateDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"CreateDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"CreateDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"CreateDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"CreateDBShardGroupResult>ComputeRedundancy,omitempty"`
}

type deleteDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"DeleteDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"DeleteDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"DeleteDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"DeleteDBShardGroupResult>Status,omitempty"`
}

type describeDBShardGroupsResponse struct {
	XMLName       xml.Name            `xml:"DescribeDBShardGroupsResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	Marker        string              `xml:"DescribeDBShardGroupsResult>Marker,omitempty"`
	DBShardGroups xmlDBShardGroupList `xml:"DescribeDBShardGroupsResult>DBShardGroups"`
}

type modifyDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"ModifyDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"ModifyDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"ModifyDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"ModifyDBShardGroupResult>Status,omitempty"`
	MaxACU                 float64  `xml:"ModifyDBShardGroupResult>MaxACU,omitempty"`
	ComputeRedundancy      int      `xml:"ModifyDBShardGroupResult>ComputeRedundancy,omitempty"`
}

type rebootDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"RebootDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"RebootDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"RebootDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"RebootDBShardGroupResult>Status,omitempty"`
}

type xmlIntegration struct {
	IntegrationName        string `xml:"IntegrationName"`
	IntegrationArn         string `xml:"IntegrationArn,omitempty"`
	Status                 string `xml:"Status,omitempty"`
	SourceArn              string `xml:"SourceArn,omitempty"`
	TargetArn              string `xml:"TargetArn,omitempty"`
	DataFilter             string `xml:"DataFilter,omitempty"`
	IntegrationDescription string `xml:"Description,omitempty"`
}

type xmlIntegrationList struct {
	Members []xmlIntegration `xml:"Integration"`
}

// CreateIntegrationOutput, DeleteIntegrationOutput, and ModifyIntegrationOutput are
// flat shapes in the real RDS API (no nested <Integration> wrapper — see the comment
// on createCustomDBEngineVersionResponse for why each field below repeats the full
// result-element chain). DescribeIntegrations is different: it returns a real list,
// so describeIntegrationsResponse below correctly keeps the xmlIntegrationList nesting.
type createIntegrationResponse struct {
	XMLName                xml.Name `xml:"CreateIntegrationResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	IntegrationName        string   `xml:"CreateIntegrationResult>IntegrationName"`
	IntegrationArn         string   `xml:"CreateIntegrationResult>IntegrationArn,omitempty"`
	Status                 string   `xml:"CreateIntegrationResult>Status,omitempty"`
	SourceArn              string   `xml:"CreateIntegrationResult>SourceArn,omitempty"`
	TargetArn              string   `xml:"CreateIntegrationResult>TargetArn,omitempty"`
	DataFilter             string   `xml:"CreateIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string   `xml:"CreateIntegrationResult>Description,omitempty"`
}

type deleteIntegrationResponse struct {
	XMLName         xml.Name `xml:"DeleteIntegrationResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	IntegrationName string   `xml:"DeleteIntegrationResult>IntegrationName"`
	IntegrationArn  string   `xml:"DeleteIntegrationResult>IntegrationArn,omitempty"`
	Status          string   `xml:"DeleteIntegrationResult>Status,omitempty"`
	SourceArn       string   `xml:"DeleteIntegrationResult>SourceArn,omitempty"`
	TargetArn       string   `xml:"DeleteIntegrationResult>TargetArn,omitempty"`
}

type describeIntegrationsResponse struct {
	XMLName      xml.Name           `xml:"DescribeIntegrationsResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Marker       string             `xml:"DescribeIntegrationsResult>Marker,omitempty"`
	Integrations xmlIntegrationList `xml:"DescribeIntegrationsResult>Integrations"`
}

type modifyIntegrationResponse struct {
	XMLName                xml.Name `xml:"ModifyIntegrationResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	IntegrationName        string   `xml:"ModifyIntegrationResult>IntegrationName"`
	IntegrationArn         string   `xml:"ModifyIntegrationResult>IntegrationArn,omitempty"`
	Status                 string   `xml:"ModifyIntegrationResult>Status,omitempty"`
	SourceArn              string   `xml:"ModifyIntegrationResult>SourceArn,omitempty"`
	TargetArn              string   `xml:"ModifyIntegrationResult>TargetArn,omitempty"`
	DataFilter             string   `xml:"ModifyIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string   `xml:"ModifyIntegrationResult>Description,omitempty"`
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
	Marker          string                `xml:"DescribeTenantDatabasesResult>Marker,omitempty"`
	TenantDatabases xmlTenantDatabaseList `xml:"DescribeTenantDatabasesResult>TenantDatabases"`
}

type modifyTenantDatabaseResponse struct {
	XMLName        xml.Name          `xml:"ModifyTenantDatabaseResponse"`
	Xmlns          string            `xml:"xmlns,attr"`
	TenantDatabase xmlTenantDatabase `xml:"ModifyTenantDatabaseResult>TenantDatabase"`
}

type xmlDBClusterAutomatedBackup struct {
	DBClusterIdentifier   string `xml:"DBClusterIdentifier"`
	DBClusterResourceID   string `xml:"DbClusterResourceId,omitempty"`
	Engine                string `xml:"Engine,omitempty"`
	EngineVersion         string `xml:"EngineVersion,omitempty"`
	Region                string `xml:"Region,omitempty"`
	Status                string `xml:"Status,omitempty"`
	BackupRetentionPeriod int    `xml:"BackupRetentionPeriod,omitempty"`
	StorageEncrypted      bool   `xml:"StorageEncrypted,omitempty"`
}

type xmlDBClusterAutomatedBackupList struct {
	Members []xmlDBClusterAutomatedBackup `xml:"DBClusterAutomatedBackup"`
}

type deleteDBClusterAutomatedBackupResult struct {
	DBClusterAutomatedBackup xmlDBClusterAutomatedBackup `xml:"DBClusterAutomatedBackup"`
}

type deleteDBClusterAutomatedBackupResponse struct {
	XMLName xml.Name                             `xml:"DeleteDBClusterAutomatedBackupResponse"`
	Xmlns   string                               `xml:"xmlns,attr"`
	Result  deleteDBClusterAutomatedBackupResult `xml:"DeleteDBClusterAutomatedBackupResult"`
}

type describeDBClusterAutomatedBackupsResult struct {
	DBClusterAutomatedBackups xmlDBClusterAutomatedBackupList `xml:"DBClusterAutomatedBackups"`
}

type describeDBClusterAutomatedBackupsResponse struct {
	XMLName xml.Name                                `xml:"DescribeDBClusterAutomatedBackupsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describeDBClusterAutomatedBackupsResult `xml:"DescribeDBClusterAutomatedBackupsResult"`
}

type xmlDBInstanceAutomatedBackup struct {
	DBInstanceIdentifier  string `xml:"DBInstanceIdentifier"`
	DbiResourceID         string `xml:"DbiResourceId,omitempty"`
	Engine                string `xml:"Engine,omitempty"`
	EngineVersion         string `xml:"EngineVersion,omitempty"`
	DBInstanceArn         string `xml:"DBInstanceArn,omitempty"`
	Region                string `xml:"Region,omitempty"`
	Status                string `xml:"Status,omitempty"`
	AllocatedStorage      int    `xml:"AllocatedStorage,omitempty"`
	BackupRetentionPeriod int    `xml:"BackupRetentionPeriod,omitempty"`
}

type xmlDBInstanceAutomatedBackupList struct {
	Members []xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type deleteDBInstanceAutomatedBackupResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type deleteDBInstanceAutomatedBackupResponse struct {
	XMLName xml.Name                              `xml:"DeleteDBInstanceAutomatedBackupResponse"`
	Xmlns   string                                `xml:"xmlns,attr"`
	Result  deleteDBInstanceAutomatedBackupResult `xml:"DeleteDBInstanceAutomatedBackupResult"`
}

type describeDBInstanceAutomatedBackupsResult struct {
	DBInstanceAutomatedBackups xmlDBInstanceAutomatedBackupList `xml:"DBInstanceAutomatedBackups"`
}

type describeDBInstanceAutomatedBackupsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeDBInstanceAutomatedBackupsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeDBInstanceAutomatedBackupsResult `xml:"DescribeDBInstanceAutomatedBackupsResult"`
}

type startDBInstanceAutomatedBackupsReplicationResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type startDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName xml.Name                                         `xml:"StartDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns   string                                           `xml:"xmlns,attr"`
	Result  startDBInstanceAutomatedBackupsReplicationResult `xml:"StartDBInstanceAutomatedBackupsReplicationResult"`
}

type stopDBInstanceAutomatedBackupsReplicationResult struct {
	DBInstanceAutomatedBackup xmlDBInstanceAutomatedBackup `xml:"DBInstanceAutomatedBackup"`
}

type stopDBInstanceAutomatedBackupsReplicationResponse struct {
	XMLName xml.Name                                        `xml:"StopDBInstanceAutomatedBackupsReplicationResponse"`
	Xmlns   string                                          `xml:"xmlns,attr"`
	Result  stopDBInstanceAutomatedBackupsReplicationResult `xml:"StopDBInstanceAutomatedBackupsReplicationResult"`
}

type xmlDBSnapshotTenantDatabase struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	TenantDatabaseName   string `xml:"TenantDatabaseName,omitempty"`
}

type xmlDBSnapshotTenantDatabaseList struct {
	Members []xmlDBSnapshotTenantDatabase `xml:"DBSnapshotTenantDatabase"`
}

type describeDBSnapshotTenantDatabasesResult struct {
	DBSnapshotTenantDatabases xmlDBSnapshotTenantDatabaseList `xml:"DBSnapshotTenantDatabases"`
}

type describeDBSnapshotTenantDatabasesResponse struct {
	XMLName xml.Name                                `xml:"DescribeDBSnapshotTenantDatabasesResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describeDBSnapshotTenantDatabasesResult `xml:"DescribeDBSnapshotTenantDatabasesResult"`
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
		Xmlns:                      rdsXMLNS,
		Engine:                     cev.Engine,
		EngineVersion:              cev.EngineVersion,
		Status:                     cev.Status,
		DBEngineVersionDescription: cev.Description,
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
		Xmlns:         rdsXMLNS,
		Engine:        cev.Engine,
		EngineVersion: cev.EngineVersion,
		Status:        cev.Status,
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
		Xmlns:                      rdsXMLNS,
		Engine:                     cev.Engine,
		EngineVersion:              cev.EngineVersion,
		Status:                     cev.Status,
		DBEngineVersionDescription: cev.Description,
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
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}, nil
}

func (h *Handler) handleDeleteDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.DeleteDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
	}, nil
}

func (h *Handler) handleDescribeDBShardGroups(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	groups, err := h.Backend.DescribeDBShardGroups(id)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, groups,
		func(a, b DBShardGroup) bool {
			return a.DBShardGroupIdentifier < b.DBShardGroupIdentifier
		},
		func(sg DBShardGroup) xmlDBShardGroup { return toXMLDBShardGroup(&sg) },
	)
	if err != nil {
		return nil, err
	}

	return &describeDBShardGroupsResponse{
		Xmlns:         rdsXMLNS,
		Marker:        marker,
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
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		MaxACU:                 sg.MaxACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}, nil
}

func (h *Handler) handleRebootDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.RebootDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
	}, nil
}

func toXMLDBShardGroup(sg *DBShardGroup) xmlDBShardGroup {
	return xmlDBShardGroup{
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}
}

func toXMLIntegration(intg *Integration) xmlIntegration {
	return xmlIntegration{
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		Status:                 intg.Status,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
	}
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")
	sourceARN := vals.Get("SourceArn")
	targetARN := vals.Get("TargetArn")
	kmsKeyID := vals.Get("KMSKeyId")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.CreateIntegration(name, sourceARN, targetARN, kmsKeyID, dataFilter, description)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		Status:                 intg.Status,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
	}, nil
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	intg, err := h.Backend.DeleteIntegration(identifier)
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns:           rdsXMLNS,
		IntegrationName: intg.IntegrationName,
		IntegrationArn:  intg.IntegrationArn,
		Status:          intg.Status,
		SourceArn:       intg.SourceArn,
		TargetArn:       intg.TargetArn,
	}, nil
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	integrations, err := h.Backend.DescribeIntegrations(identifier)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, integrations,
		func(a, b Integration) bool { return a.IntegrationName < b.IntegrationName },
		func(intg Integration) xmlIntegration { return toXMLIntegration(&intg) },
	)
	if err != nil {
		return nil, err
	}

	return &describeIntegrationsResponse{
		Xmlns:        rdsXMLNS,
		Marker:       marker,
		Integrations: xmlIntegrationList{Members: members},
	}, nil
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.ModifyIntegration(identifier, dataFilter, description)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		Status:                 intg.Status,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
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

	members, marker, err := paginateDescribe(
		vals, tdbs,
		func(a, b TenantDatabase) bool {
			ka := a.DBInstanceIdentifier + "/" + a.TenantDBName
			kb := b.DBInstanceIdentifier + "/" + b.TenantDBName

			return ka < kb
		},
		func(tdb TenantDatabase) xmlTenantDatabase {
			return xmlTenantDatabase{
				TenantDatabaseName:   tdb.TenantDBName,
				DBInstanceIdentifier: tdb.DBInstanceIdentifier,
				Status:               tdb.Status,
			}
		},
	)
	if err != nil {
		return nil, err
	}

	return &describeTenantDatabasesResponse{
		Xmlns:           rdsXMLNS,
		Marker:          marker,
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
		Result: deleteDBClusterAutomatedBackupResult{
			DBClusterAutomatedBackup: toXMLClusterBackup(backup),
		},
	}, nil
}

func toXMLClusterBackup(b *DBClusterAutomatedBackup) xmlDBClusterAutomatedBackup {
	return xmlDBClusterAutomatedBackup{
		DBClusterIdentifier:   b.DBClusterIdentifier,
		DBClusterResourceID:   b.DBClusterResourceID,
		Engine:                b.Engine,
		EngineVersion:         b.EngineVersion,
		Region:                b.Region,
		Status:                b.Status,
		BackupRetentionPeriod: b.BackupRetentionPeriod,
		StorageEncrypted:      b.StorageEncrypted,
	}
}

func toXMLInstanceBackup(ab *DBInstanceAutomatedBackup) xmlDBInstanceAutomatedBackup {
	return xmlDBInstanceAutomatedBackup{
		DBInstanceIdentifier:  ab.DBInstanceIdentifier,
		DbiResourceID:         ab.DbiResourceID,
		Engine:                ab.Engine,
		EngineVersion:         ab.EngineVersion,
		DBInstanceArn:         ab.DBInstanceArn,
		Region:                ab.Region,
		Status:                ab.Status,
		AllocatedStorage:      ab.AllocatedStorage,
		BackupRetentionPeriod: ab.BackupRetentionPeriod,
	}
}

func (h *Handler) handleDescribeDBClusterAutomatedBackups(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	backups := h.Backend.DescribeDBClusterAutomatedBackups(clusterID)

	members := make([]xmlDBClusterAutomatedBackup, 0, len(backups))
	for i := range backups {
		members = append(members, toXMLClusterBackup(&backups[i]))
	}

	return &describeDBClusterAutomatedBackupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBClusterAutomatedBackupsResult{
			DBClusterAutomatedBackups: xmlDBClusterAutomatedBackupList{Members: members},
		},
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
		Result: deleteDBInstanceAutomatedBackupResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
		},
	}, nil
}

func (h *Handler) handleDescribeDBInstanceAutomatedBackups(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	backups := h.Backend.DescribeDBInstanceAutomatedBackups(instanceID)
	members := make([]xmlDBInstanceAutomatedBackup, 0, len(backups))

	for i := range backups {
		members = append(members, toXMLInstanceBackup(&backups[i]))
	}

	return &describeDBInstanceAutomatedBackupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBInstanceAutomatedBackupsResult{
			DBInstanceAutomatedBackups: xmlDBInstanceAutomatedBackupList{Members: members},
		},
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
		Result: startDBInstanceAutomatedBackupsReplicationResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
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
		Result: stopDBInstanceAutomatedBackupsReplicationResult{
			DBInstanceAutomatedBackup: toXMLInstanceBackup(backup),
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
		Xmlns: rdsXMLNS,
		Result: describeDBSnapshotTenantDatabasesResult{
			DBSnapshotTenantDatabases: xmlDBSnapshotTenantDatabaseList{Members: members},
		},
	}, nil
}

// ---- Performance Insights XML types ----

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
