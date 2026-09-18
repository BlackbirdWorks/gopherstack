package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	if clusterID == "" {
		return nil, fmt.Errorf(
			"%w: DBClusterIdentifier is required for Neptune instances",
			ErrInvalidParameter,
		)
	}
	// Engine is a required input (api_op_CreateDBInstance.go: "Valid Values:
	// neptune") but this backend only ever creates neptune-engine instances;
	// reject anything else instead of silently ignoring the field.
	if engine := vals.Get("Engine"); engine != "" && engine != neptuneEngine {
		return nil, fmt.Errorf(
			"%w: Engine must be %q for Neptune instances",
			ErrInvalidParameter,
			neptuneEngine,
		)
	}
	instanceClass := vals.Get("DBInstanceClass")
	promotionTier := 0
	if pt := vals.Get("PromotionTier"); pt != "" {
		v, err := strconv.Atoi(pt)
		if err != nil || v < 0 || v > maxPromotionTier {
			return nil, fmt.Errorf(
				"%w: PromotionTier must be 0-%d",
				ErrInvalidParameter,
				maxPromotionTier,
			)
		}
		promotionTier = v
	}
	monitoringInterval, err := parseIntOrZero(vals, "MonitoringInterval")
	if err != nil {
		return nil, err
	}
	iops, err := parseIntOrZero(vals, "Iops")
	if err != nil {
		return nil, err
	}
	opts := DBInstanceCreateOptions{
		DBParameterGroupName:       vals.Get("DBParameterGroupName"),
		DBSubnetGroupName:          vals.Get("DBSubnetGroupName"),
		PreferredMaintenanceWindow: vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:      vals.Get("PreferredBackupWindow"),
		AvailabilityZone:           vals.Get("AvailabilityZone"),
		MonitoringRoleArn:          vals.Get("MonitoringRoleArn"),
		// Real wire key: "DBSecurityGroups.DBSecurityGroupName.N"
		// (awsAwsquery_serializeDocumentDBSecurityGroupNameList, neptune@v1.48.4
		// serializers.go:4961).
		DBSecurityGroups:                parseMemberList(vals, "DBSecurityGroups.DBSecurityGroupName"),
		AutoMinorVersionUpgrade:         vals.Get("AutoMinorVersionUpgrade") == formTrue,
		CopyTagsToSnapshot:              vals.Get("CopyTagsToSnapshot") == formTrue,
		EnableIAMDatabaseAuthentication: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		StorageEncrypted:                vals.Get("StorageEncrypted") == formTrue,
		DeletionProtection:              vals.Get("DeletionProtection") == formTrue,
		PromotionTier:                   promotionTier,
		MonitoringInterval:              monitoringInterval,
		Iops:                            iops,
	}
	tags := parseTagEntries(vals)
	if err = validateTagEntries(tags); err != nil {
		return nil, err
	}
	inst, err := h.Backend.CreateDBInstance(ctx, id, clusterID, instanceClass, opts)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, inst.DBInstanceArn, tags)
	}

	return &createDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: h.toXMLInstance(ctx, inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterFilter := parseNeptuneFilterValues(vals, "db-cluster-id")
	instances, err := h.Backend.DescribeDBInstances(ctx, id, clusterFilter)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBInstance, 0, len(instances))
	for _, inst := range instances {
		cp := inst
		members = append(members, h.toXMLInstance(ctx, &cp))
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

func (h *Handler) handleDeleteDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	opts := DBInstanceDeleteOptions{
		FinalDBSnapshotIdentifier: vals.Get("FinalDBSnapshotIdentifier"),
		SkipFinalSnapshot:         vals.Get("SkipFinalSnapshot") == formTrue,
	}
	inst, err := h.Backend.DeleteDBInstance(ctx, id, opts)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: h.toXMLInstance(ctx, inst),
	}, nil
}

func (h *Handler) handleModifyDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	rawAuto := vals.Get("AutoMinorVersionUpgrade")
	rawCopy := vals.Get("CopyTagsToSnapshot")
	rawIam := vals.Get("EnableIAMDatabaseAuthentication")
	rawDel := vals.Get("DeletionProtection")
	promotionTier := 0
	promotionTierSet := false
	if pt := vals.Get("PromotionTier"); pt != "" {
		v, err := strconv.Atoi(pt)
		if err != nil || v < 0 || v > maxPromotionTier {
			return nil, fmt.Errorf(
				"%w: PromotionTier must be 0-%d",
				ErrInvalidParameter,
				maxPromotionTier,
			)
		}
		promotionTier = v
		promotionTierSet = true
	}
	rawMonitoring := vals.Get("MonitoringInterval")
	monitoringInterval, err := parseIntOrZero(vals, "MonitoringInterval")
	if err != nil {
		return nil, err
	}
	rawIops := vals.Get("Iops")
	iops, err := parseIntOrZero(vals, "Iops")
	if err != nil {
		return nil, err
	}
	rawPort := vals.Get("DBPortNumber")
	port, err := parseIntOrZero(vals, "DBPortNumber")
	if err != nil {
		return nil, err
	}
	opts := DBInstanceModifyOptions{
		DBParameterGroupName:       vals.Get("DBParameterGroupName"),
		PreferredMaintenanceWindow: vals.Get("PreferredMaintenanceWindow"),
		PreferredBackupWindow:      vals.Get("PreferredBackupWindow"),
		MonitoringRoleArn:          vals.Get("MonitoringRoleArn"),
		// See handleCreateDBInstance for the wire-key citation.
		DBSecurityGroups:                parseMemberList(vals, "DBSecurityGroups.DBSecurityGroupName"),
		AutoMinorVersionUpgrade:         rawAuto == formTrue,
		AutoMinorVersionUpgradeSet:      rawAuto != "",
		CopyTagsToSnapshot:              rawCopy == formTrue,
		CopyTagsToSnapshotSet:           rawCopy != "",
		EnableIAMDatabaseAuthentication: rawIam == formTrue,
		IamAuthSet:                      rawIam != "",
		PromotionTier:                   promotionTier,
		PromotionTierSet:                promotionTierSet,
		DeletionProtection:              rawDel == formTrue,
		DeletionProtectionSet:           rawDel != "",
		MonitoringInterval:              monitoringInterval,
		MonitoringIntervalSet:           rawMonitoring != "",
		Iops:                            iops,
		IopsSet:                         rawIops != "",
		Port:                            port,
		PortSet:                         rawPort != "",
		// ApplyImmediately is read for wire-declaration parity but this
		// backend always applies modifications immediately.
		ApplyImmediately: vals.Get("ApplyImmediately") == formTrue,
	}
	inst, err := h.Backend.ModifyDBInstance(ctx, id, instanceClass, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: h.toXMLInstance(ctx, inst),
	}, nil
}

func (h *Handler) handleRebootDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(ctx, id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      neptuneXMLNS,
		DBInstance: h.toXMLInstance(ctx, inst),
	}, nil
}

// handleDescribeDBEngineVersions returns the static engine-version catalog.
// DefaultOnly narrows it to defaultEngineVersion -- the one version this
// backend's static catalog and every unspecified-EngineVersion create path
// (e.g. instanceClusterInherited) already treat as "the" Neptune default;
// AWS's own per-major-version default concept is not modeled beyond that
// single global default (see PARITY.md).
func (h *Handler) handleDescribeDBEngineVersions(_ context.Context, vals url.Values) (any, error) {
	members := []xmlDBEngineVersion{
		{
			Engine:                 neptuneEngine,
			EngineVersion:          engineVersion1200,
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
			DBParameterGroupFamily: pgFamilyNeptune14,
		},
	}
	if vals.Get("DefaultOnly") == formTrue {
		filtered := make([]xmlDBEngineVersion, 0, 1)
		for _, m := range members {
			if m.EngineVersion == defaultEngineVersion {
				filtered = append(filtered, m)
			}
		}
		members = filtered
	}
	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBEngineVersionsResponse{
		Xmlns:            neptuneXMLNS,
		Marker:           nextMarker,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(
	_ context.Context,
	vals url.Values,
) (any, error) {
	engineVersions := []string{engineVersion1200, "1.2.1.0", defaultEngineVersion, "1.3.1.0", "1.4.0.0"}
	instanceClasses := []string{
		"db.r5.large", "db.r5.xlarge", "db.r5.2xlarge", "db.r5.4xlarge", "db.r5.8xlarge",
		"db.r6g.large", "db.r6g.xlarge", "db.r6g.2xlarge", "db.r6g.4xlarge",
		"db.t3.medium",
	}

	engineFilter := vals.Get("Engine")
	engineVersionFilter := vals.Get("EngineVersion")
	instanceClassFilter := vals.Get("DBInstanceClass")
	members := make([]xmlOrderableDBInstanceOption, 0, len(instanceClasses)*len(engineVersions))
	for _, ev := range engineVersions {
		if engineVersionFilter != "" && ev != engineVersionFilter {
			continue
		}
		for _, ic := range instanceClasses {
			if instanceClassFilter != "" && ic != instanceClassFilter {
				continue
			}
			if engineFilter != "" && engineFilter != neptuneEngine {
				continue
			}
			members = append(members, xmlOrderableDBInstanceOption{
				Engine:          neptuneEngine,
				EngineVersion:   ev,
				DBInstanceClass: ic,
			})
		}
	}
	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
			Marker:                     nextMarker,
		},
	}, nil
}

func (h *Handler) handleApplyPendingMaintenanceAction(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	resourceID := vals.Get("ResourceIdentifier")
	applyAction := vals.Get("ApplyAction")
	optInType := vals.Get("OptInType")
	result, err := h.Backend.ApplyPendingMaintenanceAction(ctx, resourceID, applyAction, optInType)
	if err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: neptuneXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: toXMLResourcePendingMaintenanceActions(result),
		},
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	resourceFilter := parseNeptuneFilterValues(vals, "db-cluster-id")
	if len(resourceFilter) == 0 {
		resourceFilter = parseNeptuneFilterValues(vals, "db-instance-id")
	}
	resources := h.Backend.DescribePendingMaintenanceActions(ctx, resourceFilter)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(resources))
	for _, r := range resources {
		cp := r
		members = append(members, toXMLResourcePendingMaintenanceActions(&cp))
	}
	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describePendingMaintenanceActionsResponse{
		Xmlns: neptuneXMLNS,
		Result: describePendingMaintenanceActionsResult{
			PendingMaintenanceActions: xmlResourcePendingMaintenanceActionsList{Members: members},
			Marker:                    nextMarker,
		},
	}, nil
}

// toXMLResourcePendingMaintenanceActions renders a
// ResourcePendingMaintenanceActions as its wire shape.
func toXMLResourcePendingMaintenanceActions(
	r *ResourcePendingMaintenanceActions,
) xmlResourcePendingMaintenanceActions {
	details := make([]xmlPendingMaintenanceAction, 0, len(r.PendingMaintenanceActionDetails))
	for _, a := range r.PendingMaintenanceActionDetails {
		details = append(details, xmlPendingMaintenanceAction(a))
	}

	return xmlResourcePendingMaintenanceActions{
		ResourceIdentifier:              r.ResourceIdentifier,
		PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{Members: details},
	}
}

// handleDescribeValidDBInstanceModifications validates DBInstanceIdentifier
// (required, per api_op_DescribeValidDBInstanceModifications.go) exists, then
// returns an empty Storage list: the real ValidDBInstanceModificationsMessage
// shape has no per-instance-class field at all (types.ValidStorageOptions is
// IopsToStorageRatio/ProvisionedIops/StorageSize/StorageType, all doc'd "Not
// applicable. In Neptune the storage type is managed at the DB Cluster
// level."), so there is nothing genuine to report here.
func (h *Handler) handleDescribeValidDBInstanceModifications(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInstanceNotFound)
	}
	if _, err := h.Backend.DescribeDBInstances(ctx, id, nil); err != nil {
		return nil, err
	}

	return &describeValidDBInstanceModificationsResponse{
		Xmlns:  neptuneXMLNS,
		Result: describeValidDBInstanceModificationsResult{},
	}, nil
}

// toXMLInstance renders a DBInstance as its wire shape. Unlike
// DBCluster.DBSubnetGroup (a bare *string in the real SDK), DBInstance's is
// a *types.DBSubnetGroup struct (neptune@v1.48.4 types/types.go:690); this
// looks up the real subnet group so the emitted <DBSubnetGroup> element is
// nested rather than a bare name (gopherstack-qdqg).
func (h *Handler) toXMLInstance(ctx context.Context, inst *DBInstance) xmlDBInstance {
	vpcSGs := make([]xmlVpcSecurityGroupMembership, 0, len(inst.VpcSecurityGroupIDs))
	for _, sgID := range inst.VpcSecurityGroupIDs {
		vpcSGs = append(vpcSGs, xmlVpcSecurityGroupMembership{VpcSecurityGroupID: sgID})
	}
	dbSGs := make([]xmlDBSecurityGroupMembership, 0, len(inst.DBSecurityGroups))
	for _, name := range inst.DBSecurityGroups {
		dbSGs = append(dbSGs, xmlDBSecurityGroupMembership{DBSecurityGroupName: name, Status: subscriptionStatusActive})
	}

	return xmlDBInstance{
		DBInstanceIdentifier:            inst.DBInstanceIdentifier,
		DBInstanceArn:                   inst.DBInstanceArn,
		DBClusterIdentifier:             inst.DBClusterIdentifier,
		DBInstanceClass:                 inst.DBInstanceClass,
		Engine:                          inst.Engine,
		EngineVersion:                   inst.EngineVersion,
		DBInstanceStatus:                inst.DBInstanceStatus,
		InstanceCreateTime:              inst.InstanceCreateTime,
		Endpoint:                        inst.Endpoint,
		DBSubnetGroup:                   h.xmlInstanceSubnetGroup(ctx, inst.DBSubnetGroupName),
		NetworkType:                     inst.NetworkType,
		Port:                            inst.Port,
		DBInstancePort:                  inst.Port,
		StorageEncrypted:                inst.StorageEncrypted,
		AutoMinorVersionUpgrade:         inst.AutoMinorVersionUpgrade,
		MultiAZ:                         inst.MultiAZ,
		PubliclyAccessible:              inst.PubliclyAccessible,
		PreferredMaintenanceWindow:      inst.PreferredMaintenanceWindow,
		PreferredBackupWindow:           inst.PreferredBackupWindow,
		AvailabilityZone:                inst.AvailabilityZone,
		DBParameterGroupName:            inst.DBParameterGroupName,
		CopyTagsToSnapshot:              inst.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: inst.EnableIAMDatabaseAuthentication,
		PromotionTier:                   inst.PromotionTier,
		DeletionProtection:              inst.DeletionProtection,
		BackupRetentionPeriod:           inst.BackupRetentionPeriod,
		VpcSecurityGroups:               xmlVpcSecurityGroupMembershipList{Members: vpcSGs},
		DBSecurityGroups:                xmlDBSecurityGroupMembershipList{Members: dbSGs},
		MonitoringInterval:              inst.MonitoringInterval,
		MonitoringRoleArn:               inst.MonitoringRoleArn,
		Iops:                            inst.Iops,
	}
}

// xmlInstanceSubnetGroup resolves a DB instance's subnet group by name and
// renders it via the same toXMLSubnetGroup used by the subnet-group
// endpoints, reusing the wire shape rather than duplicating it. Returns nil
// (element omitted) when the instance has no subnet group name, or the
// named group no longer exists.
func (h *Handler) xmlInstanceSubnetGroup(ctx context.Context, name string) *xmlDBSubnetGroup {
	if name == "" {
		return nil
	}
	sgs, err := h.Backend.DescribeDBSubnetGroups(ctx, name)
	if err != nil || len(sgs) == 0 {
		return nil
	}
	x := toXMLSubnetGroup(&sgs[0])

	return &x
}

type xmlDBInstance struct {
	DBSubnetGroup                   *xmlDBSubnetGroup                 `xml:"DBSubnetGroup,omitempty"`
	AvailabilityZone                string                            `xml:"AvailabilityZone,omitempty"`
	NetworkType                     string                            `xml:"NetworkType,omitempty"`
	DBClusterIdentifier             string                            `xml:"DBClusterIdentifier,omitempty"`
	DBInstanceClass                 string                            `xml:"DBInstanceClass"`
	Engine                          string                            `xml:"Engine"`
	EngineVersion                   string                            `xml:"EngineVersion,omitempty"`
	DBInstanceStatus                string                            `xml:"DBInstanceStatus"`
	InstanceCreateTime              string                            `xml:"InstanceCreateTime,omitempty"`
	Endpoint                        string                            `xml:"Endpoint>Address,omitempty"`
	MonitoringRoleArn               string                            `xml:"MonitoringRoleArn,omitempty"`
	DBParameterGroupName            string                            `xml:"DBParameterGroups>DBParameterGroup>DBParameterGroupName,omitempty"` //nolint:lll // nested query-protocol tag path, cannot be shortened
	PreferredMaintenanceWindow      string                            `xml:"PreferredMaintenanceWindow,omitempty"`
	PreferredBackupWindow           string                            `xml:"PreferredBackupWindow,omitempty"`
	DBInstanceIdentifier            string                            `xml:"DBInstanceIdentifier"`
	DBInstanceArn                   string                            `xml:"DBInstanceArn,omitempty"`
	VpcSecurityGroups               xmlVpcSecurityGroupMembershipList `xml:"VpcSecurityGroups,omitempty"`
	DBSecurityGroups                xmlDBSecurityGroupMembershipList  `xml:"DBSecurityGroups,omitempty"`
	PromotionTier                   int                               `xml:"PromotionTier,omitempty"`
	DBInstancePort                  int                               `xml:"DbInstancePort"`
	Port                            int                               `xml:"Endpoint>Port"`
	BackupRetentionPeriod           int                               `xml:"BackupRetentionPeriod,omitempty"`
	MonitoringInterval              int                               `xml:"MonitoringInterval,omitempty"`
	Iops                            int                               `xml:"Iops,omitempty"`
	AutoMinorVersionUpgrade         bool                              `xml:"AutoMinorVersionUpgrade"`
	CopyTagsToSnapshot              bool                              `xml:"CopyTagsToSnapshot"`
	MultiAZ                         bool                              `xml:"MultiAZ"`
	DeletionProtection              bool                              `xml:"DeletionProtection"`
	StorageEncrypted                bool                              `xml:"StorageEncrypted"`
	PubliclyAccessible              bool                              `xml:"PubliclyAccessible"`
	EnableIAMDatabaseAuthentication bool                              `xml:"IAMDatabaseAuthenticationEnabled"`
}

// xmlDBSecurityGroupMembership mirrors types.DBSecurityGroupMembership
// (DBSecurityGroupName + Status, neptune@v1.48.4 deserializers.go:16162);
// the list wraps each entry in "DBSecurityGroup"
// (awsAwsquery_deserializeDocumentDBSecurityGroupMembershipList,
// deserializers.go:16224).
type xmlDBSecurityGroupMembership struct {
	DBSecurityGroupName string `xml:"DBSecurityGroupName"`
	Status              string `xml:"Status,omitempty"`
}

type xmlDBSecurityGroupMembershipList struct {
	Members []xmlDBSecurityGroupMembership `xml:"DBSecurityGroup"`
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
	Marker           string                 `xml:"DescribeDBEngineVersionsResult>Marker,omitempty"`
	DBEngineVersions xmlDBEngineVersionList `xml:"DescribeDBEngineVersionsResult>DBEngineVersions"`
}

type xmlOrderableDBInstanceOption struct {
	SupportedNetworkTypes *xmlSupportedNetworkTypeList `xml:"SupportedNetworkTypes,omitempty"`
	Engine                string                       `xml:"Engine"`
	EngineVersion         string                       `xml:"EngineVersion"`
	DBInstanceClass       string                       `xml:"DBInstanceClass"`
}

// xmlSupportedNetworkTypeList decodes via the generic StringList deserializer
// (neptune@v1.48.4 deserializers.go:22293, wraps each entry in <member>).
type xmlSupportedNetworkTypeList struct {
	Members []string `xml:"member"`
}

type xmlOrderableDBInstanceOptionList struct {
	Members []xmlOrderableDBInstanceOption `xml:"OrderableDBInstanceOption"`
}

type describeOrderableDBInstanceOptionsResult struct {
	Marker                     string                           `xml:"Marker,omitempty"`
	OrderableDBInstanceOptions xmlOrderableDBInstanceOptionList `xml:"OrderableDBInstanceOptions"`
}

type describeOrderableDBInstanceOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableDBInstanceOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeOrderableDBInstanceOptionsResult `xml:"DescribeOrderableDBInstanceOptionsResult"`
}

type applyPendingMaintenanceActionResult struct {
	ResourcePendingMaintenanceActions xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type applyPendingMaintenanceActionResponse struct {
	XMLName xml.Name                            `xml:"ApplyPendingMaintenanceActionResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  applyPendingMaintenanceActionResult `xml:"ApplyPendingMaintenanceActionResult"`
}

// xmlPendingMaintenanceAction represents a single queued action for a resource.
type xmlPendingMaintenanceAction struct {
	Action               string `xml:"Action"`
	Description          string `xml:"Description,omitempty"`
	AutoAppliedAfterDate string `xml:"AutoAppliedAfterDate,omitempty"`
	CurrentApplyDate     string `xml:"CurrentApplyDate,omitempty"`
	ForcedApplyDate      string `xml:"ForcedApplyDate,omitempty"`
	OptInStatus          string `xml:"OptInStatus,omitempty"`
}

// xmlPendingMaintenanceActionList wraps a resource's list of individual
// pending actions -- the PendingMaintenanceActionDetails member of
// types.ResourcePendingMaintenanceActions.
type xmlPendingMaintenanceActionList struct {
	Members []xmlPendingMaintenanceAction `xml:"PendingMaintenanceAction"`
}

// xmlResourcePendingMaintenanceActions bundles one resource's pending
// actions with the resource's identifier/ARN (types.ResourcePendingMaintenanceActions).
type xmlResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                          `xml:"ResourceIdentifier,omitempty"`
	PendingMaintenanceActionDetails xmlPendingMaintenanceActionList `xml:"PendingMaintenanceActionDetails"`
}

// xmlResourcePendingMaintenanceActionsList wraps the outer list of
// per-resource pending-action bundles returned by DescribePendingMaintenanceActions.
type xmlResourcePendingMaintenanceActionsList struct {
	Members []xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResult struct {
	Marker                    string                                   `xml:"Marker,omitempty"`
	PendingMaintenanceActions xmlResourcePendingMaintenanceActionsList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                                `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                                  `xml:"xmlns,attr"`
	Result  describePendingMaintenanceActionsResult `xml:"DescribePendingMaintenanceActionsResult"`
}

// xmlValidDBInstanceModificationsMessage mirrors
// types.ValidDBInstanceModificationsMessage (neptune@v1.48.4 types/types.go:1608):
// a Storage list of ValidStorageOptions, wrapped as <Storage><ValidStorageOptions>...
// (confirmed via deserializers.go:23164/23273 -- list member element name is
// "ValidStorageOptions", not the usual query-protocol "member"). Always empty
// here: see handleDescribeValidDBInstanceModifications doc comment.
type xmlValidDBInstanceModificationsMessage struct {
	Storage xmlValidStorageOptionList `xml:"Storage"`
}

type xmlValidStorageOptionList struct {
	Members []xmlValidStorageOption `xml:"ValidStorageOptions"`
}

type xmlValidStorageOption struct {
	StorageType string `xml:"StorageType,omitempty"`
}

type describeValidDBInstanceModificationsResult struct {
	ValidDBInstanceModificationsMessage xmlValidDBInstanceModificationsMessage `xml:"ValidDBInstanceModificationsMessage"`
}

type describeValidDBInstanceModificationsResponse struct {
	XMLName xml.Name                                   `xml:"DescribeValidDBInstanceModificationsResponse"`
	Xmlns   string                                     `xml:"xmlns,attr"`
	Result  describeValidDBInstanceModificationsResult `xml:"DescribeValidDBInstanceModificationsResult"`
}

// dispatchDBInstanceAction handles DBInstance-family actions plus the
// engine/instance metadata describes that ride alongside them; see
// dispatch's doc comment for the chaining rationale.
func (h *Handler) dispatchDBInstanceAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "CreateDBInstance":
		return h.handleCreateDBInstance(ctx, vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(ctx, vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(ctx, vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(ctx, vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(ctx, vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(ctx, vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(ctx, vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(ctx, vals)
	case "DescribePendingMaintenanceActions":
		return h.handleDescribePendingMaintenanceActions(ctx, vals)
	case "DescribeValidDBInstanceModifications":
		return h.handleDescribeValidDBInstanceModifications(ctx, vals)
	default:
		return h.dispatchSubnetAndClusterParamGroupAction(ctx, action, vals)
	}
}
