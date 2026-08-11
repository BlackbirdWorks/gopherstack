package neptune

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var errNoServerlessV2Config = errors.New("noServerlessV2Config")

// clusterARN builds a Neptune cluster ARN using the request region and account.
func (h *Handler) clusterARN(region, id string) string {
	return arn.Build("neptune", region, h.Backend.AccountID(), "cluster:"+id)
}

func (h *Handler) handleCreateDBCluster(ctx context.Context, vals url.Values) (any, error) {
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
		MasterUsername:                  vals.Get("MasterUsername"),
		DBSubnetGroupName:               vals.Get("DBSubnetGroupName"),
		StorageType:                     vals.Get("StorageType"),
		NetworkType:                     vals.Get("NetworkType"),
		EnableIAMDatabaseAuthentication: vals.Get("EnableIAMDatabaseAuthentication") == formTrue,
		ManageMasterUserPassword:        vals.Get("ManageMasterUserPassword") == formTrue,
		StorageEncrypted:                vals.Get("StorageEncrypted") == formTrue,
		DeletionProtection:              vals.Get("DeletionProtection") == formTrue,
		CopyTagsToSnapshot:              vals.Get("CopyTagsToSnapshot") == formTrue,
		VpcSecurityGroupIDs:             parseMemberList(vals, "VpcSecurityGroupIds.member"),
		AvailabilityZones:               parseMemberList(vals, "AvailabilityZones.member"),
		ServerlessV2ScalingConfig:       sv2,
	}
	if s := vals.Get("BackupRetentionPeriod"); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			opts.BackupRetentionPeriod = v
		}
	}
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	cluster, err := h.Backend.CreateDBCluster(ctx, id, paramGroupName, port, opts)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(
			ctx,
			h.clusterARN(getRegion(ctx, h.Backend.Region()), cluster.DBClusterIdentifier),
			tags,
		)
	}

	return &createDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeDBClusters(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	filters := DBClusterFilters{
		Engine:        parseNeptuneFilterValue(vals, "engine"),
		EngineVersion: parseNeptuneFilterValue(vals, "engine-version"),
		Status:        parseNeptuneFilterValue(vals, "status"),
	}
	clusters, err := h.Backend.DescribeDBClusters(ctx, id, filters)
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

func (h *Handler) handleDeleteDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	skipFinal := vals.Get("SkipFinalSnapshot") == "true"
	finalID := vals.Get("FinalDBSnapshotIdentifier")
	cluster, err := h.Backend.DeleteDBCluster(ctx, id, DBClusterDeleteOptions{
		SkipFinalSnapshot:         skipFinal,
		FinalDBSnapshotIdentifier: finalID,
	})
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	sv2, sv2Err := parseServerlessV2ScalingConfig(vals)
	if sv2Err != nil && !errors.Is(sv2Err, errNoServerlessV2Config) {
		return nil, sv2Err
	}
	rawIam := vals.Get("EnableIAMDatabaseAuthentication")
	rawDel := vals.Get("DeletionProtection")
	rawCopy := vals.Get("CopyTagsToSnapshot")
	opts := DBClusterModifyOptions{
		EngineVersion:                   vals.Get("EngineVersion"),
		NetworkType:                     vals.Get("NetworkType"),
		PreferredBackupWindow:           vals.Get("PreferredBackupWindow"),
		PreferredMaintenanceWindow:      vals.Get("PreferredMaintenanceWindow"),
		EnableIAMDatabaseAuthentication: rawIam == formTrue,
		IamAuthSet:                      rawIam != "",
		ManageMasterUserPassword:        vals.Get("ManageMasterUserPassword") == formTrue,
		DeletionProtection:              rawDel == formTrue,
		DeletionProtectionSet:           rawDel != "",
		CopyTagsToSnapshot:              rawCopy == formTrue,
		CopyTagsToSnapshotSet:           rawCopy != "",
		VpcSecurityGroupIDs:             parseMemberList(vals, "VpcSecurityGroupIds.member"),
		ServerlessV2ScalingConfig:       sv2,
	}
	rawBRP := vals.Get("BackupRetentionPeriod")
	if rawBRP != "" {
		if v, err := strconv.Atoi(rawBRP); err == nil {
			opts.BackupRetentionPeriod = v
			opts.BackupRetentionPeriodSet = true
		}
	}
	cluster, err := h.Backend.ModifyDBCluster(ctx, id, paramGroupName, opts)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStopDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StopDBCluster(ctx, id)
	if err != nil {
		return nil, err
	}

	return &stopDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStartDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StartDBCluster(ctx, id)
	if err != nil {
		return nil, err
	}

	return &startDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleFailoverDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	targetInstanceID := vals.Get("TargetDBInstanceIdentifier")
	cluster, err := h.Backend.FailoverDBCluster(ctx, id, targetInstanceID)
	if err != nil {
		return nil, err
	}

	return &failoverDBClusterResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleAddRoleToDBCluster(ctx context.Context, vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")
	if err := h.Backend.AddRoleToDBCluster(ctx, clusterID, roleARN); err != nil {
		return nil, err
	}

	return &addRoleToDBClusterResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleRemoveRoleFromDBCluster(ctx context.Context, vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	roleARN := vals.Get("RoleArn")
	if err := h.Backend.RemoveRoleFromDBCluster(ctx, clusterID, roleARN); err != nil {
		return nil, err
	}

	return &removeRoleFromDBClusterResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handlePromoteReadReplicaDBCluster(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(ctx, id, DBClusterFilters{})
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

func (h *Handler) handleRestoreDBClusterFromSnapshot(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterFromSnapshot(ctx, snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromSnapshotResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterToPointInTime(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	srcClusterID := vals.Get("SourceDBClusterIdentifier")
	targetClusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterToPointInTime(ctx, srcClusterID, targetClusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterToPointInTimeResponse{
		Xmlns:     neptuneXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
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

func toXMLCluster(c *DBCluster) xmlDBCluster {
	memberItems := make([]xmlDBClusterMember, 0, len(c.DBClusterMembers))
	for _, m := range c.DBClusterMembers {
		memberItems = append(memberItems, xmlDBClusterMember(m))
	}
	vpcSGs := make([]xmlVpcSecurityGroupMembership, 0, len(c.VpcSecurityGroupIDs))
	for _, sgID := range c.VpcSecurityGroupIDs {
		vpcSGs = append(
			vpcSGs,
			xmlVpcSecurityGroupMembership{
				VpcSecurityGroupID: sgID,
				Status:             subscriptionStatusActive,
			},
		)
	}
	roles := make([]xmlDBRole, 0, len(c.AssociatedRoles))
	for _, roleARN := range c.AssociatedRoles {
		roles = append(roles, xmlDBRole{RoleArn: roleARN, Status: "ACTIVE"})
	}
	x := xmlDBCluster{
		DBClusterIdentifier:             c.DBClusterIdentifier,
		DBClusterArn:                    c.DBClusterArn,
		DBClusterResourceID:             c.DBClusterResourceID,
		ClusterCreateTime:               c.ClusterCreateTime,
		Engine:                          c.Engine,
		EngineVersion:                   c.EngineVersion,
		EngineMode:                      c.EngineMode,
		Status:                          c.Status,
		DBClusterParameterGroupName:     c.DBClusterParameterGroupName,
		DBSubnetGroupName:               c.DBSubnetGroupName,
		Endpoint:                        c.Endpoint,
		ReaderEndpoint:                  c.ReaderEndpoint,
		MasterUsername:                  c.MasterUsername,
		StorageType:                     c.StorageType,
		HostedZoneID:                    c.HostedZoneID,
		NetworkType:                     c.NetworkType,
		Port:                            c.Port,
		StorageEncrypted:                c.StorageEncrypted,
		MultiAZ:                         c.MultiAZ,
		BackupRetentionPeriod:           c.BackupRetentionPeriod,
		AllocatedStorage:                c.AllocatedStorage,
		EnableIAMDatabaseAuthentication: c.EnableIAMDatabaseAuthentication,
		DeletionProtection:              c.DeletionProtection,
		CopyTagsToSnapshot:              c.CopyTagsToSnapshot,
		PreferredBackupWindow:           c.PreferredBackupWindow,
		PreferredMaintenanceWindow:      c.PreferredMaintenanceWindow,
		KmsKeyID:                        c.KmsKeyID,
		DBClusterMembers:                xmlDBClusterMemberList{Members: memberItems},
		VpcSecurityGroups:               xmlVpcSecurityGroupMembershipList{Members: vpcSGs},
		AssociatedRoles:                 xmlDBRoleList{Members: roles},
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

type xmlVpcSecurityGroupMembership struct {
	VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
	Status             string `xml:"Status,omitempty"`
}

type xmlVpcSecurityGroupMembershipList struct {
	Members []xmlVpcSecurityGroupMembership `xml:"VpcSecurityGroupMembership"`
}

type xmlDBRole struct {
	RoleArn     string `xml:"RoleArn"`
	Status      string `xml:"Status,omitempty"`
	FeatureName string `xml:"FeatureName,omitempty"`
}

type xmlDBRoleList struct {
	Members []xmlDBRole `xml:"DBClusterRole"`
}

type xmlDBCluster struct {
	ServerlessV2ScalingConfiguration *xmlSV2Ref                        `xml:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret          *xmlMasterUserManagedSecret       `xml:"MasterUserManagedSecret,omitempty"`
	VpcSecurityGroups                xmlVpcSecurityGroupMembershipList `xml:"VpcSecurityGroups,omitempty"`
	AssociatedRoles                  xmlDBRoleList                     `xml:"AssociatedRoles,omitempty"`
	DBClusterIdentifier              string                            `xml:"DBClusterIdentifier"`
	DBClusterArn                     string                            `xml:"DBClusterArn,omitempty"`
	DBClusterResourceID              string                            `xml:"DbClusterResourceId,omitempty"`
	ClusterCreateTime                string                            `xml:"ClusterCreateTime,omitempty"`
	Engine                           string                            `xml:"Engine"`
	EngineVersion                    string                            `xml:"EngineVersion,omitempty"`
	EngineMode                       string                            `xml:"EngineMode,omitempty"`
	Status                           string                            `xml:"Status"`
	DBClusterParameterGroupName      string                            `xml:"DBClusterParameterGroup,omitempty"`
	DBSubnetGroupName                string                            `xml:"DBSubnetGroup,omitempty"`
	Endpoint                         string                            `xml:"Endpoint,omitempty"`
	ReaderEndpoint                   string                            `xml:"ReaderEndpoint,omitempty"`
	MasterUsername                   string                            `xml:"MasterUsername,omitempty"`
	StorageType                      string                            `xml:"StorageType,omitempty"`
	HostedZoneID                     string                            `xml:"HostedZoneId,omitempty"`
	NetworkType                      string                            `xml:"NetworkType,omitempty"`
	PreferredBackupWindow            string                            `xml:"PreferredBackupWindow,omitempty"`
	PreferredMaintenanceWindow       string                            `xml:"PreferredMaintenanceWindow,omitempty"`
	KmsKeyID                         string                            `xml:"KmsKeyId,omitempty"`
	DBClusterMembers                 xmlDBClusterMemberList            `xml:"DBClusterMembers"`
	Port                             int                               `xml:"Port"`
	BackupRetentionPeriod            int                               `xml:"BackupRetentionPeriod"`
	AllocatedStorage                 int                               `xml:"AllocatedStorage,omitempty"`
	EnableIAMDatabaseAuthentication  bool                              `xml:"IAMDatabaseAuthenticationEnabled"`
	StorageEncrypted                 bool                              `xml:"StorageEncrypted"`
	MultiAZ                          bool                              `xml:"MultiAZ"`
	DeletionProtection               bool                              `xml:"DeletionProtection"`
	CopyTagsToSnapshot               bool                              `xml:"CopyTagsToSnapshot"`
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

type addRoleToDBClusterResponse struct {
	XMLName xml.Name `xml:"AddRoleToDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeRoleFromDBClusterResponse struct {
	XMLName xml.Name `xml:"RemoveRoleFromDBClusterResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
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

// dispatchDBClusterAction handles DBCluster-family actions (CRUD, lifecycle,
// roles, restore/promote); see dispatch's doc comment for the chaining rationale.
func (h *Handler) dispatchDBClusterAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "CreateDBCluster":
		return h.handleCreateDBCluster(ctx, vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(ctx, vals)
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(ctx, vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(ctx, vals)
	case "StopDBCluster":
		return h.handleStopDBCluster(ctx, vals)
	case "StartDBCluster":
		return h.handleStartDBCluster(ctx, vals)
	case "FailoverDBCluster":
		return h.handleFailoverDBCluster(ctx, vals)
	case "AddRoleToDBCluster":
		return h.handleAddRoleToDBCluster(ctx, vals)
	case "RemoveRoleFromDBCluster":
		return h.handleRemoveRoleFromDBCluster(ctx, vals)
	case "PromoteReadReplicaDBCluster":
		return h.handlePromoteReadReplicaDBCluster(ctx, vals)
	case "RestoreDBClusterFromSnapshot":
		return h.handleRestoreDBClusterFromSnapshot(ctx, vals)
	case "RestoreDBClusterToPointInTime":
		return h.handleRestoreDBClusterToPointInTime(ctx, vals)
	default:
		return h.dispatchDBInstanceAction(ctx, action, vals)
	}
}
