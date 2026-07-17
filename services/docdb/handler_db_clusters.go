package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateDBCluster(ctx context.Context, vals url.Values) (any, error) {
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
		ctx,
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

func (h *Handler) handleDescribeDBClusters(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(ctx, id)
	if err != nil {
		return nil, err
	}
	xmlClusters := make([]xmlDBCluster, 0, len(clusters))
	for _, c := range clusters {
		cp := c
		clusterXML := toXMLCluster(&cp)
		instMembers := h.Backend.GetClusterMembers(ctx, cp.DBClusterIdentifier)
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

func (h *Handler) handleDeleteDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	opts := &DeleteDBClusterOptions{
		SkipFinalSnapshot:                vals.Get("SkipFinalSnapshot") == stringTrue,
		FinalDBClusterSnapshotIdentifier: vals.Get("FinalDBClusterSnapshotIdentifier"),
	}
	cluster, err := h.Backend.DeleteDBCluster(ctx, id, opts)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(ctx context.Context, vals url.Values) (any, error) {
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
		EngineVersion:          vals.Get("EngineVersion"),
		MasterUserPassword:     vals.Get("MasterUserPassword"),
		NewDBClusterIdentifier: vals.Get("NewDBClusterIdentifier"),
		VpcSecurityGroupIDs:    parseVpcSecurityGroupIDs(vals),
		EnableLogsTypes:        parseCloudwatchEnableLogTypes(vals),
		DisableLogsTypes:       parseCloudwatchDisableLogTypes(vals),
		Port:                   port,
	}

	cluster, err := h.Backend.ModifyDBCluster(
		ctx,
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

func (h *Handler) handleStopDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StopDBCluster(ctx, id)
	if err != nil {
		return nil, err
	}

	return &stopDBClusterResponse{
		Xmlns:     docdbXMLNS,
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
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleFailoverDBCluster(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.FailoverDBCluster(ctx, id)
	if err != nil {
		return nil, err
	}

	return &failoverDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterFromSnapshot(ctx context.Context, vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	cluster, err := h.Backend.RestoreDBClusterFromSnapshot(ctx, snapshotID, clusterID, engine)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromSnapshotResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRestoreDBClusterToPointInTime(ctx context.Context, vals url.Values) (any, error) {
	sourceClusterID := vals.Get("SourceDBClusterIdentifier")
	targetClusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RestoreDBClusterToPointInTime(ctx, sourceClusterID, targetClusterID)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterToPointInTimeResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
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
	azMembers := make([]string, len(c.AvailabilityZones))
	copy(azMembers, c.AvailabilityZones)

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
		AvailabilityZones:                xmlAvailabilityZoneList{Members: azMembers},
	}
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

// xmlAvailabilityZoneList models the DBCluster.AvailabilityZones wire shape:
// a list of plain-string <AvailabilityZone> elements (unlike the singular
// AvailabilityZone document used for DBSubnetGroup.Subnet.SubnetAvailabilityZone,
// which nests a <Name> child). See awsAwsquery_deserializeDocumentAvailabilityZones
// in the real SDK, which reads each <AvailabilityZone> element's text value
// directly -- nesting a <Name> child here (as this type previously did) would
// make every real client parse the AZ as an empty string.
type xmlAvailabilityZoneList struct {
	Members []string `xml:"AvailabilityZone"`
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
	AvailabilityZones                xmlAvailabilityZoneList           `xml:"AvailabilityZones"`
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

// parseAvailabilityZones parses the AvailabilityZones list. The real
// aws-sdk-go-v2 query-protocol serializer
// (awsAwsquery_serializeDocumentAvailabilityZones) encodes each element as
// "AvailabilityZones.AvailabilityZone.N", not the generic
// "AvailabilityZones.member.N".
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

// parseVpcSecurityGroupIDs parses the VpcSecurityGroupIds list. The real
// aws-sdk-go-v2 query-protocol serializer
// (awsAwsquery_serializeDocumentVpcSecurityGroupIdList) encodes each element
// as "VpcSecurityGroupIds.VpcSecurityGroupId.N", not the generic
// "VpcSecurityGroupIds.member.N".
func parseVpcSecurityGroupIDs(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("VpcSecurityGroupIds.VpcSecurityGroupId.%d", i))
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
