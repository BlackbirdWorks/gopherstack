package docdb

import (
	"context"
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateDBInstance(ctx context.Context, vals url.Values) (any, error) {
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
	inst, err := h.Backend.CreateDBInstance(ctx, id, clusterID, instanceClass, engine, promotionTier, tags, opts)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	instances, err := h.Backend.DescribeDBInstances(ctx, id, clusterID)
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

func (h *Handler) handleDeleteDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.DeleteDBInstance(ctx, id)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleModifyDBInstance(ctx context.Context, vals url.Values) (any, error) {
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
		ctx, id, instanceClass, autoMinorVersionUpgrade, preferredMaintenanceWindow, opts,
	)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleRebootDBInstance(ctx context.Context, vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(ctx, id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
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
