package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// dispatchExtended9 routes RDS operations added in refinement 2.
// Split from dispatchExtended8 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended9(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBSecurityGroup":
		return h.handleCreateDBSecurityGroup(vals)
	case "RemoveFromGlobalCluster":
		return h.handleRemoveFromGlobalCluster(vals)
	case "FailoverGlobalCluster":
		return h.handleFailoverGlobalCluster(vals)
	case "SwitchoverGlobalCluster":
		return h.handleSwitchoverGlobalCluster(vals)
	case "SwitchoverReadReplica":
		return h.handleSwitchoverReadReplica(vals)
	case "PromoteReadReplicaDBCluster":
		return h.handlePromoteReadReplicaDBCluster(vals)
	case "DescribeAccountAttributes":
		return h.handleDescribeAccountAttributes(vals)
	case "DescribeCertificates":
		return h.handleDescribeCertificates(vals)
	case "ModifyCertificates":
		return h.handleModifyCertificates(vals)
	case "DescribePendingMaintenanceActions":
		return h.handleDescribePendingMaintenanceActions(vals)
	default:
		return h.dispatchExtended10(action, vals)
	}
}

// dispatchExtended10 routes additional RDS operations added in refinement 2.
// Split from dispatchExtended9 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended10(action string, vals url.Values) (any, error) {
	switch action {
	case "DescribeSourceRegions":
		return h.handleDescribeSourceRegions(vals)
	case "DescribeDBMajorEngineVersions":
		return h.handleDescribeDBMajorEngineVersions(vals)
	case "DescribeEngineDefaultParameters":
		return h.handleDescribeEngineDefaultParameters(vals)
	case "DescribeEngineDefaultClusterParameters":
		return h.handleDescribeEngineDefaultClusterParameters(vals)
	case "DescribeDBSnapshotAttributes":
		return h.handleDescribeDBSnapshotAttributes(vals)
	case "ModifyDBSnapshot":
		return h.handleModifyDBSnapshot(vals)
	case "ModifyDBSnapshotAttribute":
		return h.handleModifyDBSnapshotAttribute(vals)
	case "DescribeDBClusterSnapshotAttributes":
		return h.handleDescribeDBClusterSnapshotAttributes(vals)
	case "ModifyDBClusterSnapshotAttribute":
		return h.handleModifyDBClusterSnapshotAttribute(vals)
	case "DescribeDBClusterBacktracks":
		return h.handleDescribeDBClusterBacktracks(vals)
	case "EnableHttpEndpoint":
		return h.handleEnableHTTPEndpoint(vals)
	case "DisableHttpEndpoint":
		return h.handleDisableHTTPEndpoint(vals)
	case "ModifyCurrentDBClusterCapacity":
		return h.handleModifyCurrentDBClusterCapacity(vals)
	default:
		return h.dispatchExtended11(action, vals)
	}
}

// dispatchExtended11 routes remaining RDS operations added in refinement 2.
// Split from dispatchExtended10 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended11(action string, vals url.Values) (any, error) {
	switch action {
	case "RestoreDBInstanceFromS3":
		return h.handleRestoreDBInstanceFromS3(vals)
	case "RestoreDBClusterFromS3":
		return h.handleRestoreDBClusterFromS3(vals)
	case "ModifyDBRecommendation":
		return h.handleModifyDBRecommendation(vals)
	case "DescribeDBRecommendations":
		return h.handleDescribeDBRecommendations(vals)
	case "PurchaseReservedDBInstancesOffering":
		return h.handlePurchaseReservedDBInstancesOffering(vals)
	case "DescribeReservedDBInstances":
		return h.handleDescribeReservedDBInstances(vals)
	case "DescribeReservedDBInstancesOfferings":
		return h.handleDescribeReservedDBInstancesOfferings(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid RDS action", ErrUnknownAction, action)
	}
}

// ---- XML response types ----

type createDBSecurityGroupR2Response struct {
	XMLName         xml.Name           `xml:"CreateDBSecurityGroupResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"CreateDBSecurityGroupResult>DBSecurityGroup"`
}

type removeFromGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"RemoveFromGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"RemoveFromGlobalClusterResult>GlobalCluster"`
}

type failoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"FailoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"FailoverGlobalClusterResult>GlobalCluster"`
}

type switchoverGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"SwitchoverGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"SwitchoverGlobalClusterResult>GlobalCluster"`
}

type switchoverReadReplicaResponse struct {
	XMLName    xml.Name      `xml:"SwitchoverReadReplicaResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"SwitchoverReadReplicaResult>DBInstance"`
}

type promoteReadReplicaDBClusterResponse struct {
	XMLName   xml.Name     `xml:"PromoteReadReplicaDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"PromoteReadReplicaDBClusterResult>DBCluster"`
}

type xmlAccountAttribute struct {
	AttributeName string `xml:"AttributeName"`
	Used          int    `xml:"AccountQuotaName"`
	Max           int    `xml:"Max"`
}

type xmlAccountAttributeList struct {
	Members []xmlAccountAttribute `xml:"AccountQuota"`
}

type describeAccountAttributesResponse struct {
	XMLName           xml.Name                `xml:"DescribeAccountAttributesResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	AccountAttributes xmlAccountAttributeList `xml:"DescribeAccountAttributesResult>AccountQuotas"`
}

type xmlCertificate struct {
	CertificateIdentifier string `xml:"CertificateIdentifier"`
	CertificateType       string `xml:"CertificateType"`
	ValidFrom             string `xml:"ValidFrom,omitempty"`
	ValidTill             string `xml:"ValidTill,omitempty"`
	Thumbprint            string `xml:"Thumbprint,omitempty"`
	CustomerOverride      bool   `xml:"CustomerOverride,omitempty"`
}

type xmlCertificateList struct {
	Members []xmlCertificate `xml:"Certificate"`
}

type describeCertificatesResponse struct {
	XMLName      xml.Name           `xml:"DescribeCertificatesResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Certificates xmlCertificateList `xml:"DescribeCertificatesResult>Certificates"`
}

type modifyCertificatesResponse struct {
	XMLName     xml.Name       `xml:"ModifyCertificatesResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	Certificate xmlCertificate `xml:"ModifyCertificatesResult>Certificate"`
}

type xmlPendingMaintenanceActionResourceList struct {
	Members []xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type xmlPendingActionsWrapper struct {
	Actions xmlPendingMaintenanceActionResourceList `xml:"PendingMaintenanceActions"`
}

type describePendingMaintenanceActionsResponse struct {
	XMLName xml.Name                 `xml:"DescribePendingMaintenanceActionsResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlPendingActionsWrapper `xml:"DescribePendingMaintenanceActionsResult"`
}

type xmlSourceRegion struct {
	RegionName string `xml:"RegionName"`
	Endpoint   string `xml:"Endpoint,omitempty"`
	Status     string `xml:"Status,omitempty"`
}

type xmlSourceRegionList struct {
	Members []xmlSourceRegion `xml:"SourceRegion"`
}

type describeSourceRegionsResponse struct {
	XMLName       xml.Name            `xml:"DescribeSourceRegionsResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	SourceRegions xmlSourceRegionList `xml:"DescribeSourceRegionsResult>SourceRegions"`
}

type xmlDBMajorEngineVersion struct {
	Engine             string `xml:"Engine"`
	MajorEngineVersion string `xml:"MajorEngineVersion"`
	Status             string `xml:"Status,omitempty"`
}

type xmlDBMajorEngineVersionList struct {
	Members []xmlDBMajorEngineVersion `xml:"DBMajorEngineVersion"`
}

type describeDBMajorEngineVersionsResponse struct {
	XMLName               xml.Name                    `xml:"DescribeDBMajorEngineVersionsResponse"`
	Xmlns                 string                      `xml:"xmlns,attr"`
	DBMajorEngineVersions xmlDBMajorEngineVersionList `xml:"DescribeDBMajorEngineVersionsResult>DBMajorEngineVersions"`
}

type describeEngineDefaultParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeEngineDefaultParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>Parameters"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeEngineDefaultClusterParametersResult>EngineDefaults>Parameters"`
}

type xmlDBSnapshotAttribute struct {
	AttributeName   string          `xml:"AttributeName"`
	AttributeValues xmlStringValues `xml:"AttributeValues"`
}

type xmlStringValues struct {
	Members []string `xml:"AttributeValue"`
}

type xmlDBSnapshotAttributeList struct {
	Members []xmlDBSnapshotAttribute `xml:"DBSnapshotAttribute"`
}

type xmlDBSnapshotAttributesResult struct {
	DBSnapshotIdentifier string                     `xml:"DBSnapshotIdentifier"`
	DBSnapshotAttributes xmlDBSnapshotAttributeList `xml:"DBSnapshotAttributes"`
}

type describeDBSnapshotAttributesResponse struct {
	XMLName xml.Name                      `xml:"DescribeDBSnapshotAttributesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  xmlDBSnapshotAttributesResult `xml:"DescribeDBSnapshotAttributesResult>DBSnapshotAttributesResult"`
}

type modifyDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"ModifyDBSnapshotResult>DBSnapshot"`
}

type modifyDBSnapshotAttributeResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBSnapshotAttributeResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  xmlDBSnapshotAttributesResult `xml:"ModifyDBSnapshotAttributeResult>DBSnapshotAttributesResult"`
}

type xmlDBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                     `xml:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotAttributes xmlDBSnapshotAttributeList `xml:"DBClusterSnapshotAttributes"`
}

type xmlClusterSnapshotAttrWrapper struct {
	Result xmlDBClusterSnapshotAttributesResult `xml:"DBClusterSnapshotAttributesResult"`
}

type describeDBClusterSnapshotAttributesResponse struct {
	XMLName xml.Name                      `xml:"DescribeDBClusterSnapshotAttributesResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Wrapper xmlClusterSnapshotAttrWrapper `xml:"DescribeDBClusterSnapshotAttributesResult"`
}

type modifyDBClusterSnapshotAttributeResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBClusterSnapshotAttributeResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Wrapper xmlClusterSnapshotAttrWrapper `xml:"ModifyDBClusterSnapshotAttributeResult"`
}

type xmlDBClusterBacktrackList struct {
	Members []xmlDBClusterBacktrack `xml:"DBClusterBacktrack"`
}

type describeDBClusterBacktracksResponse struct {
	XMLName             xml.Name                  `xml:"DescribeDBClusterBacktracksResponse"`
	Xmlns               string                    `xml:"xmlns,attr"`
	DBClusterBacktracks xmlDBClusterBacktrackList `xml:"DescribeDBClusterBacktracksResult>DBClusterBacktracks"`
}

type enableHTTPEndpointResponse struct {
	XMLName     xml.Name `xml:"EnableHttpEndpointResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	HTTPEnabled bool     `xml:"EnableHttpEndpointResult>HttpEndpointEnabled"`
}

type disableHTTPEndpointResponse struct {
	XMLName     xml.Name `xml:"DisableHttpEndpointResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	HTTPEnabled bool     `xml:"DisableHttpEndpointResult>HttpEndpointEnabled"`
}

type modifyCurrentDBClusterCapacityResponse struct {
	XMLName             xml.Name `xml:"ModifyCurrentDBClusterCapacityResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	DBClusterIdentifier string   `xml:"ModifyCurrentDBClusterCapacityResult>DBClusterIdentifier"`
	CurrentCapacity     int      `xml:"ModifyCurrentDBClusterCapacityResult>CurrentCapacity"`
}

type restoreDBInstanceFromS3Response struct {
	XMLName    xml.Name      `xml:"RestoreDBInstanceFromS3Response"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RestoreDBInstanceFromS3Result>DBInstance"`
}

type restoreDBClusterFromS3Response struct {
	XMLName   xml.Name     `xml:"RestoreDBClusterFromS3Response"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"RestoreDBClusterFromS3Result>DBCluster"`
}

type xmlDBRecommendation struct {
	RecommendationID string `xml:"RecommendationId"`
	TypeID           string `xml:"TypeId,omitempty"`
	Severity         string `xml:"Severity,omitempty"`
	Status           string `xml:"Status,omitempty"`
	Description      string `xml:"Description,omitempty"`
	Reason           string `xml:"Reason,omitempty"`
	ResourceARN      string `xml:"ResourceArn,omitempty"`
	UpdatedTime      string `xml:"UpdatedTime,omitempty"`
	CreatedTime      string `xml:"CreatedTime,omitempty"`
}

type xmlDBRecommendationList struct {
	Members []xmlDBRecommendation `xml:"DBRecommendation"`
}

type modifyDBRecommendationResponse struct {
	XMLName          xml.Name            `xml:"ModifyDBRecommendationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBRecommendation xmlDBRecommendation `xml:"ModifyDBRecommendationResult>DBRecommendation"`
}

type describeDBRecommendationsResponse struct {
	XMLName           xml.Name                `xml:"DescribeDBRecommendationsResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	DBRecommendations xmlDBRecommendationList `xml:"DescribeDBRecommendationsResult>DBRecommendations"`
}

type xmlReservedDBInstance struct {
	ReservedDBInstanceID          string  `xml:"ReservedDBInstanceId"`
	ReservedDBInstancesOfferingID string  `xml:"ReservedDBInstancesOfferingId,omitempty"`
	DBInstanceClass               string  `xml:"DBInstanceClass,omitempty"`
	StartTime                     string  `xml:"StartTime,omitempty"`
	ProductDescription            string  `xml:"ProductDescription,omitempty"`
	OfferingType                  string  `xml:"OfferingType,omitempty"`
	State                         string  `xml:"State,omitempty"`
	CurrencyCode                  string  `xml:"CurrencyCode,omitempty"`
	FixedPrice                    float64 `xml:"FixedPrice,omitempty"`
	UsagePrice                    float64 `xml:"UsagePrice,omitempty"`
	Duration                      int     `xml:"Duration,omitempty"`
	DBInstanceCount               int     `xml:"DBInstanceCount,omitempty"`
	MultiAZ                       bool    `xml:"MultiAZ,omitempty"`
}

type xmlReservedDBInstanceList struct {
	Members []xmlReservedDBInstance `xml:"ReservedDBInstance"`
}

type purchaseReservedDBInstancesOfferingResponse struct {
	XMLName            xml.Name              `xml:"PurchaseReservedDBInstancesOfferingResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	ReservedDBInstance xmlReservedDBInstance `xml:"PurchaseReservedDBInstancesOfferingResult>ReservedDBInstance"`
}

type describeReservedDBInstancesResponse struct {
	XMLName             xml.Name                  `xml:"DescribeReservedDBInstancesResponse"`
	Xmlns               string                    `xml:"xmlns,attr"`
	ReservedDBInstances xmlReservedDBInstanceList `xml:"DescribeReservedDBInstancesResult>ReservedDBInstances"`
}

type xmlReservedDBInstancesOffering struct {
	ReservedDBInstancesOfferingID string  `xml:"ReservedDBInstancesOfferingId"`
	DBInstanceClass               string  `xml:"DBInstanceClass,omitempty"`
	ProductDescription            string  `xml:"ProductDescription,omitempty"`
	OfferingType                  string  `xml:"OfferingType,omitempty"`
	CurrencyCode                  string  `xml:"CurrencyCode,omitempty"`
	FixedPrice                    float64 `xml:"FixedPrice,omitempty"`
	UsagePrice                    float64 `xml:"UsagePrice,omitempty"`
	Duration                      int     `xml:"Duration,omitempty"`
	MultiAZ                       bool    `xml:"MultiAZ,omitempty"`
}

type xmlReservedDBInstancesOfferingList struct {
	Members []xmlReservedDBInstancesOffering `xml:"ReservedDBInstancesOffering"`
}

type xmlReservedOfferingsWrapper struct {
	Offerings xmlReservedDBInstancesOfferingList `xml:"ReservedDBInstancesOfferings"`
}

type describeReservedDBInstancesOfferingsResponse struct {
	XMLName xml.Name                    `xml:"DescribeReservedDBInstancesOfferingsResponse"`
	Xmlns   string                      `xml:"xmlns,attr"`
	Result  xmlReservedOfferingsWrapper `xml:"DescribeReservedDBInstancesOfferingsResult"`
}

// ---- Handler functions ----

func (h *Handler) handleCreateDBSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	description := vals.Get("DBSecurityGroupDescription")
	sg, err := h.Backend.CreateDBSecurityGroup(name, description)
	if err != nil {
		return nil, err
	}

	return &createDBSecurityGroupR2Response{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
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
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleFailoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDB := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.FailoverGlobalCluster(globalClusterID, targetDB)
	if err != nil {
		return nil, err
	}

	return &failoverGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverGlobalCluster(vals url.Values) (any, error) {
	globalClusterID := vals.Get("GlobalClusterIdentifier")
	targetDB := vals.Get("TargetDbClusterIdentifier")
	gc, err := h.Backend.SwitchoverGlobalCluster(globalClusterID, targetDB)
	if err != nil {
		return nil, err
	}

	return &switchoverGlobalClusterResponse{
		Xmlns:         rdsXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleSwitchoverReadReplica(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.SwitchoverReadReplica(instanceID)
	if err != nil {
		return nil, err
	}

	return &switchoverReadReplicaResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handlePromoteReadReplicaDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.PromoteReadReplicaDBCluster(clusterID)
	if err != nil {
		return nil, err
	}

	return &promoteReadReplicaDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeAccountAttributes(_ url.Values) (any, error) {
	attrs := h.Backend.DescribeAccountAttributes()
	members := make([]xmlAccountAttribute, 0, len(attrs))
	for _, a := range attrs {
		members = append(members, xmlAccountAttribute(a))
	}

	return &describeAccountAttributesResponse{
		Xmlns:             rdsXMLNS,
		AccountAttributes: xmlAccountAttributeList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeCertificates(vals url.Values) (any, error) {
	certID := vals.Get("CertificateIdentifier")
	certs, err := h.Backend.DescribeCertificates(certID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, toXMLCertificate(c))
	}

	return &describeCertificatesResponse{
		Xmlns:        rdsXMLNS,
		Certificates: xmlCertificateList{Members: members},
	}, nil
}

func (h *Handler) handleModifyCertificates(vals url.Values) (any, error) {
	certID := vals.Get("CertificateIdentifier")
	cert, err := h.Backend.ModifyCertificates(certID)
	if err != nil {
		return nil, err
	}

	return &modifyCertificatesResponse{
		Xmlns:       rdsXMLNS,
		Certificate: toXMLCertificate(*cert),
	}, nil
}

func (h *Handler) handleDescribePendingMaintenanceActions(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	actions := h.Backend.DescribePendingMaintenanceActions(resourceARN)
	members := make([]xmlResourcePendingMaintenanceActions, 0, len(actions))
	for _, a := range actions {
		members = append(members, xmlResourcePendingMaintenanceActions{
			ResourceIdentifier: a.ResourceIdentifier,
			PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{
				Members: []xmlPendingMaintenanceAction{
					{Action: a.Action, Description: a.Description},
				},
			},
		})
	}

	return &describePendingMaintenanceActionsResponse{
		Xmlns:  rdsXMLNS,
		Result: xmlPendingActionsWrapper{Actions: xmlPendingMaintenanceActionResourceList{Members: members}},
	}, nil
}

func (h *Handler) handleDescribeSourceRegions(vals url.Values) (any, error) {
	regionName := vals.Get("RegionName")
	regions := h.Backend.DescribeSourceRegions(regionName)
	members := make([]xmlSourceRegion, 0, len(regions))
	for _, r := range regions {
		members = append(members, xmlSourceRegion(r))
	}

	return &describeSourceRegionsResponse{
		Xmlns:         rdsXMLNS,
		SourceRegions: xmlSourceRegionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeDBMajorEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	versions := h.Backend.DescribeDBMajorEngineVersions(engine)
	members := make([]xmlDBMajorEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBMajorEngineVersion(v))
	}

	return &describeDBMajorEngineVersionsResponse{
		Xmlns:                 rdsXMLNS,
		DBMajorEngineVersions: xmlDBMajorEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultParameters(family)
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
		})
	}

	return &describeEngineDefaultParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultClusterParameters(family)
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
		})
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeDBSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	result, err := h.Backend.DescribeDBSnapshotAttributes(snapshotID)
	if err != nil {
		return nil, err
	}

	return &describeDBSnapshotAttributesResponse{
		Xmlns:  rdsXMLNS,
		Result: toXMLSnapshotAttributesResult(result),
	}, nil
}

func (h *Handler) handleModifyDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	optionGroupName := vals.Get("OptionGroupName")
	engineVersion := vals.Get("EngineVersion")
	snap, err := h.Backend.ModifyDBSnapshot(snapshotID, optionGroupName, engineVersion)
	if err != nil {
		return nil, err
	}

	return &modifyDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleModifyDBSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	valuesToAdd := extractMemberList(vals, "ValuesToAdd.member.")
	valuesToRemove := extractMemberList(vals, "ValuesToRemove.member.")
	result, err := h.Backend.ModifyDBSnapshotAttribute(snapshotID, attributeName, valuesToAdd, valuesToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyDBSnapshotAttributeResponse{
		Xmlns:  rdsXMLNS,
		Result: toXMLSnapshotAttributesResult(result),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshotAttributes(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	result, err := h.Backend.DescribeDBClusterSnapshotAttributes(snapshotID)
	if err != nil {
		return nil, err
	}

	return &describeDBClusterSnapshotAttributesResponse{
		Xmlns:   rdsXMLNS,
		Wrapper: xmlClusterSnapshotAttrWrapper{Result: toXMLClusterSnapshotAttributesResult(result)},
	}, nil
}

func (h *Handler) handleModifyDBClusterSnapshotAttribute(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	attributeName := vals.Get("AttributeName")
	valuesToAdd := extractMemberList(vals, "ValuesToAdd.member.")
	valuesToRemove := extractMemberList(vals, "ValuesToRemove.member.")
	result, err := h.Backend.ModifyDBClusterSnapshotAttribute(snapshotID, attributeName, valuesToAdd, valuesToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterSnapshotAttributeResponse{
		Xmlns:   rdsXMLNS,
		Wrapper: xmlClusterSnapshotAttrWrapper{Result: toXMLClusterSnapshotAttributesResult(result)},
	}, nil
}

func (h *Handler) handleDescribeDBClusterBacktracks(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	backtracks, err := h.Backend.DescribeDBClusterBacktracks(clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterBacktrack, 0, len(backtracks))
	for _, bt := range backtracks {
		members = append(members, xmlDBClusterBacktrack(bt))
	}

	return &describeDBClusterBacktracksResponse{
		Xmlns:               rdsXMLNS,
		DBClusterBacktracks: xmlDBClusterBacktrackList{Members: members},
	}, nil
}

func (h *Handler) handleEnableHTTPEndpoint(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if err := h.Backend.EnableHTTPEndpoint(resourceARN); err != nil {
		return nil, err
	}

	return &enableHTTPEndpointResponse{
		Xmlns:       rdsXMLNS,
		HTTPEnabled: true,
	}, nil
}

func (h *Handler) handleDisableHTTPEndpoint(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceArn")
	if err := h.Backend.DisableHTTPEndpoint(resourceARN); err != nil {
		return nil, err
	}

	return &disableHTTPEndpointResponse{
		Xmlns:       rdsXMLNS,
		HTTPEnabled: false,
	}, nil
}

func (h *Handler) handleModifyCurrentDBClusterCapacity(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	capacityStr := vals.Get("Capacity")
	capacity, _ := strconv.Atoi(capacityStr)
	cluster, err := h.Backend.ModifyCurrentDBClusterCapacity(clusterID, capacity)
	if err != nil {
		return nil, err
	}

	return &modifyCurrentDBClusterCapacityResponse{
		Xmlns:               rdsXMLNS,
		DBClusterIdentifier: cluster.DBClusterIdentifier,
		CurrentCapacity:     cluster.ServerlessCapacity,
	}, nil
}

func (h *Handler) handleRestoreDBInstanceFromS3(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	engine := vals.Get("Engine")
	dbInstanceClass := vals.Get("DBInstanceClass")
	s3Bucket := vals.Get("S3BucketName")
	inst, err := h.Backend.RestoreDBInstanceFromS3(id, engine, dbInstanceClass, s3Bucket)
	if err != nil {
		return nil, err
	}

	return &restoreDBInstanceFromS3Response{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleRestoreDBClusterFromS3(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	masterUsername := vals.Get("MasterUsername")
	s3Bucket := vals.Get("S3BucketName")
	cluster, err := h.Backend.RestoreDBClusterFromS3(id, engine, masterUsername, s3Bucket)
	if err != nil {
		return nil, err
	}

	return &restoreDBClusterFromS3Response{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBRecommendation(vals url.Values) (any, error) {
	recID := vals.Get("RecommendationId")
	status := vals.Get("Status")
	rec, err := h.Backend.ModifyDBRecommendation(recID, status)
	if err != nil {
		return nil, err
	}

	return &modifyDBRecommendationResponse{
		Xmlns:            rdsXMLNS,
		DBRecommendation: toXMLRecommendation(rec),
	}, nil
}

func (h *Handler) handleDescribeDBRecommendations(vals url.Values) (any, error) {
	recID := vals.Get("RecommendationId")
	status := vals.Get("Status")
	recs := h.Backend.DescribeDBRecommendations(recID, status)
	members := make([]xmlDBRecommendation, 0, len(recs))
	for i := range recs {
		members = append(members, toXMLRecommendation(&recs[i]))
	}

	return &describeDBRecommendationsResponse{
		Xmlns:             rdsXMLNS,
		DBRecommendations: xmlDBRecommendationList{Members: members},
	}, nil
}

func (h *Handler) handlePurchaseReservedDBInstancesOffering(vals url.Values) (any, error) {
	offeringID := vals.Get("ReservedDBInstancesOfferingId")
	reservedID := vals.Get("ReservedDBInstanceId")
	countStr := vals.Get("DBInstanceCount")
	count := 1
	if countStr != "" {
		if n, err := strconv.Atoi(countStr); err == nil {
			count = n
		}
	}
	ri, err := h.Backend.PurchaseReservedDBInstancesOffering(offeringID, reservedID, count)
	if err != nil {
		return nil, err
	}

	return &purchaseReservedDBInstancesOfferingResponse{
		Xmlns:              rdsXMLNS,
		ReservedDBInstance: toXMLReservedDBInstance(ri),
	}, nil
}

func (h *Handler) handleDescribeReservedDBInstances(vals url.Values) (any, error) {
	reservedID := vals.Get("ReservedDBInstanceId")
	dbInstanceClass := vals.Get("DBInstanceClass")
	instances := h.Backend.DescribeReservedDBInstances(reservedID, dbInstanceClass)
	members := make([]xmlReservedDBInstance, 0, len(instances))
	for i := range instances {
		members = append(members, toXMLReservedDBInstance(&instances[i]))
	}

	return &describeReservedDBInstancesResponse{
		Xmlns:               rdsXMLNS,
		ReservedDBInstances: xmlReservedDBInstanceList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeReservedDBInstancesOfferings(vals url.Values) (any, error) {
	offeringID := vals.Get("ReservedDBInstancesOfferingId")
	dbInstanceClass := vals.Get("DBInstanceClass")
	offerings := h.Backend.DescribeReservedDBInstancesOfferings(offeringID, dbInstanceClass)
	members := make([]xmlReservedDBInstancesOffering, 0, len(offerings))
	for _, o := range offerings {
		members = append(members, xmlReservedDBInstancesOffering(o))
	}

	return &describeReservedDBInstancesOfferingsResponse{
		Xmlns: rdsXMLNS,
		Result: xmlReservedOfferingsWrapper{
			Offerings: xmlReservedDBInstancesOfferingList{Members: members},
		},
	}, nil
}

// ---- XML conversion helpers ----

func toXMLCertificate(c Certificate) xmlCertificate {
	return xmlCertificate{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateType:       c.CertificateType,
		ValidFrom:             c.ValidFrom,
		ValidTill:             c.ValidTill,
		Thumbprint:            c.Thumbprint,
		CustomerOverride:      c.CustomerOverride,
	}
}

func toXMLSnapshotAttributesResult(r *DBSnapshotAttributesResult) xmlDBSnapshotAttributesResult {
	attrs := make([]xmlDBSnapshotAttribute, 0, len(r.DBSnapshotAttributes))
	for _, a := range r.DBSnapshotAttributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		attrs = append(attrs, xmlDBSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlStringValues{Members: vals},
		})
	}

	return xmlDBSnapshotAttributesResult{
		DBSnapshotIdentifier: r.DBSnapshotIdentifier,
		DBSnapshotAttributes: xmlDBSnapshotAttributeList{Members: attrs},
	}
}

func toXMLClusterSnapshotAttributesResult(r *DBClusterSnapshotAttributesResult) xmlDBClusterSnapshotAttributesResult {
	attrs := make([]xmlDBSnapshotAttribute, 0, len(r.DBClusterSnapshotAttributes))
	for _, a := range r.DBClusterSnapshotAttributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		attrs = append(attrs, xmlDBSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: xmlStringValues{Members: vals},
		})
	}

	return xmlDBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: r.DBClusterSnapshotIdentifier,
		DBClusterSnapshotAttributes: xmlDBSnapshotAttributeList{Members: attrs},
	}
}

func toXMLRecommendation(r *DBRecommendation) xmlDBRecommendation {
	return xmlDBRecommendation{
		RecommendationID: r.RecommendationID,
		TypeID:           r.TypeID,
		Severity:         r.Severity,
		Status:           r.Status,
		Description:      r.Description,
		Reason:           r.Reason,
		ResourceARN:      r.ResourceARN,
		UpdatedTime:      r.UpdatedTime,
		CreatedTime:      r.CreatedTime,
	}
}

func toXMLReservedDBInstance(r *ReservedDBInstance) xmlReservedDBInstance {
	return xmlReservedDBInstance{
		ReservedDBInstanceID:          r.ReservedDBInstanceID,
		ReservedDBInstancesOfferingID: r.ReservedDBInstancesOfferingID,
		DBInstanceClass:               r.DBInstanceClass,
		StartTime:                     r.StartTime,
		Duration:                      r.Duration,
		FixedPrice:                    r.FixedPrice,
		UsagePrice:                    r.UsagePrice,
		DBInstanceCount:               r.DBInstanceCount,
		ProductDescription:            r.ProductDescription,
		OfferingType:                  r.OfferingType,
		MultiAZ:                       r.MultiAZ,
		State:                         r.State,
		CurrencyCode:                  r.CurrencyCode,
	}
}

func extractMemberList(vals url.Values, prefix string) []string {
	result := make([]string, 0)
	for i := 1; ; i++ {
		v := vals.Get(prefix + strconv.Itoa(i))
		if v == "" {
			break
		}
		result = append(result, v)
	}

	return result
}
