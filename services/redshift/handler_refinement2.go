package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

const (
	modelVersion10 = "1.0"
)

// ---- DeletePartner ----

type deletePartnerResponse struct {
	XMLName           xml.Name `xml:"DeletePartnerResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	ClusterIdentifier string   `xml:"DeletePartnerResult>ClusterIdentifier"`
	DatabaseName      string   `xml:"DeletePartnerResult>DatabaseName"`
	PartnerName       string   `xml:"DeletePartnerResult>PartnerIntegrationId"`
}

func (h *Handler) handleDeletePartner(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerIntegrationId")

	if accountID == "" {
		accountID = h.Backend.AccountID()
	}

	if err := h.Backend.DeletePartner(accountID, clusterID, databaseName, partnerName); err != nil {
		return nil, err
	}

	return &deletePartnerResponse{
		Xmlns:             redshiftXMLNS,
		ClusterIdentifier: clusterID,
		DatabaseName:      databaseName,
		PartnerName:       partnerName,
	}, nil
}

// ---- DescribePartners ----

type xmlPartner struct {
	ClusterIdentifier string `xml:"ClusterIdentifier"`
	DatabaseName      string `xml:"DatabaseName"`
	PartnerName       string `xml:"PartnerIntegrationId"`
	Status            string `xml:"Status,omitempty"`
	StatusMessage     string `xml:"StatusMessage,omitempty"`
}

type xmlPartnerList struct {
	Members []xmlPartner `xml:"PartnerIntegrationInfo"`
}

type describePartnersResponse struct {
	XMLName  xml.Name       `xml:"DescribePartnersResponse"`
	Xmlns    string         `xml:"xmlns,attr"`
	Partners xmlPartnerList `xml:"DescribePartnersResult>PartnerIntegrationInfoList"`
}

func partnerToXML(p *Partner) xmlPartner {
	return xmlPartner{
		ClusterIdentifier: p.ClusterIdentifier,
		DatabaseName:      p.DatabaseName,
		PartnerName:       p.PartnerName,
		Status:            p.Status,
		StatusMessage:     p.StatusMessage,
	}
}

func (h *Handler) handleDescribePartners(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerIntegrationId")

	if accountID == "" {
		accountID = h.Backend.AccountID()
	}

	partners, err := h.Backend.DescribePartners(accountID, clusterID, databaseName, partnerName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlPartner, 0, len(partners))

	for _, p := range partners {
		pp := p
		members = append(members, partnerToXML(&pp))
	}

	return &describePartnersResponse{
		Xmlns:    redshiftXMLNS,
		Partners: xmlPartnerList{Members: members},
	}, nil
}

// ---- UpdatePartnerStatus ----

type updatePartnerStatusResponse struct {
	XMLName           xml.Name `xml:"UpdatePartnerStatusResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	ClusterIdentifier string   `xml:"UpdatePartnerStatusResult>ClusterIdentifier"`
	DatabaseName      string   `xml:"UpdatePartnerStatusResult>DatabaseName"`
	PartnerName       string   `xml:"UpdatePartnerStatusResult>PartnerIntegrationId"`
}

func (h *Handler) handleUpdatePartnerStatus(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerIntegrationId")
	status := vals.Get("Status")
	statusMessage := vals.Get("StatusMessage")

	if accountID == "" {
		accountID = h.Backend.AccountID()
	}

	p, err := h.Backend.UpdatePartnerStatus(accountID, clusterID, databaseName, partnerName, status, statusMessage)
	if err != nil {
		return nil, err
	}

	return &updatePartnerStatusResponse{
		Xmlns:             redshiftXMLNS,
		ClusterIdentifier: p.ClusterIdentifier,
		DatabaseName:      p.DatabaseName,
		PartnerName:       p.PartnerName,
	}, nil
}

// ---- DescribeDataShares ----

type xmlDataShareList struct {
	Members []xmlDataShare `xml:"DataShare"`
}

type describeDataSharesResponse struct {
	XMLName    xml.Name         `xml:"DescribeDataSharesResponse"`
	Xmlns      string           `xml:"xmlns,attr"`
	DataShares xmlDataShareList `xml:"DescribeDataSharesResult>DataShares"`
}

func (h *Handler) handleDescribeDataShares(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")

	shares, err := h.Backend.DescribeDataShares(dataShareArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDataShare, 0, len(shares))

	for _, s := range shares {
		sp := s
		members = append(members, dataShareToXML(&sp))
	}

	return &describeDataSharesResponse{
		Xmlns:      redshiftXMLNS,
		DataShares: xmlDataShareList{Members: members},
	}, nil
}

// ---- DescribeDataSharesForConsumer ----

type describeDataSharesForConsumerResponse struct {
	XMLName    xml.Name         `xml:"DescribeDataSharesForConsumerResponse"`
	Xmlns      string           `xml:"xmlns,attr"`
	DataShares xmlDataShareList `xml:"DescribeDataSharesForConsumerResult>DataShares"`
}

func (h *Handler) handleDescribeDataSharesForConsumer(vals url.Values) (any, error) {
	consumerArn := vals.Get("ConsumerArn")
	status := vals.Get("Status")

	shares, err := h.Backend.DescribeDataSharesForConsumer(consumerArn, status)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDataShare, 0, len(shares))

	for _, s := range shares {
		sp := s
		members = append(members, dataShareToXML(&sp))
	}

	return &describeDataSharesForConsumerResponse{
		Xmlns:      redshiftXMLNS,
		DataShares: xmlDataShareList{Members: members},
	}, nil
}

// ---- DescribeDataSharesForProducer ----

type describeDataSharesForProducerResponse struct {
	XMLName    xml.Name         `xml:"DescribeDataSharesForProducerResponse"`
	Xmlns      string           `xml:"xmlns,attr"`
	DataShares xmlDataShareList `xml:"DescribeDataSharesForProducerResult>DataShares"`
}

func (h *Handler) handleDescribeDataSharesForProducer(vals url.Values) (any, error) {
	producerArn := vals.Get("ProducerArn")
	status := vals.Get("Status")

	shares, err := h.Backend.DescribeDataSharesForProducer(producerArn, status)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDataShare, 0, len(shares))

	for _, s := range shares {
		sp := s
		members = append(members, dataShareToXML(&sp))
	}

	return &describeDataSharesForProducerResponse{
		Xmlns:      redshiftXMLNS,
		DataShares: xmlDataShareList{Members: members},
	}, nil
}

// ---- DeauthorizeDataShare ----

type deauthorizeDataShareResponse struct {
	XMLName xml.Name     `xml:"DeauthorizeDataShareResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"DeauthorizeDataShareResult"`
}

func (h *Handler) handleDeauthorizeDataShare(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerIdentifier := vals.Get("ConsumerIdentifier")

	ds, err := h.Backend.DeauthorizeDataShare(dataShareArn, consumerIdentifier)
	if err != nil {
		return nil, err
	}

	return &deauthorizeDataShareResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- DisassociateDataShareConsumer ----

type disassociateDataShareConsumerResponse struct {
	XMLName xml.Name     `xml:"DisassociateDataShareConsumerResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"DisassociateDataShareConsumerResult"`
}

func (h *Handler) handleDisassociateDataShareConsumer(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerArn := vals.Get("ConsumerArn")
	consumerRegion := vals.Get("ConsumerRegion")
	disassociateEntireAccount := vals.Get("DisassociateEntireAccount") == paramValueTrue

	ds, err := h.Backend.DisassociateDataShareConsumer(
		dataShareArn,
		consumerArn,
		consumerRegion,
		disassociateEntireAccount,
	)
	if err != nil {
		return nil, err
	}

	return &disassociateDataShareConsumerResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- RejectDataShare ----

type rejectDataShareResponse struct {
	XMLName xml.Name     `xml:"RejectDataShareResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"RejectDataShareResult"`
}

func (h *Handler) handleRejectDataShare(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")

	ds, err := h.Backend.RejectDataShare(dataShareArn)
	if err != nil {
		return nil, err
	}

	return &rejectDataShareResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- DescribeEndpointAuthorization ----

type xmlEndpointAuthorizationList struct {
	Members []xmlEndpointAuthorization `xml:"EndpointAuthorization"`
}

type describeEndpointAuthorizationResponse struct {
	XMLName        xml.Name                     `xml:"DescribeEndpointAuthorizationResponse"`
	Xmlns          string                       `xml:"xmlns,attr"`
	Authorizations xmlEndpointAuthorizationList `xml:"DescribeEndpointAuthorizationResult>EndpointAuthorizationList"`
}

func (h *Handler) handleDescribeEndpointAuthorization(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	grantee := vals.Get("Grantee") == paramValueTrue

	auths, err := h.Backend.DescribeEndpointAuthorization(clusterID, account, grantee)
	if err != nil {
		return nil, err
	}

	members := make([]xmlEndpointAuthorization, 0, len(auths))

	for _, a := range auths {
		ap := a
		members = append(members, endpointAuthToXML(&ap))
	}

	return &describeEndpointAuthorizationResponse{
		Xmlns:          redshiftXMLNS,
		Authorizations: xmlEndpointAuthorizationList{Members: members},
	}, nil
}

// ---- RevokeEndpointAccess ----

type revokeEndpointAccessResponse struct {
	XMLName xml.Name                 `xml:"RevokeEndpointAccessResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  xmlEndpointAuthorization `xml:"RevokeEndpointAccessResult"`
}

func (h *Handler) handleRevokeEndpointAccess(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	account := vals.Get("Account")
	vpcIDs := parseStringList(vals, "VpcIds.VpcIdentifier.")
	force := vals.Get("Force") == paramValueTrue

	ea, err := h.Backend.RevokeEndpointAccess(clusterID, account, vpcIDs, force)
	if err != nil {
		return nil, err
	}

	return &revokeEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAuthToXML(ea),
	}, nil
}

// ---- DescribeResize ----

type describeResizeResponse struct {
	XMLName xml.Name          `xml:"DescribeResizeResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  xmlResizeProgress `xml:"DescribeResizeResult"`
}

func (h *Handler) handleDescribeResize(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")

	rp, err := h.Backend.DescribeResize(clusterID)
	if err != nil {
		return nil, err
	}

	return &describeResizeResponse{
		Xmlns:  redshiftXMLNS,
		Result: resizeProgressToXML(rp),
	}, nil
}

// ---- RevokeSnapshotAccess ----

type revokeSnapshotAccessResponse struct {
	XMLName  xml.Name    `xml:"RevokeSnapshotAccessResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"RevokeSnapshotAccessResult>Snapshot"`
}

func (h *Handler) handleRevokeSnapshotAccess(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	accountWithRestoreAccess := vals.Get("AccountWithRestoreAccess")

	snap, err := h.Backend.RevokeSnapshotAccess(snapshotID, accountWithRestoreAccess)
	if err != nil {
		return nil, err
	}

	return &revokeSnapshotAccessResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- ModifyClusterSnapshot ----

type modifyClusterSnapshotResponse struct {
	XMLName  xml.Name    `xml:"ModifyClusterSnapshotResponse"`
	Xmlns    string      `xml:"xmlns,attr"`
	Snapshot xmlSnapshot `xml:"ModifyClusterSnapshotResult>Snapshot"`
}

func (h *Handler) handleModifyClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("SnapshotIdentifier")
	retentionPeriodStr := vals.Get("ManualSnapshotRetentionPeriod")
	force := vals.Get("Force") == paramValueTrue

	retentionPeriod := -1

	if retentionPeriodStr != "" {
		n, err := strconv.Atoi(retentionPeriodStr)
		if err != nil {
			return nil, ErrInvalidParameter
		}

		retentionPeriod = n
	}

	snap, err := h.Backend.ModifyClusterSnapshot(snapshotID, retentionPeriod, force)
	if err != nil {
		return nil, err
	}

	return &modifyClusterSnapshotResponse{
		Xmlns:    redshiftXMLNS,
		Snapshot: snapshotToXML(snap),
	}, nil
}

// ---- GetClusterCredentials ----

type xmlClusterCredentials struct {
	DBUser     string `xml:"DbUser"`
	DBPassword string `xml:"DbPassword"`
	Expiration string `xml:"Expiration,omitempty"`
}

type getClusterCredentialsResponse struct {
	XMLName xml.Name              `xml:"GetClusterCredentialsResponse"`
	Xmlns   string                `xml:"xmlns,attr"`
	Result  xmlClusterCredentials `xml:"GetClusterCredentialsResult"`
}

func (h *Handler) handleGetClusterCredentials(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	dbUser := vals.Get("DbUser")
	autoCreate := vals.Get("AutoCreate") == paramValueTrue

	creds, err := h.Backend.GetClusterCredentials(clusterID, dbUser, autoCreate)
	if err != nil {
		return nil, err
	}

	return &getClusterCredentialsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlClusterCredentials{
			DBUser:     creds.DBUser,
			DBPassword: creds.DBPassword,
			Expiration: creds.Expiration.UTC().Format(time.RFC3339),
		},
	}, nil
}

// ---- DescribeAccountAttributes ----

type xmlAccountAttribute struct {
	AttributeName   string `xml:"AttributeName"`
	AttributeValues string `xml:"AttributeValues>AttributeValueTarget>AttributeValue,omitempty"`
}

type xmlAccountAttributeList struct {
	Attributes []xmlAccountAttribute `xml:"AccountAttribute,omitempty"`
}

type xmlDescribeAccountAttributesResult struct {
	AccountAttributeList xmlAccountAttributeList `xml:"AccountAttributeList"`
}

type describeAccountAttributesResponse struct {
	XMLName xml.Name                           `xml:"DescribeAccountAttributesResponse"`
	Xmlns   string                             `xml:"xmlns,attr"`
	Result  xmlDescribeAccountAttributesResult `xml:"DescribeAccountAttributesResult"`
}

func (h *Handler) handleDescribeAccountAttributes(_ url.Values) (any, error) {
	return &describeAccountAttributesResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeClusterTracks ----

type xmlMaintenanceTrack struct {
	MaintenanceTrackName string `xml:"MaintenanceTrackName"`
	DatabaseVersion      string `xml:"DatabaseVersion,omitempty"`
}

type xmlMaintenanceTracks struct {
	Tracks []xmlMaintenanceTrack `xml:"MaintenanceTrack,omitempty"`
}

type xmlDescribeClusterTracksResult struct {
	MaintenanceTracks xmlMaintenanceTracks `xml:"MaintenanceTracks"`
}

type describeClusterTracksResponse struct {
	XMLName xml.Name                       `xml:"DescribeClusterTracksResponse"`
	Xmlns   string                         `xml:"xmlns,attr"`
	Result  xmlDescribeClusterTracksResult `xml:"DescribeClusterTracksResult"`
}

func (h *Handler) handleDescribeClusterTracks(_ url.Values) (any, error) {
	return &describeClusterTracksResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeClusterTracksResult{
			MaintenanceTracks: xmlMaintenanceTracks{
				Tracks: []xmlMaintenanceTrack{
					{MaintenanceTrackName: "current", DatabaseVersion: modelVersion10},
					{MaintenanceTrackName: "trailing", DatabaseVersion: modelVersion10},
				},
			},
		},
	}, nil
}

// ---- DescribeClusterVersions ----

type xmlClusterVersion struct {
	ClusterVersion              string `xml:"ClusterVersion"`
	ClusterParameterGroupFamily string `xml:"ClusterParameterGroupFamily"`
	Description                 string `xml:"Description,omitempty"`
}

type xmlClusterVersionList struct {
	Versions []xmlClusterVersion `xml:"ClusterVersion,omitempty"`
}

type xmlDescribeClusterVersionsResult struct {
	ClusterVersions xmlClusterVersionList `xml:"ClusterVersions"`
}

type describeClusterVersionsResponse struct {
	XMLName xml.Name                         `xml:"DescribeClusterVersionsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  xmlDescribeClusterVersionsResult `xml:"DescribeClusterVersionsResult"`
}

func (h *Handler) handleDescribeClusterVersions(_ url.Values) (any, error) {
	return &describeClusterVersionsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeClusterVersionsResult{
			ClusterVersions: xmlClusterVersionList{
				Versions: []xmlClusterVersion{
					{
						ClusterVersion:              modelVersion10,
						ClusterParameterGroupFamily: "redshift-1.0",
						Description:                 "Amazon Redshift 1.0",
					},
				},
			},
		},
	}, nil
}

// ---- DescribeOrderableClusterOptions ----

type xmlOrderableClusterOption struct {
	ClusterVersion string `xml:"ClusterVersion"`
	ClusterType    string `xml:"ClusterType"`
	NodeType       string `xml:"NodeType"`
}

type xmlOrderableClusterOptionList struct {
	Options []xmlOrderableClusterOption `xml:"OrderableClusterOption,omitempty"`
}

type xmlDescribeOrderableClusterOptionsResult struct {
	OrderableClusterOptions xmlOrderableClusterOptionList `xml:"OrderableClusterOptions"`
}

type describeOrderableClusterOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableClusterOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  xmlDescribeOrderableClusterOptionsResult `xml:"DescribeOrderableClusterOptionsResult"`
}

func (h *Handler) handleDescribeOrderableClusterOptions(_ url.Values) (any, error) {
	return &describeOrderableClusterOptionsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeOrderableClusterOptionsResult{
			OrderableClusterOptions: xmlOrderableClusterOptionList{
				Options: []xmlOrderableClusterOption{
					{ClusterVersion: modelVersion10, ClusterType: "multi-node", NodeType: defaultNodeType},
					{ClusterVersion: modelVersion10, ClusterType: "single-node", NodeType: defaultNodeType},
					{ClusterVersion: modelVersion10, ClusterType: "multi-node", NodeType: nodeTypeDC28xlarge},
					{ClusterVersion: modelVersion10, ClusterType: "single-node", NodeType: nodeTypeDC28xlarge},
				},
			},
		},
	}, nil
}

// ---- DescribeStorage ----

type describeStorageResponse struct {
	XMLName                            xml.Name `xml:"DescribeStorageResponse"`
	Xmlns                              string   `xml:"xmlns,attr"`
	TotalBackupSizeInMegaBytes         float64  `xml:"DescribeStorageResult>TotalBackupSizeInMegaBytes"`
	TotalProvisionedStorageInMegaBytes float64  `xml:"DescribeStorageResult>TotalProvisionedStorageInMegaBytes"`
}

func (h *Handler) handleDescribeStorage(_ url.Values) (any, error) {
	return &describeStorageResponse{Xmlns: redshiftXMLNS}, nil
}
