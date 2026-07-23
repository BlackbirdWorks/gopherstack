package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- AddPartner ----

type addPartnerResponse struct {
	XMLName           xml.Name `xml:"AddPartnerResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	ClusterIdentifier string   `xml:"AddPartnerResult>ClusterIdentifier"`
	DatabaseName      string   `xml:"AddPartnerResult>DatabaseName"`
	PartnerName       string   `xml:"AddPartnerResult>PartnerName"`
}

func (h *Handler) handleAddPartner(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerName")

	if accountID == "" {
		accountID = h.Backend.AccountID()
	}

	partner, err := h.Backend.AddPartner(accountID, clusterID, databaseName, partnerName)
	if err != nil {
		return nil, err
	}

	return &addPartnerResponse{
		Xmlns:             redshiftXMLNS,
		ClusterIdentifier: partner.ClusterIdentifier,
		DatabaseName:      partner.DatabaseName,
		PartnerName:       partner.PartnerName,
	}, nil
}

// ---- DeletePartner ----

type deletePartnerResponse struct {
	XMLName           xml.Name `xml:"DeletePartnerResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	ClusterIdentifier string   `xml:"DeletePartnerResult>ClusterIdentifier"`
	DatabaseName      string   `xml:"DeletePartnerResult>DatabaseName"`
	PartnerName       string   `xml:"DeletePartnerResult>PartnerName"`
}

func (h *Handler) handleDeletePartner(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerName")

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
	PartnerName       string `xml:"PartnerName"`
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
	partnerName := vals.Get("PartnerName")

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
	PartnerName       string   `xml:"UpdatePartnerStatusResult>PartnerName"`
}

func (h *Handler) handleUpdatePartnerStatus(vals url.Values) (any, error) {
	accountID := vals.Get("AccountId")
	clusterID := vals.Get("ClusterIdentifier")
	databaseName := vals.Get("DatabaseName")
	partnerName := vals.Get("PartnerName")
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
