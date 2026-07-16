package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- AssociateDataShareConsumer ----

type xmlDataShareAssociation struct {
	ConsumerIdentifier string `xml:"ConsumerIdentifier"`
	ConsumerRegion     string `xml:"ConsumerRegion,omitempty"`
	Status             string `xml:"Status"`
	Type               string `xml:"Type,omitempty"`
}

type xmlDataShare struct {
	DataShareArn                     string                    `xml:"DataShareArn"`
	ProducerArn                      string                    `xml:"ProducerArn,omitempty"`
	ManagedBy                        string                    `xml:"ManagedBy,omitempty"`
	DataShareAssociations            []xmlDataShareAssociation `xml:"DataShareAssociations>member,omitempty"`
	AllowPubliclyAccessibleConsumers bool                      `xml:"AllowPubliclyAccessibleConsumers"`
}

type associateDataShareConsumerResponse struct {
	XMLName xml.Name     `xml:"AssociateDataShareConsumerResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"AssociateDataShareConsumerResult"`
}

func dataShareToXML(ds *DataShare) xmlDataShare {
	assocs := make([]xmlDataShareAssociation, 0, len(ds.DataShareAssociations))
	for _, a := range ds.DataShareAssociations {
		assocs = append(assocs, xmlDataShareAssociation{
			ConsumerIdentifier: a.ConsumerIdentifier,
			ConsumerRegion:     a.ConsumerRegion,
			Status:             a.Status,
			Type:               a.Type,
		})
	}

	return xmlDataShare{
		DataShareArn:                     ds.DataShareArn,
		ProducerArn:                      ds.ProducerArn,
		AllowPubliclyAccessibleConsumers: ds.AllowPubliclyAccessibleConsumers,
		ManagedBy:                        ds.ManagedBy,
		DataShareAssociations:            assocs,
	}
}

func (h *Handler) handleAssociateDataShareConsumer(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerArn := vals.Get("ConsumerArn")
	consumerRegion := vals.Get("ConsumerRegion")
	associateEntireAccount := vals.Get("AssociateEntireAccount") == paramValueTrue

	ds, err := h.Backend.AssociateDataShareConsumer(dataShareArn, consumerArn, consumerRegion, associateEntireAccount)
	if err != nil {
		return nil, err
	}

	return &associateDataShareConsumerResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
	}, nil
}

// ---- AuthorizeDataShare ----

type authorizeDataShareResponse struct {
	XMLName xml.Name     `xml:"AuthorizeDataShareResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Result  xmlDataShare `xml:"AuthorizeDataShareResult"`
}

func (h *Handler) handleAuthorizeDataShare(vals url.Values) (any, error) {
	dataShareArn := vals.Get("DataShareArn")
	consumerIdentifier := vals.Get("ConsumerIdentifier")

	ds, err := h.Backend.AuthorizeDataShare(dataShareArn, consumerIdentifier)
	if err != nil {
		return nil, err
	}

	return &authorizeDataShareResponse{
		Xmlns:  redshiftXMLNS,
		Result: dataShareToXML(ds),
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
