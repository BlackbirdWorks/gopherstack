package rds

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

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
