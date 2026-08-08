package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ---- CreateUsageLimit ----

type xmlUsageLimit struct {
	UsageLimitID      string       `xml:"UsageLimitId"`
	ClusterIdentifier string       `xml:"ClusterIdentifier"`
	FeatureType       string       `xml:"FeatureType,omitempty"`
	LimitType         string       `xml:"LimitType,omitempty"`
	BreachAction      string       `xml:"BreachAction,omitempty"`
	Tags              []svcTags.KV `xml:"Tags>Tag,omitempty"`
	Amount            int64        `xml:"Amount"`
}

type createUsageLimitResponse struct {
	XMLName xml.Name      `xml:"CreateUsageLimitResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Limit   xmlUsageLimit `xml:"CreateUsageLimitResult"`
}

func (h *Handler) handleCreateUsageLimit(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	featureType := vals.Get("FeatureType")
	limitType := vals.Get("LimitType")
	breachAction := vals.Get("BreachAction")
	amount, _ := strconv.ParseInt(vals.Get("Amount"), 10, 64)
	tagMap := parseRedshiftTags(vals)

	ul, err := h.Backend.CreateUsageLimit(clusterID, featureType, limitType, breachAction, amount, tagMap)
	if err != nil {
		return nil, err
	}

	return &createUsageLimitResponse{
		Xmlns: redshiftXMLNS,
		Limit: usageLimitToXML(ul),
	}, nil
}

func usageLimitToXML(ul *UsageLimit) xmlUsageLimit {
	return xmlUsageLimit{
		UsageLimitID:      ul.UsageLimitID,
		ClusterIdentifier: ul.ClusterIdentifier,
		FeatureType:       ul.FeatureType,
		LimitType:         ul.LimitType,
		Amount:            ul.Amount,
		BreachAction:      ul.BreachAction,
		Tags:              tagMapToKVList(ul.Tags),
	}
}

// ---- DeleteUsageLimit ----

type deleteUsageLimitResponse struct {
	XMLName   xml.Name `xml:"DeleteUsageLimitResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleDeleteUsageLimit(vals url.Values) (any, error) {
	usageLimitID := vals.Get("UsageLimitId")

	if err := h.Backend.DeleteUsageLimit(usageLimitID); err != nil {
		return nil, err
	}

	return &deleteUsageLimitResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeUsageLimits ----

type xmlUsageLimitList struct {
	Limits []xmlUsageLimit `xml:"UsageLimit"`
}

type describeUsageLimitsResponse struct {
	XMLName xml.Name          `xml:"DescribeUsageLimitsResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Limits  xmlUsageLimitList `xml:"DescribeUsageLimitsResult>UsageLimits"`
}

func (h *Handler) handleDescribeUsageLimits(vals url.Values) (any, error) {
	clusterID := vals.Get("ClusterIdentifier")
	featureType := vals.Get("FeatureType")

	limits, err := h.Backend.DescribeUsageLimits(clusterID, featureType)
	if err != nil {
		return nil, err
	}

	members := make([]xmlUsageLimit, 0, len(limits))

	for _, ul := range limits {
		ulCopy := ul
		members = append(members, usageLimitToXML(&ulCopy))
	}

	return &describeUsageLimitsResponse{
		Xmlns:  redshiftXMLNS,
		Limits: xmlUsageLimitList{Limits: members},
	}, nil
}

// ---- ModifyUsageLimit ----

type modifyUsageLimitResponse struct {
	XMLName xml.Name      `xml:"ModifyUsageLimitResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Limit   xmlUsageLimit `xml:"ModifyUsageLimitResult"`
}

func (h *Handler) handleModifyUsageLimit(vals url.Values) (any, error) {
	usageLimitID := vals.Get("UsageLimitId")
	breachAction := vals.Get("BreachAction")
	amount, _ := strconv.ParseInt(vals.Get("Amount"), 10, 64)

	ul, err := h.Backend.ModifyUsageLimit(usageLimitID, breachAction, amount)
	if err != nil {
		return nil, err
	}

	return &modifyUsageLimitResponse{
		Xmlns: redshiftXMLNS,
		Limit: usageLimitToXML(ul),
	}, nil
}
