package rds

import (
	"encoding/xml"
	"net/url"
)

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
