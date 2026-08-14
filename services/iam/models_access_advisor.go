package iam

import (
	"encoding/xml"
	"time"
)

// ServiceLastAccessedDetail tracks when a service was last accessed.
type ServiceLastAccessedDetail struct {
	ServiceName                string    `json:"ServiceName,omitempty"`
	ServiceNamespace           string    `json:"ServiceNamespace,omitempty"`
	LastAuthenticated          time.Time `json:"LastAuthenticated"`
	LastAuthenticatedArn       string    `json:"LastAuthenticatedArn,omitempty"`
	TotalAuthenticatedEntities int       `json:"TotalAuthenticatedEntities,omitempty"`
}

// generateSLADResult contains the generated access-advisor job ID.
type generateSLADResult struct {
	JobID string `xml:"JobId"`
}

// generateServiceLastAccessedDetailsResponse is the XML response for GenerateServiceLastAccessedDetails.
type generateServiceLastAccessedDetailsResponse struct {
	XMLName                                  xml.Name           `xml:"GenerateServiceLastAccessedDetailsResponse"`
	Xmlns                                    string             `xml:"xmlns,attr"`
	GenerateServiceLastAccessedDetailsResult generateSLADResult `xml:"GenerateServiceLastAccessedDetailsResult"`
	ResponseMetadata                         ResponseMetadata   `xml:"ResponseMetadata"`
}

// getSLADWithEntitiesResult contains the (always-empty, mock) entity-scoped access-advisor result.
type getSLADWithEntitiesResult struct {
	JobStatus         string   `xml:"JobStatus"`
	JobCreationDate   string   `xml:"JobCreationDate"`
	JobCompletionDate string   `xml:"JobCompletionDate"`
	EntityDetailsList []string `xml:"EntityDetailsList>member"`
	IsTruncated       bool     `xml:"IsTruncated"`
}

// getServiceLastAccessedDetailsWithEntitiesResponse is the XML envelope for
// GetServiceLastAccessedDetailsWithEntities.
type getServiceLastAccessedDetailsWithEntitiesResponse struct {
	XMLName                                         xml.Name                  `xml:"GetServiceLastAccessedDetailsWithEntitiesResponse"` //nolint:lll // long name required by AWS API
	Xmlns                                           string                    `xml:"xmlns,attr"`
	ResponseMetadata                                ResponseMetadata          `xml:"ResponseMetadata"`
	GetServiceLastAccessedDetailsWithEntitiesResult getSLADWithEntitiesResult `xml:"GetServiceLastAccessedDetailsWithEntitiesResult"` //nolint:lll // long name required by AWS API
}
