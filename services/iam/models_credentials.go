package iam

import (
	"encoding/xml"
	"time"
)

// ---- Service-Specific Credential types ----

// ServiceSpecificCredential represents a service-specific credential for an IAM user.
type ServiceSpecificCredential struct {
	CreateDate                  time.Time `json:"CreateDate"`
	UserName                    string    `json:"UserName,omitempty"`
	ServiceName                 string    `json:"ServiceName,omitempty"`
	ServiceUserName             string    `json:"ServiceUserName,omitempty"`
	ServicePassword             string    `json:"ServicePassword,omitempty"`
	ServiceSpecificCredentialID string    `json:"ServiceSpecificCredentialId,omitempty"`
	Status                      string    `json:"Status,omitempty"`
}

// ServiceSpecificCredentialXML is the XML representation of a service-specific credential.
type ServiceSpecificCredentialXML struct {
	UserName                    string `xml:"UserName"`
	ServiceName                 string `xml:"ServiceName"`
	ServiceUserName             string `xml:"ServiceUserName"`
	ServicePassword             string `xml:"ServicePassword"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	Status                      string `xml:"Status"`
	CreateDate                  string `xml:"CreateDate"`
}

// CreateServiceSpecificCredentialResult wraps the created credential.
type CreateServiceSpecificCredentialResult struct {
	ServiceSpecificCredential ServiceSpecificCredentialXML `xml:"ServiceSpecificCredential"`
}

// CreateServiceSpecificCredentialResponse is the XML response for CreateServiceSpecificCredential.
type CreateServiceSpecificCredentialResponse struct {
	XMLName                               xml.Name `xml:"CreateServiceSpecificCredentialResponse"`
	Xmlns                                 string   `xml:"xmlns,attr"`
	ResponseMetadata                      ResponseMetadata
	CreateServiceSpecificCredentialResult CreateServiceSpecificCredentialResult `xml:"CreateServiceSpecificCredentialResult"` //nolint:lll // AWS XML field name is necessarily long
}

// ServiceSpecificCredentialMetadataXML is the XML representation of credential metadata.
type ServiceSpecificCredentialMetadataXML struct {
	UserName                    string `xml:"UserName"`
	ServiceName                 string `xml:"ServiceName"`
	ServiceUserName             string `xml:"ServiceUserName"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	Status                      string `xml:"Status"`
	CreateDate                  string `xml:"CreateDate"`
}

// ListServiceSpecificCredentialsResult contains the list of credentials.
type ListServiceSpecificCredentialsResult struct {
	ServiceSpecificCredentials []ServiceSpecificCredentialMetadataXML `xml:"ServiceSpecificCredentials>member"`
}

// ListServiceSpecificCredentialsResponse is the XML response for ListServiceSpecificCredentials.
type ListServiceSpecificCredentialsResponse struct {
	XMLName xml.Name                             `xml:"ListServiceSpecificCredentialsResponse"`
	Xmlns   string                               `xml:"xmlns,attr"`
	Meta    ResponseMetadata                     `xml:"ResponseMetadata"`
	Result  ListServiceSpecificCredentialsResult `xml:"ListServiceSpecificCredentialsResult"`
}

// DeleteServiceSpecificCredentialResponse is the XML response for DeleteServiceSpecificCredential.
type DeleteServiceSpecificCredentialResponse struct {
	XMLName          xml.Name         `xml:"DeleteServiceSpecificCredentialResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// UpdateServiceSpecificCredentialResponse is the XML response for UpdateServiceSpecificCredential.
type UpdateServiceSpecificCredentialResponse struct {
	XMLName          xml.Name         `xml:"UpdateServiceSpecificCredentialResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Service Specific Credential Reset ----

// serviceSpecificCredXML is the XML representation used by ResetServiceSpecificCredential.
type serviceSpecificCredXML struct {
	UserName                    string `xml:"UserName"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	ServiceName                 string `xml:"ServiceName"`
	ServiceUserName             string `xml:"ServiceUserName"`
	ServicePassword             string `xml:"ServicePassword"`
	Status                      string `xml:"Status"`
	CreateDate                  string `xml:"CreateDate"`
}

// resetSSCResult wraps the reset credential.
type resetSSCResult struct {
	ServiceSpecificCredential serviceSpecificCredXML `xml:"ServiceSpecificCredential"`
}

// resetServiceSpecificCredentialResponse is the XML response for ResetServiceSpecificCredential.
type resetServiceSpecificCredentialResponse struct {
	XMLName                              xml.Name         `xml:"ResetServiceSpecificCredentialResponse"`
	Xmlns                                string           `xml:"xmlns,attr"`
	ResetServiceSpecificCredentialResult resetSSCResult   `xml:"ResetServiceSpecificCredentialResult"`
	ResponseMetadata                     ResponseMetadata `xml:"ResponseMetadata"`
}
