package iam

import (
	"encoding/xml"
	"time"
)

// ---- Account Alias types ----

// CreateAccountAliasResponse is the XML response for CreateAccountAlias.
type CreateAccountAliasResponse struct {
	XMLName          xml.Name         `xml:"CreateAccountAliasResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ---- Policy Version types ----

// StoredPolicyVersion is the in-memory representation of a managed policy version.
type StoredPolicyVersion struct {
	CreateDate       time.Time `json:"CreateDate"`
	PolicyDocument   string    `json:"PolicyDocument,omitempty"`
	VersionID        string    `json:"VersionId,omitempty"`
	IsDefaultVersion bool      `json:"IsDefaultVersion,omitempty"`
}

// CreatePolicyVersionResult contains the created policy version details.
type CreatePolicyVersionResult struct {
	PolicyVersion PolicyVersionXML `xml:"PolicyVersion"`
}

// CreatePolicyVersionResponse is the XML response for CreatePolicyVersion.
type CreatePolicyVersionResponse struct {
	XMLName                   xml.Name                  `xml:"CreatePolicyVersionResponse"`
	Xmlns                     string                    `xml:"xmlns,attr"`
	ResponseMetadata          ResponseMetadata          `xml:"ResponseMetadata"`
	CreatePolicyVersionResult CreatePolicyVersionResult `xml:"CreatePolicyVersionResult"`
}

// ---- Service-Linked Role types ----

// CreateServiceLinkedRoleResult wraps the created role.
type CreateServiceLinkedRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// CreateServiceLinkedRoleResponse is the XML response for CreateServiceLinkedRole.
type CreateServiceLinkedRoleResponse struct {
	XMLName                       xml.Name                      `xml:"CreateServiceLinkedRoleResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
	CreateServiceLinkedRoleResult CreateServiceLinkedRoleResult `xml:"CreateServiceLinkedRoleResult"`
}

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

// ---- Virtual MFA Device types ----

// MFA device status constants mirror AWS IAM virtual MFA device states.
const (
	MFAStatusNotAssigned = "not_assigned" // PENDING_ENABLE / unlinked
	MFAStatusEnabled     = "Active"       // linked via EnableMFADevice
	MFAStatusDeactivated = "Deactivated"  // unlinked via DeactivateMFADevice
)

// VirtualMFADevice represents an IAM virtual MFA device.
type VirtualMFADevice struct {
	CreateDate           time.Time `json:"CreateDate"`
	SerialNumber         string    `json:"SerialNumber,omitempty"`
	VirtualMFADeviceName string    `json:"VirtualMFADeviceName,omitempty"`
	Path                 string    `json:"Path,omitempty"`
	// Status tracks the MFA device lifecycle: not_assigned → Active → Deactivated.
	Status           string `json:"Status,omitempty"`
	Base32StringSeed string `json:"Base32StringSeed,omitempty"`
	QRCodePNG        string `json:"QRCodePNG,omitempty"`
}

// VirtualMFADeviceXML is the XML representation of a virtual MFA device.
type VirtualMFADeviceXML struct {
	SerialNumber     string `xml:"SerialNumber"`
	Base32StringSeed string `xml:"Base32StringSeed,omitempty"`
	QRCodePNG        string `xml:"QRCodePNG,omitempty"`
}

// CreateVirtualMFADeviceResult wraps the created MFA device.
type CreateVirtualMFADeviceResult struct {
	VirtualMFADevice VirtualMFADeviceXML `xml:"VirtualMFADevice"`
}

// CreateVirtualMFADeviceResponse is the XML response for CreateVirtualMFADevice.
type CreateVirtualMFADeviceResponse struct {
	XMLName                      xml.Name                     `xml:"CreateVirtualMFADeviceResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	CreateVirtualMFADeviceResult CreateVirtualMFADeviceResult `xml:"CreateVirtualMFADeviceResult"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
}

// ---- Delegation Request types ----

// DelegationRequest represents an IAM delegation request (stub).
type DelegationRequest struct {
	CreateDate      time.Time `json:"CreateDate"`
	DelegationID    string    `json:"DelegationId,omitempty"`
	TargetAccountID string    `json:"TargetAccountId,omitempty"`
	Status          string    `json:"Status,omitempty"`
	PolicyArn       string    `json:"PolicyArn,omitempty"`
}

// DelegationRequestXML is the XML representation of a delegation request.
type DelegationRequestXML struct {
	DelegationID    string `xml:"DelegationId"`
	TargetAccountID string `xml:"TargetAccountId"`
	Status          string `xml:"Status"`
	CreateDate      string `xml:"CreateDate"`
}

// CreateDelegationRequestResult wraps the created delegation request.
type CreateDelegationRequestResult struct {
	DelegationRequest DelegationRequestXML `xml:"DelegationRequest"`
}

// CreateDelegationRequestResponse is the XML response for CreateDelegationRequest.
type CreateDelegationRequestResponse struct {
	XMLName                       xml.Name                      `xml:"CreateDelegationRequestResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	CreateDelegationRequestResult CreateDelegationRequestResult `xml:"CreateDelegationRequestResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// AcceptDelegationRequestResponse is the XML response for AcceptDelegationRequest.
type AcceptDelegationRequestResponse struct {
	XMLName          xml.Name         `xml:"AcceptDelegationRequestResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AssociateDelegationRequestResponse is the XML response for AssociateDelegationRequest.
type AssociateDelegationRequestResponse struct {
	XMLName          xml.Name         `xml:"AssociateDelegationRequestResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// ChangePasswordResponse is the XML response for ChangePassword.
type ChangePasswordResponse struct {
	XMLName          xml.Name         `xml:"ChangePasswordResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// AddClientIDToOpenIDConnectProviderResponse is the XML response for AddClientIDToOpenIDConnectProvider.
type AddClientIDToOpenIDConnectProviderResponse struct {
	XMLName          xml.Name         `xml:"AddClientIDToOpenIDConnectProviderResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}
