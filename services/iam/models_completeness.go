package iam

import "encoding/xml"

// ----- MFA Devices -----

type mfaDeviceXML struct {
	UserName     string `xml:"UserName"`
	SerialNumber string `xml:"SerialNumber"`
	EnableDate   string `xml:"EnableDate"`
}

type listMFADevicesResult struct {
	MFADevices  []mfaDeviceXML `xml:"MFADevices>member"`
	IsTruncated bool           `xml:"IsTruncated"`
}

type listMFADevicesResponse struct {
	XMLName              xml.Name             `xml:"ListMFADevicesResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	ResponseMetadata     ResponseMetadata     `xml:"ResponseMetadata"`
	ListMFADevicesResult listMFADevicesResult `xml:"ListMFADevicesResult"`
}

// ----- Server Certificates -----

type serverCertMetaXML struct {
	ServerCertificateName string `xml:"ServerCertificateName"`
	ServerCertificateID   string `xml:"ServerCertificateId"`
	Arn                   string `xml:"Arn"`
	Path                  string `xml:"Path"`
	UploadDate            string `xml:"UploadDate"`
	Expiration            string `xml:"Expiration,omitempty"`
}

type serverCertXML struct {
	ServerCertificateMetadata serverCertMetaXML `xml:"ServerCertificateMetadata"`
	CertificateBody           string            `xml:"CertificateBody"`
	CertificateChain          string            `xml:"CertificateChain,omitempty"`
}

type listServerCertificatesResult struct {
	ServerCertificateMetadataList []serverCertMetaXML `xml:"ServerCertificateMetadataList>member"`
	IsTruncated                   bool                `xml:"IsTruncated"`
}

type listServerCertificatesResponse struct {
	XMLName                      xml.Name                     `xml:"ListServerCertificatesResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	ListServerCertificatesResult listServerCertificatesResult `xml:"ListServerCertificatesResult"`
}

type getServerCertificateResult struct {
	ServerCertificate serverCertXML `xml:"ServerCertificate"`
}

type getServerCertificateResponse struct {
	XMLName                    xml.Name                   `xml:"GetServerCertificateResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	GetServerCertificateResult getServerCertificateResult `xml:"GetServerCertificateResult"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
}

type uploadServerCertificateResult struct {
	ServerCertificateMetadata serverCertMetaXML `xml:"ServerCertificateMetadata"`
}

type uploadServerCertificateResponse struct {
	XMLName                       xml.Name                      `xml:"UploadServerCertificateResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	UploadServerCertificateResult uploadServerCertificateResult `xml:"UploadServerCertificateResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// ----- SSH Public Keys -----

type sshPublicKeyMetaXML struct {
	UserName       string `xml:"UserName"`
	SSHPublicKeyID string `xml:"SSHPublicKeyId"`
	Status         string `xml:"Status"`
	UploadDate     string `xml:"UploadDate"`
}

type sshPublicKeyXML struct {
	UserName         string `xml:"UserName"`
	SSHPublicKeyID   string `xml:"SSHPublicKeyId"`
	Fingerprint      string `xml:"Fingerprint"`
	SSHPublicKeyBody string `xml:"SSHPublicKeyBody"`
	Status           string `xml:"Status"`
	UploadDate       string `xml:"UploadDate"`
}

type listSSHPublicKeysResult struct {
	SSHPublicKeys []sshPublicKeyMetaXML `xml:"SSHPublicKeys>member"`
	IsTruncated   bool                  `xml:"IsTruncated"`
}

type listSSHPublicKeysResponse struct {
	XMLName                 xml.Name                `xml:"ListSSHPublicKeysResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListSSHPublicKeysResult listSSHPublicKeysResult `xml:"ListSSHPublicKeysResult"`
}

type getSSHPublicKeyResult struct {
	SSHPublicKey sshPublicKeyXML `xml:"SSHPublicKey"`
}

type getSSHPublicKeyResponse struct {
	XMLName               xml.Name              `xml:"GetSSHPublicKeyResponse"`
	Xmlns                 string                `xml:"xmlns,attr"`
	GetSSHPublicKeyResult getSSHPublicKeyResult `xml:"GetSSHPublicKeyResult"`
	ResponseMetadata      ResponseMetadata      `xml:"ResponseMetadata"`
}

type uploadSSHPublicKeyResult struct {
	SSHPublicKey sshPublicKeyXML `xml:"SSHPublicKey"`
}

type uploadSSHPublicKeyResponse struct {
	XMLName                  xml.Name                 `xml:"UploadSSHPublicKeyResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	UploadSSHPublicKeyResult uploadSSHPublicKeyResult `xml:"UploadSSHPublicKeyResult"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
}

// ----- Signing Certificates -----

type signingCertXML struct {
	CertificateID   string `xml:"CertificateId"`
	UserName        string `xml:"UserName"`
	CertificateBody string `xml:"CertificateBody"`
	Status          string `xml:"Status"`
	UploadDate      string `xml:"UploadDate"`
}

type listSigningCertificatesResult struct {
	Certificates []signingCertXML `xml:"Certificates>member"`
	IsTruncated  bool             `xml:"IsTruncated"`
}

type listSigningCertificatesResponse struct {
	XMLName                       xml.Name                      `xml:"ListSigningCertificatesResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
	ListSigningCertificatesResult listSigningCertificatesResult `xml:"ListSigningCertificatesResult"`
}

type uploadSigningCertificateResult struct {
	Certificate signingCertXML `xml:"Certificate"`
}

type uploadSigningCertificateResponse struct {
	XMLName                        xml.Name                       `xml:"UploadSigningCertificateResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	UploadSigningCertificateResult uploadSigningCertificateResult `xml:"UploadSigningCertificateResult"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
}

// ----- Service Specific Credential Reset -----

type serviceSpecificCredXML struct {
	UserName                    string `xml:"UserName"`
	ServiceSpecificCredentialID string `xml:"ServiceSpecificCredentialId"`
	ServiceName                 string `xml:"ServiceName"`
	ServiceUserName             string `xml:"ServiceUserName"`
	ServicePassword             string `xml:"ServicePassword"`
	Status                      string `xml:"Status"`
	CreateDate                  string `xml:"CreateDate"`
}

type resetSSCResult struct {
	ServiceSpecificCredential serviceSpecificCredXML `xml:"ServiceSpecificCredential"`
}

type resetServiceSpecificCredentialResponse struct {
	XMLName                              xml.Name         `xml:"ResetServiceSpecificCredentialResponse"`
	Xmlns                                string           `xml:"xmlns,attr"`
	ResetServiceSpecificCredentialResult resetSSCResult   `xml:"ResetServiceSpecificCredentialResult"`
	ResponseMetadata                     ResponseMetadata `xml:"ResponseMetadata"`
}

// ----- Delete Service Linked Role -----

type deleteServiceLinkedRoleResult struct {
	DeletionTaskID string `xml:"DeletionTaskId"`
}

type deleteServiceLinkedRoleResponse struct {
	XMLName                       xml.Name                      `xml:"DeleteServiceLinkedRoleResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	DeleteServiceLinkedRoleResult deleteServiceLinkedRoleResult `xml:"DeleteServiceLinkedRoleResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}

// ----- Organizations -----

type listOrganizationsFeaturesResult struct {
	RootID               string   `xml:"RootId,omitempty"`
	OrganizationFeatures []string `xml:"OrganizationFeatures>member"`
}

type listOrganizationsFeaturesResponse struct {
	XMLName                         xml.Name                        `xml:"ListOrganizationsFeaturesResponse"`
	Xmlns                           string                          `xml:"xmlns,attr"`
	ResponseMetadata                ResponseMetadata                `xml:"ResponseMetadata"`
	ListOrganizationsFeaturesResult listOrganizationsFeaturesResult `xml:"ListOrganizationsFeaturesResult"`
}

type listPGSAResult struct {
	PolicyGroups []string `xml:"PolicyGroups>member"`
	IsTruncated  bool     `xml:"IsTruncated"`
}

type listPoliciesGrantingServiceAccessResponse struct {
	XMLName                                 xml.Name         `xml:"ListPoliciesGrantingServiceAccessResponse"`
	Xmlns                                   string           `xml:"xmlns,attr"`
	ResponseMetadata                        ResponseMetadata `xml:"ResponseMetadata"`
	ListPoliciesGrantingServiceAccessResult listPGSAResult   `xml:"ListPoliciesGrantingServiceAccessResult"`
}

// ----- Delegation -----

type listDelegationRequestsResult struct {
	DelegationRequests []string `xml:"DelegationRequests>member"`
	IsTruncated        bool     `xml:"IsTruncated"`
}

type listDelegationRequestsResponse struct {
	XMLName                      xml.Name                     `xml:"ListDelegationRequestsResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	ListDelegationRequestsResult listDelegationRequestsResult `xml:"ListDelegationRequestsResult"`
}

// ----- Orgs Access Report -----

type generateOrgAccessReportResult struct {
	JobID string `xml:"JobId"`
}

type generateOrganizationsAccessReportResponse struct {
	XMLName                                 xml.Name                      `xml:"GenerateOrganizationsAccessReportResponse"`
	Xmlns                                   string                        `xml:"xmlns,attr"`
	GenerateOrganizationsAccessReportResult generateOrgAccessReportResult `xml:"GenerateOrganizationsAccessReportResult"`
	ResponseMetadata                        ResponseMetadata              `xml:"ResponseMetadata"`
}

type generateSLADResult struct {
	JobID string `xml:"JobId"`
}

type generateServiceLastAccessedDetailsResponse struct {
	XMLName                                  xml.Name           `xml:"GenerateServiceLastAccessedDetailsResponse"`
	Xmlns                                    string             `xml:"xmlns,attr"`
	GenerateServiceLastAccessedDetailsResult generateSLADResult `xml:"GenerateServiceLastAccessedDetailsResult"`
	ResponseMetadata                         ResponseMetadata   `xml:"ResponseMetadata"`
}

type getOrgAccessReportResult struct {
	JobStatus                   string   `xml:"JobStatus"`
	JobCreationDate             string   `xml:"JobCreationDate"`
	AccessDetails               []string `xml:"AccessDetails>member"`
	IsTruncated                 bool     `xml:"IsTruncated"`
	NumberOfServicesAccessible  int      `xml:"NumberOfServicesAccessible"`
	NumberOfServicesNotAccessed int      `xml:"NumberOfServicesNotAccessed"`
}

type getOrganizationsAccessReportResponse struct {
	XMLName                            xml.Name                 `xml:"GetOrganizationsAccessReportResponse"`
	Xmlns                              string                   `xml:"xmlns,attr"`
	ResponseMetadata                   ResponseMetadata         `xml:"ResponseMetadata"`
	GetOrganizationsAccessReportResult getOrgAccessReportResult `xml:"GetOrganizationsAccessReportResult"`
}

type getSLADWithEntitiesResult struct {
	JobStatus         string   `xml:"JobStatus"`
	JobCreationDate   string   `xml:"JobCreationDate"`
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
