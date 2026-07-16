package iam

import (
	"encoding/xml"
	"time"
)

// ---- SSH Public Keys ----

// SSHPublicKey represents an IAM SSH public key for a user.
type SSHPublicKey struct {
	UploadDate       time.Time `json:"UploadDate"`
	UserName         string    `json:"UserName,omitempty"`
	SSHPublicKeyID   string    `json:"SSHPublicKeyId,omitempty"`
	SSHPublicKeyBody string    `json:"SSHPublicKeyBody,omitempty"`
	Fingerprint      string    `json:"Fingerprint,omitempty"`
	Status           string    `json:"Status,omitempty"`
}

// sshPublicKeyMetaXML is the XML representation of SSH public key metadata (no body).
type sshPublicKeyMetaXML struct {
	UserName       string `xml:"UserName"`
	SSHPublicKeyID string `xml:"SSHPublicKeyId"`
	Status         string `xml:"Status"`
	UploadDate     string `xml:"UploadDate"`
}

// sshPublicKeyXML is the XML representation of a full SSH public key.
type sshPublicKeyXML struct {
	UserName         string `xml:"UserName"`
	SSHPublicKeyID   string `xml:"SSHPublicKeyId"`
	Fingerprint      string `xml:"Fingerprint"`
	SSHPublicKeyBody string `xml:"SSHPublicKeyBody"`
	Status           string `xml:"Status"`
	UploadDate       string `xml:"UploadDate"`
}

// listSSHPublicKeysResult contains the list of SSH public keys.
type listSSHPublicKeysResult struct {
	SSHPublicKeys []sshPublicKeyMetaXML `xml:"SSHPublicKeys>member"`
	IsTruncated   bool                  `xml:"IsTruncated"`
}

// listSSHPublicKeysResponse is the XML response for ListSSHPublicKeys.
type listSSHPublicKeysResponse struct {
	XMLName                 xml.Name                `xml:"ListSSHPublicKeysResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListSSHPublicKeysResult listSSHPublicKeysResult `xml:"ListSSHPublicKeysResult"`
}

// getSSHPublicKeyResult wraps a single SSH public key.
type getSSHPublicKeyResult struct {
	SSHPublicKey sshPublicKeyXML `xml:"SSHPublicKey"`
}

// getSSHPublicKeyResponse is the XML response for GetSSHPublicKey.
type getSSHPublicKeyResponse struct {
	XMLName               xml.Name              `xml:"GetSSHPublicKeyResponse"`
	Xmlns                 string                `xml:"xmlns,attr"`
	GetSSHPublicKeyResult getSSHPublicKeyResult `xml:"GetSSHPublicKeyResult"`
	ResponseMetadata      ResponseMetadata      `xml:"ResponseMetadata"`
}

// uploadSSHPublicKeyResult wraps the uploaded SSH public key.
type uploadSSHPublicKeyResult struct {
	SSHPublicKey sshPublicKeyXML `xml:"SSHPublicKey"`
}

// uploadSSHPublicKeyResponse is the XML response for UploadSSHPublicKey.
type uploadSSHPublicKeyResponse struct {
	XMLName                  xml.Name                 `xml:"UploadSSHPublicKeyResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	UploadSSHPublicKeyResult uploadSSHPublicKeyResult `xml:"UploadSSHPublicKeyResult"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
}

// ---- Signing Certificates ----

// signingCertXML is the XML representation of a signing certificate.
type signingCertXML struct {
	CertificateID   string `xml:"CertificateId"`
	UserName        string `xml:"UserName"`
	CertificateBody string `xml:"CertificateBody"`
	Status          string `xml:"Status"`
	UploadDate      string `xml:"UploadDate"`
}

// listSigningCertificatesResult contains the list of signing certificates.
type listSigningCertificatesResult struct {
	Certificates []signingCertXML `xml:"Certificates>member"`
	IsTruncated  bool             `xml:"IsTruncated"`
}

// listSigningCertificatesResponse is the XML response for ListSigningCertificates.
type listSigningCertificatesResponse struct {
	XMLName                       xml.Name                      `xml:"ListSigningCertificatesResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
	ListSigningCertificatesResult listSigningCertificatesResult `xml:"ListSigningCertificatesResult"`
}

// uploadSigningCertificateResult wraps the uploaded signing certificate.
type uploadSigningCertificateResult struct {
	Certificate signingCertXML `xml:"Certificate"`
}

// uploadSigningCertificateResponse is the XML response for UploadSigningCertificate.
type uploadSigningCertificateResponse struct {
	XMLName                        xml.Name                       `xml:"UploadSigningCertificateResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	UploadSigningCertificateResult uploadSigningCertificateResult `xml:"UploadSigningCertificateResult"`
	ResponseMetadata               ResponseMetadata               `xml:"ResponseMetadata"`
}
