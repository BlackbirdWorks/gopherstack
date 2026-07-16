package iam

import "encoding/xml"

// serverCertMetaXML is the XML representation of server certificate metadata (no body).
type serverCertMetaXML struct {
	ServerCertificateName string `xml:"ServerCertificateName"`
	ServerCertificateID   string `xml:"ServerCertificateId"`
	Arn                   string `xml:"Arn"`
	Path                  string `xml:"Path"`
	UploadDate            string `xml:"UploadDate"`
	Expiration            string `xml:"Expiration,omitempty"`
}

// serverCertXML is the XML representation of a full server certificate.
type serverCertXML struct {
	ServerCertificateMetadata serverCertMetaXML `xml:"ServerCertificateMetadata"`
	CertificateBody           string            `xml:"CertificateBody"`
	CertificateChain          string            `xml:"CertificateChain,omitempty"`
}

// listServerCertificatesResult contains the list of server certificates.
type listServerCertificatesResult struct {
	ServerCertificateMetadataList []serverCertMetaXML `xml:"ServerCertificateMetadataList>member"`
	IsTruncated                   bool                `xml:"IsTruncated"`
}

// listServerCertificatesResponse is the XML response for ListServerCertificates.
type listServerCertificatesResponse struct {
	XMLName                      xml.Name                     `xml:"ListServerCertificatesResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	ListServerCertificatesResult listServerCertificatesResult `xml:"ListServerCertificatesResult"`
}

// getServerCertificateResult wraps a single server certificate.
type getServerCertificateResult struct {
	ServerCertificate serverCertXML `xml:"ServerCertificate"`
}

// getServerCertificateResponse is the XML response for GetServerCertificate.
type getServerCertificateResponse struct {
	XMLName                    xml.Name                   `xml:"GetServerCertificateResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	GetServerCertificateResult getServerCertificateResult `xml:"GetServerCertificateResult"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
}

// uploadServerCertificateResult wraps the uploaded server certificate metadata.
type uploadServerCertificateResult struct {
	ServerCertificateMetadata serverCertMetaXML `xml:"ServerCertificateMetadata"`
}

// uploadServerCertificateResponse is the XML response for UploadServerCertificate.
type uploadServerCertificateResponse struct {
	XMLName                       xml.Name                      `xml:"UploadServerCertificateResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	UploadServerCertificateResult uploadServerCertificateResult `xml:"UploadServerCertificateResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}
