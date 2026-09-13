package iam

import "encoding/xml"

// ---- Access Key Last Used ----

// AccessKeyLastUsed contains information about the last time an access key was used.
type AccessKeyLastUsed struct {
	UserName     string `json:"UserName,omitempty"`
	AccessKeyID  string `json:"AccessKeyId,omitempty"`
	LastUsedDate string `json:"LastUsedDate,omitempty"`
	ServiceName  string `json:"ServiceName,omitempty"`
	Region       string `json:"Region,omitempty"`
}

// AccessKeyLastUsedXML is the XML representation for GetAccessKeyLastUsed
// response. LastUsedDate must be omitted (not the literal "N/A") when the
// key has never been used -- see access_keys.go's GetAccessKeyLastUsed.
type AccessKeyLastUsedXML struct {
	LastUsedDate string `xml:"LastUsedDate,omitempty"`
	ServiceName  string `xml:"ServiceName"`
	Region       string `xml:"Region"`
}

// GetAccessKeyLastUsedResult contains the access key last used details.
type GetAccessKeyLastUsedResult struct {
	UserName          string               `xml:"UserName"`
	AccessKeyLastUsed AccessKeyLastUsedXML `xml:"AccessKeyLastUsed"`
}

// GetAccessKeyLastUsedResponse is the XML response for GetAccessKeyLastUsed.
type GetAccessKeyLastUsedResponse struct {
	XMLName                    xml.Name                   `xml:"GetAccessKeyLastUsedResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           ResponseMetadata           `xml:"ResponseMetadata"`
	GetAccessKeyLastUsedResult GetAccessKeyLastUsedResult `xml:"GetAccessKeyLastUsedResult"`
}

// UpdateAccessKeyResponse is the XML response for UpdateAccessKey.
type UpdateAccessKeyResponse struct {
	XMLName          xml.Name         `xml:"UpdateAccessKeyResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}
