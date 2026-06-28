package iam

import "encoding/xml"

// GetContextKeysResult holds the list of context keys for a policy.
type GetContextKeysResult struct {
	ContextKeyNames []string `xml:"ContextKeyNames>member"`
}

// GetContextKeysResponse is the XML response for GetContextKeysForCustomPolicy/GetContextKeysForPrincipalPolicy.
type GetContextKeysResponse struct {
	XMLName              xml.Name             `xml:"GetContextKeysForCustomPolicyResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	ResponseMetadata     ResponseMetadata     `xml:"ResponseMetadata"`
	GetContextKeysResult GetContextKeysResult `xml:"GetContextKeysForCustomPolicyResult"`
}

// UpdateServiceSpecificCredentialResponse is the XML response for UpdateServiceSpecificCredential.
type UpdateServiceSpecificCredentialResponse struct {
	XMLName          xml.Name         `xml:"UpdateServiceSpecificCredentialResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// GetMFADeviceResult holds MFA device details.
type GetMFADeviceResult struct {
	UserName     string `xml:"UserName,omitempty"`
	SerialNumber string `xml:"SerialNumber"`
	EnableDate   string `xml:"EnableDate"`
}

// GetMFADeviceResponse is the XML response for GetMFADevice.
type GetMFADeviceResponse struct {
	XMLName            xml.Name           `xml:"GetMFADeviceResponse"`
	Xmlns              string             `xml:"xmlns,attr"`
	GetMFADeviceResult GetMFADeviceResult `xml:"GetMFADeviceResult"`
	ResponseMetadata   ResponseMetadata   `xml:"ResponseMetadata"`
}
