package iam

import "encoding/xml"

// ---- GetInstanceProfile ----

// GetInstanceProfileResult wraps the instance profile.
type GetInstanceProfileResult struct {
	InstanceProfile InstanceProfileXML `xml:"InstanceProfile"`
}

// GetInstanceProfileResponse is the XML response for GetInstanceProfile.
type GetInstanceProfileResponse struct {
	XMLName                  xml.Name                 `xml:"GetInstanceProfileResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
	GetInstanceProfileResult GetInstanceProfileResult `xml:"GetInstanceProfileResult"`
}

// ---- ListInstanceProfilesForRole ----

// ListInstanceProfilesForRoleResult contains the instance profile list.
type ListInstanceProfilesForRoleResult struct {
	InstanceProfiles []InstanceProfileXML `xml:"InstanceProfiles>member"`
	IsTruncated      bool                 `xml:"IsTruncated"`
}

// ListInstanceProfilesForRoleResponse is the XML response for ListInstanceProfilesForRole.
type ListInstanceProfilesForRoleResponse struct {
	XMLName                           xml.Name                          `xml:"ListInstanceProfilesForRoleResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  ResponseMetadata                  `xml:"ResponseMetadata"`
	ListInstanceProfilesForRoleResult ListInstanceProfilesForRoleResult `xml:"ListInstanceProfilesForRoleResult"`
}
