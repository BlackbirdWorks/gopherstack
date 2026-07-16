package iam

import "encoding/xml"

// ---- Groups For User ----

// ListGroupsForUserXML holds a single group entry returned by ListGroupsForUser.
type ListGroupsForUserXML struct {
	GroupName  string `xml:"GroupName"`
	GroupID    string `xml:"GroupId"`
	Arn        string `xml:"Arn"`
	Path       string `xml:"Path"`
	CreateDate string `xml:"CreateDate"`
}

// ListGroupsForUserResult contains the list of groups.
type ListGroupsForUserResult struct {
	Groups      []ListGroupsForUserXML `xml:"Groups>member"`
	IsTruncated bool                   `xml:"IsTruncated"`
}

// ListGroupsForUserResponse is the XML response for ListGroupsForUser.
type ListGroupsForUserResponse struct {
	XMLName                 xml.Name                `xml:"ListGroupsForUserResponse"`
	Xmlns                   string                  `xml:"xmlns,attr"`
	ResponseMetadata        ResponseMetadata        `xml:"ResponseMetadata"`
	ListGroupsForUserResult ListGroupsForUserResult `xml:"ListGroupsForUserResult"`
}

// ---- Update Group ----

// UpdateGroupResponse is the XML response for UpdateGroup.
type UpdateGroupResponse struct {
	XMLName          xml.Name         `xml:"UpdateGroupResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}
