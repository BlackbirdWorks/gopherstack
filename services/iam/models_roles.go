package iam

import "encoding/xml"

// ---- Update Role ----

// UpdateRoleResult wraps the updated role.
type UpdateRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// UpdateRoleResponse is the XML response for UpdateRole.
type UpdateRoleResponse struct {
	XMLName          xml.Name         `xml:"UpdateRoleResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	UpdateRoleResult UpdateRoleResult `xml:"UpdateRoleResult"`
}

// UpdateRoleDescriptionResult wraps the updated role.
type UpdateRoleDescriptionResult struct {
	Role RoleXML `xml:"Role"`
}

// UpdateRoleDescriptionResponse is the XML response for UpdateRoleDescription.
type UpdateRoleDescriptionResponse struct {
	XMLName                     xml.Name                    `xml:"UpdateRoleDescriptionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	ResponseMetadata            ResponseMetadata            `xml:"ResponseMetadata"`
	UpdateRoleDescriptionResult UpdateRoleDescriptionResult `xml:"UpdateRoleDescriptionResult"`
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

// GetServiceLinkedRoleDeletionStatusResult contains the deletion status.
type GetServiceLinkedRoleDeletionStatusResult struct {
	Status string `xml:"Status"`
}

// GetServiceLinkedRoleDeletionStatusResponse is the XML response for GetServiceLinkedRoleDeletionStatus.
type GetServiceLinkedRoleDeletionStatusResponse struct {
	XMLName          xml.Name         `xml:"GetServiceLinkedRoleDeletionStatusResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	// GetServiceLinkedRoleDeletionStatusResult mirrors the AWS API field name.
	GetServiceLinkedRoleDeletionStatusResult GetServiceLinkedRoleDeletionStatusResult `xml:"GetServiceLinkedRoleDeletionStatusResult"` //nolint:lll // AWS contract
}

// deleteServiceLinkedRoleResult contains the deletion task ID for DeleteServiceLinkedRole.
type deleteServiceLinkedRoleResult struct {
	DeletionTaskID string `xml:"DeletionTaskId"`
}

// deleteServiceLinkedRoleResponse is the XML response for DeleteServiceLinkedRole.
type deleteServiceLinkedRoleResponse struct {
	XMLName                       xml.Name                      `xml:"DeleteServiceLinkedRoleResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	DeleteServiceLinkedRoleResult deleteServiceLinkedRoleResult `xml:"DeleteServiceLinkedRoleResult"`
	ResponseMetadata              ResponseMetadata              `xml:"ResponseMetadata"`
}
