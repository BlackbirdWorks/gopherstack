package iam

import (
	"encoding/xml"
	"time"
)

// RoleTemplateInlinePolicy is one inline policy template embedded in roles
// created from a RoleTemplateVersion (iam@v1.63.0 types.InlinePolicy).
type RoleTemplateInlinePolicy struct {
	PolicyName     string `json:"PolicyName"`
	PolicyDocument string `json:"PolicyDocument"`
}

// RoleTemplateParameter defines one substitutable @{name} parameter in a
// role template's patterns (types.ParameterDefinition).
type RoleTemplateParameter struct {
	Name         string `json:"Name"`
	Type         string `json:"Type"`
	DefaultValue string `json:"DefaultValue,omitempty"`
	Description  string `json:"Description,omitempty"`
	SubType      string `json:"SubType,omitempty"`
	Immutable    bool   `json:"Immutable,omitempty"`
	IsRequired   bool   `json:"IsRequired,omitempty"`
}

// RoleTemplateTag is one literal (non-substituted) tag applied to roles
// created from a RoleTemplateVersion (types.TagTemplate).
type RoleTemplateTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// RoleTemplateVersion represents one minor version of an IAM role template
// (iam@v1.63.0 types.RoleTemplateVersion). Real AWS has no Create/Put/List
// operation for role templates in this SDK version -- GetRoleTemplateVersion
// and AcquireRole are the only two operations that ever reference one -- so
// this backend has no way to originate a template itself. It is seeded only
// through AddRoleTemplateVersionInternal (role_templates.go), the same
// console/out-of-band-provisioned-resource seam services/cloudwatchlogs's
// AddAnomalyInternal and services/quicksight's AddAppInternal already
// establish for a comparable gap.
type RoleTemplateVersion struct {
	CreateTimestamp                  time.Time                  `json:"CreateTimestamp"`
	TemplateArn                      string                     `json:"TemplateArn"`
	TemplateName                     string                     `json:"TemplateName,omitempty"`
	Description                      string                     `json:"Description,omitempty"`
	AssumeRolePolicyDocumentTemplate string                     `json:"AssumeRolePolicyDocumentTemplate,omitempty"`
	RoleNamePattern                  string                     `json:"RoleNamePattern,omitempty"`
	RolePathPattern                  string                     `json:"RolePathPattern,omitempty"`
	RoleDescriptionPattern           string                     `json:"RoleDescriptionPattern,omitempty"`
	PermissionBoundaryArn            string                     `json:"PermissionBoundaryArn,omitempty"`
	ManagedByType                    string                     `json:"ManagedByType,omitempty"`
	ManagedByValue                   string                     `json:"ManagedByValue,omitempty"`
	ManagedPolicyArns                []string                   `json:"ManagedPolicyArns,omitempty"`
	InlinePolicyTemplates            []RoleTemplateInlinePolicy `json:"InlinePolicyTemplates,omitempty"`
	ParametersDefinition             []RoleTemplateParameter    `json:"ParametersDefinition,omitempty"`
	RoleTagsTemplate                 []RoleTemplateTag          `json:"RoleTagsTemplate,omitempty"`
	MajorVersion                     int32                      `json:"MajorVersion,omitempty"`
	MinorVersion                     int32                      `json:"MinorVersion"`
	DefaultMinorVersion              int32                      `json:"DefaultMinorVersion,omitempty"`
	MaxSessionDuration               int32                      `json:"MaxSessionDuration,omitempty"`
	Enabled                          bool                       `json:"Enabled"`
}

// ---- XML wire shapes ----

// RoleTemplateInlinePolicyXML is the XML shape of one InlinePolicyTemplates entry.
type RoleTemplateInlinePolicyXML struct {
	PolicyDocument string `xml:"PolicyDocument"`
	PolicyName     string `xml:"PolicyName"`
}

// RoleTemplateParameterXML is the XML shape of one ParametersDefinition entry.
type RoleTemplateParameterXML struct {
	Name         string `xml:"Name"`
	Type         string `xml:"Type"`
	DefaultValue string `xml:"DefaultValue,omitempty"`
	Description  string `xml:"Description,omitempty"`
	SubType      string `xml:"SubType,omitempty"`
	Immutable    bool   `xml:"Immutable"`
	IsRequired   bool   `xml:"IsRequired"`
}

// RoleTemplateVersionXML is the XML shape of RoleTemplateVersion
// (GetRoleTemplateVersionResult.RoleTemplateVersion).
type RoleTemplateVersionXML struct {
	CreateTimestamp                  string                        `xml:"CreateTimestamp"`
	TemplateArn                      string                        `xml:"TemplateArn"`
	TemplateName                     string                        `xml:"TemplateName,omitempty"`
	Description                      string                        `xml:"Description,omitempty"`
	AssumeRolePolicyDocumentTemplate string                        `xml:"AssumeRolePolicyDocumentTemplate,omitempty"`
	RoleNamePattern                  string                        `xml:"RoleNamePattern,omitempty"`
	RolePathPattern                  string                        `xml:"RolePathPattern,omitempty"`
	RoleDescriptionPattern           string                        `xml:"RoleDescriptionPattern,omitempty"`
	PermissionBoundaryArn            string                        `xml:"PermissionBoundaryArn,omitempty"`
	ManagedByType                    string                        `xml:"ManagedByType,omitempty"`
	ManagedByValue                   string                        `xml:"ManagedByValue,omitempty"`
	ManagedPolicyArns                []string                      `xml:"ManagedPolicyArns>member,omitempty"`
	InlinePolicyTemplates            []RoleTemplateInlinePolicyXML `xml:"InlinePolicyTemplates>member,omitempty"`
	ParametersDefinition             []RoleTemplateParameterXML    `xml:"ParametersDefinition>member,omitempty"`
	RoleTagsTemplate                 []TagXML                      `xml:"RoleTagsTemplate>member,omitempty"`
	MajorVersion                     int32                         `xml:"MajorVersion,omitempty"`
	MinorVersion                     int32                         `xml:"MinorVersion"`
	DefaultMinorVersion              int32                         `xml:"DefaultMinorVersion,omitempty"`
	MaxSessionDuration               int32                         `xml:"MaxSessionDuration,omitempty"`
	Enabled                          bool                          `xml:"Enabled"`
}

// GetRoleTemplateVersionResult wraps the requested role template version.
type GetRoleTemplateVersionResult struct {
	RoleTemplateVersion RoleTemplateVersionXML `xml:"RoleTemplateVersion"`
}

// GetRoleTemplateVersionResponse is the XML response for GetRoleTemplateVersion.
type GetRoleTemplateVersionResponse struct {
	XMLName                      xml.Name                     `xml:"GetRoleTemplateVersionResponse"`
	Xmlns                        string                       `xml:"xmlns,attr"`
	ResponseMetadata             ResponseMetadata             `xml:"ResponseMetadata"`
	GetRoleTemplateVersionResult GetRoleTemplateVersionResult `xml:"GetRoleTemplateVersionResult"`
}

// AcquireRoleResult wraps the role AcquireRole created.
type AcquireRoleResult struct {
	Role RoleXML `xml:"Role"`
}

// AcquireRoleResponse is the XML response for AcquireRole.
type AcquireRoleResponse struct {
	XMLName           xml.Name          `xml:"AcquireRoleResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	ResponseMetadata  ResponseMetadata  `xml:"ResponseMetadata"`
	AcquireRoleResult AcquireRoleResult `xml:"AcquireRoleResult"`
}

// toRoleTemplateVersionXML converts a RoleTemplateVersion to its XML shape.
func toRoleTemplateVersionXML(v *RoleTemplateVersion) RoleTemplateVersionXML {
	x := RoleTemplateVersionXML{
		CreateTimestamp:                  isoTime(v.CreateTimestamp),
		TemplateArn:                      v.TemplateArn,
		TemplateName:                     v.TemplateName,
		Description:                      v.Description,
		AssumeRolePolicyDocumentTemplate: v.AssumeRolePolicyDocumentTemplate,
		RoleNamePattern:                  v.RoleNamePattern,
		RolePathPattern:                  v.RolePathPattern,
		RoleDescriptionPattern:           v.RoleDescriptionPattern,
		PermissionBoundaryArn:            v.PermissionBoundaryArn,
		ManagedByType:                    v.ManagedByType,
		ManagedByValue:                   v.ManagedByValue,
		ManagedPolicyArns:                v.ManagedPolicyArns,
		MajorVersion:                     v.MajorVersion,
		MinorVersion:                     v.MinorVersion,
		DefaultMinorVersion:              v.DefaultMinorVersion,
		MaxSessionDuration:               v.MaxSessionDuration,
		Enabled:                          v.Enabled,
	}

	for _, ip := range v.InlinePolicyTemplates {
		x.InlinePolicyTemplates = append(x.InlinePolicyTemplates, RoleTemplateInlinePolicyXML{
			PolicyName:     ip.PolicyName,
			PolicyDocument: ip.PolicyDocument,
		})
	}

	for _, p := range v.ParametersDefinition {
		x.ParametersDefinition = append(x.ParametersDefinition, RoleTemplateParameterXML(p))
	}

	for _, t := range v.RoleTagsTemplate {
		x.RoleTagsTemplate = append(x.RoleTagsTemplate, TagXML(t))
	}

	return x
}
