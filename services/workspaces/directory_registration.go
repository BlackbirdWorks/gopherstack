package workspaces

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	userIdentityTypeDirectoryService = "AWS_DIRECTORY_SERVICE"
	userIdentityTypeCustomerManaged  = "CUSTOMER_MANAGED"
	userIdentityTypeIdentityCenter   = "AWS_IAM_IDENTITY_CENTER"
	workspaceTypePersonal            = "PERSONAL"
	workspaceTypePools               = "POOLS"
	tenancyShared                    = "SHARED"
	tenancyDedicated                 = "DEDICATED"
	dirFilterUserIdentityType        = "USER_IDENTITY_TYPE"
	dirFilterWorkspaceType           = "WORKSPACE_TYPE"
	streamingPropertiesKey           = "Streaming_Properties"
	attrUserIdentityType             = "UserIdentityType"
	attrWorkspaceType                = "WorkspaceType"
)

// DirectoryActiveDirectoryConfig mirrors types.ActiveDirectoryConfig.
type DirectoryActiveDirectoryConfig struct {
	DomainName              string
	ServiceAccountSecretArn string
}

// DirectoryMicrosoftEntraConfig mirrors types.MicrosoftEntraConfig.
type DirectoryMicrosoftEntraConfig struct {
	ApplicationConfigSecretArn string
	TenantID                   string
}

// DirectoryIDCConfig mirrors types.IDCConfig; ApplicationArn is not modeled.
type DirectoryIDCConfig struct {
	InstanceArn string
}

// DirectoryRegistration carries the RegisterWorkspaceDirectoryInput members.
type DirectoryRegistration struct {
	Tags                          map[string]string
	ActiveDirectoryConfig         *DirectoryActiveDirectoryConfig
	MicrosoftEntraConfig          *DirectoryMicrosoftEntraConfig
	DirectoryID                   string
	IdcInstanceArn                string
	Tenancy                       string
	UserIdentityType              string
	WorkspaceType                 string
	WorkspaceDirectoryName        string
	WorkspaceDirectoryDescription string
	SubnetIDs                     []string
}

// DirectoryFilter mirrors types.DescribeWorkspaceDirectoriesFilter.
type DirectoryFilter struct {
	Name   string
	Values []string
}

func invalidDirParam(format string, args ...any) error {
	return awserr.New(fmt.Sprintf(format, args...), awserr.ErrInvalidParameter)
}

func validateDirectoryRegistration(reg DirectoryRegistration) error {
	if reg.DirectoryID == "" && reg.IdcInstanceArn == "" && reg.MicrosoftEntraConfig == nil {
		return invalidDirParam("DirectoryId is required unless IdcInstanceArn or MicrosoftEntraConfig is set")
	}

	enums := []struct {
		field string
		value string
		valid []string
	}{
		{
			"UserIdentityType", reg.UserIdentityType,
			[]string{userIdentityTypeDirectoryService, userIdentityTypeCustomerManaged, userIdentityTypeIdentityCenter},
		},
		{"WorkspaceType", reg.WorkspaceType, []string{workspaceTypePersonal, workspaceTypePools}},
		{"Tenancy", reg.Tenancy, []string{tenancyShared, tenancyDedicated}},
	}

	for _, e := range enums {
		if e.value != "" && !slices.Contains(e.valid, e.value) {
			return invalidDirParam("invalid %s %q", e.field, e.value)
		}
	}

	if ad := reg.ActiveDirectoryConfig; ad != nil && (ad.DomainName == "" || ad.ServiceAccountSecretArn == "") {
		return invalidDirParam("ActiveDirectoryConfig requires DomainName and ServiceAccountSecretArn")
	}

	return nil
}

// applyRegistrationAttributes stores the optional registration members on ds.Properties.
func applyRegistrationAttributes(ds *storedDirSettings, reg DirectoryRegistration) {
	set := func(key, val string) {
		if val != "" {
			ds.Properties[key] = val
		}
	}

	set(attrUserIdentityType, reg.UserIdentityType)
	set(attrWorkspaceType, reg.WorkspaceType)
	set("Tenancy", reg.Tenancy)
	set("WorkspaceDirectoryName", reg.WorkspaceDirectoryName)
	set("WorkspaceDirectoryDescription", reg.WorkspaceDirectoryDescription)
	set("IdcInstanceArn", reg.IdcInstanceArn)

	if reg.IdcInstanceArn != "" {
		ds.Properties["DirectoryType"] = "AWS_IAM_IDENTITY_CENTER"
	}

	if ad := reg.ActiveDirectoryConfig; ad != nil {
		set("AD_DomainName", ad.DomainName)
		set("AD_ServiceAccountSecretArn", ad.ServiceAccountSecretArn)
	}

	if me := reg.MicrosoftEntraConfig; me != nil {
		set("Entra_TenantId", me.TenantID)
		set("Entra_ApplicationConfigSecretArn", me.ApplicationConfigSecretArn)
	}
}

// dirAttr reads a registration attribute, applying the defaults real AWS reports.
func dirAttr(ds *storedDirSettings, key string) string {
	if v := ds.Properties[key]; v != "" {
		return v
	}

	switch key {
	case attrWorkspaceType:
		return workspaceTypePersonal
	case "Tenancy":
		return tenancyShared
	case attrUserIdentityType:
		switch {
		case ds.Properties["IdcInstanceArn"] != "":
			return userIdentityTypeIdentityCenter
		case ds.Properties["AD_DomainName"] != "":
			return userIdentityTypeCustomerManaged
		default:
			return userIdentityTypeDirectoryService
		}
	}

	return ""
}

func activeDirectoryConfigFromDS(ds *storedDirSettings) *DirectoryActiveDirectoryConfig {
	if ds.Properties["AD_DomainName"] == "" {
		return nil
	}

	return &DirectoryActiveDirectoryConfig{
		DomainName:              ds.Properties["AD_DomainName"],
		ServiceAccountSecretArn: ds.Properties["AD_ServiceAccountSecretArn"],
	}
}

func microsoftEntraConfigFromDS(ds *storedDirSettings) *DirectoryMicrosoftEntraConfig {
	if !dsHasAnyKey(ds, []string{"Entra_TenantId", "Entra_ApplicationConfigSecretArn"}) {
		return nil
	}

	return &DirectoryMicrosoftEntraConfig{
		TenantID:                   ds.Properties["Entra_TenantId"],
		ApplicationConfigSecretArn: ds.Properties["Entra_ApplicationConfigSecretArn"],
	}
}

func idcConfigFromDS(ds *storedDirSettings) *DirectoryIDCConfig {
	if ds.Properties["IdcInstanceArn"] == "" {
		return nil
	}

	return &DirectoryIDCConfig{InstanceArn: ds.Properties["IdcInstanceArn"]}
}

// directoryFilterProps validates filters and maps each to its attribute key.
func directoryFilterProps(filters []DirectoryFilter) (map[string][]string, error) {
	out := make(map[string][]string, len(filters))

	for _, f := range filters {
		if f.Name != dirFilterUserIdentityType && f.Name != dirFilterWorkspaceType {
			return nil, invalidDirParam("invalid filter name %q", f.Name)
		}

		if len(f.Values) == 0 {
			return nil, invalidDirParam("filter %s requires at least one value", f.Name)
		}

		key := attrUserIdentityType
		if f.Name == dirFilterWorkspaceType {
			key = attrWorkspaceType
		}

		out[key] = append(out[key], f.Values...)
	}

	return out, nil
}

func matchesPropFilters(ds *storedDirSettings, filters map[string][]string) bool {
	for key, values := range filters {
		if !slices.Contains(values, dirAttr(ds, key)) {
			return false
		}
	}

	return true
}

// streamingPropertiesFromDS returns nil when ModifyStreamingProperties never ran.
func streamingPropertiesFromDS(ds *storedDirSettings) *StreamingProperties {
	raw := ds.Properties[streamingPropertiesKey]
	if raw == "" {
		return nil
	}

	var sp StreamingProperties
	if err := json.Unmarshal([]byte(raw), &sp); err != nil {
		return nil
	}

	return &sp
}
