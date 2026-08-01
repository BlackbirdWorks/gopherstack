package grafana

import (
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func fromNetworkAccessWire(w *networkAccessConfigurationWire) *NetworkAccessConfiguration {
	if w == nil {
		return nil
	}

	return &NetworkAccessConfiguration{
		PrefixListIDs: cloneStrs(w.PrefixListIDs),
		VpceIDs:       cloneStrs(w.VpceIDs),
	}
}

func toNetworkAccessWire(n *NetworkAccessConfiguration) *networkAccessConfigurationWire {
	if n == nil {
		return nil
	}

	return &networkAccessConfigurationWire{
		PrefixListIDs: cloneStrs(n.PrefixListIDs),
		VpceIDs:       cloneStrs(n.VpceIDs),
	}
}

func fromVpcConfigWire(w *vpcConfigurationWire) *VpcConfiguration {
	if w == nil {
		return nil
	}

	return &VpcConfiguration{
		SecurityGroupIDs: cloneStrs(w.SecurityGroupIDs),
		SubnetIDs:        cloneStrs(w.SubnetIDs),
	}
}

func toVpcConfigWire(v *VpcConfiguration) *vpcConfigurationWire {
	if v == nil {
		return nil
	}

	return &vpcConfigurationWire{
		SecurityGroupIDs: cloneStrs(v.SecurityGroupIDs),
		SubnetIDs:        cloneStrs(v.SubnetIDs),
	}
}

// toAuthenticationSummaryWire builds the AuthenticationSummary embedded in
// WorkspaceDescription/WorkspaceSummary responses.
func toAuthenticationSummaryWire(w *Workspace) *authenticationSummaryWire {
	return &authenticationSummaryWire{
		Providers:               cloneStrs(w.AuthenticationProviders),
		SamlConfigurationStatus: w.SamlConfigurationStatus,
	}
}

// toWorkspaceDescriptionWire builds the full WorkspaceDescription response
// shape for CreateWorkspace/DescribeWorkspace/UpdateWorkspace/DeleteWorkspace/
// AssociateLicense/DisassociateLicense.
func toWorkspaceDescriptionWire(w *Workspace) *workspaceDescriptionWire {
	out := &workspaceDescriptionWire{
		ID:                       w.ID,
		Authentication:           toAuthenticationSummaryWire(w),
		Created:                  awstime.Epoch(w.Created),
		Modified:                 awstime.Epoch(w.Modified),
		DataSources:              cloneStrs(w.DataSources),
		Endpoint:                 w.Endpoint,
		GrafanaVersion:           w.GrafanaVersion,
		Status:                   w.Status,
		AccountAccessType:        w.AccountAccessType,
		DegradedWorkspaceReason:  w.DegradedWorkspaceReason,
		Description:              w.Description,
		FreeTrialConsumed:        w.FreeTrialConsumed,
		GrafanaToken:             w.GrafanaToken,
		IPAddressType:            w.IPAddressType,
		KmsKeyID:                 w.KmsKeyID,
		LicenseType:              w.LicenseType,
		Name:                     w.Name,
		NetworkAccessControl:     toNetworkAccessWire(w.NetworkAccessControl),
		NotificationDestinations: cloneStrs(w.NotificationDestinations),
		OrganizationRoleName:     w.OrganizationRoleName,
		OrganizationalUnits:      cloneStrs(w.OrganizationalUnits),
		PermissionType:           w.PermissionType,
		StackSetName:             w.StackSetName,
		VpcConfiguration:         toVpcConfigWire(w.VpcConfiguration),
		WorkspaceRoleArn:         w.WorkspaceRoleARN,
	}

	if w.Tags != nil {
		out.Tags = w.Tags.Clone()
	}

	if !w.FreeTrialExpiration.IsZero() {
		out.FreeTrialExpiration = awstime.Epoch(w.FreeTrialExpiration)
	}

	if !w.LicenseExpiration.IsZero() {
		out.LicenseExpiration = awstime.Epoch(w.LicenseExpiration)
	}

	return out
}

// toWorkspaceSummaryWire builds the WorkspaceSummary response shape for
// ListWorkspaces.
func toWorkspaceSummaryWire(w *Workspace) workspaceSummaryWire {
	out := workspaceSummaryWire{
		Authentication:           toAuthenticationSummaryWire(w),
		Created:                  awstime.Epoch(w.Created),
		Modified:                 awstime.Epoch(w.Modified),
		ID:                       w.ID,
		Endpoint:                 w.Endpoint,
		GrafanaVersion:           w.GrafanaVersion,
		Status:                   w.Status,
		Description:              w.Description,
		GrafanaToken:             w.GrafanaToken,
		LicenseType:              w.LicenseType,
		Name:                     w.Name,
		NotificationDestinations: cloneStrs(w.NotificationDestinations),
	}

	if w.Tags != nil {
		out.Tags = w.Tags.Clone()
	}

	return out
}

// toAuthenticationDescriptionWire builds the fuller AuthenticationDescription
// response shape for DescribeWorkspaceAuthentication/UpdateWorkspaceAuthentication.
func toAuthenticationDescriptionWire(w *Workspace) *authenticationDescriptionWire {
	out := &authenticationDescriptionWire{Providers: cloneStrs(w.AuthenticationProviders)}

	if w.SsoClientID != "" {
		out.AwsSso = &awsSsoAuthenticationWire{SsoClientID: w.SsoClientID}
	}

	if w.SamlConfigurationStatus != "" {
		out.Saml = &samlAuthenticationWire{
			Status:        w.SamlConfigurationStatus,
			Configuration: toSamlConfigWire(w.SamlConfig),
		}
	}

	return out
}

func toSamlConfigWire(s *SamlConfiguration) *samlConfigurationWire {
	if s == nil {
		return nil
	}

	out := &samlConfigurationWire{
		AllowedOrganizations:  cloneStrs(s.AllowedOrganizations),
		LoginValidityDuration: s.LoginValidityDuration,
	}

	if s.IdpMetadataURL != "" || s.IdpMetadataXML != "" {
		out.IdpMetadata = &idpMetadataWire{URL: s.IdpMetadataURL, XML: s.IdpMetadataXML}
	}

	if s.AssertionAttributes != nil {
		a := s.AssertionAttributes
		out.AssertionAttributes = &assertionAttributesWire{
			Email: a.Email, Groups: a.Groups, Login: a.Login,
			Name: a.Name, Org: a.Org, Role: a.Role,
		}
	}

	if s.RoleValues != nil {
		out.RoleValues = &roleValuesWire{
			Admin:  cloneStrs(s.RoleValues.Admin),
			Editor: cloneStrs(s.RoleValues.Editor),
		}
	}

	return out
}

func fromSamlConfigWire(w *samlConfigurationWire) *SamlConfiguration {
	if w == nil {
		return nil
	}

	out := &SamlConfiguration{
		AllowedOrganizations:  cloneStrs(w.AllowedOrganizations),
		LoginValidityDuration: w.LoginValidityDuration,
	}

	if w.IdpMetadata != nil {
		out.IdpMetadataURL = w.IdpMetadata.URL
		out.IdpMetadataXML = w.IdpMetadata.XML
	}

	if w.AssertionAttributes != nil {
		a := w.AssertionAttributes
		out.AssertionAttributes = &AssertionAttributes{
			Email: a.Email, Groups: a.Groups, Login: a.Login,
			Name: a.Name, Org: a.Org, Role: a.Role,
		}
	}

	if w.RoleValues != nil {
		out.RoleValues = &RoleValues{
			Admin:  cloneStrs(w.RoleValues.Admin),
			Editor: cloneStrs(w.RoleValues.Editor),
		}
	}

	return out
}

func toServiceAccountSummaryWire(sa *ServiceAccount) serviceAccountSummaryWire {
	return serviceAccountSummaryWire{
		ID:          sa.ID,
		Name:        sa.Name,
		GrafanaRole: sa.GrafanaRole,
		IsDisabled:  strconv.FormatBool(sa.IsDisabled),
	}
}

func toServiceAccountTokenSummaryWire(t *ServiceAccountToken) serviceAccountTokenSummaryWire {
	out := serviceAccountTokenSummaryWire{
		ID:        t.ID,
		Name:      t.Name,
		CreatedAt: awstime.Epoch(t.CreatedAt),
		ExpiresAt: awstime.Epoch(t.ExpiresAt),
	}

	if !t.LastUsedAt.IsZero() {
		out.LastUsedAt = awstime.Epoch(t.LastUsedAt)
	}

	return out
}

func toPermissionEntryWire(p *Permission) permissionEntryWire {
	return permissionEntryWire{
		Role: p.Role,
		User: userWire{ID: p.UserID, Type: p.UserType},
	}
}
