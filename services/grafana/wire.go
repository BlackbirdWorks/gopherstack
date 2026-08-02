package grafana

// This file defines the JSON wire shapes for every operation's request and
// response bodies, verified field-by-field against aws-sdk-go-v2/service/grafana's
// serializers.go/deserializers.go (see PARITY.md for the verification method).
// Field names are the smithy-generated lowerCamel form of the Go SDK's
// exported field names; every name below was checked against a
// serializeOpDocument*/deserializeDocument* case switch rather than assumed.

// networkAccessConfigurationWire mirrors types.NetworkAccessConfiguration.
type networkAccessConfigurationWire struct {
	PrefixListIDs []string `json:"prefixListIds"`
	VpceIDs       []string `json:"vpceIds"`
}

// vpcConfigurationWire mirrors types.VpcConfiguration.
type vpcConfigurationWire struct {
	SecurityGroupIDs []string `json:"securityGroupIds"`
	SubnetIDs        []string `json:"subnetIds"`
}

// assertionAttributesWire mirrors types.AssertionAttributes.
type assertionAttributesWire struct {
	Email  string `json:"email,omitempty"`
	Groups string `json:"groups,omitempty"`
	Login  string `json:"login,omitempty"`
	Name   string `json:"name,omitempty"`
	Org    string `json:"org,omitempty"`
	Role   string `json:"role,omitempty"`
}

// roleValuesWire mirrors types.RoleValues.
type roleValuesWire struct {
	Admin  []string `json:"admin,omitempty"`
	Editor []string `json:"editor,omitempty"`
}

// idpMetadataWire mirrors the types.IdpMetadata union (IdpMetadataMemberUrl
// XOR IdpMetadataMemberXml).
type idpMetadataWire struct {
	URL string `json:"url,omitempty"`
	XML string `json:"xml,omitempty"`
}

// samlConfigurationWire mirrors types.SamlConfiguration.
type samlConfigurationWire struct {
	IdpMetadata           *idpMetadataWire         `json:"idpMetadata,omitempty"`
	AssertionAttributes   *assertionAttributesWire `json:"assertionAttributes,omitempty"`
	RoleValues            *roleValuesWire          `json:"roleValues,omitempty"`
	AllowedOrganizations  []string                 `json:"allowedOrganizations,omitempty"`
	LoginValidityDuration int32                    `json:"loginValidityDuration,omitempty"`
}

// awsSsoAuthenticationWire mirrors types.AwsSsoAuthentication.
type awsSsoAuthenticationWire struct {
	SsoClientID string `json:"ssoClientId,omitempty"`
}

// samlAuthenticationWire mirrors types.SamlAuthentication.
type samlAuthenticationWire struct {
	Configuration *samlConfigurationWire `json:"configuration,omitempty"`
	Status        string                 `json:"status"`
}

// authenticationDescriptionWire mirrors types.AuthenticationDescription, the
// response shape for DescribeWorkspaceAuthentication/UpdateWorkspaceAuthentication.
type authenticationDescriptionWire struct {
	AwsSso    *awsSsoAuthenticationWire `json:"awsSso,omitempty"`
	Saml      *samlAuthenticationWire   `json:"saml,omitempty"`
	Providers []string                  `json:"providers"`
}

// authenticationSummaryWire mirrors types.AuthenticationSummary, embedded in
// WorkspaceDescription/WorkspaceSummary.
type authenticationSummaryWire struct {
	SamlConfigurationStatus string   `json:"samlConfigurationStatus,omitempty"`
	Providers               []string `json:"providers"`
}

// workspaceDescriptionWire mirrors types.WorkspaceDescription.
type workspaceDescriptionWire struct {
	Authentication           *authenticationSummaryWire      `json:"authentication"`
	Tags                     map[string]string               `json:"tags,omitempty"`
	VpcConfiguration         *vpcConfigurationWire           `json:"vpcConfiguration,omitempty"`
	NetworkAccessControl     *networkAccessConfigurationWire `json:"networkAccessControl,omitempty"`
	DegradedWorkspaceReason  string                          `json:"degradedWorkspaceReason,omitempty"`
	Description              string                          `json:"description,omitempty"`
	WorkspaceRoleArn         string                          `json:"workspaceRoleArn,omitempty"`
	StackSetName             string                          `json:"stackSetName,omitempty"`
	PermissionType           string                          `json:"permissionType,omitempty"`
	ID                       string                          `json:"id"`
	Endpoint                 string                          `json:"endpoint"`
	GrafanaVersion           string                          `json:"grafanaVersion"`
	Status                   string                          `json:"status"`
	AccountAccessType        string                          `json:"accountAccessType,omitempty"`
	OrganizationRoleName     string                          `json:"organizationRoleName,omitempty"`
	Name                     string                          `json:"name,omitempty"`
	GrafanaToken             string                          `json:"grafanaToken,omitempty"`
	IPAddressType            string                          `json:"ipAddressType,omitempty"`
	KmsKeyID                 string                          `json:"kmsKeyId,omitempty"`
	LicenseType              string                          `json:"licenseType,omitempty"`
	DataSources              []string                        `json:"dataSources"`
	NotificationDestinations []string                        `json:"notificationDestinations,omitempty"`
	OrganizationalUnits      []string                        `json:"organizationalUnits,omitempty"`
	LicenseExpiration        float64                         `json:"licenseExpiration,omitempty"`
	Created                  float64                         `json:"created"`
	Modified                 float64                         `json:"modified"`
	FreeTrialExpiration      float64                         `json:"freeTrialExpiration,omitempty"`
	FreeTrialConsumed        bool                            `json:"freeTrialConsumed,omitempty"`
}

// workspaceSummaryWire mirrors types.WorkspaceSummary, the ListWorkspaces
// item shape (a subset of workspaceDescriptionWire's fields).
type workspaceSummaryWire struct {
	Tags                     map[string]string          `json:"tags,omitempty"`
	Authentication           *authenticationSummaryWire `json:"authentication"`
	GrafanaVersion           string                     `json:"grafanaVersion"`
	ID                       string                     `json:"id"`
	Endpoint                 string                     `json:"endpoint"`
	Status                   string                     `json:"status"`
	Description              string                     `json:"description,omitempty"`
	GrafanaToken             string                     `json:"grafanaToken,omitempty"`
	LicenseType              string                     `json:"licenseType,omitempty"`
	Name                     string                     `json:"name,omitempty"`
	NotificationDestinations []string                   `json:"notificationDestinations,omitempty"`
	Modified                 float64                    `json:"modified"`
	Created                  float64                    `json:"created"`
}

// createWorkspaceRequest mirrors CreateWorkspaceInput's JSON body.
type createWorkspaceRequest struct {
	NetworkAccessControl              *networkAccessConfigurationWire `json:"networkAccessControl,omitempty"`
	VpcConfiguration                  *vpcConfigurationWire           `json:"vpcConfiguration,omitempty"`
	Tags                              map[string]string               `json:"tags,omitempty"`
	AccountAccessType                 string                          `json:"accountAccessType"`
	PermissionType                    string                          `json:"permissionType"`
	ClientToken                       string                          `json:"clientToken,omitempty"`
	Configuration                     string                          `json:"configuration,omitempty"`
	GrafanaVersion                    string                          `json:"grafanaVersion,omitempty"`
	IPAddressType                     string                          `json:"ipAddressType,omitempty"`
	KmsKeyID                          string                          `json:"kmsKeyId,omitempty"`
	OrganizationRoleName              string                          `json:"organizationRoleName,omitempty"`
	StackSetName                      string                          `json:"stackSetName,omitempty"`
	WorkspaceDescription              string                          `json:"workspaceDescription,omitempty"`
	WorkspaceName                     string                          `json:"workspaceName,omitempty"`
	WorkspaceRoleArn                  string                          `json:"workspaceRoleArn,omitempty"`
	AuthenticationProviders           []string                        `json:"authenticationProviders"`
	WorkspaceDataSources              []string                        `json:"workspaceDataSources,omitempty"`
	WorkspaceNotificationDestinations []string                        `json:"workspaceNotificationDestinations,omitempty"`
	WorkspaceOrganizationalUnits      []string                        `json:"workspaceOrganizationalUnits,omitempty"`
}

// updateWorkspaceRequest mirrors UpdateWorkspaceInput's JSON body.
type updateWorkspaceRequest struct {
	NetworkAccessControl              *networkAccessConfigurationWire `json:"networkAccessControl,omitempty"`
	VpcConfiguration                  *vpcConfigurationWire           `json:"vpcConfiguration,omitempty"`
	AccountAccessType                 string                          `json:"accountAccessType,omitempty"`
	IPAddressType                     string                          `json:"ipAddressType,omitempty"`
	OrganizationRoleName              string                          `json:"organizationRoleName,omitempty"`
	PermissionType                    string                          `json:"permissionType,omitempty"`
	StackSetName                      string                          `json:"stackSetName,omitempty"`
	WorkspaceDescription              string                          `json:"workspaceDescription,omitempty"`
	WorkspaceName                     string                          `json:"workspaceName,omitempty"`
	WorkspaceRoleArn                  string                          `json:"workspaceRoleArn,omitempty"`
	WorkspaceDataSources              []string                        `json:"workspaceDataSources,omitempty"`
	WorkspaceNotificationDestinations []string                        `json:"workspaceNotificationDestinations,omitempty"`
	WorkspaceOrganizationalUnits      []string                        `json:"workspaceOrganizationalUnits,omitempty"`
	RemoveNetworkAccessConfiguration  bool                            `json:"removeNetworkAccessConfiguration,omitempty"`
	RemoveVpcConfiguration            bool                            `json:"removeVpcConfiguration,omitempty"`
}

// updateWorkspaceAuthenticationRequest mirrors UpdateWorkspaceAuthenticationInput's JSON body.
type updateWorkspaceAuthenticationRequest struct {
	SamlConfiguration       *samlConfigurationWire `json:"samlConfiguration,omitempty"`
	AuthenticationProviders []string               `json:"authenticationProviders"`
}

// updateWorkspaceConfigurationRequest mirrors UpdateWorkspaceConfigurationInput's JSON body.
type updateWorkspaceConfigurationRequest struct {
	Configuration  string `json:"configuration"`
	GrafanaVersion string `json:"grafanaVersion,omitempty"`
}

// permissionEntryWire mirrors types.PermissionEntry.
type permissionEntryWire struct {
	User userWire `json:"user"`
	Role string   `json:"role"`
}

// userWire mirrors types.User.
type userWire struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// updateInstructionWire mirrors types.UpdateInstruction.
type updateInstructionWire struct {
	Action string     `json:"action"`
	Role   string     `json:"role"`
	Users  []userWire `json:"users"`
}

// updatePermissionsRequest mirrors UpdatePermissionsInput's JSON body.
type updatePermissionsRequest struct {
	UpdateInstructionBatch []updateInstructionWire `json:"updateInstructionBatch"`
}

// updateErrorWire mirrors types.UpdateError.
type updateErrorWire struct {
	CausedBy *updateInstructionWire `json:"causedBy"`
	Message  string                 `json:"message"`
	Code     int32                  `json:"code"`
}

// serviceAccountSummaryWire mirrors types.ServiceAccountSummary. IsDisabled
// is wire-typed as *string (not *bool) on the real SDK -- confirmed via
// types.go -- so this preserves that exact (unusual) shape rather than
// "fixing" it to a bool.
type serviceAccountSummaryWire struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	GrafanaRole string `json:"grafanaRole"`
	IsDisabled  string `json:"isDisabled"`
}

// serviceAccountTokenSummaryWire mirrors types.ServiceAccountTokenSummary
// (the list/describe shape -- no Key field).
type serviceAccountTokenSummaryWire struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	CreatedAt  float64 `json:"createdAt"`
	ExpiresAt  float64 `json:"expiresAt"`
	LastUsedAt float64 `json:"lastUsedAt,omitempty"`
}

// serviceAccountTokenSummaryWithKeyWire mirrors
// types.ServiceAccountTokenSummaryWithKey (the create-response shape --
// carries the plaintext Key exactly once).
type serviceAccountTokenSummaryWithKeyWire struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}
