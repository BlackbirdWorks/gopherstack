package azuregraph

const microsoftServicesTenantID = "f8cdef31-a31e-4b4a-93e4-5f571e91255a"

type seedRole struct{ id, value, name string }

// graphRoles is a subset of Microsoft Graph application permissions (appRoles).
func graphRoles() []seedRole {
	return []seedRole{
		{"1bfefb4e-e0b5-418b-a88f-73c46d2cc8e9", "Application.ReadWrite.All", "Read and write all applications"},
		{"9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30", "Application.Read.All", "Read all applications"},
		{"7ab1d382-f21e-4acd-a863-ba3e13f7da61", "Directory.Read.All", "Read directory data"},
		{"19dbc75e-c2e2-444c-a770-ec69d8559fc7", "Directory.ReadWrite.All", "Read and write directory data"},
		{"df021288-bdef-4463-88db-98f22de89214", "User.Read.All", "Read all users' full profiles"},
		{"741f803b-c850-494e-b5df-cde7c675a1ca", "User.ReadWrite.All", "Read and write all users' full profiles"},
		{"5b567255-7703-4780-807c-7be8301ae99b", "Group.Read.All", "Read all groups"},
		{"62a82d76-70ea-41e2-9197-370581804d09", "Group.ReadWrite.All", "Read and write all groups"},
		{"b633e1c5-b582-4048-a93e-9f11b44c7e96", "Mail.Send", "Send mail as any user"},
	}
}

// graphScopes is a subset of Microsoft Graph delegated permissions (oauth2PermissionScopes).
func graphScopes() []seedRole {
	return []seedRole{
		{"e1fe6dd8-ba31-4d61-89e7-88639da4683d", "User.Read", "Sign in and read user profile"},
		{"37f7f235-527c-4136-accd-4a02d197296e", "openid", "Sign users in"},
		{"14dad69e-099b-42c9-810b-d002981feec1", "profile", "View users' basic profile"},
		{"64a6cdd6-aab1-4aaf-94b8-3cc8405e90d0", "email", "View users' email address"},
		{
			"7427e0e9-2fba-42fe-b0c0-848c9e6a8182",
			"offline_access",
			"Maintain access to data you have given it access to",
		},
	}
}

// seed installs the first-party Microsoft Graph service principal so appRoleAssignments and
// requiredResourceAccess can reference its well-known role and scope IDs.
func (b *Backend) seed() {
	roles := make([]AppRole, 0, len(graphRoles()))
	for _, r := range graphRoles() {
		roles = append(roles, AppRole{
			ID: r.id, Value: r.value, DisplayName: r.name, Description: r.name,
			AllowedMemberTypes: []string{typeApplication}, IsEnabled: true, Origin: typeApplication,
		})
	}

	scopes := make([]PermissionScope, 0, len(graphScopes()))
	for _, s := range graphScopes() {
		scopes = append(scopes, PermissionScope{
			ID: s.id, Value: s.value, AdminConsentDisplayName: s.name, AdminConsentDescription: s.name,
			Type: typeUser, IsEnabled: true,
		})
	}

	sp := &ServicePrincipal{
		ID: microsoftGraphSPID, AppID: MicrosoftGraphAppID, DisplayName: "Microsoft Graph",
		AppDisplayName: "Microsoft Graph", AppOwnerOrganizationID: microsoftServicesTenantID,
		ServicePrincipalType: typeApplication, SignInAudience: "AzureADMultipleOrgs", AccountEnabled: true,
		ServicePrincipalNames: []string{MicrosoftGraphAppID, "https://graph.microsoft.com"},
		AppRoles:              roles, OAuth2PermissionScopes: scopes, CreatedDateTime: b.stamp(),
	}
	sp.normalize()
	b.sps.put(sp.ID, sp)

	dev := &ServicePrincipal{
		ID: devClientSPID, AppID: lc(b.cfg.DevClientID), DisplayName: "gopherstack-dev-client",
		AppDisplayName: "gopherstack-dev-client", AppOwnerOrganizationID: b.cfg.TenantID,
		ServicePrincipalType: typeApplication, SignInAudience: audienceMyOrg, AccountEnabled: true,
		ServicePrincipalNames: []string{lc(b.cfg.DevClientID)}, CreatedDateTime: b.stamp(),
	}
	dev.normalize()
	b.sps.put(dev.ID, dev)
}

func isBuiltin(id string) bool { return lc(id) == microsoftGraphSPID || lc(id) == devClientSPID }
