package azuregraph

const (
	propAppID             = "appId"
	propCreatedDateTime   = "createdDateTime"
	propValue             = "value"
	audienceMyOrg         = "AzureADMyOrg"
	typeApplication       = "Application"
	typeUser              = "User"
	jsonNull              = "null"
	segRef                = "$ref"
	collApplications      = "applications"
	collUsers             = "users"
	fnEndsWith            = "endswith"
	pathWithID            = 2
	collServicePrincipals = "servicePrincipals"
	collGroups            = "groups"
)

func signInAudiences() []string {
	return []string{
		audienceMyOrg, "AzureADMultipleOrgs", "AzureADandPersonalMicrosoftAccount", "PersonalMicrosoftAccount",
	}
}

func appReadOnly() []string {
	return []string{
		"id",
		propAppID,
		propCreatedDateTime,
		"passwordCredentials",
		"keyCredentials",
		"publisherDomain",
		"certification",
	}
}

func spReadOnly() []string {
	return []string{"id", propCreatedDateTime, "passwordCredentials", "keyCredentials"}
}

func userReadOnly() []string { return []string{"id", propCreatedDateTime} }

func groupReadOnly() []string { return []string{"id", propCreatedDateTime, "mail"} }

func assignReadOnly() []string {
	return []string{"id", propCreatedDateTime, "principalDisplayName", "principalType", "resourceDisplayName"}
}

func grantReadOnly() []string { return []string{"id"} }
