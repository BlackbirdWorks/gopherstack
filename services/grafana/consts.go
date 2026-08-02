package grafana

// Path segment literals shared by handler.go's route matcher (goconst: each
// appears at 3+ call sites across the route tree).
const (
	segTags            = "tags"
	segWorkspaces      = "workspaces"
	segServiceAccounts = "serviceaccounts"
	segAuthentication  = "authentication"
)

// JSON response key literals shared across handler_*.go (goconst: each
// appears at 3+ response-building call sites).
const (
	keyWorkspaceID      = "workspaceId"
	keyServiceAccountID = "serviceAccountId"
	// keyWorkspace is the CreateWorkspace/DescribeWorkspace/UpdateWorkspace/
	// DeleteWorkspace/AssociateLicense/DisassociateLicense response envelope
	// key ("workspace"); it reuses resourceTypeWorkspace's identical value.
	keyWorkspace = resourceTypeWorkspace
)
