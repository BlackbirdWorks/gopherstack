package grafana

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func workspaceKeyFn(v *Workspace) string { return v.ID }

func apiKeyKeyFn(v *APIKey) string { return v.WorkspaceID + "::" + v.KeyName }

func apiKeyWorkspaceIndexKeyFn(v *APIKey) string { return v.WorkspaceID }

func serviceAccountKeyFn(v *ServiceAccount) string { return v.WorkspaceID + "::" + v.ID }

func serviceAccountWorkspaceIndexKeyFn(v *ServiceAccount) string { return v.WorkspaceID }

func tokenKeyFn(v *ServiceAccountToken) string {
	return v.WorkspaceID + "::" + v.ServiceAccountID + "::" + v.ID
}

func tokenServiceAccountIndexKeyFn(v *ServiceAccountToken) string {
	return v.WorkspaceID + "::" + v.ServiceAccountID
}

func permissionKeyFn(v *Permission) string {
	return v.WorkspaceID + "::" + v.UserType + "::" + v.UserID
}

func permissionWorkspaceIndexKeyFn(v *Permission) string { return v.WorkspaceID }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/s3tables/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.workspaces = store.Register(b.registry, "workspaces", store.New(workspaceKeyFn))

	b.apiKeys = store.Register(b.registry, "apiKeys", store.New(apiKeyKeyFn))
	b.apiKeysByWorkspace = b.apiKeys.AddIndex("byWorkspace", apiKeyWorkspaceIndexKeyFn)

	b.serviceAccounts = store.Register(b.registry, "serviceAccounts", store.New(serviceAccountKeyFn))
	b.serviceAccountsByWorkspace = b.serviceAccounts.AddIndex("byWorkspace", serviceAccountWorkspaceIndexKeyFn)

	b.tokens = store.Register(b.registry, "tokens", store.New(tokenKeyFn))
	b.tokensByServiceAccount = b.tokens.AddIndex("byServiceAccount", tokenServiceAccountIndexKeyFn)

	b.permissions = store.Register(b.registry, "permissions", store.New(permissionKeyFn))
	b.permissionsByWorkspace = b.permissions.AddIndex("byWorkspace", permissionWorkspaceIndexKeyFn)
}
