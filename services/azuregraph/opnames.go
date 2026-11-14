package azuregraph

import (
	"net/http"
	"slices"
	"strings"
)

const (
	restoreSegments    = 4
	deletedItemSegment = 3
	roleKindSegment    = 2
	roleItemSegment    = 3
	ficBase            = "FederatedIdentityCredential"
)

func crudVerbs() map[string]string {
	return map[string]string{http.MethodGet: verbGet, http.MethodPatch: "Update", http.MethodDelete: verbDelete}
}

// extendedOperation names batch, deleted-item, role-management and federated credential requests.
func extendedOperation(method, name string, segs []string) (string, bool) {
	switch {
	case name == "$batch":
		return "Batch", true
	case name == "Directory":
		return directoryOperation(method, segs), true
	case name == "RoleManagement":
		return roleOperation(method, segs), true
	case slices.ContainsFunc(segs, func(x string) bool { return strings.EqualFold(x, "federatedIdentityCredentials") }):
		return ficOperation(method, segs), true
	}

	return "", false
}

func directoryOperation(method string, segs []string) string {
	switch {
	case len(segs) == restoreSegments:
		return "RestoreDeletedItem"
	case method == http.MethodDelete:
		return "PermanentDeleteDeletedItem"
	case len(segs) == deletedItemSegment && !strings.HasPrefix(segs[2], graphTypePrefix):
		return "GetDeletedItem"
	}

	return "ListDeletedItems"
}

func roleOperation(method string, segs []string) string {
	definitions := len(segs) > roleKindSegment && strings.EqualFold(segs[roleKindSegment], "roleDefinitions")
	single := len(segs) > roleItemSegment

	switch {
	case definitions && single:
		return "GetRoleDefinition"
	case definitions:
		return "ListRoleDefinitions"
	case method == http.MethodPost:
		return "CreateRoleAssignment"
	case method == http.MethodDelete:
		return "DeleteRoleAssignment"
	case single:
		return "GetRoleAssignment"
	}

	return "ListRoleAssignments"
}

func ficOperation(method string, segs []string) string {
	p := slices.IndexFunc(segs, func(x string) bool { return strings.EqualFold(x, "federatedIdentityCredentials") })
	if len(segs)-p-1 == 0 {
		if method == http.MethodPost {
			return "Create" + ficBase
		}

		return "ListFederatedIdentityCredentials"
	}

	return crudVerbs()[method] + ficBase
}
