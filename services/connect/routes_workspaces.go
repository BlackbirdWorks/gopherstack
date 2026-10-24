package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func workspaceRoutes() []restroute.Route {
	const (
		list = "/workspaces/{InstanceId}"
		one  = list + "/{WorkspaceId}"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: list, Op: "CreateWorkspace"},
		{Method: http.MethodGet, Path: list, Op: "ListWorkspaces"},
		{Method: http.MethodGet, Path: one, Op: "DescribeWorkspace"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteWorkspace"},
		{Method: http.MethodPost, Path: one + "/metadata", Op: "UpdateWorkspaceMetadata"},
		{Method: http.MethodPost, Path: one + "/theme", Op: "UpdateWorkspaceTheme"},
		{Method: http.MethodPost, Path: one + "/visibility", Op: "UpdateWorkspaceVisibility"},
		{Method: http.MethodPost, Path: one + "/associate", Op: "AssociateWorkspace"},
		{Method: http.MethodPost, Path: one + "/disassociate", Op: "DisassociateWorkspace"},
		{Method: http.MethodPut, Path: one + "/pages", Op: "CreateWorkspacePage"},
		{Method: http.MethodGet, Path: one + "/pages", Op: "ListWorkspacePages"},
		{Method: http.MethodPost, Path: one + "/pages/{Page}", Op: "UpdateWorkspacePage"},
		{Method: http.MethodDelete, Path: one + "/pages/{Page}", Op: "DeleteWorkspacePage"},
		{Method: http.MethodPost, Path: one + "/media", Op: "ImportWorkspaceMedia"},
		{Method: http.MethodGet, Path: one + "/media", Op: "ListWorkspaceMedia"},
		{Method: http.MethodDelete, Path: one + "/media", Op: "DeleteWorkspaceMedia"},
	}
}
