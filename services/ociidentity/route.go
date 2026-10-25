package ociidentity

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opGetTenancy         = "GetTenancy"
	opListRegions        = "ListRegions"
	opListRegionSubs     = "ListRegionSubscriptions"
	opListADs            = "ListAvailabilityDomains"
	opListFDs            = "ListFaultDomains"
	opCreateCompartment  = "CreateCompartment"
	opGetCompartment     = "GetCompartment"
	opListCompartments   = "ListCompartments"
	opUpdateCompartment  = "UpdateCompartment"
	opDeleteCompartment  = "DeleteCompartment"
	opMoveCompartment    = "MoveCompartment"
	opRecoverCompartment = "RecoverCompartment"
	opCreateUser         = "CreateUser"
	opGetUser            = "GetUser"
	opListUsers          = "ListUsers"
	opUpdateUser         = "UpdateUser"
	opDeleteUser         = "DeleteUser"
	opCreateGroup        = "CreateGroup"
	opGetGroup           = "GetGroup"
	opListGroups         = "ListGroups"
	opUpdateGroup        = "UpdateGroup"
	opDeleteGroup        = "DeleteGroup"
	opAddUserToGroup     = "AddUserToGroup"
	opGetMembership      = "GetUserGroupMembership"
	opListMemberships    = "ListUserGroupMemberships"
	opRemoveFromGroup    = "RemoveUserFromGroup"
	opCreateDynGroup     = "CreateDynamicGroup"
	opGetDynGroup        = "GetDynamicGroup"
	opListDynGroups      = "ListDynamicGroups"
	opUpdateDynGroup     = "UpdateDynamicGroup"
	opDeleteDynGroup     = "DeleteDynamicGroup"
	opCreatePolicy       = "CreatePolicy"
	opGetPolicy          = "GetPolicy"
	opListPolicies       = "ListPolicies"
	opUpdatePolicy       = "UpdatePolicy"
	opDeletePolicy       = "DeletePolicy"
	opUploadAPIKey       = "UploadApiKey"
	opListAPIKeys        = "ListApiKeys"
	opDeleteAPIKey       = "DeleteApiKey"
	opCreateAuthTok      = "CreateAuthToken"
	opListAuthToks       = "ListAuthTokens"
	opUpdateAuthTok      = "UpdateAuthToken"
	opDeleteAuthTok      = "DeleteAuthToken"
	opCreateSecretKey    = "CreateCustomerSecretKey"
	opListSecretKeys     = "ListCustomerSecretKeys"
	opUpdateSecretKey    = "UpdateCustomerSecretKey"
	opDeleteSecretKey    = "DeleteCustomerSecretKey"
	opCreateSMTP         = "CreateSmtpCredential"
	opListSMTP           = "ListSmtpCredentials"
	opUpdateSMTP         = "UpdateSmtpCredential"
	opDeleteSMTP         = "DeleteSmtpCredential"
	opResetUIPassword    = "CreateOrResetUIPassword"
	opGetUIPassword      = "GetUserUIPasswordInformation"
	opCreateTagNS        = "CreateTagNamespace"
	opGetTagNS           = "GetTagNamespace"
	opListTagNS          = "ListTagNamespaces"
	opUpdateTagNS        = "UpdateTagNamespace"
	opDeleteTagNS        = "DeleteTagNamespace"
	opCreateTag          = "CreateTag"
	opGetTag             = "GetTag"
	opListTags           = "ListTags"
	opUpdateTag          = "UpdateTag"
	opDeleteTag          = "DeleteTag"
	opCreateTagDefault   = "CreateTagDefault"
	opGetTagDefault      = "GetTagDefault"
	opListTagDefaults    = "ListTagDefaults"
	opUpdateTagDefault   = "UpdateTagDefault"
	opDeleteTagDefault   = "DeleteTagDefault"
	opGetWorkRequest     = "GetWorkRequest"
	opListWorkRequests   = "ListWorkRequests"
	opAdminKeys          = "ManageAPIKeys"
	opUnknown            = "Unknown"
)

const (
	adminPrefix = "_gopherstack"
	apiBase     = "/20160918"
)

func rt(method, path, op string) restroute.Route {
	return restroute.Route{Method: method, Path: apiBase + path, Op: op}
}

func crud(base, idParam, create, get, list, update, del string) []restroute.Route {
	routes := []restroute.Route{
		rt(http.MethodPost, base, create), rt(http.MethodGet, base, list),
		rt(http.MethodDelete, base+"/{"+idParam+"}", del),
	}

	if get != "" {
		routes = append(routes, rt(http.MethodGet, base+"/{"+idParam+"}", get))
	}

	if update != "" {
		routes = append(routes, rt(http.MethodPut, base+"/{"+idParam+"}", update))
	}

	return routes
}

const routeCap = 96

func routeTable() []restroute.Route {
	routes := make([]restroute.Route, 0, routeCap)

	routes = append(routes,
		rt(http.MethodGet, "/tenancies/{tenancyId}", opGetTenancy),
		rt(http.MethodGet, "/tenancies/{tenancyId}/regionSubscriptions", opListRegionSubs),
		rt(http.MethodGet, "/regions", opListRegions),
		rt(http.MethodGet, "/availabilityDomains", opListADs),
		rt(http.MethodGet, "/faultDomains", opListFDs),
		rt(http.MethodPost, "/compartments/{compartmentId}/actions/moveCompartment", opMoveCompartment),
		rt(http.MethodPost, "/compartments/{compartmentId}/actions/recoverCompartment", opRecoverCompartment),
		rt(http.MethodPost, "/userGroupMemberships", opAddUserToGroup),
		rt(http.MethodGet, "/userGroupMemberships", opListMemberships),
		rt(http.MethodGet, "/userGroupMemberships/{membershipId}", opGetMembership),
		rt(http.MethodDelete, "/userGroupMemberships/{membershipId}", opRemoveFromGroup),
		rt(http.MethodPost, "/users/{userId}/apiKeys", opUploadAPIKey),
		rt(http.MethodGet, "/users/{userId}/apiKeys", opListAPIKeys),
		rt(http.MethodDelete, "/users/{userId}/apiKeys/{fingerprint}", opDeleteAPIKey),
		rt(http.MethodPost, "/users/{userId}/uiPassword", opResetUIPassword),
		rt(http.MethodGet, "/users/{userId}/uiPassword", opGetUIPassword),
		rt(http.MethodPost, "/tagNamespaces/{tagNamespaceId}/tags", opCreateTag),
		rt(http.MethodGet, "/tagNamespaces/{tagNamespaceId}/tags", opListTags),
		rt(http.MethodGet, "/tagNamespaces/{tagNamespaceId}/tags/{tagName}", opGetTag),
		rt(http.MethodPut, "/tagNamespaces/{tagNamespaceId}/tags/{tagName}", opUpdateTag),
		rt(http.MethodDelete, "/tagNamespaces/{tagNamespaceId}/tags/{tagName}", opDeleteTag),
		rt(http.MethodGet, "/workRequests", opListWorkRequests),
		rt(http.MethodGet, "/workRequests/{workRequestId}", opGetWorkRequest),
	)

	routes = append(routes, crud("/compartments", "compartmentId", opCreateCompartment, opGetCompartment,
		opListCompartments, opUpdateCompartment, opDeleteCompartment)...)
	routes = append(routes, crud("/users", "userId", opCreateUser, opGetUser, opListUsers,
		opUpdateUser, opDeleteUser)...)
	routes = append(routes, crud("/groups", "groupId", opCreateGroup, opGetGroup, opListGroups,
		opUpdateGroup, opDeleteGroup)...)
	routes = append(routes, crud("/dynamicGroups", "dynamicGroupId", opCreateDynGroup, opGetDynGroup,
		opListDynGroups, opUpdateDynGroup, opDeleteDynGroup)...)
	routes = append(routes, crud("/policies", "policyId", opCreatePolicy, opGetPolicy, opListPolicies,
		opUpdatePolicy, opDeletePolicy)...)
	routes = append(routes, crud("/tagNamespaces", "tagNamespaceId", opCreateTagNS, opGetTagNS, opListTagNS,
		opUpdateTagNS, opDeleteTagNS)...)
	routes = append(routes, crud("/tagDefaults", "tagDefaultId", opCreateTagDefault, opGetTagDefault,
		opListTagDefaults, opUpdateTagDefault, opDeleteTagDefault)...)
	routes = append(routes, crud("/users/{userId}/authTokens", "authTokenId", opCreateAuthTok, "", opListAuthToks,
		opUpdateAuthTok, opDeleteAuthTok)...)
	routes = append(routes, crud("/users/{userId}/customerSecretKeys", "customerSecretKeyId", opCreateSecretKey, "",
		opListSecretKeys, opUpdateSecretKey, opDeleteSecretKey)...)
	routes = append(routes, crud("/users/{userId}/smtpCredentials", "smtpCredentialId", opCreateSMTP, "",
		opListSMTP, opUpdateSMTP, opDeleteSMTP)...)

	return routes
}

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(routeTable())

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
