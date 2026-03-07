package iam

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// iamAPIVersion is the IAM query protocol version used to identify IAM requests.
const (
	iamAPIVersion = "Version=2010-05-08"
	unknownOp     = "Unknown"
)

// Handler is the Echo HTTP handler for IAM operations.
type Handler struct {
	Backend StorageBackend
	actions map[string]iamActionFn
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new IAM handler with the given storage backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("iam.tags"),
	}
	h.actions = h.buildDispatchTable()

	return h
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("iam." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "IAM"
}

// GetSupportedOperations returns the list of supported IAM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateUser", "DeleteUser", "ListUsers", "GetUser",
		"CreateRole", "DeleteRole", "ListRoles", "GetRole",
		"CreatePolicy", "DeletePolicy", "ListPolicies",
		"GetPolicy", "GetPolicyVersion",
		"AttachUserPolicy", "DetachUserPolicy", "AttachRolePolicy",
		"DetachRolePolicy",
		"ListAttachedUserPolicies", "ListAttachedRolePolicies",
		"PutUserPolicy", "GetUserPolicy", "DeleteUserPolicy", "ListUserPolicies",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"PutGroupPolicy", "GetGroupPolicy", "DeleteGroupPolicy", "ListGroupPolicies",
		"PutUserPermissionsBoundary", "DeleteUserPermissionsBoundary",
		"PutRolePermissionsBoundary", "DeleteRolePermissionsBoundary",
		"UpdateAssumeRolePolicy",
		"CreateGroup", "DeleteGroup", "AddUserToGroup", "ListGroups",
		"AttachGroupPolicy", "DetachGroupPolicy", "ListAttachedGroupPolicies",
		"CreateAccessKey", "DeleteAccessKey", "ListAccessKeys",
		"CreateInstanceProfile", "DeleteInstanceProfile", "ListInstanceProfiles",
		"ListRoleTags", "TagRole", "UntagRole",
		"ListPolicyTags", "TagPolicy", "UntagPolicy",
		"ListUserTags", "TagUser", "UntagUser",
	}
}

// RouteMatcher returns a function that matches IAM requests.
// IAM requests are form-encoded POSTs containing the IAM API version.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		return strings.Contains(string(body), iamAPIVersion)
	}
}

// MatchPriority returns the routing priority for the IAM handler.
// Higher than Dashboard (50) but lower than DynamoDB/SSM (100).
func (h *Handler) MatchPriority() int {
	return service.PriorityFormStandard
}

// ExtractOperation extracts the IAM action from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOp
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOp
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource extracts the primary resource name from the IAM request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	for _, key := range []string{"UserName", "RoleName", "PolicyName", "GroupName", "InstanceProfileName"} {
		if v := vals.Get(key); v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for IAM requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read IAM request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid request body")
		}

		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log.DebugContext(ctx, "IAM request", "action", action)

		response, reqErr := h.dispatch(ctx, action, vals)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "text/xml")

		xmlBytes, marshalErr := marshalXML(response)
		if marshalErr != nil {
			log.ErrorContext(ctx, "failed to marshal IAM response", "action", action, "error", marshalErr)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

type iamActionFn func(vals url.Values, reqID string) (any, error)

// iamListTagsResult is the inner result element for ListRoleTags, ListPolicyTags, and ListUserTags.
// The XMLName field is set dynamically per action to produce the correct element name.
type iamListTagsResult struct {
	XMLName     xml.Name     `xml:""`
	Tags        []svcTags.KV `xml:"Tags>member"`
	IsTruncated bool         `xml:"IsTruncated"`
}

// iamListTagsResponse is the XML envelope for ListRoleTags, ListPolicyTags, and ListUserTags.
// The XMLName field is set dynamically per action to produce the correct element name.
type iamListTagsResponse struct {
	XMLName          xml.Name         `xml:""`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
	Result           iamListTagsResult
}

// iamSimpleTagResponse is the XML response for TagRole, UntagRole, TagPolicy, UntagPolicy,
// TagUser, and UntagUser. The XMLName field is set dynamically per action.
type iamSimpleTagResponse struct {
	XMLName          xml.Name         `xml:""`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata ResponseMetadata `xml:"ResponseMetadata"`
}

// buildDispatchTable merges all IAM sub-tables into a single map, called once at construction.
func (h *Handler) buildDispatchTable() map[string]iamActionFn {
	subtables := []map[string]iamActionFn{
		h.iamUserDispatchTable(),
		h.iamRoleDispatchTable(),
		h.iamPolicyBasicDispatchTable(),
		h.iamPolicyAttachDispatchTable(),
		h.iamGroupAttachedPolicyDispatchTable(),
		h.iamInlinePolicyDispatchTable(),
		h.iamPermissionBoundaryDispatchTable(),
		h.iamOtherOperationsDispatchTable(),
		h.iamGroupDispatchTable(),
		h.iamAccessKeyDispatchTable(),
		h.iamInstanceProfileDispatchTable(),
		h.iamTagDispatchTable(),
	}

	combined := make(map[string]iamActionFn)
	for _, t := range subtables {
		maps.Copy(combined, t)
	}

	return combined
}

func (h *Handler) iamUserDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateUser": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.CreateUser(vals.Get("UserName"), vals.Get("Path"), vals.Get("PermissionsBoundary"))
			if err != nil {
				return nil, err
			}

			return &CreateUserResponse{
				Xmlns:            iamXMLNS,
				CreateUserResult: CreateUserResult{User: toUserXML(u)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetUser": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.GetUser(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &GetUserResponse{
				Xmlns:            iamXMLNS,
				GetUserResult:    GetUserResult{User: toUserXML(u)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUser(vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &DeleteUserResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListUsers": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListUsers(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlUsers := make([]UserXML, 0, len(p.Data))
			for i := range p.Data {
				xmlUsers = append(xmlUsers, toUserXML(&p.Data[i]))
			}

			return &ListUsersResponse{
				Xmlns: iamXMLNS,
				ListUsersResult: ListUsersResult{
					Users:       xmlUsers,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamRoleDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateRole": func(vals url.Values, reqID string) (any, error) {
			r, err := h.Backend.CreateRole(
				vals.Get(
					"RoleName",
				),
				vals.Get("Path"),
				vals.Get("AssumeRolePolicyDocument"),
				vals.Get("PermissionsBoundary"),
			)
			if err != nil {
				return nil, err
			}

			return &CreateRoleResponse{
				Xmlns:            iamXMLNS,
				CreateRoleResult: CreateRoleResult{Role: toRoleXML(r)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetRole": func(vals url.Values, reqID string) (any, error) {
			r, err := h.Backend.GetRole(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			return &GetRoleResponse{
				Xmlns:            iamXMLNS,
				GetRoleResult:    GetRoleResult{Role: toRoleXML(r)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRole": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRole(vals.Get("RoleName")); err != nil {
				return nil, err
			}

			return &DeleteRoleResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListRoles": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListRoles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlRoles := make([]RoleXML, 0, len(p.Data))
			for i := range p.Data {
				xmlRoles = append(xmlRoles, toRoleXML(&p.Data[i]))
			}

			return &ListRolesResponse{
				Xmlns: iamXMLNS,
				ListRolesResult: ListRolesResult{
					Roles:       xmlRoles,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamPolicyBasicDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreatePolicy": func(vals url.Values, reqID string) (any, error) {
			pol, err := h.Backend.CreatePolicy(
				vals.Get("PolicyName"), vals.Get("Path"), vals.Get("PolicyDocument"),
			)
			if err != nil {
				return nil, err
			}

			return &CreatePolicyResponse{
				Xmlns:              iamXMLNS,
				CreatePolicyResult: CreatePolicyResult{Policy: toPolicyXML(pol)},
				ResponseMetadata:   ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeletePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeletePolicy(vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DeletePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListPolicies": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListPolicies(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]PolicyXML, 0, len(p.Data))
			for i := range p.Data {
				xmlPolicies = append(xmlPolicies, toPolicyXML(&p.Data[i]))
			}

			return &ListPoliciesResponse{
				Xmlns: iamXMLNS,
				ListPoliciesResult: ListPoliciesResult{
					Policies:    xmlPolicies,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetPolicy": func(vals url.Values, reqID string) (any, error) {
			pol, err := h.Backend.GetPolicy(vals.Get("PolicyArn"))
			if err != nil {
				return nil, err
			}

			return &GetPolicyResponse{
				Xmlns:            iamXMLNS,
				GetPolicyResult:  GetPolicyResult{Policy: toPolicyXML(pol)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetPolicyVersion": func(vals url.Values, reqID string) (any, error) {
			pol, err := h.Backend.GetPolicyVersion(vals.Get("PolicyArn"), vals.Get("VersionId"))
			if err != nil {
				return nil, err
			}

			return &GetPolicyVersionResponse{
				Xmlns: iamXMLNS,
				GetPolicyVersionResult: GetPolicyVersionResult{PolicyVersion: PolicyVersionXML{
					Document:         pol.PolicyDocument,
					VersionID:        "v1",
					IsDefaultVersion: true,
					CreateDate:       isoTime(pol.CreateDate),
				}},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPolicyVersions": func(_ url.Values, reqID string) (any, error) {
			type policyVersionXML struct {
				VersionID        string `xml:"VersionId"`
				IsDefaultVersion bool   `xml:"IsDefaultVersion"`
			}
			type listPolicyVersionsResult struct {
				XMLName  xml.Name           `xml:"ListPolicyVersionsResult"`
				Versions []policyVersionXML `xml:"Versions>member"`
			}
			type listPolicyVersionsResponse struct {
				XMLName                  xml.Name                 `xml:"ListPolicyVersionsResponse"`
				Xmlns                    string                   `xml:"xmlns,attr"`
				ResponseMetadata         ResponseMetadata         `xml:"ResponseMetadata"`
				ListPolicyVersionsResult listPolicyVersionsResult `xml:"ListPolicyVersionsResult"`
			}

			return &listPolicyVersionsResponse{
				Xmlns: iamXMLNS,
				ListPolicyVersionsResult: listPolicyVersionsResult{
					Versions: []policyVersionXML{{VersionID: "v1", IsDefaultVersion: true}},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamPolicyAttachDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"AttachUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachUserPolicy(vals.Get("UserName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"DetachUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachUserPolicy(vals.Get("UserName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"AttachRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachRolePolicy(vals.Get("RoleName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"DetachRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachRolePolicy(vals.Get("RoleName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListAttachedUserPolicies": func(vals url.Values, reqID string) (any, error) {
			policies, err := h.Backend.ListAttachedUserPolicies(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]AttachedPolicyXML, 0, len(policies))
			for _, p := range policies {
				xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
			}

			return &ListAttachedUserPoliciesResponse{
				Xmlns:                          iamXMLNS,
				ListAttachedUserPoliciesResult: ListAttachedUserPoliciesResult{AttachedPolicies: xmlPolicies},
				ResponseMetadata:               ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListAttachedRolePolicies": func(vals url.Values, reqID string) (any, error) {
			policies, err := h.Backend.ListAttachedRolePolicies(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]AttachedPolicyXML, 0, len(policies))
			for _, p := range policies {
				xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
			}

			return &ListAttachedRolePoliciesResponse{
				Xmlns:                          iamXMLNS,
				ListAttachedRolePoliciesResult: ListAttachedRolePoliciesResult{AttachedPolicies: xmlPolicies},
				ResponseMetadata:               ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListRolePolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListRolePolicies(vals.Get("RoleName"))
			if err != nil {
				return nil, err
			}

			return &ListRolePoliciesResponse{
				Xmlns:                  iamXMLNS,
				ListRolePoliciesResult: ListRolePoliciesResult{PolicyNames: names},
				ResponseMetadata:       ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListInstanceProfilesForRole": func(_ url.Values, reqID string) (any, error) {
			type listInstanceProfilesResult struct {
				XMLName          xml.Name `xml:"ListInstanceProfilesForRoleResult"`
				InstanceProfiles []any    `xml:"InstanceProfiles>member"`
				IsTruncated      bool     `xml:"IsTruncated"`
			}
			type listInstanceProfilesResponse struct {
				XMLName                           xml.Name                   `xml:"ListInstanceProfilesForRoleResponse"`
				Xmlns                             string                     `xml:"xmlns,attr"`
				ResponseMetadata                  ResponseMetadata           `xml:"ResponseMetadata"`
				ListInstanceProfilesForRoleResult listInstanceProfilesResult `xml:"ListInstanceProfilesForRoleResult"`
			}

			return &listInstanceProfilesResponse{
				Xmlns:                             iamXMLNS,
				ListInstanceProfilesForRoleResult: listInstanceProfilesResult{InstanceProfiles: []any{}},
				ResponseMetadata:                  ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamGroupAttachedPolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"AttachGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AttachGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &AttachGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DetachGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DetachGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyArn")); err != nil {
				return nil, err
			}

			return &DetachGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListAttachedGroupPolicies": func(vals url.Values, reqID string) (any, error) {
			policies, err := h.Backend.ListAttachedGroupPolicies(vals.Get("GroupName"))
			if err != nil {
				return nil, err
			}

			xmlPolicies := make([]AttachedPolicyXML, 0, len(policies))
			for _, p := range policies {
				xmlPolicies = append(xmlPolicies, AttachedPolicyXML(p))
			}

			return &ListAttachedGroupPoliciesResponse{
				Xmlns:                           iamXMLNS,
				ListAttachedGroupPoliciesResult: ListAttachedGroupPoliciesResult{AttachedPolicies: xmlPolicies},
				ResponseMetadata:                ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamInlinePolicyDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamUserRoleInlinePolicyDispatchTable())
	maps.Copy(combined, h.iamGroupInlinePolicyDispatchTable())

	return combined
}

// iamUserRoleInlinePolicyDispatchTable returns dispatch entries for user and role inline policies.
func (h *Handler) iamUserRoleInlinePolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutUserPolicy(
				vals.Get("UserName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetUserPolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetUserPolicy(vals.Get("UserName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetUserPolicyResponse{
				Xmlns: iamXMLNS,
				GetUserPolicyResult: GetUserPolicyResult{
					UserName:       vals.Get("UserName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: doc,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUserPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUserPolicy(vals.Get("UserName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListUserPolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListUserPolicies(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &ListUserPoliciesResponse{
				Xmlns:                  iamXMLNS,
				ListUserPoliciesResult: ListUserPoliciesResult{PolicyNames: names},
				ResponseMetadata:       ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"PutRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutRolePolicy(
				vals.Get("RoleName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetRolePolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetRolePolicy(vals.Get("RoleName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetRolePolicyResponse{
				Xmlns: iamXMLNS,
				GetRolePolicyResult: GetRolePolicyResult{
					RoleName:       vals.Get("RoleName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: doc,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRolePolicy(vals.Get("RoleName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
	}
}

// iamGroupInlinePolicyDispatchTable returns dispatch entries for group inline policies.
func (h *Handler) iamGroupInlinePolicyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutGroupPolicy(
				vals.Get("GroupName"), vals.Get("PolicyName"), vals.Get("PolicyDocument"),
			); err != nil {
				return nil, err
			}

			return &PutGroupPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"GetGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			doc, err := h.Backend.GetGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyName"))
			if err != nil {
				return nil, err
			}

			return &GetGroupPolicyResponse{
				Xmlns: iamXMLNS,
				GetGroupPolicyResult: GetGroupPolicyResult{
					GroupName:      vals.Get("GroupName"),
					PolicyName:     vals.Get("PolicyName"),
					PolicyDocument: doc,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteGroupPolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteGroupPolicy(vals.Get("GroupName"), vals.Get("PolicyName")); err != nil {
				return nil, err
			}

			return &DeleteGroupPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListGroupPolicies": func(vals url.Values, reqID string) (any, error) {
			names, err := h.Backend.ListGroupPolicies(vals.Get("GroupName"))
			if err != nil {
				return nil, err
			}

			return &ListGroupPoliciesResponse{
				Xmlns:                   iamXMLNS,
				ListGroupPoliciesResult: ListGroupPoliciesResult{PolicyNames: names},
				ResponseMetadata:        ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
func (h *Handler) iamPermissionBoundaryDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"PutUserPermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutUserPermissionsBoundary(
				vals.Get("UserName"),
				vals.Get("PermissionsBoundary"),
			); err != nil {
				return nil, err
			}

			return &PutUserPermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUserPermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUserPermissionsBoundary(vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &DeleteUserPermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"PutRolePermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.PutRolePermissionsBoundary(
				vals.Get("RoleName"),
				vals.Get("PermissionsBoundary"),
			); err != nil {
				return nil, err
			}

			return &PutRolePermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteRolePermissionsBoundary": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteRolePermissionsBoundary(vals.Get("RoleName")); err != nil {
				return nil, err
			}

			return &DeleteRolePermissionsBoundaryResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamOtherOperationsDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateAssumeRolePolicy": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateAssumeRolePolicy(vals.Get("RoleName"), vals.Get("PolicyDocument")); err != nil {
				return nil, err
			}

			return &UpdateAssumeRolePolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamGroupDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateGroup": func(vals url.Values, reqID string) (any, error) {
			g, err := h.Backend.CreateGroup(vals.Get("GroupName"), vals.Get("Path"))
			if err != nil {
				return nil, err
			}

			return &CreateGroupResponse{
				Xmlns:             iamXMLNS,
				CreateGroupResult: CreateGroupResult{Group: toGroupXML(g)},
				ResponseMetadata:  ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteGroup(vals.Get("GroupName")); err != nil {
				return nil, err
			}

			return &DeleteGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"AddUserToGroup": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AddUserToGroup(vals.Get("GroupName"), vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &AddUserToGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListGroups": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListGroups(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlGroups := make([]GroupXML, 0, len(p.Data))
			for i := range p.Data {
				xmlGroups = append(xmlGroups, toGroupXML(&p.Data[i]))
			}

			return &ListGroupsResponse{
				Xmlns: iamXMLNS,
				ListGroupsResult: ListGroupsResult{
					Groups:      xmlGroups,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamAccessKeyDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateAccessKey": func(vals url.Values, reqID string) (any, error) {
			ak, err := h.Backend.CreateAccessKey(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &CreateAccessKeyResponse{
				Xmlns:                 iamXMLNS,
				CreateAccessKeyResult: CreateAccessKeyResult{AccessKey: toAccessKeyXML(ak)},
				ResponseMetadata:      ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccessKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccessKey(vals.Get("UserName"), vals.Get("AccessKeyId")); err != nil {
				return nil, err
			}

			return &DeleteAccessKeyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		"ListAccessKeys": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListAccessKeys(
				vals.Get("UserName"),
				vals.Get("Marker"),
				parseMaxItems(vals.Get("MaxItems")),
			)
			if err != nil {
				return nil, err
			}

			xmlKeys := make([]AccessKeyMetadataXML, 0, len(p.Data))
			for i := range p.Data {
				xmlKeys = append(xmlKeys, toAccessKeyMetadataXML(&p.Data[i]))
			}

			return &ListAccessKeysResponse{
				Xmlns: iamXMLNS,
				ListAccessKeysResult: ListAccessKeysResult{
					AccessKeyMetadata: xmlKeys,
					IsTruncated:       p.Next != "",
					Marker:            p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamInstanceProfileDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			ip, err := h.Backend.CreateInstanceProfile(vals.Get("InstanceProfileName"), vals.Get("Path"))
			if err != nil {
				return nil, err
			}

			return &CreateInstanceProfileResponse{
				Xmlns:                       iamXMLNS,
				CreateInstanceProfileResult: CreateInstanceProfileResult{InstanceProfile: toInstanceProfileXML(ip)},
				ResponseMetadata:            ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteInstanceProfile(vals.Get("InstanceProfileName")); err != nil {
				return nil, err
			}

			return &DeleteInstanceProfileResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListInstanceProfiles": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListInstanceProfiles(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlProfiles := make([]InstanceProfileXML, 0, len(p.Data))
			for i := range p.Data {
				xmlProfiles = append(xmlProfiles, toInstanceProfileXML(&p.Data[i]))
			}

			return &ListInstanceProfilesResponse{
				Xmlns: iamXMLNS,
				ListInstanceProfilesResult: ListInstanceProfilesResult{
					InstanceProfiles: xmlProfiles,
					IsTruncated:      p.Next != "",
					Marker:           p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamTagDispatchTable() map[string]iamActionFn {
	table := make(map[string]iamActionFn)
	maps.Copy(table, h.iamListTagActions())
	maps.Copy(table, h.iamMutateTagActions())

	return table
}

// iamListTagActions returns the List*Tags dispatch entries.
func (h *Handler) iamListTagActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListRoleTags": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			tags := h.getTags(roleName)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListRoleTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListRoleTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPolicyTags": func(vals url.Values, reqID string) (any, error) {
			policyArn := vals.Get("PolicyArn")
			tags := h.getTags(policyArn)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListPolicyTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListPolicyTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListUserTags": func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")
			tags := h.getTags(userName)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListUserTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListUserTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamMutateTagActions returns the Tag*/Untag* dispatch entries.
func (h *Handler) iamMutateTagActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"TagRole": func(vals url.Values, reqID string) (any, error) {
			h.setTags(vals.Get("RoleName"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagRoleResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagRole": func(vals url.Values, reqID string) (any, error) {
			h.removeTags(vals.Get("RoleName"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagRoleResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagPolicy": func(vals url.Values, reqID string) (any, error) {
			h.setTags(vals.Get("PolicyArn"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagPolicyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagPolicy": func(vals url.Values, reqID string) (any, error) {
			h.removeTags(vals.Get("PolicyArn"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagPolicyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagUser": func(vals url.Values, reqID string) (any, error) {
			h.setTags(vals.Get("UserName"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagUserResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagUser": func(vals url.Values, reqID string) (any, error) {
			h.removeTags(vals.Get("UserName"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagUserResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// parseIAMTags parses Tags.member.N.Key / Tags.member.N.Value form values.
func parseIAMTags(vals url.Values) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return tags
		}
		tags[k] = vals.Get(fmt.Sprintf("Tags.member.%d.Value", i))
	}
}

// parseIAMTagKeys parses TagKeys.member.N form values.
func parseIAMTagKeys(vals url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

// dispatch routes the IAM action to the appropriate handler.
func (h *Handler) dispatch(
	_ context.Context,
	action string,
	vals url.Values,
) (any, error) {
	reqID := newRequestID()

	fn, ok := h.actions[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s is not a valid IAM action", ErrInvalidAction, action)
	}

	return fn(vals, reqID)
}

// handleError writes a standardized IAM XML error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)

	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(reqErr, ErrUserNotFound),
		errors.Is(reqErr, ErrRoleNotFound),
		errors.Is(reqErr, ErrPolicyNotFound),
		errors.Is(reqErr, ErrGroupNotFound),
		errors.Is(reqErr, ErrAccessKeyNotFound),
		errors.Is(reqErr, ErrInstanceProfileNotFound),
		errors.Is(reqErr, ErrInlinePolicyNotFound):
		code = "NoSuchEntity"
	case errors.Is(reqErr, ErrUserAlreadyExists),
		errors.Is(reqErr, ErrRoleAlreadyExists),
		errors.Is(reqErr, ErrPolicyAlreadyExists),
		errors.Is(reqErr, ErrGroupAlreadyExists),
		errors.Is(reqErr, ErrInstanceProfileAlreadyExists):
		code = "EntityAlreadyExists"
	case errors.Is(reqErr, ErrDeleteConflict):
		code = "DeleteConflict"
	case errors.Is(reqErr, ErrMalformedPolicyDocument):
		code = "MalformedPolicyDocument"
	case errors.Is(reqErr, ErrInvalidAction):
		code = "InvalidAction"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "IAM internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "IAM request error", "error", reqErr, "action", action)
	}

	return h.writeError(c, statusCode, code, reqErr.Error())
}

// writeError writes an IAM XML error response.
func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &ErrorResponse{
		Xmlns:     iamXMLNS,
		Error:     IAMError{Code: code, Message: message, Type: "Sender"},
		RequestID: newRequestID(),
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

// marshalXML encodes the payload with the XML declaration header.
func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// newRequestID generates a simple unique request ID.
func newRequestID() string {
	return fmt.Sprintf("gopherstack-%s", newID("req"))
}

// ---- XML conversion helpers ----

func toUserXML(u *User) UserXML {
	x := UserXML{
		Path:       u.Path,
		UserName:   u.UserName,
		UserID:     u.UserID,
		Arn:        u.Arn,
		CreateDate: isoTime(u.CreateDate),
	}

	if u.PermissionsBoundary != "" {
		x.PermissionsBoundary = &PermissionsBoundaryXML{
			PermissionsBoundaryArn:  u.PermissionsBoundary,
			PermissionsBoundaryType: "Policy",
		}
	}

	return x
}

func toRoleXML(r *Role) RoleXML {
	x := RoleXML{
		Path:                     r.Path,
		RoleName:                 r.RoleName,
		RoleID:                   r.RoleID,
		Arn:                      r.Arn,
		CreateDate:               isoTime(r.CreateDate),
		AssumeRolePolicyDocument: r.AssumeRolePolicyDocument,
	}

	if r.PermissionsBoundary != "" {
		x.PermissionsBoundary = &PermissionsBoundaryXML{
			PermissionsBoundaryArn:  r.PermissionsBoundary,
			PermissionsBoundaryType: "Policy",
		}
	}

	return x
}

func toPolicyXML(p *Policy) PolicyXML {
	return PolicyXML{
		PolicyName: p.PolicyName,
		PolicyID:   p.PolicyID,
		Arn:        p.Arn,
		Path:       p.Path,
		CreateDate: isoTime(p.CreateDate),
	}
}

func toGroupXML(g *Group) GroupXML {
	return GroupXML{
		Path:       g.Path,
		GroupName:  g.GroupName,
		GroupID:    g.GroupID,
		Arn:        g.Arn,
		CreateDate: isoTime(g.CreateDate),
	}
}

func toAccessKeyXML(ak *AccessKey) AccessKeyXML {
	return AccessKeyXML{
		AccessKeyID:     ak.AccessKeyID,
		SecretAccessKey: ak.SecretAccessKey,
		UserName:        ak.UserName,
		Status:          ak.Status,
		CreateDate:      isoTime(ak.CreateDate),
	}
}

func toAccessKeyMetadataXML(ak *AccessKey) AccessKeyMetadataXML {
	return AccessKeyMetadataXML{
		AccessKeyID: ak.AccessKeyID,
		UserName:    ak.UserName,
		Status:      ak.Status,
		CreateDate:  isoTime(ak.CreateDate),
	}
}

func toInstanceProfileXML(ip *InstanceProfile) InstanceProfileXML {
	return InstanceProfileXML{
		Path:                ip.Path,
		InstanceProfileName: ip.InstanceProfileName,
		InstanceProfileID:   ip.InstanceProfileID,
		Arn:                 ip.Arn,
		CreateDate:          isoTime(ip.CreateDate),
	}
}

// parseMaxItems converts a query-string MaxItems value to an int.
// Returns 0 for empty, non-numeric, or non-positive values; returning 0 signals
// the backend to apply its own default page size.
func parseMaxItems(s string) int {
	if s == "" {
		return 0
	}

	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}

	return n
}
