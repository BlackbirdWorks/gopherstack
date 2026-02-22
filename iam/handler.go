package iam

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// iamAPIVersion is the IAM query protocol version used to identify IAM requests.
const (
	iamAPIVersion = "Version=2010-05-08"
	unknownOp     = "Unknown"
)

// Handler is the Echo HTTP handler for IAM operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new IAM handler with the given storage backend.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
	}
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
		"AttachUserPolicy", "AttachRolePolicy",
		"CreateGroup", "DeleteGroup", "AddUserToGroup",
		"CreateAccessKey", "DeleteAccessKey", "ListAccessKeys",
		"CreateInstanceProfile", "DeleteInstanceProfile", "ListInstanceProfiles",
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

		body, err := httputil.ReadBody(r)
		if err != nil {
			return false
		}

		return strings.Contains(string(body), iamAPIVersion)
	}
}

// MatchPriority returns the routing priority for the IAM handler.
// Higher than Dashboard (50) but lower than DynamoDB/SSM (100).
const iamMatchPriority = 80

func (h *Handler) MatchPriority() int {
	return iamMatchPriority
}

// ExtractOperation extracts the IAM action from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
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
	body, err := httputil.ReadBody(c.Request())
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

		body, err := httputil.ReadBody(c.Request())
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

// dispatch routes the IAM action to the appropriate handler.
func (h *Handler) dispatch( //nolint:funlen,gocognit,gocyclo,cyclop // one switch handles all 22 IAM operations
	_ context.Context,
	action string,
	vals url.Values,
) (any, error) {
	reqID := newRequestID()

	switch action {
	case "CreateUser":
		u, err := h.Backend.CreateUser(vals.Get("UserName"), vals.Get("Path"))
		if err != nil {
			return nil, err
		}

		return &CreateUserResponse{Xmlns: iamXMLNS, CreateUserResult: CreateUserResult{User: toUserXML(u)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "GetUser":
		u, err := h.Backend.GetUser(vals.Get("UserName"))
		if err != nil {
			return nil, err
		}

		return &GetUserResponse{Xmlns: iamXMLNS, GetUserResult: GetUserResult{User: toUserXML(u)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "DeleteUser":
		if err := h.Backend.DeleteUser(vals.Get("UserName")); err != nil {
			return nil, err
		}

		return &DeleteUserResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "ListUsers":
		users, err := h.Backend.ListUsers()
		if err != nil {
			return nil, err
		}

		xmlUsers := make([]UserXML, 0, len(users))
		for i := range users {
			xmlUsers = append(xmlUsers, toUserXML(&users[i]))
		}

		return &ListUsersResponse{Xmlns: iamXMLNS,
			ListUsersResult:  ListUsersResult{Users: xmlUsers},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "CreateRole":
		r, err := h.Backend.CreateRole(vals.Get("RoleName"), vals.Get("Path"), vals.Get("AssumeRolePolicyDocument"))
		if err != nil {
			return nil, err
		}

		return &CreateRoleResponse{Xmlns: iamXMLNS, CreateRoleResult: CreateRoleResult{Role: toRoleXML(r)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "GetRole":
		r, err := h.Backend.GetRole(vals.Get("RoleName"))
		if err != nil {
			return nil, err
		}

		return &GetRoleResponse{Xmlns: iamXMLNS, GetRoleResult: GetRoleResult{Role: toRoleXML(r)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "DeleteRole":
		if err := h.Backend.DeleteRole(vals.Get("RoleName")); err != nil {
			return nil, err
		}

		return &DeleteRoleResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "ListRoles":
		roles, err := h.Backend.ListRoles()
		if err != nil {
			return nil, err
		}

		xmlRoles := make([]RoleXML, 0, len(roles))
		for i := range roles {
			xmlRoles = append(xmlRoles, toRoleXML(&roles[i]))
		}

		return &ListRolesResponse{Xmlns: iamXMLNS,
			ListRolesResult:  ListRolesResult{Roles: xmlRoles},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "CreatePolicy":
		pol, err := h.Backend.CreatePolicy(vals.Get("PolicyName"), vals.Get("Path"), vals.Get("PolicyDocument"))
		if err != nil {
			return nil, err
		}

		return &CreatePolicyResponse{Xmlns: iamXMLNS, CreatePolicyResult: CreatePolicyResult{Policy: toPolicyXML(pol)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "DeletePolicy":
		if err := h.Backend.DeletePolicy(vals.Get("PolicyArn")); err != nil {
			return nil, err
		}

		return &DeletePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "ListPolicies":
		policies, err := h.Backend.ListPolicies()
		if err != nil {
			return nil, err
		}

		xmlPolicies := make([]PolicyXML, 0, len(policies))
		for i := range policies {
			xmlPolicies = append(xmlPolicies, toPolicyXML(&policies[i]))
		}

		return &ListPoliciesResponse{Xmlns: iamXMLNS,
			ListPoliciesResult: ListPoliciesResult{Policies: xmlPolicies},
			ResponseMetadata:   ResponseMetadata{RequestID: reqID}}, nil

	case "AttachUserPolicy":
		if err := h.Backend.AttachUserPolicy(vals.Get("UserName"), vals.Get("PolicyArn")); err != nil {
			return nil, err
		}

		return &AttachUserPolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "AttachRolePolicy":
		if err := h.Backend.AttachRolePolicy(vals.Get("RoleName"), vals.Get("PolicyArn")); err != nil {
			return nil, err
		}

		return &AttachRolePolicyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "CreateGroup":
		g, err := h.Backend.CreateGroup(vals.Get("GroupName"), vals.Get("Path"))
		if err != nil {
			return nil, err
		}

		return &CreateGroupResponse{Xmlns: iamXMLNS, CreateGroupResult: CreateGroupResult{Group: toGroupXML(g)},
			ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "DeleteGroup":
		if err := h.Backend.DeleteGroup(vals.Get("GroupName")); err != nil {
			return nil, err
		}

		return &DeleteGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "AddUserToGroup":
		if err := h.Backend.AddUserToGroup(vals.Get("GroupName"), vals.Get("UserName")); err != nil {
			return nil, err
		}

		return &AddUserToGroupResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "CreateAccessKey":
		ak, err := h.Backend.CreateAccessKey(vals.Get("UserName"))
		if err != nil {
			return nil, err
		}

		return &CreateAccessKeyResponse{Xmlns: iamXMLNS,
			CreateAccessKeyResult: CreateAccessKeyResult{AccessKey: toAccessKeyXML(ak)},
			ResponseMetadata:      ResponseMetadata{RequestID: reqID}}, nil

	case "DeleteAccessKey":
		if err := h.Backend.DeleteAccessKey(vals.Get("UserName"), vals.Get("AccessKeyId")); err != nil {
			return nil, err
		}

		return &DeleteAccessKeyResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil

	case "ListAccessKeys":
		keys, err := h.Backend.ListAccessKeys(vals.Get("UserName"))
		if err != nil {
			return nil, err
		}

		xmlKeys := make([]AccessKeyMetadataXML, 0, len(keys))
		for i := range keys {
			xmlKeys = append(xmlKeys, toAccessKeyMetadataXML(&keys[i]))
		}

		return &ListAccessKeysResponse{Xmlns: iamXMLNS,
			ListAccessKeysResult: ListAccessKeysResult{AccessKeyMetadata: xmlKeys},
			ResponseMetadata:     ResponseMetadata{RequestID: reqID}}, nil

	case "CreateInstanceProfile":
		ip, err := h.Backend.CreateInstanceProfile(vals.Get("InstanceProfileName"), vals.Get("Path"))
		if err != nil {
			return nil, err
		}

		return &CreateInstanceProfileResponse{Xmlns: iamXMLNS,
			CreateInstanceProfileResult: CreateInstanceProfileResult{InstanceProfile: toInstanceProfileXML(ip)},
			ResponseMetadata:            ResponseMetadata{RequestID: reqID}}, nil

	case "DeleteInstanceProfile":
		if err := h.Backend.DeleteInstanceProfile(vals.Get("InstanceProfileName")); err != nil {
			return nil, err
		}

		return &DeleteInstanceProfileResponse{
			Xmlns:            iamXMLNS,
			ResponseMetadata: ResponseMetadata{RequestID: reqID},
		}, nil

	case "ListInstanceProfiles":
		profiles, err := h.Backend.ListInstanceProfiles()
		if err != nil {
			return nil, err
		}

		xmlProfiles := make([]InstanceProfileXML, 0, len(profiles))
		for i := range profiles {
			xmlProfiles = append(xmlProfiles, toInstanceProfileXML(&profiles[i]))
		}

		return &ListInstanceProfilesResponse{Xmlns: iamXMLNS,
			ListInstanceProfilesResult: ListInstanceProfilesResult{InstanceProfiles: xmlProfiles},
			ResponseMetadata:           ResponseMetadata{RequestID: reqID}}, nil

	default:
		return nil, fmt.Errorf("%w: %s is not a valid IAM action", ErrInvalidAction, action)
	}
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
		errors.Is(reqErr, ErrInstanceProfileNotFound):
		code = "NoSuchEntity"
	case errors.Is(reqErr, ErrUserAlreadyExists),
		errors.Is(reqErr, ErrRoleAlreadyExists),
		errors.Is(reqErr, ErrPolicyAlreadyExists),
		errors.Is(reqErr, ErrGroupAlreadyExists),
		errors.Is(reqErr, ErrInstanceProfileAlreadyExists):
		code = "EntityAlreadyExists"
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
	return UserXML{
		Path:       u.Path,
		UserName:   u.UserName,
		UserID:     u.UserID,
		Arn:        u.Arn,
		CreateDate: isoTime(u.CreateDate),
	}
}

func toRoleXML(r *Role) RoleXML {
	return RoleXML{
		Path:                     r.Path,
		RoleName:                 r.RoleName,
		RoleID:                   r.RoleID,
		Arn:                      r.Arn,
		CreateDate:               isoTime(r.CreateDate),
		AssumeRolePolicyDocument: r.AssumeRolePolicyDocument,
	}
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
