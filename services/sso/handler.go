package sso

import (
	"errors"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ssooidc"
)

const (
	signingName      = "awsssoportal"
	bearerHeader     = "X-Amz-Sso_bearer_token" //nolint:gosec // G101: header name, not a credential
	errorTypeHeader  = "X-Amzn-Errortype"
	pathCredentials  = "/federation/credentials"
	pathAccounts     = "/assignment/accounts"
	pathRoles        = "/assignment/roles"
	pathLogout       = "/logout"
	defaultPageSize  = 10
	maxPageSize      = 1000
	opGetRoleCreds   = "GetRoleCredentials" //nolint:gosec // G101: operation name, not a credential
	opListAccounts   = "ListAccounts"
	opListRoles      = "ListAccountRoles"
	opLogout         = "Logout"
	codeInvalid      = "InvalidRequestException"
	codeNotFound     = "ResourceNotFoundException"
	codeUnauthorized = "UnauthorizedException"
	codeInternal     = "InternalFailure"
)

var errUnwired = errors.New("the SSO portal is not wired to a token source and directory")

// Handler is the Echo handler for the IAM Identity Center portal API.
type Handler struct {
	source atomic.Pointer[TokenSource]
	dir    atomic.Pointer[Directory]
}

// NewHandler creates an unwired Handler.
func NewHandler() *Handler { return &Handler{} }

// SetTokenSource wires the OIDC token validator.
func (h *Handler) SetTokenSource(s TokenSource) { h.source.Store(&s) }

// SetDirectory wires the entitlement and credential directory.
func (h *Handler) SetDirectory(d Directory) { h.dir.Store(&d) }

// Name returns the service name.
func (h *Handler) Name() string { return "SSOPortal" }

// GetSupportedOperations returns the supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{opGetRoleCreds, opListAccounts, opListRoles, opLogout}
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return "sso" }

// ChaosOperations returns the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions served.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

func operationFor(r *http.Request) string {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == pathCredentials:
		return opGetRoleCreds
	case r.Method == http.MethodGet && r.URL.Path == pathAccounts:
		return opListAccounts
	case r.Method == http.MethodGet && r.URL.Path == pathRoles:
		return opListRoles
	case r.Method == http.MethodPost && r.URL.Path == pathLogout:
		return opLogout
	}

	return ""
}

// RouteMatcher matches portal calls. The SDK sends them unsigned with the bearer token header,
// which is what separates them from S3 path-style requests on the same paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if operationFor(r) == "" || r.Header.Get(bearerHeader) == "" {
			return false
		}

		svc := httputils.ExtractServiceFromRequest(r)

		return svc == "" || svc == signingName
	}
}

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op := operationFor(c.Request()); op != "" {
		return op
	}

	return "Unknown"
}

// ExtractResource returns a fixed label to avoid high-cardinality metrics.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "portal" }

func writeError(c *echo.Context, status int, code, msg string) error {
	c.Response().Header().Set(errorTypeHeader, code)

	return c.JSON(status, map[string]string{"message": msg})
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		op := operationFor(c.Request())
		if op == "" {
			return writeError(c, http.StatusNotFound, codeNotFound, "The requested operation is not supported")
		}

		src, dir := h.source.Load(), h.dir.Load()
		if src == nil || dir == nil {
			return writeError(c, http.StatusInternalServerError, codeInternal, errUnwired.Error())
		}

		token := c.Request().Header.Get(bearerHeader)
		if token == "" {
			return writeError(c, http.StatusBadRequest, codeInvalid, "The access token is required")
		}

		sess, err := (*src).SessionForAccessToken(token)
		if err != nil {
			return writeError(c, http.StatusUnauthorized, codeUnauthorized, "Session token not found or invalid")
		}

		switch op {
		case opLogout:
			return h.logout(c, *src, token)
		case opListAccounts:
			return h.listAccounts(c, *dir, sess)
		case opListRoles:
			return h.listAccountRoles(c, *dir, sess)
		}

		return h.getRoleCredentials(c, *dir, sess)
	}
}

func (h *Handler) logout(c *echo.Context, src TokenSource, token string) error {
	if err := src.RevokeSession(token); err != nil {
		return writeError(c, http.StatusUnauthorized, codeUnauthorized, "Session token not found or invalid")
	}

	return c.NoContent(http.StatusOK)
}

type pageRequest struct {
	token string
	size  int
}

func parsePage(c *echo.Context) (pageRequest, bool) {
	size := defaultPageSize

	if raw := c.QueryParam("max_result"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			_ = writeError(c, http.StatusBadRequest, codeInvalid, "max_result must be a positive integer")

			return pageRequest{}, false
		}

		size = min(n, maxPageSize)
	}

	return pageRequest{token: c.QueryParam("next_token"), size: size}, true
}

type accountInfoWire struct {
	AccountID    string `json:"accountId"`
	AccountName  string `json:"accountName"`
	EmailAddress string `json:"emailAddress"`
}

type listAccountsResponse struct {
	NextToken   string            `json:"nextToken,omitempty"`
	AccountList []accountInfoWire `json:"accountList"`
}

func (h *Handler) listAccounts(c *echo.Context, dir Directory, sess ssooidc.Session) error {
	pr, ok := parsePage(c)
	if !ok {
		return nil
	}

	ents, err := dir.Entitlements(sess.Subject)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, codeInternal, err.Error())
	}

	ids := make([]string, 0, len(ents))
	for _, e := range ents {
		ids = append(ids, e.AccountID)
	}

	sort.Strings(ids)
	ids = slices.Compact(ids)

	pg := page.New(ids, pr.token, pr.size, defaultPageSize)
	out := listAccountsResponse{NextToken: pg.Next, AccountList: make([]accountInfoWire, 0, len(pg.Data))}

	for _, id := range pg.Data {
		info := dir.AccountInfo(id)
		out.AccountList = append(out.AccountList, accountInfoWire{
			AccountID: id, AccountName: info.Name, EmailAddress: info.Email,
		})
	}

	return c.JSON(http.StatusOK, out)
}

type roleInfoWire struct {
	RoleName  string `json:"roleName"`
	AccountID string `json:"accountId"`
}

type listRolesResponse struct {
	NextToken string         `json:"nextToken,omitempty"`
	RoleList  []roleInfoWire `json:"roleList"`
}

func (h *Handler) listAccountRoles(c *echo.Context, dir Directory, sess ssooidc.Session) error {
	accountID := c.QueryParam("account_id")
	if accountID == "" {
		return writeError(c, http.StatusBadRequest, codeInvalid, "account_id is required")
	}

	pr, ok := parsePage(c)
	if !ok {
		return nil
	}

	ents, err := dir.Entitlements(sess.Subject)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, codeInternal, err.Error())
	}

	var names []string

	for _, e := range ents {
		if e.AccountID == accountID {
			names = append(names, e.PermissionSetName)
		}
	}

	if len(names) == 0 {
		return writeError(c, http.StatusNotFound, codeNotFound, "No access")
	}

	sort.Strings(names)
	names = slices.Compact(names)

	pg := page.New(names, pr.token, pr.size, defaultPageSize)
	out := listRolesResponse{NextToken: pg.Next, RoleList: make([]roleInfoWire, 0, len(pg.Data))}

	for _, n := range pg.Data {
		out.RoleList = append(out.RoleList, roleInfoWire{RoleName: n, AccountID: accountID})
	}

	return c.JSON(http.StatusOK, out)
}

type roleCredentialsWire struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Expiration      int64  `json:"expiration"`
}

type getRoleCredentialsResponse struct {
	RoleCredentials roleCredentialsWire `json:"roleCredentials"`
}

func (h *Handler) getRoleCredentials(c *echo.Context, dir Directory, sess ssooidc.Session) error {
	accountID, roleName := c.QueryParam("account_id"), c.QueryParam("role_name")
	if accountID == "" || roleName == "" {
		return writeError(c, http.StatusBadRequest, codeInvalid, "account_id and role_name are required")
	}

	ents, err := dir.Entitlements(sess.Subject)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, codeInternal, err.Error())
	}

	idx := slices.IndexFunc(ents, func(e Entitlement) bool {
		return e.AccountID == accountID && strings.EqualFold(e.PermissionSetName, roleName)
	})
	if idx < 0 {
		return writeError(c, http.StatusNotFound, codeNotFound, "No access")
	}

	creds, err := dir.IssueCredentials(ents[idx], sess.Subject)
	if err != nil {
		return writeError(c, http.StatusInternalServerError, codeInternal, err.Error())
	}

	return c.JSON(http.StatusOK, getRoleCredentialsResponse{RoleCredentials: roleCredentialsWire{
		AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey,
		SessionToken: creds.SessionToken, Expiration: creds.Expiration.UnixMilli(),
	}})
}
