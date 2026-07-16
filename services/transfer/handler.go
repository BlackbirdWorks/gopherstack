package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField    = "__type"
	keyMessageField = "message"
)

const transferTargetPrefix = "TransferService."

const (
	keyDescription        = "Description"
	keyStatus             = "Status"
	keyWorkflowID         = "WorkflowId"
	keyConnectorID        = "ConnectorId"
	keyURL                = "Url"
	keyTransferID         = "TransferId"
	keyStepType           = "Type"
	keyStepName           = "Name"
	keySourceFileLoc      = "SourceFileLocation"
	keyLocalProfileID     = "LocalProfileId"
	keyPartnerProfileID   = "PartnerProfileId"
	keyArn                = "Arn"
	keyTags               = "Tags"
	keyWebAppID           = "WebAppId"
	keySecurityPolicyName = "SecurityPolicyName"
	keyRole               = "Role"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Transfer Family operations.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new Transfer handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears the handler state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// Shutdown stops the backend's scheduled server state-transition timers so no
// timer goroutine outlives the service. Invoked on server shutdown via
// service.Shutdowner.
func (h *Handler) Shutdown(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Transfer" }

// GetSupportedOperations returns the list of supported Transfer operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateAccess",
		"CreateAgreement",
		"CreateConnector",
		"CreateProfile",
		"CreateServer",
		"CreateUser",
		"CreateWebApp",
		"CreateWorkflow",
		"DeleteAccess",
		"DeleteAgreement",
		"DeleteCertificate",
		"DeleteConnector",
		"DeleteHostKey",
		"DeleteProfile",
		"DeleteServer",
		"DeleteSshPublicKey",
		"DeleteUser",
		"DeleteWebApp",
		"DeleteWebAppCustomization",
		"DeleteWorkflow",
		"DescribeAccess",
		"DescribeAgreement",
		"DescribeCertificate",
		"DescribeConnector",
		"DescribeExecution",
		"DescribeHostKey",
		"DescribeProfile",
		"DescribeSecurityPolicy",
		"DescribeServer",
		"DescribeUser",
		"DescribeWebApp",
		"DescribeWebAppCustomization",
		"DescribeWorkflow",
		"ImportCertificate",
		"ImportHostKey",
		"ImportSshPublicKey",
		"ListAccesses",
		"ListAgreements",
		"ListCertificates",
		"ListConnectors",
		"ListExecutions",
		"ListFileTransferResults",
		"ListHostKeys",
		"ListProfiles",
		"ListSecurityPolicies",
		"ListServers",
		"ListTagsForResource",
		"ListUsers",
		"ListWebApps",
		"ListWorkflows",
		"SendWorkflowStepState",
		"StartDirectoryListing",
		"StartFileTransfer",
		"StartRemoteDelete",
		"StartRemoteMove",
		"StartServer",
		"StopServer",
		"TagResource",
		"TestConnection",
		"TestIdentityProvider",
		"UntagResource",
		"UpdateAccess",
		"UpdateAgreement",
		"UpdateCertificate",
		"UpdateConnector",
		"UpdateHostKey",
		"UpdateProfile",
		"UpdateServer",
		"UpdateUser",
		"UpdateWebApp",
		"UpdateWebAppCustomization",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "transfer" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Transfer instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Transfer API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), transferTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Transfer action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, transferTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ServerID string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	_ = json.Unmarshal(body, &req)

	if req.ServerID != "" && req.UserName != "" {
		return req.ServerID + "/" + req.UserName
	}

	return req.ServerID
}

// Handler returns the Echo handler function for Transfer requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Transfer", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateServer":                service.WrapOp(h.handleCreateServer),
		"DescribeServer":              service.WrapOp(h.handleDescribeServer),
		"ListServers":                 service.WrapOp(h.handleListServers),
		"StartServer":                 service.WrapOp(h.handleStartServer),
		"StopServer":                  service.WrapOp(h.handleStopServer),
		"DeleteServer":                service.WrapOp(h.handleDeleteServer),
		"UpdateServer":                service.WrapOp(h.handleUpdateServer),
		"CreateUser":                  service.WrapOp(h.handleCreateUser),
		"DescribeUser":                service.WrapOp(h.handleDescribeUser),
		"ListUsers":                   service.WrapOp(h.handleListUsers),
		"DeleteUser":                  service.WrapOp(h.handleDeleteUser),
		"UpdateUser":                  service.WrapOp(h.handleUpdateUser),
		"CreateAccess":                service.WrapOp(h.handleCreateAccess),
		"DeleteAccess":                service.WrapOp(h.handleDeleteAccess),
		"DescribeAccess":              service.WrapOp(h.handleDescribeAccess),
		"ListAccesses":                service.WrapOp(h.handleListAccesses),
		"UpdateAccess":                service.WrapOp(h.handleUpdateAccess),
		"CreateAgreement":             service.WrapOp(h.handleCreateAgreement),
		"DeleteAgreement":             service.WrapOp(h.handleDeleteAgreement),
		"DescribeAgreement":           service.WrapOp(h.handleDescribeAgreement),
		"ListAgreements":              service.WrapOp(h.handleListAgreements),
		"UpdateAgreement":             service.WrapOp(h.handleUpdateAgreement),
		"CreateConnector":             service.WrapOp(h.handleCreateConnector),
		"DeleteConnector":             service.WrapOp(h.handleDeleteConnector),
		"DescribeConnector":           service.WrapOp(h.handleDescribeConnector),
		"ListConnectors":              service.WrapOp(h.handleListConnectors),
		"UpdateConnector":             service.WrapOp(h.handleUpdateConnector),
		"CreateProfile":               service.WrapOp(h.handleCreateProfile),
		"DeleteProfile":               service.WrapOp(h.handleDeleteProfile),
		"DescribeProfile":             service.WrapOp(h.handleDescribeProfile),
		"ListProfiles":                service.WrapOp(h.handleListProfiles),
		"UpdateProfile":               service.WrapOp(h.handleUpdateProfile),
		"CreateWebApp":                service.WrapOp(h.handleCreateWebApp),
		"DeleteWebApp":                service.WrapOp(h.handleDeleteWebApp),
		"DescribeWebApp":              service.WrapOp(h.handleDescribeWebApp),
		"ListWebApps":                 service.WrapOp(h.handleListWebApps),
		"UpdateWebApp":                service.WrapOp(h.handleUpdateWebApp),
		"DeleteWebAppCustomization":   service.WrapOp(h.handleDeleteWebAppCustomization),
		"DescribeWebAppCustomization": service.WrapOp(h.handleDescribeWebAppCustomization),
		"UpdateWebAppCustomization":   service.WrapOp(h.handleUpdateWebAppCustomization),
		"CreateWorkflow":              service.WrapOp(h.handleCreateWorkflow),
		"DeleteWorkflow":              service.WrapOp(h.handleDeleteWorkflow),
		"DescribeWorkflow":            service.WrapOp(h.handleDescribeWorkflow),
		"ListWorkflows":               service.WrapOp(h.handleListWorkflows),
		"DeleteCertificate":           service.WrapOp(h.handleDeleteCertificate),
		"ImportCertificate":           service.WrapOp(h.handleImportCertificate),
		"DescribeCertificate":         service.WrapOp(h.handleDescribeCertificate),
		"ListCertificates":            service.WrapOp(h.handleListCertificates),
		"UpdateCertificate":           service.WrapOp(h.handleUpdateCertificate),
		"ImportHostKey":               service.WrapOp(h.handleImportHostKey),
		"DeleteHostKey":               service.WrapOp(h.handleDeleteHostKey),
		"DescribeHostKey":             service.WrapOp(h.handleDescribeHostKey),
		"ListHostKeys":                service.WrapOp(h.handleListHostKeys),
		"UpdateHostKey":               service.WrapOp(h.handleUpdateHostKey),
		"ImportSshPublicKey":          service.WrapOp(h.handleImportSSHPublicKey),
		"DeleteSshPublicKey":          service.WrapOp(h.handleDeleteSSHPublicKey),
		"TagResource":                 service.WrapOp(h.handleTagResource),
		"UntagResource":               service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":         service.WrapOp(h.handleListTagsForResource),
		"DescribeExecution":           service.WrapOp(h.handleDescribeExecution),
		"ListExecutions":              service.WrapOp(h.handleListExecutions),
		"ListFileTransferResults":     service.WrapOp(h.handleListFileTransferResults),
		"DescribeSecurityPolicy":      service.WrapOp(h.handleDescribeSecurityPolicy),
		"ListSecurityPolicies":        service.WrapOp(h.handleListSecurityPolicies),
		"SendWorkflowStepState":       service.WrapOp(h.handleSendWorkflowStepState),
		"StartDirectoryListing":       service.WrapOp(h.handleStartDirectoryListing),
		"StartFileTransfer":           service.WrapOp(h.handleStartFileTransfer),
		"StartRemoteDelete":           service.WrapOp(h.handleStartRemoteDelete),
		"StartRemoteMove":             service.WrapOp(h.handleStartRemoteMove),
		"TestConnection":              service.WrapOp(h.handleTestConnection),
		"TestIdentityProvider":        service.WrapOp(h.handleTestIdentityProvider),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceExistsException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "InvalidRequestException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServiceError",
			keyMessageField: err.Error(),
		})
	}
}

// tagsToList converts a map of tags to the AWS list format sorted by key.
func tagsToList(tags map[string]string) []map[string]string {
	keys := collections.SortedKeys(tags)

	list := make([]map[string]string, 0, len(tags))
	for _, k := range keys {
		list = append(list, map[string]string{"Key": k, "Value": tags[k]})
	}

	return list
}

// tagsFromList converts the AWS tag list format to a map.
func tagsFromList(tags []map[string]string) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t["Key"]] = t["Value"]
	}

	return m
}

const defaultTransferMaxResults = 1000

// applyNextTokenItems applies NextToken-based pagination to a slice using the
// shared pkgs/page opaque token format.
func applyNextTokenItems[T any](items []T, nextToken string, maxResults int) ([]T, string) {
	p := page.New(items, nextToken, maxResults, defaultTransferMaxResults)

	return p.Data, p.Next
}
