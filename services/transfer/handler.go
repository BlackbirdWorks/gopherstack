package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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
		"CreateServer":            service.WrapOp(h.handleCreateServer),
		"DescribeServer":          service.WrapOp(h.handleDescribeServer),
		"ListServers":             service.WrapOp(h.handleListServers),
		"StartServer":             service.WrapOp(h.handleStartServer),
		"StopServer":              service.WrapOp(h.handleStopServer),
		"DeleteServer":            service.WrapOp(h.handleDeleteServer),
		"UpdateServer":            service.WrapOp(h.handleUpdateServer),
		"CreateUser":              service.WrapOp(h.handleCreateUser),
		"DescribeUser":            service.WrapOp(h.handleDescribeUser),
		"ListUsers":               service.WrapOp(h.handleListUsers),
		"DeleteUser":              service.WrapOp(h.handleDeleteUser),
		"UpdateUser":              service.WrapOp(h.handleUpdateUser),
		"CreateAccess":            service.WrapOp(h.handleCreateAccess),
		"DeleteAccess":            service.WrapOp(h.handleDeleteAccess),
		"DescribeAccess":          service.WrapOp(h.handleDescribeAccess),
		"ListAccesses":            service.WrapOp(h.handleListAccesses),
		"UpdateAccess":            service.WrapOp(h.handleUpdateAccess),
		"CreateAgreement":         service.WrapOp(h.handleCreateAgreement),
		"DeleteAgreement":         service.WrapOp(h.handleDeleteAgreement),
		"DescribeAgreement":       service.WrapOp(h.handleDescribeAgreement),
		"ListAgreements":          service.WrapOp(h.handleListAgreements),
		"UpdateAgreement":         service.WrapOp(h.handleUpdateAgreement),
		"CreateConnector":         service.WrapOp(h.handleCreateConnector),
		"DeleteConnector":         service.WrapOp(h.handleDeleteConnector),
		"DescribeConnector":       service.WrapOp(h.handleDescribeConnector),
		"ListConnectors":          service.WrapOp(h.handleListConnectors),
		"UpdateConnector":         service.WrapOp(h.handleUpdateConnector),
		"CreateProfile":           service.WrapOp(h.handleCreateProfile),
		"DeleteProfile":           service.WrapOp(h.handleDeleteProfile),
		"DescribeProfile":         service.WrapOp(h.handleDescribeProfile),
		"ListProfiles":            service.WrapOp(h.handleListProfiles),
		"UpdateProfile":           service.WrapOp(h.handleUpdateProfile),
		"CreateWebApp":            service.WrapOp(h.handleCreateWebApp),
		"DeleteWebApp":            service.WrapOp(h.handleDeleteWebApp),
		"DescribeWebApp":          service.WrapOp(h.handleDescribeWebApp),
		"ListWebApps":             service.WrapOp(h.handleListWebApps),
		"UpdateWebApp":            service.WrapOp(h.handleUpdateWebApp),
		"DeleteWebAppCustomization":  service.WrapOp(h.handleDeleteWebAppCustomization),
		"DescribeWebAppCustomization": service.WrapOp(h.handleDescribeWebAppCustomization),
		"UpdateWebAppCustomization":  service.WrapOp(h.handleUpdateWebAppCustomization),
		"CreateWorkflow":          service.WrapOp(h.handleCreateWorkflow),
		"DeleteWorkflow":          service.WrapOp(h.handleDeleteWorkflow),
		"DescribeWorkflow":        service.WrapOp(h.handleDescribeWorkflow),
		"ListWorkflows":           service.WrapOp(h.handleListWorkflows),
		"DeleteCertificate":       service.WrapOp(h.handleDeleteCertificate),
		"ImportCertificate":       service.WrapOp(h.handleImportCertificate),
		"DescribeCertificate":     service.WrapOp(h.handleDescribeCertificate),
		"ListCertificates":        service.WrapOp(h.handleListCertificates),
		"UpdateCertificate":       service.WrapOp(h.handleUpdateCertificate),
		"ImportHostKey":           service.WrapOp(h.handleImportHostKey),
		"DeleteHostKey":           service.WrapOp(h.handleDeleteHostKey),
		"DescribeHostKey":         service.WrapOp(h.handleDescribeHostKey),
		"ListHostKeys":            service.WrapOp(h.handleListHostKeys),
		"UpdateHostKey":           service.WrapOp(h.handleUpdateHostKey),
		"ImportSshPublicKey":      service.WrapOp(h.handleImportSshPublicKey),
		"DeleteSshPublicKey":      service.WrapOp(h.handleDeleteSshPublicKey),
		"TagResource":             service.WrapOp(h.handleTagResource),
		"UntagResource":           service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":     service.WrapOp(h.handleListTagsForResource),
		"DescribeExecution":       service.WrapOp(h.handleDescribeExecution),
		"ListExecutions":          service.WrapOp(h.handleListExecutions),
		"ListFileTransferResults": service.WrapOp(h.handleListFileTransferResults),
		"DescribeSecurityPolicy":  service.WrapOp(h.handleDescribeSecurityPolicy),
		"ListSecurityPolicies":    service.WrapOp(h.handleListSecurityPolicies),
		"SendWorkflowStepState":   service.WrapOp(h.handleSendWorkflowStepState),
		"StartDirectoryListing":   service.WrapOp(h.handleStartDirectoryListing),
		"StartFileTransfer":       service.WrapOp(h.handleStartFileTransfer),
		"StartRemoteDelete":       service.WrapOp(h.handleStartRemoteDelete),
		"StartRemoteMove":         service.WrapOp(h.handleStartRemoteMove),
		"TestConnection":          service.WrapOp(h.handleTestConnection),
		"TestIdentityProvider":    service.WrapOp(h.handleTestIdentityProvider),
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

// --- Server operations ---

type createServerInput struct {
	Protocols []string            `json:"Protocols"`
	Tags      []map[string]string `json:"Tags"`
}

type createServerOutput struct {
	ServerID string `json:"ServerId"`
}

func (h *Handler) handleCreateServer(_ context.Context, in *createServerInput) (*createServerOutput, error) {
	tags := tagsFromList(in.Tags)

	s, err := h.Backend.CreateServer(in.Protocols, tags)
	if err != nil {
		return nil, err
	}

	return &createServerOutput{ServerID: s.ServerID}, nil
}

type serverIDInput struct {
	ServerID string `json:"ServerId"`
}

type serverView struct {
	Arn       string              `json:"Arn"`
	ServerID  string              `json:"ServerId"`
	State     string              `json:"State"`
	Protocols []string            `json:"Protocols"`
	Domain    string              `json:"Domain"`
	Tags      []map[string]string `json:"Tags"`
}

type describeServerOutput struct {
	Server serverView `json:"Server"`
}

func (h *Handler) handleDescribeServer(_ context.Context, in *serverIDInput) (*describeServerOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	s, err := h.Backend.DescribeServer(in.ServerID)
	if err != nil {
		return nil, err
	}

	return &describeServerOutput{Server: toServerView(s, serverARN(s.AccountID, s.Region, s.ServerID))}, nil
}

type listServersOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	Servers   []serverListItem `json:"Servers"`
}

type listServersInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type serverListItem struct {
	Arn      string `json:"Arn"`
	ServerID string `json:"ServerId"`
	State    string `json:"State"`
	Domain   string `json:"Domain"`
}

func (h *Handler) handleListServers(_ context.Context, in *listServersInput) (*listServersOutput, error) {
	servers := h.Backend.ListServers()
	items := make([]serverListItem, 0, len(servers))

	for i := range servers {
		s := &servers[i]
		items = append(items, serverListItem{
			Arn:      serverARN(s.AccountID, s.Region, s.ServerID),
			ServerID: s.ServerID,
			State:    s.State,
			Domain:   s.Domain,
		})
	}

	items, nextToken := applyNextTokenItems(items, in.NextToken, in.MaxResults)

	return &listServersOutput{Servers: items, NextToken: nextToken}, nil
}

func (h *Handler) handleStartServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.StartServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.StopServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteServer(_ context.Context, in *serverIDInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteServer(in.ServerID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateServerInput struct {
	ServerID  string   `json:"ServerId"`
	Protocols []string `json:"Protocols"`
}

type updateServerOutput struct {
	ServerID string `json:"ServerId"`
}

func (h *Handler) handleUpdateServer(_ context.Context, in *updateServerInput) (*updateServerOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	s, err := h.Backend.UpdateServer(in.ServerID, in.Protocols)
	if err != nil {
		return nil, err
	}

	return &updateServerOutput{ServerID: s.ServerID}, nil
}

// --- User operations ---

type createUserInput struct {
	ServerID string              `json:"ServerId"`
	UserName string              `json:"UserName"`
	HomeDir  string              `json:"HomeDirectory"`
	Role     string              `json:"Role"`
	Tags     []map[string]string `json:"Tags"`
}

type createUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

func (h *Handler) handleCreateUser(_ context.Context, in *createUserInput) (*createUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	u, err := h.Backend.CreateUser(in.ServerID, in.UserName, in.HomeDir, in.Role, tags)
	if err != nil {
		return nil, err
	}

	return &createUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

type describeUserInput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

type userView struct {
	Arn      string              `json:"Arn"`
	UserName string              `json:"UserName"`
	HomeDir  string              `json:"HomeDirectory"`
	Role     string              `json:"Role"`
	Tags     []map[string]string `json:"Tags"`
}

type describeUserOutput struct {
	ServerID string   `json:"ServerId"`
	User     userView `json:"User"`
}

func (h *Handler) handleDescribeUser(_ context.Context, in *describeUserInput) (*describeUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.DescribeUser(in.ServerID, in.UserName)
	if err != nil {
		return nil, err
	}

	return &describeUserOutput{
		ServerID: u.ServerID,
		User:     toUserView(u, userARN(u.AccountID, u.Region, u.ServerID, u.UserName)),
	}, nil
}

type listUsersInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type userListItem struct {
	Arn      string `json:"Arn"`
	UserName string `json:"UserName"`
	HomeDir  string `json:"HomeDirectory"`
	Role     string `json:"Role"`
}

type listUsersOutput struct {
	ServerID  string         `json:"ServerId"`
	NextToken string         `json:"NextToken,omitempty"`
	Users     []userListItem `json:"Users"`
}

func (h *Handler) handleListUsers(_ context.Context, in *listUsersInput) (*listUsersOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	users, err := h.Backend.ListUsers(in.ServerID)
	if err != nil {
		return nil, err
	}

	items := make([]userListItem, 0, len(users))

	for i := range users {
		u := &users[i]
		items = append(items, userListItem{
			Arn:      userARN(u.AccountID, u.Region, u.ServerID, u.UserName),
			UserName: u.UserName,
			HomeDir:  u.HomeDir,
			Role:     u.Role,
		})
	}

	items, nextToken := applyNextTokenItems(items, in.NextToken, in.MaxResults)

	return &listUsersOutput{ServerID: in.ServerID, Users: items, NextToken: nextToken}, nil
}

func (h *Handler) handleDeleteUser(_ context.Context, in *describeUserInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteUser(in.ServerID, in.UserName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateUserInput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
	HomeDir  string `json:"HomeDirectory"`
	Role     string `json:"Role"`
}

type updateUserOutput struct {
	ServerID string `json:"ServerId"`
	UserName string `json:"UserName"`
}

func (h *Handler) handleUpdateUser(_ context.Context, in *updateUserInput) (*updateUserOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	u, err := h.Backend.UpdateUser(in.ServerID, in.UserName, in.HomeDir, in.Role)
	if err != nil {
		return nil, err
	}

	return &updateUserOutput{ServerID: u.ServerID, UserName: u.UserName}, nil
}

// --- View helpers ---

func toServerView(s *Server, arnStr string) serverView {
	return serverView{
		Arn:       arnStr,
		ServerID:  s.ServerID,
		State:     s.State,
		Protocols: s.Protocols,
		Domain:    s.Domain,
		Tags:      tagsToList(s.Tags),
	}
}

func toUserView(u *User, arnStr string) userView {
	return userView{
		Arn:      arnStr,
		UserName: u.UserName,
		HomeDir:  u.HomeDir,
		Role:     u.Role,
		Tags:     tagsToList(u.Tags),
	}
}

// --- Access operations ---

type createAccessInput struct {
	ServerID   string              `json:"ServerId"`
	ExternalID string              `json:"ExternalId"`
	Role       string              `json:"Role"`
	HomeDir    string              `json:"HomeDirectory"`
	Tags       []map[string]string `json:"Tags"`
}

type createAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleCreateAccess(_ context.Context, in *createAccessInput) (*createAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	a, err := h.Backend.CreateAccess(in.ServerID, in.ExternalID, in.Role, in.HomeDir, tags)
	if err != nil {
		return nil, err
	}

	return &createAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}

type deleteAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleDeleteAccess(_ context.Context, in *deleteAccessInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAccess(in.ServerID, in.ExternalID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Agreement operations ---

type createAgreementInput struct {
	ServerID         string              `json:"ServerId"`
	Description      string              `json:"Description"`
	LocalProfileID   string              `json:"LocalProfileId"`
	PartnerProfileID string              `json:"PartnerProfileId"`
	BaseDirectory    string              `json:"BaseDirectory"`
	AccessRole       string              `json:"AccessRole"`
	Tags             []map[string]string `json:"Tags"`
}

type createAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleCreateAgreement(_ context.Context, in *createAgreementInput) (*createAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	ag, err := h.Backend.CreateAgreement(
		in.ServerID,
		in.Description,
		in.LocalProfileID,
		in.PartnerProfileID,
		in.BaseDirectory,
		in.AccessRole,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createAgreementOutput{AgreementID: ag.AgreementID}, nil
}

type deleteAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleDeleteAgreement(_ context.Context, in *deleteAgreementInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAgreement(in.ServerID, in.AgreementID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Connector operations ---

type createConnectorInput struct {
	URL        string              `json:"Url"`
	AccessRole string              `json:"AccessRole"`
	Tags       []map[string]string `json:"Tags"`
}

type createConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleCreateConnector(_ context.Context, in *createConnectorInput) (*createConnectorOutput, error) {
	if in.URL == "" {
		return nil, fmt.Errorf("%w: Url is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	c, err := h.Backend.CreateConnector(in.URL, in.AccessRole, tags)
	if err != nil {
		return nil, err
	}

	return &createConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

type deleteConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleDeleteConnector(_ context.Context, in *deleteConnectorInput) (*struct{}, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnector(in.ConnectorID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Profile operations ---

type createProfileInput struct {
	ProfileType string              `json:"ProfileType"`
	As2ID       string              `json:"As2Id"`
	Tags        []map[string]string `json:"Tags"`
}

type createProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleCreateProfile(_ context.Context, in *createProfileInput) (*createProfileOutput, error) {
	if in.ProfileType == "" {
		return nil, fmt.Errorf("%w: ProfileType is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	p, err := h.Backend.CreateProfile(in.ProfileType, in.As2ID, tags)
	if err != nil {
		return nil, err
	}

	return &createProfileOutput{ProfileID: p.ProfileID}, nil
}

// --- WebApp operations ---

type createWebAppInput struct {
	Tags []map[string]string `json:"Tags"`
}

type createWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleCreateWebApp(_ context.Context, in *createWebAppInput) (*createWebAppOutput, error) {
	tags := tagsFromList(in.Tags)

	w, err := h.Backend.CreateWebApp(tags)
	if err != nil {
		return nil, err
	}

	return &createWebAppOutput{WebAppID: w.WebAppID}, nil
}

// --- Workflow operations ---

type createWorkflowInput struct {
	Description string              `json:"Description"`
	Tags        []map[string]string `json:"Tags"`
}

type createWorkflowOutput struct {
	WorkflowID string `json:"WorkflowId"`
}

func (h *Handler) handleCreateWorkflow(_ context.Context, in *createWorkflowInput) (*createWorkflowOutput, error) {
	tags := tagsFromList(in.Tags)

	wf, err := h.Backend.CreateWorkflow(in.Description, tags)
	if err != nil {
		return nil, err
	}

	return &createWorkflowOutput{WorkflowID: wf.WorkflowID}, nil
}

// --- Certificate operations ---

type deleteCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleDeleteCertificate(_ context.Context, in *deleteCertificateInput) (*struct{}, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCertificate(in.CertificateID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Extended Access operations ---

type describeAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

type describeAccessOutput struct {
	Access   map[string]any `json:"Access"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeAccess(_ context.Context, in *describeAccessInput) (*describeAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.DescribeAccess(in.ServerID, in.ExternalID)
	if err != nil {
		return nil, err
	}

	return &describeAccessOutput{
		ServerID: a.ServerID,
		Access: map[string]any{
			"ExternalId":    a.ExternalID,
			"ServerId":      a.ServerID,
			"Role":          a.Role,
			"HomeDirectory": a.HomeDir,
		},
	}, nil
}

type listAccessesInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAccessesOutput struct {
	Accesses  []map[string]any `json:"Accesses"`
	NextToken string           `json:"NextToken,omitempty"`
	ServerId  string           `json:"ServerId"`
}

func (h *Handler) handleListAccesses(_ context.Context, in *listAccessesInput) (*listAccessesOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAccesses(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, a := range page {
		out[i] = map[string]any{
			"ExternalId":    a.ExternalID,
			"HomeDirectory": a.HomeDir,
			"Role":          a.Role,
		}
	}

	return &listAccessesOutput{Accesses: out, NextToken: next, ServerId: in.ServerID}, nil
}

type updateAccessInput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
	Role       string `json:"Role"`
	HomeDir    string `json:"HomeDirectory"`
}

type updateAccessOutput struct {
	ServerID   string `json:"ServerId"`
	ExternalID string `json:"ExternalId"`
}

func (h *Handler) handleUpdateAccess(_ context.Context, in *updateAccessInput) (*updateAccessOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: ExternalId is required", errInvalidRequest)
	}

	a, err := h.Backend.UpdateAccess(in.ServerID, in.ExternalID, in.Role, in.HomeDir)
	if err != nil {
		return nil, err
	}

	return &updateAccessOutput{ServerID: a.ServerID, ExternalID: a.ExternalID}, nil
}

// --- Extended Agreement operations ---

type describeAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

type describeAgreementOutput struct {
	Agreement map[string]any `json:"Agreement"`
}

func (h *Handler) handleDescribeAgreement(_ context.Context, in *describeAgreementInput) (*describeAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.DescribeAgreement(in.ServerID, in.AgreementID)
	if err != nil {
		return nil, err
	}

	return &describeAgreementOutput{
		Agreement: map[string]any{
			"AgreementId":      ag.AgreementID,
			"ServerId":         ag.ServerID,
			"Description":      ag.Description,
			"Status":           ag.Status,
			"LocalProfileId":   ag.LocalProfileID,
			"PartnerProfileId": ag.PartnerProfileID,
			"BaseDirectory":    ag.BaseDirectory,
			"AccessRole":       ag.AccessRole,
		},
	}, nil
}

type listAgreementsInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAgreementsOutput struct {
	Agreements []map[string]any `json:"Agreements"`
	NextToken  string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListAgreements(_ context.Context, in *listAgreementsInput) (*listAgreementsOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAgreements(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, ag := range page {
		out[i] = map[string]any{
			"AgreementId": ag.AgreementID,
			"Description": ag.Description,
			"Status":      ag.Status,
		}
	}

	return &listAgreementsOutput{Agreements: out, NextToken: next}, nil
}

type updateAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
	Description string `json:"Description"`
	Status      string `json:"Status"`
}

type updateAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleUpdateAgreement(_ context.Context, in *updateAgreementInput) (*updateAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.UpdateAgreement(in.ServerID, in.AgreementID, in.Description, in.Status)
	if err != nil {
		return nil, err
	}

	return &updateAgreementOutput{AgreementID: ag.AgreementID}, nil
}

// --- Extended Connector operations ---

type describeConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
}

type describeConnectorOutput struct {
	Connector map[string]any `json:"Connector"`
}

func (h *Handler) handleDescribeConnector(_ context.Context, in *describeConnectorInput) (*describeConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeConnector(in.ConnectorID)
	if err != nil {
		return nil, err
	}

	return &describeConnectorOutput{
		Connector: map[string]any{
			"ConnectorId": c.ConnectorID,
			"Url":         c.URL,
			"AccessRole":  c.AccessRole,
		},
	}, nil
}

type listConnectorsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listConnectorsOutput struct {
	Connectors []map[string]any `json:"Connectors"`
	NextToken  string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListConnectors(_ context.Context, in *listConnectorsInput) (*listConnectorsOutput, error) {
	items := h.Backend.ListConnectors()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			"ConnectorId": c.ConnectorID,
			"Url":         c.URL,
		}
	}

	return &listConnectorsOutput{Connectors: out, NextToken: next}, nil
}

type updateConnectorInput struct {
	ConnectorID string `json:"ConnectorId"`
	URL         string `json:"Url"`
	AccessRole  string `json:"AccessRole"`
}

type updateConnectorOutput struct {
	ConnectorID string `json:"ConnectorId"`
}

func (h *Handler) handleUpdateConnector(_ context.Context, in *updateConnectorInput) (*updateConnectorOutput, error) {
	if in.ConnectorID == "" {
		return nil, fmt.Errorf("%w: ConnectorId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateConnector(in.ConnectorID, in.URL, in.AccessRole)
	if err != nil {
		return nil, err
	}

	return &updateConnectorOutput{ConnectorID: c.ConnectorID}, nil
}

// --- Extended Profile operations ---

type deleteProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleDeleteProfile(_ context.Context, in *deleteProfileInput) (*struct{}, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProfile(in.ProfileID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeProfileInput struct {
	ProfileID string `json:"ProfileId"`
}

type describeProfileOutput struct {
	Profile map[string]any `json:"Profile"`
}

func (h *Handler) handleDescribeProfile(_ context.Context, in *describeProfileInput) (*describeProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProfile(in.ProfileID)
	if err != nil {
		return nil, err
	}

	return &describeProfileOutput{
		Profile: map[string]any{
			"ProfileId":   p.ProfileID,
			"ProfileType": p.ProfileType,
			"As2Id":       p.As2ID,
		},
	}, nil
}

type listProfilesInput struct {
	NextToken   string `json:"NextToken"`
	MaxResults  int    `json:"MaxResults"`
	ProfileType string `json:"ProfileType"`
}

type listProfilesOutput struct {
	Profiles  []map[string]any `json:"Profiles"`
	NextToken string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListProfiles(_ context.Context, in *listProfilesInput) (*listProfilesOutput, error) {
	items := h.Backend.ListProfiles()

	if in.ProfileType != "" {
		filtered := items[:0]
		for _, p := range items {
			if p.ProfileType == in.ProfileType {
				filtered = append(filtered, p)
			}
		}
		items = filtered
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, p := range page {
		out[i] = map[string]any{
			"ProfileId":   p.ProfileID,
			"ProfileType": p.ProfileType,
			"As2Id":       p.As2ID,
		}
	}

	return &listProfilesOutput{Profiles: out, NextToken: next}, nil
}

type updateProfileInput struct {
	ProfileID string `json:"ProfileId"`
	As2ID     string `json:"As2Id"`
}

type updateProfileOutput struct {
	ProfileID string `json:"ProfileId"`
}

func (h *Handler) handleUpdateProfile(_ context.Context, in *updateProfileInput) (*updateProfileOutput, error) {
	if in.ProfileID == "" {
		return nil, fmt.Errorf("%w: ProfileId is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProfile(in.ProfileID, in.As2ID)
	if err != nil {
		return nil, err
	}

	return &updateProfileOutput{ProfileID: p.ProfileID}, nil
}

// --- Extended WebApp operations ---

type deleteWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleDeleteWebApp(_ context.Context, in *deleteWebAppInput) (*struct{}, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebApp(in.WebAppID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

type describeWebAppOutput struct {
	WebApp map[string]any `json:"WebApp"`
}

func (h *Handler) handleDescribeWebApp(_ context.Context, in *describeWebAppInput) (*describeWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	w, err := h.Backend.DescribeWebApp(in.WebAppID)
	if err != nil {
		return nil, err
	}

	return &describeWebAppOutput{
		WebApp: map[string]any{
			"WebAppId": w.WebAppID,
		},
	}, nil
}

type listWebAppsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWebAppsOutput struct {
	WebApps   []map[string]any `json:"WebApps"`
	NextToken string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListWebApps(_ context.Context, in *listWebAppsInput) (*listWebAppsOutput, error) {
	items := h.Backend.ListWebApps()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, w := range page {
		out[i] = map[string]any{
			"WebAppId": w.WebAppID,
		}
	}

	return &listWebAppsOutput{WebApps: out, NextToken: next}, nil
}

type updateWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

type updateWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleUpdateWebApp(_ context.Context, in *updateWebAppInput) (*updateWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	w, err := h.Backend.UpdateWebApp(in.WebAppID)
	if err != nil {
		return nil, err
	}

	return &updateWebAppOutput{WebAppID: w.WebAppID}, nil
}

// --- WebApp Customization stubs ---

func (h *Handler) handleDeleteWebAppCustomization(_ context.Context, _ *struct{}) (*struct{}, error) {
	return &struct{}{}, nil
}

func (h *Handler) handleDescribeWebAppCustomization(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"WebAppCustomization": map[string]any{}}, nil
}

func (h *Handler) handleUpdateWebAppCustomization(_ context.Context, _ *struct{}) (*struct{}, error) {
	return &struct{}{}, nil
}

// --- Extended Workflow operations ---

type deleteWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

func (h *Handler) handleDeleteWorkflow(_ context.Context, in *deleteWorkflowInput) (*struct{}, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkflow(in.WorkflowID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWorkflowInput struct {
	WorkflowID string `json:"WorkflowId"`
}

type describeWorkflowOutput struct {
	Workflow map[string]any `json:"Workflow"`
}

func (h *Handler) handleDescribeWorkflow(_ context.Context, in *describeWorkflowInput) (*describeWorkflowOutput, error) {
	if in.WorkflowID == "" {
		return nil, fmt.Errorf("%w: WorkflowId is required", errInvalidRequest)
	}

	wf, err := h.Backend.DescribeWorkflow(in.WorkflowID)
	if err != nil {
		return nil, err
	}

	return &describeWorkflowOutput{
		Workflow: map[string]any{
			"WorkflowId":  wf.WorkflowID,
			"Description": wf.Description,
		},
	}, nil
}

type listWorkflowsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWorkflowsOutput struct {
	Workflows []map[string]any `json:"Workflows"`
	NextToken string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListWorkflows(_ context.Context, in *listWorkflowsInput) (*listWorkflowsOutput, error) {
	items := h.Backend.ListWorkflows()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, wf := range page {
		out[i] = map[string]any{
			"WorkflowId":  wf.WorkflowID,
			"Description": wf.Description,
		}
	}

	return &listWorkflowsOutput{Workflows: out, NextToken: next}, nil
}

// --- Extended Certificate operations ---

type importCertificateInput struct {
	Usage       string              `json:"Usage"`
	Body        string              `json:"Certificate"`
	Description string              `json:"Description"`
	Tags        []map[string]string `json:"Tags"`
}

type importCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleImportCertificate(_ context.Context, in *importCertificateInput) (*importCertificateOutput, error) {
	tags := tagsFromList(in.Tags)

	c, err := h.Backend.ImportCertificate(in.Usage, in.Body, in.Description, tags)
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{CertificateID: c.CertificateID}, nil
}

type describeCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

type describeCertificateOutput struct {
	Certificate map[string]any `json:"Certificate"`
}

func (h *Handler) handleDescribeCertificate(_ context.Context, in *describeCertificateInput) (*describeCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeCertificate(in.CertificateID)
	if err != nil {
		return nil, err
	}

	return &describeCertificateOutput{
		Certificate: map[string]any{
			"CertificateId": c.CertificateID,
			"Usage":         c.Usage,
			"Description":   c.Description,
			"Status":        c.Status,
		},
	}, nil
}

type listCertificatesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCertificatesOutput struct {
	Certificates []map[string]any `json:"Certificates"`
	NextToken    string           `json:"NextToken,omitempty"`
}

func (h *Handler) handleListCertificates(_ context.Context, in *listCertificatesInput) (*listCertificatesOutput, error) {
	items := h.Backend.ListCertificates()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			"CertificateId": c.CertificateID,
			"Usage":         c.Usage,
			"Status":        c.Status,
		}
	}

	return &listCertificatesOutput{Certificates: out, NextToken: next}, nil
}

type updateCertificateInput struct {
	CertificateID string `json:"CertificateId"`
	Description   string `json:"Description"`
}

type updateCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleUpdateCertificate(_ context.Context, in *updateCertificateInput) (*updateCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateCertificate(in.CertificateID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateCertificateOutput{CertificateID: c.CertificateID}, nil
}

// --- HostKey operations ---

type importHostKeyInput struct {
	ServerID    string              `json:"ServerId"`
	HostKeyBody string              `json:"HostKeyBody"`
	Description string              `json:"Description"`
	Tags        []map[string]string `json:"Tags"`
}

type importHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleImportHostKey(_ context.Context, in *importHostKeyInput) (*importHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	hk, err := h.Backend.ImportHostKey(in.ServerID, in.HostKeyBody, in.Description, tags)
	if err != nil {
		return nil, err
	}

	return &importHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

type deleteHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleDeleteHostKey(_ context.Context, in *deleteHostKeyInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHostKey(in.ServerID, in.HostKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeHostKeyInput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

type describeHostKeyOutput struct {
	HostKey  map[string]any `json:"HostKey"`
	ServerID string         `json:"ServerId"`
}

func (h *Handler) handleDescribeHostKey(_ context.Context, in *describeHostKeyInput) (*describeHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.DescribeHostKey(in.ServerID, in.HostKeyID)
	if err != nil {
		return nil, err
	}

	return &describeHostKeyOutput{
		ServerID: hk.ServerID,
		HostKey: map[string]any{
			"HostKeyId":   hk.HostKeyID,
			"Description": hk.Description,
			"Type":        hk.Type,
		},
	}, nil
}

type listHostKeysInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listHostKeysOutput struct {
	HostKeys  []map[string]any `json:"HostKeys"`
	NextToken string           `json:"NextToken,omitempty"`
	ServerId  string           `json:"ServerId"`
}

func (h *Handler) handleListHostKeys(_ context.Context, in *listHostKeysInput) (*listHostKeysOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListHostKeys(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, hk := range page {
		out[i] = map[string]any{
			"HostKeyId":   hk.HostKeyID,
			"Description": hk.Description,
			"Type":        hk.Type,
		}
	}

	return &listHostKeysOutput{HostKeys: out, NextToken: next, ServerId: in.ServerID}, nil
}

type updateHostKeyInput struct {
	ServerID    string `json:"ServerId"`
	HostKeyID   string `json:"HostKeyId"`
	Description string `json:"Description"`
}

type updateHostKeyOutput struct {
	ServerID  string `json:"ServerId"`
	HostKeyID string `json:"HostKeyId"`
}

func (h *Handler) handleUpdateHostKey(_ context.Context, in *updateHostKeyInput) (*updateHostKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.HostKeyID == "" {
		return nil, fmt.Errorf("%w: HostKeyId is required", errInvalidRequest)
	}

	hk, err := h.Backend.UpdateHostKey(in.ServerID, in.HostKeyID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateHostKeyOutput{ServerID: hk.ServerID, HostKeyID: hk.HostKeyID}, nil
}

// --- SSH public key operations ---

type importSshPublicKeyInput struct {
	ServerID         string `json:"ServerId"`
	UserName         string `json:"UserName"`
	SshPublicKeyBody string `json:"SshPublicKeyBody"`
}

type importSshPublicKeyOutput struct {
	ServerID       string `json:"ServerId"`
	SshPublicKeyID string `json:"SshPublicKeyId"`
	UserName       string `json:"UserName"`
}

func (h *Handler) handleImportSshPublicKey(_ context.Context, in *importSshPublicKeyInput) (*importSshPublicKeyOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	k, err := h.Backend.ImportSshPublicKey(in.ServerID, in.UserName, in.SshPublicKeyBody)
	if err != nil {
		return nil, err
	}

	return &importSshPublicKeyOutput{
		ServerID:       k.ServerID,
		SshPublicKeyID: k.SshPublicKeyID,
		UserName:       k.UserName,
	}, nil
}

type deleteSshPublicKeyInput struct {
	ServerID       string `json:"ServerId"`
	UserName       string `json:"UserName"`
	SshPublicKeyID string `json:"SshPublicKeyId"`
}

func (h *Handler) handleDeleteSshPublicKey(_ context.Context, in *deleteSshPublicKeyInput) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.UserName == "" {
		return nil, fmt.Errorf("%w: UserName is required", errInvalidRequest)
	}

	if in.SshPublicKeyID == "" {
		return nil, fmt.Errorf("%w: SshPublicKeyId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSshPublicKey(in.ServerID, in.UserName, in.SshPublicKeyID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// --- Tag operations ---

type tagResourceInput struct {
	Arn  string              `json:"Arn"`
	Tags []map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)
	if err := h.Backend.TagResource(in.Arn, tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	Arn     string   `json:"Arn"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.Arn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	Arn        string `json:"Arn"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	Arn       string              `json:"Arn"`
	Tags      []map[string]string `json:"Tags"`
	NextToken string              `json:"NextToken,omitempty"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, in *listTagsForResourceInput) (*listTagsForResourceOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := h.Backend.ListTagsForResource(in.Arn)

	return &listTagsForResourceOutput{
		Arn:  in.Arn,
		Tags: tagsToList(tags),
	}, nil
}

// --- Stub operations ---

func (h *Handler) handleDescribeExecution(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"Execution": map[string]any{}}, nil
}

func (h *Handler) handleListExecutions(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"Executions": []any{}, "WorkflowId": ""}, nil
}

func (h *Handler) handleListFileTransferResults(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"FileTransferResults": []any{}}, nil
}

type describeSecurityPolicyInput struct {
	SecurityPolicyName string `json:"SecurityPolicyName"`
}

func (h *Handler) handleDescribeSecurityPolicy(_ context.Context, in *describeSecurityPolicyInput) (*map[string]any, error) {
	name := in.SecurityPolicyName
	if name == "" {
		name = "TransferSecurityPolicy-2024-01"
	}

	return &map[string]any{
		"SecurityPolicy": map[string]any{
			"SecurityPolicyName": name,
			"Protocols":          []string{"SFTP"},
		},
	}, nil
}

func (h *Handler) handleListSecurityPolicies(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{
		"SecurityPolicyNames": []string{
			"TransferSecurityPolicy-2024-01",
			"TransferSecurityPolicy-2023-05",
			"TransferSecurityPolicy-2022-03",
			"TransferSecurityPolicy-FIPS-2024-01",
		},
	}, nil
}

func (h *Handler) handleSendWorkflowStepState(_ context.Context, _ *struct{}) (*struct{}, error) {
	return &struct{}{}, nil
}

func (h *Handler) handleStartDirectoryListing(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"DirectoryListingId": "listing-" + strings.Repeat("0", 8)}, nil
}

func (h *Handler) handleStartFileTransfer(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"TransferId": "transfer-" + strings.Repeat("0", 8)}, nil
}

func (h *Handler) handleStartRemoteDelete(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"TransferId": "transfer-" + strings.Repeat("0", 8)}, nil
}

func (h *Handler) handleStartRemoteMove(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"TransferId": "transfer-" + strings.Repeat("0", 8)}, nil
}

func (h *Handler) handleTestConnection(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"Status": "OK", "StatusMessage": "Connection successful"}, nil
}

func (h *Handler) handleTestIdentityProvider(_ context.Context, _ *struct{}) (*map[string]any, error) {
	return &map[string]any{"StatusCode": 200, "Message": "Identity provider test successful"}, nil
}

// tagsToList converts a map of tags to the AWS list format sorted by key.
func tagsToList(tags map[string]string) []map[string]string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

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
