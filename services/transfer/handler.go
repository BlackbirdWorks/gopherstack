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

const transferTargetPrefix = "TransferService."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Transfer Family operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Transfer handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"DeleteServer",
		"DeleteUser",
		"DescribeServer",
		"DescribeUser",
		"ListServers",
		"ListUsers",
		"StartServer",
		"StopServer",
		"UpdateServer",
		"UpdateUser",
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

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateServer":      service.WrapOp(h.handleCreateServer),
		"DescribeServer":    service.WrapOp(h.handleDescribeServer),
		"ListServers":       service.WrapOp(h.handleListServers),
		"StartServer":       service.WrapOp(h.handleStartServer),
		"StopServer":        service.WrapOp(h.handleStopServer),
		"DeleteServer":      service.WrapOp(h.handleDeleteServer),
		"UpdateServer":      service.WrapOp(h.handleUpdateServer),
		"CreateUser":        service.WrapOp(h.handleCreateUser),
		"DescribeUser":      service.WrapOp(h.handleDescribeUser),
		"ListUsers":         service.WrapOp(h.handleListUsers),
		"DeleteUser":        service.WrapOp(h.handleDeleteUser),
		"UpdateUser":        service.WrapOp(h.handleUpdateUser),
		"CreateAccess":      service.WrapOp(h.handleCreateAccess),
		"DeleteAccess":      service.WrapOp(h.handleDeleteAccess),
		"CreateAgreement":   service.WrapOp(h.handleCreateAgreement),
		"DeleteAgreement":   service.WrapOp(h.handleDeleteAgreement),
		"CreateConnector":   service.WrapOp(h.handleCreateConnector),
		"DeleteConnector":   service.WrapOp(h.handleDeleteConnector),
		"CreateProfile":     service.WrapOp(h.handleCreateProfile),
		"CreateWebApp":      service.WrapOp(h.handleCreateWebApp),
		"CreateWorkflow":    service.WrapOp(h.handleCreateWorkflow),
		"DeleteCertificate": service.WrapOp(h.handleDeleteCertificate),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
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
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "ResourceExistsException",
			"message": err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			"__type":  "InvalidRequestException",
			"message": err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"__type":  "InternalServiceError",
			"message": err.Error(),
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

	return &describeServerOutput{Server: toServerView(s, h.Backend.serverARNForServer(s))}, nil
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
			Arn:      h.Backend.serverARNForServer(s),
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
		User:     toUserView(u, h.Backend.userARNForUser(u)),
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
			Arn:      h.Backend.userARNForUser(u),
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
