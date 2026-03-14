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
		"CreateServer",
		"DescribeServer",
		"ListServers",
		"StartServer",
		"StopServer",
		"DeleteServer",
		"UpdateServer",
		"CreateUser",
		"DescribeUser",
		"ListUsers",
		"DeleteUser",
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
		"CreateServer":   service.WrapOp(h.handleCreateServer),
		"DescribeServer": service.WrapOp(h.handleDescribeServer),
		"ListServers":    service.WrapOp(h.handleListServers),
		"StartServer":    service.WrapOp(h.handleStartServer),
		"StopServer":     service.WrapOp(h.handleStopServer),
		"DeleteServer":   service.WrapOp(h.handleDeleteServer),
		"UpdateServer":   service.WrapOp(h.handleUpdateServer),
		"CreateUser":     service.WrapOp(h.handleCreateUser),
		"DescribeUser":   service.WrapOp(h.handleDescribeUser),
		"ListUsers":      service.WrapOp(h.handleListUsers),
		"DeleteUser":     service.WrapOp(h.handleDeleteUser),
		"UpdateUser":     service.WrapOp(h.handleUpdateUser),
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
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
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
