package codestarconnections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codestarTargetPrefix = "CodeStar_connections_20191201."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodeStar Connections operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeStar Connections handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeStarConnections" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateConnection",
		"GetConnection",
		"ListConnections",
		"DeleteConnection",
		"CreateHost",
		"GetHost",
		"ListHosts",
		"DeleteHost",
		"UpdateHost",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codestar-connections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodeStar Connections requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codestarTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeStar Connections action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codestarTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CodeStar Connections).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CodeStar Connections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeStarConnections", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnection":    service.WrapOp(h.handleCreateConnection),
		"GetConnection":       service.WrapOp(h.handleGetConnection),
		"ListConnections":     service.WrapOp(h.handleListConnections),
		"DeleteConnection":    service.WrapOp(h.handleDeleteConnection),
		"CreateHost":          service.WrapOp(h.handleCreateHost),
		"GetHost":             service.WrapOp(h.handleGetHost),
		"ListHosts":           service.WrapOp(h.handleListHosts),
		"DeleteHost":          service.WrapOp(h.handleDeleteHost),
		"UpdateHost":          service.WrapOp(h.handleUpdateHost),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
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
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidInputException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// tagEntry is a key-value pair used in the API tag array format.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsToArray(tags map[string]string) []tagEntry {
	result := make([]tagEntry, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagEntry{Key: k, Value: v})
	}

	return result
}

// --- Connection operations ---

type createConnectionInput struct {
	Tags           map[string]string `json:"Tags"`
	ConnectionName string            `json:"ConnectionName"`
	ProviderType   string            `json:"ProviderType"`
	HostArn        string            `json:"HostArn"`
}

type createConnectionOutput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	conn, err := h.Backend.CreateConnection(in.ConnectionName, in.ProviderType, in.HostArn, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{ConnectionArn: conn.ConnectionArn}, nil
}

type getConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type connectionView struct {
	Tags             map[string]string `json:"Tags,omitempty"`
	ConnectionName   string            `json:"ConnectionName"`
	ConnectionArn    string            `json:"ConnectionArn"`
	ConnectionStatus string            `json:"ConnectionStatus"`
	OwnerAccountID   string            `json:"OwnerAccountId"`
	ProviderType     string            `json:"ProviderType"`
	HostArn          string            `json:"HostArn,omitempty"`
}

type getConnectionOutput struct {
	Connection connectionView `json:"Connection"`
}

func connectionToView(c *Connection) connectionView {
	return connectionView{
		ConnectionName:   c.ConnectionName,
		ConnectionArn:    c.ConnectionArn,
		ConnectionStatus: c.ConnectionStatus,
		OwnerAccountID:   c.OwnerAccountID,
		ProviderType:     c.ProviderType,
		HostArn:          c.HostArn,
	}
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	conn, err := h.Backend.GetConnection(in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connectionToView(conn)}, nil
}

type listConnectionsInput struct {
	ProviderTypeFilter string `json:"ProviderTypeFilter"`
	HostArnFilter      string `json:"HostArnFilter"`
	NextToken          string `json:"NextToken"`
	MaxResults         int32  `json:"MaxResults"`
}

type listConnectionsOutput struct {
	Connections []connectionView `json:"Connections"`
}

func (h *Handler) handleListConnections(
	_ context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	connections := h.Backend.ListConnections(in.ProviderTypeFilter, in.HostArnFilter)

	views := make([]connectionView, 0, len(connections))
	for _, c := range connections {
		views = append(views, connectionToView(c))
	}

	return &listConnectionsOutput{Connections: views}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct{}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnection(in.ConnectionArn); err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{}, nil
}

// --- Host operations ---

type createHostInput struct {
	Tags             map[string]string `json:"Tags"`
	Name             string            `json:"Name"`
	ProviderType     string            `json:"ProviderType"`
	ProviderEndpoint string            `json:"ProviderEndpoint"`
}

type createHostOutput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleCreateHost(
	_ context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	host, err := h.Backend.CreateHost(in.Name, in.ProviderType, in.ProviderEndpoint, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

type hostView struct {
	Name             string `json:"Name"`
	HostArn          string `json:"HostArn"`
	ProviderType     string `json:"ProviderType"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
}

type getHostOutput struct {
	hostView
}

func hostToView(h *Host) hostView {
	return hostView{
		Name:             h.Name,
		HostArn:          h.HostArn,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		StatusMessage:    h.StatusMessage,
	}
}

func (h *Handler) handleGetHost(
	_ context.Context,
	in *getHostInput,
) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	host, err := h.Backend.GetHost(in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{hostToView(host)}, nil
}

type listHostsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listHostsOutput struct {
	Hosts []hostView `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	_ context.Context,
	_ *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts()

	views := make([]hostView, 0, len(hosts))
	for _, host := range hosts {
		views = append(views, hostToView(host))
	}

	return &listHostsOutput{Hosts: views}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

type deleteHostOutput struct{}

func (h *Handler) handleDeleteHost(
	_ context.Context,
	in *deleteHostInput,
) (*deleteHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHost(in.HostArn); err != nil {
		return nil, err
	}

	return &deleteHostOutput{}, nil
}

type updateHostInput struct {
	HostArn          string `json:"HostArn"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
}

type updateHostOutput struct{}

func (h *Handler) handleUpdateHost(
	_ context.Context,
	in *updateHostInput,
) (*updateHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateHost(in.HostArn, in.ProviderEndpoint); err != nil {
		return nil, err
	}

	return &updateHostOutput{}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToArray(tags)}, nil
}

type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagMap); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}
