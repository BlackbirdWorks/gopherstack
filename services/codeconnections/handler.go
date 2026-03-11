package codeconnections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codeconnectionsMatchPriority = service.PriorityHeaderExact
	ccTargetPrefix               = "CodeConnections_20231201."
	ccContentType                = "application/x-amz-json-1.0"
)

// Handler is the Echo HTTP handler for AWS CodeConnections operations (JSON 1.0 protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeConnections handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeConnections" }

// GetSupportedOperations returns the list of supported CodeConnections operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateConnection",
		"GetConnection",
		"ListConnections",
		"DeleteConnection",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codeconnections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeConnections instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeConnections JSON 1.0 requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), ccTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return codeconnectionsMatchPriority }

// ExtractOperation extracts the CodeConnections operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, ccTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the JSON request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ConnectionArn string `json:"ConnectionArn"`
		ResourceArn   string `json:"ResourceArn"`
	}

	_ = json.Unmarshal(body, &req)

	if req.ConnectionArn != "" {
		return req.ConnectionArn
	}

	return req.ResourceArn
}

// Handler returns the Echo handler function for CodeConnections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeConnections", ccContentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleEchoError,
		)
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	table := map[string]service.JSONOpFunc{
		"CreateConnection":    service.WrapOp(h.handleCreateConnection),
		"GetConnection":       service.WrapOp(h.handleGetConnection),
		"ListConnections":     service.WrapOp(h.handleListConnections),
		"DeleteConnection":    service.WrapOp(h.handleDeleteConnection),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
	}

	fn, ok := table[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

var (
	errUnknownAction = errors.New("UnknownOperationException")
	errValidation    = errors.New("ValidationException")
)

func (h *Handler) handleEchoError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, statusCode := resolveErrorType(err)

	return c.JSON(statusCode, service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
}

func resolveErrorType(err error) (string, int) {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, errValidation):
		return "ValidationException", http.StatusBadRequest
	case errors.Is(err, errUnknownAction):
		return "UnknownOperationException", http.StatusBadRequest
	default:
		return "InternalFailure", http.StatusInternalServerError
	}
}

// tag is the JSON representation of a CodeConnections tag (array format).
type tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsToArray(m map[string]string) []tag {
	out := make([]tag, 0, len(m))
	for k, v := range m {
		out = append(out, tag{Key: k, Value: v})
	}

	return out
}

func tagsFromArray(tags []tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// --- Connection handlers ---

type createConnectionInput struct {
	ConnectionName string `json:"ConnectionName"`
	ProviderType   string `json:"ProviderType"`
	Tags           []tag  `json:"Tags"`
}

type createConnectionOutput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errValidation)
	}

	conn, err := h.Backend.CreateConnection(in.ConnectionName, in.ProviderType, tagsFromArray(in.Tags))
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{ConnectionArn: conn.ConnectionArn}, nil
}

type getConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type connectionItem struct {
	ConnectionName   string `json:"ConnectionName"`
	ConnectionArn    string `json:"ConnectionArn"`
	ProviderType     string `json:"ProviderType"`
	ConnectionStatus string `json:"ConnectionStatus"`
	OwnerAccountID   string `json:"OwnerAccountId"`
}

type getConnectionOutput struct {
	Connection connectionItem `json:"Connection"`
}

func (h *Handler) handleGetConnection(_ context.Context, in *getConnectionInput) (*getConnectionOutput, error) {
	conn, err := h.Backend.GetConnection(in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connectionItem{
		ConnectionName:   conn.ConnectionName,
		ConnectionArn:    conn.ConnectionArn,
		ProviderType:     conn.ProviderType,
		ConnectionStatus: conn.Status,
		OwnerAccountID:   conn.OwnerAccountID,
	}}, nil
}

type listConnectionsInput struct {
	ProviderTypeFilter string `json:"ProviderTypeFilter"`
}

type listConnectionsOutput struct {
	Connections []connectionItem `json:"Connections"`
}

func (h *Handler) handleListConnections(_ context.Context, in *listConnectionsInput) (*listConnectionsOutput, error) {
	conns := h.Backend.ListConnections(in.ProviderTypeFilter)

	items := make([]connectionItem, 0, len(conns))
	for _, conn := range conns {
		items = append(items, connectionItem{
			ConnectionName:   conn.ConnectionName,
			ConnectionArn:    conn.ConnectionArn,
			ProviderType:     conn.ProviderType,
			ConnectionStatus: conn.Status,
			OwnerAccountID:   conn.OwnerAccountID,
		})
	}

	return &listConnectionsOutput{Connections: items}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type emptyOutput struct{}

func (h *Handler) handleDeleteConnection(_ context.Context, in *deleteConnectionInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteConnection(in.ConnectionArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Tag handlers ---

type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []tag  `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToArray(tags)}, nil
}
