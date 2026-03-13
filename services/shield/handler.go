package shield

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
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	shieldService       = "shield"
	shieldTargetPrefix  = "AWSShield_20160616."
	shieldMatchPriority = service.PriorityHeaderExact
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS Shield Advanced API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Shield handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Shield" }

// GetSupportedOperations returns the list of supported Shield operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateSubscription",
		"DescribeSubscription",
		"GetSubscriptionState",
		"CreateProtection",
		"DescribeProtection",
		"DeleteProtection",
		"ListProtections",
		"TagResource",
		"ListTagsForResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return shieldService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Shield API requests.
// Requests are identified by the X-Amz-Target header prefix "AWSShield_20160616.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), shieldTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return shieldMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, shieldTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for Shield requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "shield: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte("{}"))
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	switch op {
	case "CreateSubscription":
		return h.handleCreateSubscription(ctx)
	case "DescribeSubscription":
		return h.handleDescribeSubscription()
	case "GetSubscriptionState":
		return h.handleGetSubscriptionState()
	case "CreateProtection":
		return h.handleCreateProtection(ctx, body)
	case "DescribeProtection":
		return h.handleDescribeProtection(body)
	case "DeleteProtection":
		return h.handleDeleteProtection(ctx, body)
	case "ListProtections":
		return h.handleListProtections()
	case "TagResource":
		return h.handleTagResource(body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceAlreadyExistsException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InvalidParameterException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InternalErrorException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

func (h *Handler) handleCreateSubscription(ctx context.Context) ([]byte, error) {
	if err := h.Backend.CreateSubscription(); err != nil {
		// Shield returns empty body on success; ignore "already exists" per AWS behavior
		if errors.Is(err, awserr.ErrConflict) {
			return nil, nil
		}

		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: created subscription")

	return nil, nil
}

func (h *Handler) handleDescribeSubscription() ([]byte, error) {
	sub, err := h.Backend.DescribeSubscription()
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Subscription": map[string]any{
			"StartTime":            sub.StartTime.Unix(),
			"EndTime":              sub.EndTime.Unix(),
			"AutoRenew":            sub.AutoRenew,
			"TimeCommitmentInDays": sub.TimeCommitmentInDays,
		},
	})
}

func (h *Handler) handleGetSubscriptionState() ([]byte, error) {
	state := h.Backend.GetSubscriptionState()

	return json.Marshal(map[string]string{
		"SubscriptionState": state,
	})
}

// createProtectionRequest is the request body for CreateProtection.
type createProtectionRequest struct {
	Name        string    `json:"Name"`
	ResourceArn string    `json:"ResourceArn"`
	Tags        []tagItem `json:"Tags"`
}

// tagItem represents a key/value pair for the Tags field in Shield API requests.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsFromItems(items []tagItem) map[string]string {
	m := make(map[string]string, len(items))

	for _, t := range items {
		m[t.Key] = t.Value
	}

	return m
}

func tagsToItems(tags map[string]string) []tagItem {
	items := make([]tagItem, 0, len(tags))

	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items
}

func (h *Handler) handleCreateProtection(ctx context.Context, body []byte) ([]byte, error) {
	var req createProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := tagsFromItems(req.Tags)

	p, err := h.Backend.CreateProtection(req.Name, req.ResourceArn, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: created protection", "name", p.Name, "id", p.ID)

	return json.Marshal(map[string]string{
		"ProtectionId": p.ID,
	})
}

// describeProtectionRequest is the request body for DescribeProtection.
type describeProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
	ResourceArn  string `json:"ResourceArn"`
}

func (h *Handler) handleDescribeProtection(body []byte) ([]byte, error) {
	var req describeProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" && req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ProtectionId or ResourceArn is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProtection(req.ProtectionID, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Protection": protectionToMap(p),
	})
}

// deleteProtectionRequest is the request body for DeleteProtection.
type deleteProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
}

func (h *Handler) handleDeleteProtection(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return nil, fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProtection(req.ProtectionID); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: deleted protection", "id", req.ProtectionID)

	return nil, nil
}

func (h *Handler) handleListProtections() ([]byte, error) {
	protections := h.Backend.ListProtections()
	items := make([]map[string]any, 0, len(protections))

	for _, p := range protections {
		items = append(items, protectionToMap(p))
	}

	return json.Marshal(map[string]any{
		"Protections": items,
	})
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagItem `json:"Tags"`
}

func (h *Handler) handleTagResource(body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(req.ResourceARN, tagsFromItems(req.Tags)); err != nil {
		return nil, err
	}

	return nil, nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tagsToItems(tags),
	})
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

func protectionToMap(p *Protection) map[string]any {
	return map[string]any{
		"Id":          p.ID,
		"Name":        p.Name,
		"ResourceArn": p.ResourceARN,
	}
}
