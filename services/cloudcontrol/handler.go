package cloudcontrol

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
	cloudControlTargetPrefix = "CloudApiService."
	cloudControlContentType  = "application/x-amz-json-1.0"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CloudControl API operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CloudControl handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudControl" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateResource",
		"DeleteResource",
		"GetResource",
		"ListResources",
		"UpdateResource",
		"GetResourceRequestStatus",
		"CancelResourceRequest",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cloudcontrol" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CloudControl requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), cloudControlTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CloudControl action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, cloudControlTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request (not used for CloudControl).
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CloudControl requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CloudControl", cloudControlContentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateResource":           service.WrapOp(h.handleCreateResource),
		"DeleteResource":           service.WrapOp(h.handleDeleteResource),
		"GetResource":              service.WrapOp(h.handleGetResource),
		"ListResources":            service.WrapOp(h.handleListResources),
		"UpdateResource":           service.WrapOp(h.handleUpdateResource),
		"GetResourceRequestStatus": service.WrapOp(h.handleGetResourceRequestStatus),
		"CancelResourceRequest":    service.WrapOp(h.handleCancelResourceRequest),
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

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "AlreadyExistsException",
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

// --- CreateResource ---

type createResourceInput struct {
	TypeName     string `json:"TypeName"`
	DesiredState string `json:"DesiredState"`
}

type createResourceOutput struct {
	ProgressEvent *ProgressEvent `json:"ProgressEvent"`
}

func (h *Handler) handleCreateResource(
	_ context.Context,
	in *createResourceInput,
) (*createResourceOutput, error) {
	if in.TypeName == "" {
		return nil, fmt.Errorf("%w: TypeName is required", errInvalidRequest)
	}

	event, err := h.Backend.CreateResource(in.TypeName, in.DesiredState)
	if err != nil {
		return nil, err
	}

	return &createResourceOutput{ProgressEvent: event}, nil
}

// --- DeleteResource ---

type deleteResourceInput struct {
	TypeName   string `json:"TypeName"`
	Identifier string `json:"Identifier"`
}

type deleteResourceOutput struct {
	ProgressEvent *ProgressEvent `json:"ProgressEvent"`
}

func (h *Handler) handleDeleteResource(
	_ context.Context,
	in *deleteResourceInput,
) (*deleteResourceOutput, error) {
	if in.TypeName == "" {
		return nil, fmt.Errorf("%w: TypeName is required", errInvalidRequest)
	}

	if in.Identifier == "" {
		return nil, fmt.Errorf("%w: Identifier is required", errInvalidRequest)
	}

	event, err := h.Backend.DeleteResource(in.TypeName, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &deleteResourceOutput{ProgressEvent: event}, nil
}

// --- GetResource ---

type getResourceInput struct {
	TypeName   string `json:"TypeName"`
	Identifier string `json:"Identifier"`
}

type resourceDescription struct {
	Identifier string `json:"Identifier"`
	Properties string `json:"Properties"`
}

type getResourceOutput struct {
	ResourceDescription *resourceDescription `json:"ResourceDescription"`
	TypeName            string               `json:"TypeName"`
}

func (h *Handler) handleGetResource(
	_ context.Context,
	in *getResourceInput,
) (*getResourceOutput, error) {
	if in.TypeName == "" {
		return nil, fmt.Errorf("%w: TypeName is required", errInvalidRequest)
	}

	if in.Identifier == "" {
		return nil, fmt.Errorf("%w: Identifier is required", errInvalidRequest)
	}

	r, err := h.Backend.GetResource(in.TypeName, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &getResourceOutput{
		TypeName: in.TypeName,
		ResourceDescription: &resourceDescription{
			Identifier: r.Identifier,
			Properties: r.Properties,
		},
	}, nil
}

// --- ListResources ---

type listResourcesInput struct {
	TypeName string `json:"TypeName"`
}

type listResourcesOutput struct {
	TypeName             string                 `json:"TypeName"`
	ResourceDescriptions []*resourceDescription `json:"ResourceDescriptions"`
}

func (h *Handler) handleListResources(
	_ context.Context,
	in *listResourcesInput,
) (*listResourcesOutput, error) {
	if in.TypeName == "" {
		return nil, fmt.Errorf("%w: TypeName is required", errInvalidRequest)
	}

	resources := h.Backend.ListResources(in.TypeName)
	descs := make([]*resourceDescription, 0, len(resources))

	for _, r := range resources {
		descs = append(descs, &resourceDescription{
			Identifier: r.Identifier,
			Properties: r.Properties,
		})
	}

	return &listResourcesOutput{
		TypeName:             in.TypeName,
		ResourceDescriptions: descs,
	}, nil
}

// --- UpdateResource ---

type updateResourceInput struct {
	TypeName      string `json:"TypeName"`
	Identifier    string `json:"Identifier"`
	PatchDocument string `json:"PatchDocument"`
}

type updateResourceOutput struct {
	ProgressEvent *ProgressEvent `json:"ProgressEvent"`
}

func (h *Handler) handleUpdateResource(
	_ context.Context,
	in *updateResourceInput,
) (*updateResourceOutput, error) {
	if in.TypeName == "" {
		return nil, fmt.Errorf("%w: TypeName is required", errInvalidRequest)
	}

	if in.Identifier == "" {
		return nil, fmt.Errorf("%w: Identifier is required", errInvalidRequest)
	}

	event, err := h.Backend.UpdateResource(in.TypeName, in.Identifier, in.PatchDocument)
	if err != nil {
		return nil, err
	}

	return &updateResourceOutput{ProgressEvent: event}, nil
}

// --- GetResourceRequestStatus ---

type getResourceRequestStatusInput struct {
	RequestToken string `json:"RequestToken"`
}

type getResourceRequestStatusOutput struct {
	ProgressEvent *ProgressEvent `json:"ProgressEvent"`
}

func (h *Handler) handleGetResourceRequestStatus(
	_ context.Context,
	in *getResourceRequestStatusInput,
) (*getResourceRequestStatusOutput, error) {
	if in.RequestToken == "" {
		return nil, fmt.Errorf("%w: RequestToken is required", errInvalidRequest)
	}

	event, err := h.Backend.GetResourceRequestStatus(in.RequestToken)
	if err != nil {
		return nil, err
	}

	return &getResourceRequestStatusOutput{ProgressEvent: event}, nil
}

// --- CancelResourceRequest ---

type cancelResourceRequestInput struct {
	RequestToken string `json:"RequestToken"`
}

type cancelResourceRequestOutput struct {
	ProgressEvent *ProgressEvent `json:"ProgressEvent"`
}

func (h *Handler) handleCancelResourceRequest(
	_ context.Context,
	in *cancelResourceRequestInput,
) (*cancelResourceRequestOutput, error) {
	if in.RequestToken == "" {
		return nil, fmt.Errorf("%w: RequestToken is required", errInvalidRequest)
	}

	event, err := h.Backend.CancelResourceRequest(in.RequestToken)
	if err != nil {
		return nil, err
	}

	return &cancelResourceRequestOutput{ProgressEvent: event}, nil
}
