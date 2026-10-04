package ecrpublic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ecrPublicTargetPrefix is the X-Amz-Target service prefix, confirmed against
// aws-sdk-go-v2/service/ecrpublic@v1.47.1 api_client.go:
// options.Protocol = awsjson.New11(schemas.SpencerFrontendService).
const ecrPublicTargetPrefix = "SpencerFrontendService."

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the HTTP handler for the Amazon ECR Public control-plane API.
type Handler struct {
	Backend       Backend
	ops           map[string]service.JSONOpFunc
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Amazon ECR Public handler.
func NewHandler(backend Backend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "ECRPublic" }

// GetSupportedOperations returns the list of supported operations, matching
// aws-sdk-go-v2/service/ecrpublic@v1.47.1's client method set exactly.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.ops))
	for op := range h.ops {
		ops = append(ops, op)
	}

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ecrpublic" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles. Amazon ECR Public is
// us-east-1-only, matching the real service.
func (h *Handler) ChaosRegions() []string { return []string{"us-east-1"} }

// RouteMatcher matches requests carrying the SpencerFrontendService X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), ecrPublicTargetPrefix)
	}
}

// MatchPriority returns the routing priority for header-exact matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the action name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, ecrPublicTargetPrefix)
}

// ExtractResource extracts the repository name from the request body, when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		RepositoryName string `json:"repositoryName"`
	}

	_ = json.Unmarshal(body, &req)

	return req.RepositoryName
}

// Handler returns the Echo handler function for Amazon ECR Public requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ECRPublic", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// registryIDOrDefault returns registryID if set, else the backend's own account ID.
func (h *Handler) registryIDOrDefault(registryID string) string {
	if registryID != "" {
		return registryID
	}

	return h.Backend.AccountID()
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := make(map[string]service.JSONOpFunc)

	maps.Copy(ops, h.buildRepositoryOps())
	maps.Copy(ops, h.buildCatalogOps())
	maps.Copy(ops, h.buildPolicyOps())
	maps.Copy(ops, h.buildTagOps())
	maps.Copy(ops, h.buildAuthOps())
	maps.Copy(ops, h.buildImageOps())
	maps.Copy(ops, h.buildLayerOps())

	return ops
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
	status, errType := classifyError(err)

	return c.JSON(status, map[string]string{"__type": errType, "message": err.Error()})
}

// classifyError maps a backend/dispatch error to its HTTP status and AWS
// exception name. Every entry's status/name was confirmed against this
// service's response_snapshot/*.error.snap fixtures.
func classifyError(err error) (int, string) {
	singleErrStatus := []struct {
		err     error
		errType string
	}{
		{ErrRepositoryNotFound, "RepositoryNotFoundException"},
		{ErrRepositoryAlreadyExists, "RepositoryAlreadyExistsException"},
		{ErrRepositoryNotEmpty, "RepositoryNotEmptyException"},
		{ErrRepositoryPolicyNotFound, "RepositoryPolicyNotFoundException"},
		{ErrRegistryNotFound, "RegistryNotFoundException"},
		{ErrTooManyTags, "TooManyTagsException"},
		{ErrInvalidTagParameter, "InvalidTagParameterException"},
		{ErrUploadNotFound, "UploadNotFoundException"},
		{ErrEmptyUpload, "EmptyUploadException"},
		{ErrLayerPartTooSmall, "LayerPartTooSmallException"},
		{ErrInvalidLayerPart, "InvalidLayerPartException"},
		{ErrInvalidLayer, "InvalidLayerException"},
		{ErrLayerAlreadyExists, "LayerAlreadyExistsException"},
		{ErrLayersNotFound, "LayersNotFoundException"},
		{ErrImageNotFound, "ImageNotFoundException"},
		{ErrImageAlreadyExists, "ImageAlreadyExistsException"},
		{ErrImageDigestDoesNotMatch, "ImageDigestDoesNotMatchException"},
		{ErrImageTagAlreadyExists, "ImageTagAlreadyExistsException"},
		{ErrInvalidParameter, "InvalidParameterException"},
	}

	for _, e := range singleErrStatus {
		if errors.Is(err, e.err) {
			return http.StatusBadRequest, e.errType
		}
	}

	var syntaxErr *json.SyntaxError

	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, errUnknownAction):
		return http.StatusBadRequest, "UnknownOperationException"
	case errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return http.StatusBadRequest, "InvalidParameterException"
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusBadRequest, "RepositoryNotFoundException"
	default:
		return http.StatusInternalServerError, "ServerException"
	}
}
