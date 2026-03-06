package ecr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ecrTargetPrefix   = "AmazonEC2ContainerRegistry_V20150921."
	dummyPassword     = "dummy-password"
	dummyUser         = "AWS"
	tokenTTL          = 12 * time.Hour
	v2Root            = "/v2"
	v2Prefix          = "/v2/"
	unknownActionName = "Unknown"
)

var (
	errUnknownAction  = errors.New("UnknownOperationException")
	errInvalidRequest = errors.New("InvalidParameterException")
)

// Handler is the Echo HTTP handler for ECR operations.
type Handler struct {
	Backend         Backend
	registryHandler http.Handler
	setEndpointOnce sync.Once
	registryEnabled bool
}

// NewHandler creates a new ECR handler.
// registryHandler may be nil when the local registry is disabled.
func NewHandler(backend Backend, registryHandler http.Handler) *Handler {
	return &Handler{
		Backend:         backend,
		registryHandler: registryHandler,
		registryEnabled: registryHandler != nil,
	}
}

// RegistryEnabled returns true if the embedded Docker registry is enabled.
func (h *Handler) RegistryEnabled() bool { return h.registryEnabled }

// Name returns the service name.
func (h *Handler) Name() string { return "ECR" }

// GetSupportedOperations returns the list of supported ECR operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateRepository",
		"DescribeRepositories",
		"DeleteRepository",
		"GetAuthorizationToken",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// isRegistryPath returns true when path is exactly "/v2" or starts with "/v2/".
// This prevents false matches against unrelated paths like "/v20180820/..." (S3Control).
func isRegistryPath(path string) bool {
	return path == v2Root || strings.HasPrefix(path, v2Prefix)
}

// RouteMatcher returns a function that matches ECR requests.
// It matches on:
//   - X-Amz-Target header with AmazonEC2ContainerRegistry_V20150921. prefix (control plane)
//   - /v2 or /v2/ path prefix (Docker registry v2 API, when local registry is enabled)
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if strings.HasPrefix(target, ecrTargetPrefix) {
			return true
		}

		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			return true
		}

		return false
	}
}

// MatchPriority returns the routing priority for ECR.
// Control plane uses header-exact priority; registry uses path-based priority
// elevated above catch-alls.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the ECR action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, ecrTargetPrefix)

	if action == "" || action == target {
		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			return "RegistryV2"
		}

		return unknownActionName
	}

	return action
}

// ExtractResource extracts the repository name from the request.
// For registry v2 paths (/v2/<name>/...) the name is taken from the URL to
// avoid buffering potentially large binary upload bodies.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	if h.registryEnabled && isRegistryPath(path) {
		// /v2 alone (root ping) has no repository component.
		if path == v2Root {
			return ""
		}

		// Extract name from /v2/<name>/...
		trimmed := strings.TrimPrefix(path, v2Prefix)
		name, _, _ := strings.Cut(trimmed, "/")

		return name
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		RepositoryName  string   `json:"repositoryName"`
		RepositoryNames []string `json:"repositoryNames"`
	}

	_ = json.Unmarshal(body, &req)

	if req.RepositoryName != "" {
		return req.RepositoryName
	}

	if len(req.RepositoryNames) > 0 {
		return req.RepositoryNames[0]
	}

	return ""
}

// Handler returns the Echo handler function for ECR requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Lazily set the proxy endpoint from the first request's Host header so
		// that repository URIs and authorization tokens reflect the local server
		// address rather than a default AWS-style endpoint.
		h.setEndpointOnce.Do(func() {
			if h.Backend.ProxyEndpoint() == "" {
				if host := c.Request().Host; host != "" {
					h.Backend.SetEndpoint(host)
				}
			}
		})

		// Docker registry v2 requests are proxied to the embedded registry.
		if h.registryEnabled && isRegistryPath(c.Request().URL.Path) {
			h.registryHandler.ServeHTTP(c.Response(), c.Request())

			return nil
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ECR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateRepository":      service.WrapOp(h.handleCreateRepository),
		"DescribeRepositories":  service.WrapOp(h.handleDescribeRepositories),
		"DeleteRepository":      service.WrapOp(h.handleDeleteRepository),
		"GetAuthorizationToken": service.WrapOp(h.handleGetAuthorizationToken),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
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
		return c.JSON(
			http.StatusNotFound,
			map[string]string{"__type": "RepositoryNotFoundException", "message": err.Error()},
		)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "RepositoryAlreadyExistsException", "message": err.Error()},
		)
	case errors.Is(err, errUnknownAction):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "UnknownOperationException", "message": err.Error()},
		)
	case errors.Is(err, ErrInvalidRepositoryName), errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "InvalidParameterException", "message": err.Error()},
		)
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{"__type": "InternalServerError", "message": err.Error()},
		)
	}
}

// repositoryView is the JSON representation of a repository.
// createdAt is serialised as a Unix epoch float64 (seconds) so that the AWS
// SDK v2 deserialiser, which expects a JSON Number for timestamp fields, can
// decode it correctly.
type repositoryView struct {
	RegistryID     string  `json:"registryId"`
	RepositoryARN  string  `json:"repositoryArn"`
	RepositoryName string  `json:"repositoryName"`
	RepositoryURI  string  `json:"repositoryUri"`
	CreatedAt      float64 `json:"createdAt"`
}

func toRepositoryView(r Repository) repositoryView {
	return repositoryView{
		CreatedAt:      float64(r.CreatedAt.Unix()),
		RegistryID:     r.RegistryID,
		RepositoryARN:  r.RepositoryARN,
		RepositoryName: r.RepositoryName,
		RepositoryURI:  r.RepositoryURI,
	}
}

// createRepositoryInput is the request body for CreateRepository.
type createRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type createRepositoryOutput struct {
	Repository repositoryView `json:"repository"`
}

func (h *Handler) handleCreateRepository(
	_ context.Context,
	in *createRepositoryInput,
) (*createRepositoryOutput, error) {
	repo, err := h.Backend.CreateRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &createRepositoryOutput{Repository: toRepositoryView(*repo)}, nil
}

// describeRepositoriesInput is the request body for DescribeRepositories.
type describeRepositoriesInput struct {
	RepositoryNames []string `json:"repositoryNames"`
}

type describeRepositoriesOutput struct {
	Repositories []repositoryView `json:"repositories"`
}

func (h *Handler) handleDescribeRepositories(
	_ context.Context,
	in *describeRepositoriesInput,
) (*describeRepositoriesOutput, error) {
	repos, err := h.Backend.DescribeRepositories(in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	views := make([]repositoryView, 0, len(repos))
	for _, r := range repos {
		views = append(views, toRepositoryView(r))
	}

	return &describeRepositoriesOutput{Repositories: views}, nil
}

// deleteRepositoryInput is the request body for DeleteRepository.
type deleteRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type deleteRepositoryOutput struct {
	Repository repositoryView `json:"repository"`
}

func (h *Handler) handleDeleteRepository(
	_ context.Context,
	in *deleteRepositoryInput,
) (*deleteRepositoryOutput, error) {
	repo, err := h.Backend.DeleteRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &deleteRepositoryOutput{Repository: toRepositoryView(*repo)}, nil
}

// getAuthorizationTokenInput is the (empty) request body for GetAuthorizationToken.
type getAuthorizationTokenInput struct{}

type authorizationDataView struct {
	AuthorizationToken string `json:"authorizationToken"`
	ProxyEndpoint      string `json:"proxyEndpoint,omitempty"`
	ExpiresAt          int64  `json:"expiresAt"`
}

type getAuthorizationTokenOutput struct {
	AuthorizationData []authorizationDataView `json:"authorizationData"`
}

func (h *Handler) handleGetAuthorizationToken(
	_ context.Context,
	_ *getAuthorizationTokenInput,
) (*getAuthorizationTokenOutput, error) {
	token := base64.StdEncoding.EncodeToString([]byte(dummyUser + ":" + dummyPassword))
	expiresAt := time.Now().Add(tokenTTL).Unix()

	proxyEndpoint := h.Backend.ProxyEndpoint()

	return &getAuthorizationTokenOutput{
		AuthorizationData: []authorizationDataView{
			{
				AuthorizationToken: token,
				ExpiresAt:          expiresAt,
				ProxyEndpoint:      proxyEndpoint,
			},
		},
	}, nil
}

// listTagsForResourceInput is the request body for ListTagsForResource.
type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

// tagView is a key-value tag pair.
type tagView struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsForResourceOutput struct {
	Tags []tagView `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	_ *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	return &listTagsForResourceOutput{Tags: []tagView{}}, nil
}

// tagResourceInput is the request body for TagResource.
type tagResourceInput struct {
	ResourceArn string            `json:"resourceArn"`
	Tags        map[string]string `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	_ *tagResourceInput,
) (*tagResourceOutput, error) {
	return &tagResourceOutput{}, nil
}

// untagResourceInput is the request body for UntagResource.
type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	_ *untagResourceInput,
) (*untagResourceOutput, error) {
	return &untagResourceOutput{}, nil
}
