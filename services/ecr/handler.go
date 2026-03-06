package ecr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	v2PathPrefix      = "/v2"
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
	}
}

// RouteMatcher returns a function that matches ECR requests.
// It matches on:
//   - X-Amz-Target header with AmazonEC2ContainerRegistry_V20150921. prefix (control plane)
//   - /v2/ path prefix (Docker registry v2 API, when local registry is enabled)
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if strings.HasPrefix(target, ecrTargetPrefix) {
			return true
		}

		if h.registryEnabled && strings.HasPrefix(c.Request().URL.Path, v2PathPrefix) {
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
		if h.registryEnabled && strings.HasPrefix(c.Request().URL.Path, v2PathPrefix) {
			return "RegistryV2"
		}

		return unknownActionName
	}

	return action
}

// ExtractResource extracts the repository name from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
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
		// Docker registry v2 requests are proxied to the embedded registry.
		if h.registryEnabled && strings.HasPrefix(c.Request().URL.Path, v2PathPrefix) {
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
	case errors.Is(err, ErrInvalidRepositoryName), errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
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
type repositoryView struct {
	CreatedAt      *time.Time `json:"createdAt,omitempty"`
	RegistryID     string     `json:"registryId"`
	RepositoryARN  string     `json:"repositoryArn"`
	RepositoryName string     `json:"repositoryName"`
	RepositoryURI  string     `json:"repositoryUri"`
}

func toRepositoryView(r Repository) repositoryView {
	t := r.CreatedAt

	return repositoryView{
		CreatedAt:      &t,
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
