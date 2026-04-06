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

	"github.com/blackbirdworks/gopherstack/pkgs/config"
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
		"BatchCheckLayerAvailability",
		"BatchDeleteImage",
		"BatchGetImage",
		"BatchGetRepositoryScanningConfiguration",
		"CompleteLayerUpload",
		"CreatePullThroughCacheRule",
		"CreateRepository",
		"CreateRepositoryCreationTemplate",
		"DeleteLifecyclePolicy",
		"DeletePullThroughCacheRule",
		"DeleteRegistryPolicy",
		"DeleteRepository",
		"DescribeRepositories",
		"GetAuthorizationToken",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ecr" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ECR instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

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
		"BatchCheckLayerAvailability":             service.WrapOp(h.handleBatchCheckLayerAvailability),
		"BatchDeleteImage":                        service.WrapOp(h.handleBatchDeleteImage),
		"BatchGetImage":                           service.WrapOp(h.handleBatchGetImage),
		"BatchGetRepositoryScanningConfiguration": service.WrapOp(h.handleBatchGetRepositoryScanningConfiguration),
		"CompleteLayerUpload":                     service.WrapOp(h.handleCompleteLayerUpload),
		"CreatePullThroughCacheRule":              service.WrapOp(h.handleCreatePullThroughCacheRule),
		"CreateRepository":                        service.WrapOp(h.handleCreateRepository),
		"CreateRepositoryCreationTemplate":        service.WrapOp(h.handleCreateRepositoryCreationTemplate),
		"DeleteLifecyclePolicy":                   service.WrapOp(h.handleDeleteLifecyclePolicy),
		"DeletePullThroughCacheRule":              service.WrapOp(h.handleDeletePullThroughCacheRule),
		"DeleteRegistryPolicy":                    service.WrapOp(h.handleDeleteRegistryPolicy),
		"DeleteRepository":                        service.WrapOp(h.handleDeleteRepository),
		"DescribeRepositories":                    service.WrapOp(h.handleDescribeRepositories),
		"GetAuthorizationToken":                   service.WrapOp(h.handleGetAuthorizationToken),
		"ListTagsForResource":                     service.WrapOp(h.handleListTagsForResource),
		"TagResource":                             service.WrapOp(h.handleTagResource),
		"UntagResource":                           service.WrapOp(h.handleUntagResource),
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
	case errors.Is(err, ErrRepositoryNotFound):
		return c.JSON(
			http.StatusNotFound,
			map[string]string{"__type": "RepositoryNotFoundException", "message": err.Error()},
		)
	case errors.Is(err, ErrPullThroughCacheRuleNotFound),
		errors.Is(err, ErrLifecyclePolicyNotFound),
		errors.Is(err, ErrRepositoryCreationTemplateNotFound),
		errors.Is(err, ErrRegistryPolicyNotFound):
		return c.JSON(
			http.StatusNotFound,
			map[string]string{"__type": "NotFoundException", "message": err.Error()},
		)
	case errors.Is(err, ErrRepositoryAlreadyExists):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "RepositoryAlreadyExistsException", "message": err.Error()},
		)
	case errors.Is(err, ErrPullThroughCacheRuleAlreadyExists):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{"__type": "PullThroughCacheRuleAlreadyExistsException", "message": err.Error()},
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
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
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

// batchCheckLayerAvailabilityInput is the request body for BatchCheckLayerAvailability.
type batchCheckLayerAvailabilityInput struct {
	RepositoryName string   `json:"repositoryName"`
	RegistryID     string   `json:"registryId,omitempty"`
	LayerDigests   []string `json:"layerDigests"`
}

type batchCheckLayerAvailabilityOutput struct {
	Layers   []LayerAvailability `json:"layers"`
	Failures []LayerFailure      `json:"failures"`
}

func (h *Handler) handleBatchCheckLayerAvailability(
	_ context.Context,
	in *batchCheckLayerAvailabilityInput,
) (*batchCheckLayerAvailabilityOutput, error) {
	layers, failures, err := h.Backend.BatchCheckLayerAvailability(in.RepositoryName, in.LayerDigests)
	if err != nil {
		return nil, err
	}

	if layers == nil {
		layers = []LayerAvailability{}
	}

	if failures == nil {
		failures = []LayerFailure{}
	}

	return &batchCheckLayerAvailabilityOutput{Layers: layers, Failures: failures}, nil
}

// batchDeleteImageInput is the request body for BatchDeleteImage.
type batchDeleteImageInput struct {
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	ImageIDs       []ImageIdentifier `json:"imageIds"`
}

type batchDeleteImageOutput struct {
	ImageIDs []ImageIdentifier `json:"imageIds"`
	Failures []ImageFailure    `json:"failures"`
}

func (h *Handler) handleBatchDeleteImage(
	_ context.Context,
	in *batchDeleteImageInput,
) (*batchDeleteImageOutput, error) {
	deleted, failures, err := h.Backend.BatchDeleteImage(in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	if deleted == nil {
		deleted = []ImageIdentifier{}
	}

	if failures == nil {
		failures = []ImageFailure{}
	}

	return &batchDeleteImageOutput{ImageIDs: deleted, Failures: failures}, nil
}

// batchGetImageInput is the request body for BatchGetImage.
type batchGetImageInput struct {
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	ImageIDs       []ImageIdentifier `json:"imageIds"`
}

type batchGetImageOutput struct {
	Images   []Image        `json:"images"`
	Failures []ImageFailure `json:"failures"`
}

func (h *Handler) handleBatchGetImage(
	_ context.Context,
	in *batchGetImageInput,
) (*batchGetImageOutput, error) {
	imgs, failures, err := h.Backend.BatchGetImage(in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	if imgs == nil {
		imgs = []Image{}
	}

	if failures == nil {
		failures = []ImageFailure{}
	}

	return &batchGetImageOutput{Images: imgs, Failures: failures}, nil
}

// batchGetRepositoryScanningConfigurationInput is the request body for BatchGetRepositoryScanningConfiguration.
type batchGetRepositoryScanningConfigurationInput struct {
	RepositoryNames []string `json:"repositoryNames"`
}

type batchGetRepositoryScanningConfigurationOutput struct {
	ScanningConfigurations []RepositoryScanningConfiguration        `json:"scanningConfigurations"`
	Failures               []RepositoryScanningConfigurationFailure `json:"failures"`
}

func (h *Handler) handleBatchGetRepositoryScanningConfiguration(
	_ context.Context,
	in *batchGetRepositoryScanningConfigurationInput,
) (*batchGetRepositoryScanningConfigurationOutput, error) {
	configs, failures, err := h.Backend.BatchGetRepositoryScanningConfiguration(in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	if configs == nil {
		configs = []RepositoryScanningConfiguration{}
	}

	if failures == nil {
		failures = []RepositoryScanningConfigurationFailure{}
	}

	return &batchGetRepositoryScanningConfigurationOutput{
		ScanningConfigurations: configs,
		Failures:               failures,
	}, nil
}

// completeLayerUploadInput is the request body for CompleteLayerUpload.
type completeLayerUploadInput struct {
	RepositoryName string   `json:"repositoryName"`
	UploadID       string   `json:"uploadId"`
	RegistryID     string   `json:"registryId,omitempty"`
	LayerDigests   []string `json:"layerDigests"`
}

func (h *Handler) handleCompleteLayerUpload(
	_ context.Context,
	in *completeLayerUploadInput,
) (*CompleteLayerUploadResult, error) {
	result, err := h.Backend.CompleteLayerUpload(in.RepositoryName, in.UploadID, in.LayerDigests)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// createPullThroughCacheRuleInput is the request body for CreatePullThroughCacheRule.
type createPullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string `json:"upstreamRegistryUrl"`
	CredentialArn       string `json:"credentialArn,omitempty"`
	UpstreamRegistry    string `json:"upstreamRegistry,omitempty"`
}

type createPullThroughCacheRuleOutput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string `json:"upstreamRegistryUrl"`
	CredentialArn       string `json:"credentialArn,omitempty"`
	UpstreamRegistry    string `json:"upstreamRegistry,omitempty"`
	RegistryID          string `json:"registryId"`
	CreatedAt           int64  `json:"createdAt"`
}

func (h *Handler) handleCreatePullThroughCacheRule(
	_ context.Context,
	in *createPullThroughCacheRuleInput,
) (*createPullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.CreatePullThroughCacheRule(
		in.EcrRepositoryPrefix, in.UpstreamRegistryURL, in.CredentialArn, in.UpstreamRegistry,
	)
	if err != nil {
		return nil, err
	}

	return &createPullThroughCacheRuleOutput{
		EcrRepositoryPrefix: rule.EcrRepositoryPrefix,
		UpstreamRegistryURL: rule.UpstreamRegistryURL,
		CredentialArn:       rule.CredentialArn,
		UpstreamRegistry:    rule.UpstreamRegistry,
		RegistryID:          rule.RegistryID,
		CreatedAt:           rule.CreatedAt.Unix(),
	}, nil
}

// createRepositoryCreationTemplateInput is the request body for CreateRepositoryCreationTemplate.
type createRepositoryCreationTemplateInput struct {
	Prefix             string   `json:"prefix"`
	Description        string   `json:"description,omitempty"`
	ImageTagMutability string   `json:"imageTagMutability,omitempty"`
	RepositoryPolicy   string   `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy    string   `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn      string   `json:"customRoleArn,omitempty"`
	AppliedFor         []string `json:"appliedFor,omitempty"`
}

type createRepositoryCreationTemplateOutput struct {
	Template   *RepositoryCreationTemplate `json:"repositoryCreationTemplate"`
	RegistryID string                      `json:"registryId"`
}

func (h *Handler) handleCreateRepositoryCreationTemplate(
	_ context.Context,
	in *createRepositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	req := &RepositoryCreationTemplate{
		Prefix:             in.Prefix,
		Description:        in.Description,
		ImageTagMutability: in.ImageTagMutability,
		RepositoryPolicy:   in.RepositoryPolicy,
		LifecyclePolicy:    in.LifecyclePolicy,
		AppliedFor:         in.AppliedFor,
		CustomRoleArn:      in.CustomRoleArn,
	}

	tmpl, err := h.Backend.CreateRepositoryCreationTemplate(req)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{Template: tmpl}, nil
}

// deleteLifecyclePolicyInput is the request body for DeleteLifecyclePolicy.
type deleteLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleDeleteLifecyclePolicy(
	_ context.Context,
	in *deleteLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.DeleteLifecyclePolicy(in.RepositoryName)
}

// deletePullThroughCacheRuleInput is the request body for DeletePullThroughCacheRule.
type deletePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	RegistryID          string `json:"registryId,omitempty"`
}

type deletePullThroughCacheRuleOutput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL string `json:"upstreamRegistryUrl"`
	RegistryID          string `json:"registryId"`
	CreatedAt           int64  `json:"createdAt"`
}

func (h *Handler) handleDeletePullThroughCacheRule(
	_ context.Context,
	in *deletePullThroughCacheRuleInput,
) (*deletePullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.DeletePullThroughCacheRule(in.EcrRepositoryPrefix)
	if err != nil {
		return nil, err
	}

	return &deletePullThroughCacheRuleOutput{
		EcrRepositoryPrefix: rule.EcrRepositoryPrefix,
		UpstreamRegistryURL: rule.UpstreamRegistryURL,
		RegistryID:          rule.RegistryID,
		CreatedAt:           rule.CreatedAt.Unix(),
	}, nil
}

// deleteRegistryPolicyInput is the (empty) request body for DeleteRegistryPolicy.
type deleteRegistryPolicyInput struct{}

func (h *Handler) handleDeleteRegistryPolicy(
	_ context.Context,
	_ *deleteRegistryPolicyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.DeleteRegistryPolicy()
}
