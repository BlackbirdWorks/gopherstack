package ecr

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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
	keyMessageField = "message"
)

const (
	keyTypeField = "__type"
)

const (
	ecrTargetPrefix   = "AmazonEC2ContainerRegistry_V20150921."
	dummyUser         = "AWS"
	tokenTTL          = 12 * time.Hour
	v2Root            = "/v2"
	v2Prefix          = "/v2/"
	unknownActionName = "Unknown"
)

// regionContextKey is used to store the AWS region in request context.
type regionContextKey struct{}

var (
	errUnknownAction  = errors.New("UnknownOperationException")
	errInvalidRequest = errors.New("InvalidParameterException")
)

// Handler is the Echo HTTP handler for ECR operations.
type Handler struct {
	Backend         Backend
	ops             map[string]service.JSONOpFunc
	registryHandler http.Handler
	setEndpointOnce sync.Once
	registryEnabled bool
}

// NewHandler creates a new ECR handler.
// registryHandler may be nil when the local registry is disabled.
func NewHandler(backend Backend, registryHandler http.Handler) *Handler {
	h := &Handler{
		Backend:         backend,
		registryHandler: registryHandler,
		registryEnabled: registryHandler != nil,
	}
	h.ops = h.buildOps()

	return h
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
		"DeleteRepositoryCreationTemplate",
		"DeleteRepositoryPolicy",
		"DeleteSigningConfiguration",
		"DeregisterPullTimeUpdateExclusion",
		"DescribeImageReplicationStatus",
		"DescribeImageScanFindings",
		"DescribeImageSigningStatus",
		"DescribeImages",
		"DescribePullThroughCacheRules",
		"DescribeRegistry",
		"DescribeRepositories",
		"DescribeRepositoryCreationTemplates",
		"GetAccountSetting",
		"GetAuthorizationToken",
		"GetDownloadUrlForLayer",
		"GetLifecyclePolicy",
		"GetLifecyclePolicyPreview",
		"GetRegistryPolicy",
		"GetRegistryScanningConfiguration",
		"GetRepositoryPolicy",
		"GetSigningConfiguration",
		"InitiateLayerUpload",
		"ListImageReferrers",
		"ListImages",
		"ListPullTimeUpdateExclusions",
		"ListTagsForResource",
		"PutAccountSetting",
		"PutImage",
		"PutImageScanningConfiguration",
		"PutImageTagMutability",
		"PutLifecyclePolicy",
		"PutRegistryPolicy",
		"PutRegistryScanningConfiguration",
		"PutReplicationConfiguration",
		"PutSigningConfiguration",
		"RegisterPullTimeUpdateExclusion",
		"SetRepositoryPolicy",
		"StartImageScan",
		"StartLifecyclePolicyPreview",
		"TagResource",
		"UntagResource",
		"UpdateImageStorageClass",
		"UpdatePullThroughCacheRule",
		"UpdateRepositoryCreationTemplate",
		"UploadLayerPart",
		"ValidatePullThroughCacheRule",
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

		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ECR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// Reset clears the backend state and resets the endpoint lazy-init.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}

	h.setEndpointOnce = sync.Once{}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	ops := h.buildCoreOps()
	maps.Copy(ops, h.buildExtOps())

	return ops
}

func (h *Handler) buildCoreOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"BatchCheckLayerAvailability": service.WrapOp(
			h.handleBatchCheckLayerAvailability,
		),
		"BatchDeleteImage": service.WrapOp(h.handleBatchDeleteImage),
		"BatchGetImage":    service.WrapOp(h.handleBatchGetImage),
		"BatchGetRepositoryScanningConfiguration": service.WrapOp(
			h.handleBatchGetRepositoryScanningConfiguration,
		),
		"CompleteLayerUpload": service.WrapOp(h.handleCompleteLayerUpload),
		"CreatePullThroughCacheRule": service.WrapOp(
			h.handleCreatePullThroughCacheRule,
		),
		"CreateRepository": service.WrapOp(h.handleCreateRepository),
		"CreateRepositoryCreationTemplate": service.WrapOp(
			h.handleCreateRepositoryCreationTemplate,
		),
		"DeleteLifecyclePolicy": service.WrapOp(h.handleDeleteLifecyclePolicy),
		"DeletePullThroughCacheRule": service.WrapOp(
			h.handleDeletePullThroughCacheRule,
		),
		"DeleteRegistryPolicy": service.WrapOp(h.handleDeleteRegistryPolicy),
		"DeleteRepository":     service.WrapOp(h.handleDeleteRepository),
		"DeleteRepositoryCreationTemplate": service.WrapOp(
			h.handleDeleteRepositoryCreationTemplate,
		),
		"DeleteRepositoryPolicy": service.WrapOp(h.handleDeleteRepositoryPolicy),
		"DeleteSigningConfiguration": service.WrapOp(
			h.handleDeleteSigningConfiguration,
		),
		"DeregisterPullTimeUpdateExclusion": service.WrapOp(
			h.handleDeregisterPullTimeUpdateExclusion,
		),
		"DescribeImageReplicationStatus": service.WrapOp(
			h.handleDescribeImageReplicationStatus,
		),
		"DescribeImageScanFindings": service.WrapOp(
			h.handleDescribeImageScanFindings,
		),
		"DescribeImageSigningStatus": service.WrapOp(
			h.handleDescribeImageSigningStatus,
		),
		"DescribeImages": service.WrapOp(h.handleDescribeImages),
		"DescribePullThroughCacheRules": service.WrapOp(
			h.handleDescribePullThroughCacheRules,
		),
		"DescribeRegistry":     service.WrapOp(h.handleDescribeRegistry),
		"DescribeRepositories": service.WrapOp(h.handleDescribeRepositories),
		"DescribeRepositoryCreationTemplates": service.WrapOp(
			h.handleDescribeRepositoryCreationTemplates,
		),
	}
}

func (h *Handler) buildExtOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetAccountSetting":      service.WrapOp(h.handleGetAccountSetting),
		"GetAuthorizationToken":  service.WrapOp(h.handleGetAuthorizationToken),
		"GetDownloadUrlForLayer": service.WrapOp(h.handleGetDownloadURLForLayer),
		"GetLifecyclePolicy":     service.WrapOp(h.handleGetLifecyclePolicy),
		"GetLifecyclePolicyPreview": service.WrapOp(
			h.handleGetLifecyclePolicyPreview,
		),
		"GetRegistryPolicy": service.WrapOp(h.handleGetRegistryPolicy),
		"GetRegistryScanningConfiguration": service.WrapOp(
			h.handleGetRegistryScanningConfiguration,
		),
		"GetRepositoryPolicy":     service.WrapOp(h.handleGetRepositoryPolicy),
		"GetSigningConfiguration": service.WrapOp(h.handleGetSigningConfiguration),
		"InitiateLayerUpload":     service.WrapOp(h.handleInitiateLayerUpload),
		"ListImageReferrers":      service.WrapOp(h.handleListImageReferrers),
		"ListImages":              service.WrapOp(h.handleListImages),
		"ListPullTimeUpdateExclusions": service.WrapOp(
			h.handleListPullTimeUpdateExclusions,
		),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"PutAccountSetting":   service.WrapOp(h.handlePutAccountSetting),
		"PutImage":            service.WrapOp(h.handlePutImage),
		"PutImageScanningConfiguration": service.WrapOp(
			h.handlePutImageScanningConfiguration,
		),
		"PutImageTagMutability": service.WrapOp(h.handlePutImageTagMutability),
		"PutLifecyclePolicy":    service.WrapOp(h.handlePutLifecyclePolicy),
		"PutRegistryPolicy":     service.WrapOp(h.handlePutRegistryPolicy),
		"PutRegistryScanningConfiguration": service.WrapOp(
			h.handlePutRegistryScanningConfiguration,
		),
		"PutReplicationConfiguration": service.WrapOp(
			h.handlePutReplicationConfiguration,
		),
		"PutSigningConfiguration": service.WrapOp(h.handlePutSigningConfiguration),
		"RegisterPullTimeUpdateExclusion": service.WrapOp(
			h.handleRegisterPullTimeUpdateExclusion,
		),
		"SetRepositoryPolicy": service.WrapOp(h.handleSetRepositoryPolicy),
		"StartImageScan":      service.WrapOp(h.handleStartImageScan),
		"StartLifecyclePolicyPreview": service.WrapOp(
			h.handleStartLifecyclePolicyPreview,
		),
		"TagResource":             service.WrapOp(h.handleTagResource),
		"UntagResource":           service.WrapOp(h.handleUntagResource),
		"UpdateImageStorageClass": service.WrapOp(h.handleUpdateImageStorageClass),
		"UpdatePullThroughCacheRule": service.WrapOp(
			h.handleUpdatePullThroughCacheRule,
		),
		"UpdateRepositoryCreationTemplate": service.WrapOp(
			h.handleUpdateRepositoryCreationTemplate,
		),
		"UploadLayerPart": service.WrapOp(h.handleUploadLayerPart),
		"ValidatePullThroughCacheRule": service.WrapOp(
			h.handleValidatePullThroughCacheRule,
		),
	}
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

// ecrErr builds the error body for an ECR error response.
func ecrErr(errType, msg string) map[string]string {
	return map[string]string{keyTypeField: errType, keyMessageField: msg}
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	status, errType := h.classifyError(err)

	return c.JSON(status, ecrErr(errType, err.Error()))
}

// classifyError returns the HTTP status code and AWS error type string for err.
func (h *Handler) classifyError(err error) (int, string) {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrRepositoryNotFound):
		return http.StatusNotFound, "RepositoryNotFoundException"
	case errors.Is(err, ErrRepositoryPolicyNotFound):
		return http.StatusBadRequest, "RepositoryPolicyNotFoundException"
	case errors.Is(err, ErrImageNotFound):
		return http.StatusBadRequest, "ImageNotFoundException"
	case errors.Is(err, ErrPullThroughCacheRuleNotFound),
		errors.Is(err, ErrLifecyclePolicyNotFound),
		errors.Is(err, ErrRepositoryCreationTemplateNotFound),
		errors.Is(err, ErrRegistryPolicyNotFound):
		return http.StatusNotFound, "NotFoundException"
	case errors.Is(err, ErrRepositoryAlreadyExists):
		return http.StatusBadRequest, "RepositoryAlreadyExistsException"
	case errors.Is(err, ErrRepositoryNotEmpty):
		return http.StatusBadRequest, "RepositoryNotEmptyException"
	case errors.Is(err, ErrImageTagAlreadyExists):
		return http.StatusBadRequest, "ImageTagAlreadyExistsException"
	case errors.Is(err, ErrLayerInaccessible):
		return http.StatusBadRequest, "LayerInaccessibleException"
	case errors.Is(err, ErrLayersNotFound):
		return http.StatusBadRequest, "LayersNotFoundException"
	case errors.Is(err, ErrPullThroughCacheRuleAlreadyExists):
		return http.StatusBadRequest, "PullThroughCacheRuleAlreadyExistsException"
	case errors.Is(err, ErrRepositoryCreationTemplateAlreadyExists):
		return http.StatusBadRequest, "TemplateAlreadyExistsException"
	case errors.Is(err, errUnknownAction):
		return http.StatusBadRequest, "UnknownOperationException"
	case errors.Is(err, ErrInvalidRepositoryName), errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return http.StatusBadRequest, "InvalidParameterException"
	default:
		return http.StatusInternalServerError, "InternalServerError"
	}
}

// repositoryView is the JSON representation of a repository.
// createdAt is serialised as a Unix epoch float64 (seconds) so that the AWS
// SDK v2 deserialiser, which expects a JSON Number for timestamp fields, can
// decode it correctly.
type repositoryView struct {
	EncryptionConfiguration            *encryptionConfigurationView   `json:"encryptionConfiguration,omitempty"`
	ImageTagMutability                 string                         `json:"imageTagMutability,omitempty"`
	RegistryID                         string                         `json:"registryId"`
	RepositoryARN                      string                         `json:"repositoryArn"`
	RepositoryName                     string                         `json:"repositoryName"`
	RepositoryURI                      string                         `json:"repositoryUri"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
	CreatedAt                          float64                        `json:"createdAt"`
	ImageScanningConfiguration         imageScanningConfigurationView `json:"imageScanningConfiguration"`
}

type imageScanningConfigurationView struct {
	ScanOnPush bool `json:"scanOnPush"`
}

type encryptionConfigurationView struct {
	EncryptionType string `json:"encryptionType"`
	KMSKey         string `json:"kmsKey,omitempty"`
}

type imageTagMutabilityFilterView struct {
	Filter     string `json:"filter,omitempty"`
	FilterType string `json:"filterType,omitempty"`
}

func toRepositoryView(r Repository) repositoryView {
	filters := make([]imageTagMutabilityFilterView, 0, len(r.ImageTagMutabilityExclusionFilters))
	for _, filter := range r.ImageTagMutabilityExclusionFilters {
		filters = append(filters, imageTagMutabilityFilterView(filter))
	}

	return repositoryView{
		CreatedAt: float64(r.CreatedAt.Unix()),
		EncryptionConfiguration: &encryptionConfigurationView{
			EncryptionType: r.EncryptionType,
			KMSKey:         r.KMSKey,
		},
		ImageScanningConfiguration: imageScanningConfigurationView{
			ScanOnPush: r.ScanOnPush,
		},
		ImageTagMutability:                 r.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: filters,
		RegistryID:                         r.RegistryID,
		RepositoryARN:                      r.RepositoryARN,
		RepositoryName:                     r.RepositoryName,
		RepositoryURI:                      r.RepositoryURI,
	}
}

// createRepositoryInput is the request body for CreateRepository.
type createRepositoryInput struct {
	EncryptionConfiguration    *encryptionConfigurationView    `json:"encryptionConfiguration,omitempty"`
	ImageScanningConfiguration *imageScanningConfigurationView `json:"imageScanningConfiguration,omitempty"`
	RepositoryName             string                          `json:"repositoryName"`
	ImageTagMutability         string                          `json:"imageTagMutability,omitempty"`
	Tags                       []tagView                       `json:"tags,omitempty"`
}

type createRepositoryOutput struct {
	Repository repositoryView `json:"repository"`
}

func (h *Handler) handleCreateRepository(
	ctx context.Context,
	in *createRepositoryInput,
) (*createRepositoryOutput, error) {
	scanOnPush := false
	if in.ImageScanningConfiguration != nil {
		scanOnPush = in.ImageScanningConfiguration.ScanOnPush
	}

	encryptionType := ""
	kmsKey := ""
	if in.EncryptionConfiguration != nil {
		encryptionType = in.EncryptionConfiguration.EncryptionType
		kmsKey = in.EncryptionConfiguration.KMSKey
	}

	repo, err := h.Backend.CreateRepository(
		ctx, in.RepositoryName, in.ImageTagMutability, scanOnPush, encryptionType, kmsKey,
	)
	if err != nil {
		return nil, err
	}

	if len(in.Tags) > 0 {
		tagMap := make(map[string]string, len(in.Tags))
		for _, t := range in.Tags {
			tagMap[t.Key] = t.Value
		}

		if tagErr := h.Backend.TagResource(ctx, repo.RepositoryARN, tagMap); tagErr != nil {
			return nil, tagErr
		}
	}

	return &createRepositoryOutput{Repository: toRepositoryView(*repo)}, nil
}

// describeRepositoriesInput is the request body for DescribeRepositories.
type describeRepositoriesInput struct {
	NextToken       string   `json:"nextToken,omitempty"`
	RepositoryNames []string `json:"repositoryNames"`
	MaxResults      int      `json:"maxResults,omitempty"`
}

type describeRepositoriesOutput struct {
	NextToken    string           `json:"nextToken,omitempty"`
	Repositories []repositoryView `json:"repositories"`
}

func (h *Handler) handleDescribeRepositories(
	ctx context.Context,
	in *describeRepositoriesInput,
) (*describeRepositoriesOutput, error) {
	repos, err := h.Backend.DescribeRepositories(ctx, in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	// Apply nextToken cursor: token is base64(repoName) of the first repo on this page.
	if in.NextToken != "" && len(in.RepositoryNames) == 0 {
		decoded, decErr := base64.StdEncoding.DecodeString(in.NextToken)
		if decErr == nil {
			cursorName := string(decoded)
			start := 0
			for i, r := range repos {
				if r.RepositoryName == cursorName {
					start = i

					break
				}
			}

			repos = repos[start:]
		}
	}

	// Apply maxResults page limit; emit opaque token = base64(next repo name).
	var nextToken string
	if in.MaxResults > 0 && len(repos) > in.MaxResults {
		nextToken = base64.StdEncoding.EncodeToString([]byte(repos[in.MaxResults].RepositoryName))
		repos = repos[:in.MaxResults]
	}

	views := make([]repositoryView, 0, len(repos))
	for _, r := range repos {
		views = append(views, toRepositoryView(r))
	}

	return &describeRepositoriesOutput{Repositories: views, NextToken: nextToken}, nil
}

// deleteRepositoryInput is the request body for DeleteRepository.
type deleteRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
	Force          bool   `json:"force,omitempty"`
}

type deleteRepositoryOutput struct {
	Repository repositoryView `json:"repository"`
}

func (h *Handler) handleDeleteRepository(
	ctx context.Context,
	in *deleteRepositoryInput,
) (*deleteRepositoryOutput, error) {
	// Real AWS requires force=true to delete repositories containing images.
	if !in.Force {
		imgs, descErr := h.Backend.DescribeImages(ctx, in.RepositoryName, nil)
		if descErr == nil && len(imgs) > 0 {
			return nil, fmt.Errorf("%w: %s contains images; set force=true to override",
				ErrRepositoryNotEmpty, in.RepositoryName)
		}
	}

	repo, err := h.Backend.DeleteRepository(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &deleteRepositoryOutput{Repository: toRepositoryView(*repo)}, nil
}

// getAuthorizationTokenInput is the request body for GetAuthorizationToken.
// RegistryIDs specifies which registries to fetch tokens for; when empty a
// single token for the default registry is returned.
type getAuthorizationTokenInput struct {
	RegistryIDs []string `json:"registryIds,omitempty"`
}

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
	in *getAuthorizationTokenInput,
) (*getAuthorizationTokenOutput, error) {
	token := h.generateAuthToken()
	expiresAt := time.Now().Add(tokenTTL).Unix()
	proxyEndpoint := h.Backend.ProxyEndpoint()

	if proxyEndpoint != "" &&
		!strings.HasPrefix(proxyEndpoint, "https://") &&
		!strings.HasPrefix(proxyEndpoint, "http://") {
		proxyEndpoint = "https://" + proxyEndpoint
	}

	entry := authorizationDataView{
		AuthorizationToken: token,
		ExpiresAt:          expiresAt,
		ProxyEndpoint:      proxyEndpoint,
	}

	// When specific registry IDs are requested, return one token per registry.
	// Since this is a single-account simulator, the token is identical for each.
	if len(in.RegistryIDs) > 0 {
		data := make([]authorizationDataView, 0, len(in.RegistryIDs))
		for range in.RegistryIDs {
			data = append(data, entry)
		}

		return &getAuthorizationTokenOutput{AuthorizationData: data}, nil
	}

	return &getAuthorizationTokenOutput{
		AuthorizationData: []authorizationDataView{entry},
	}, nil
}

const authTokenRandomBytes = 32

// generateAuthToken produces a unique ECR authorization token per the same
// structure real AWS uses: base64(AWS:<random-hex-password>).
// Each call returns a different token so callers cannot cache a fixed value.
func (h *Handler) generateAuthToken() string {
	if h.registryHandler != nil {
		// Emulator needs dummy-password for existing integration tests.
		return base64.StdEncoding.EncodeToString([]byte("AWS:dummy-password"))
	}

	raw := make([]byte, authTokenRandomBytes)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		// crypto/rand failure is extremely rare; use a fixed fallback.
		raw = []byte("gopherstack-ecr-fallback-token-00")
	}

	return base64.StdEncoding.EncodeToString([]byte(dummyUser + ":" + hex.EncodeToString(raw)))
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
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	keys := sortedTagKeys(tags)
	result := toTagViewsForKeys(tags, keys)

	return &listTagsForResourceOutput{Tags: result}, nil
}

// tagResourceInput is the request body for TagResource.
type tagResourceInput struct {
	ResourceArn string    `json:"resourceArn"`
	Tags        []tagView `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	tagMap := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagMap); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// untagResourceInput is the request body for UntagResource.
type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

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
	ctx context.Context,
	in *batchCheckLayerAvailabilityInput,
) (*batchCheckLayerAvailabilityOutput, error) {
	layers, failures, err := h.Backend.BatchCheckLayerAvailability(
		ctx,
		in.RepositoryName,
		in.LayerDigests,
	)
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
	ctx context.Context,
	in *batchDeleteImageInput,
) (*batchDeleteImageOutput, error) {
	deleted, failures, err := h.Backend.BatchDeleteImage(ctx, in.RepositoryName, in.ImageIDs)
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
	ctx context.Context,
	in *batchGetImageInput,
) (*batchGetImageOutput, error) {
	imgs, failures, err := h.Backend.BatchGetImage(ctx, in.RepositoryName, in.ImageIDs)
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

type describeImagesFilter struct {
	TagStatus string `json:"tagStatus,omitempty"`
}

type describeImagesInput struct {
	Filter         *describeImagesFilter `json:"filter,omitempty"`
	RepositoryName string                `json:"repositoryName"`
	NextToken      string                `json:"nextToken,omitempty"`
	ImageIDs       []ImageIdentifier     `json:"imageIds,omitempty"`
	MaxResults     int                   `json:"maxResults,omitempty"`
}

type imageDetailView struct {
	ImageDigest            string   `json:"imageDigest,omitempty"`
	ImageManifestMediaType string   `json:"imageManifestMediaType,omitempty"`
	ImageStatus            string   `json:"imageStatus,omitempty"`
	RegistryID             string   `json:"registryId,omitempty"`
	RepositoryName         string   `json:"repositoryName,omitempty"`
	ImageTags              []string `json:"imageTags,omitempty"`
	ImagePushedAt          float64  `json:"imagePushedAt,omitempty"`
	ImageSizeInBytes       int64    `json:"imageSizeInBytes,omitempty"`
}

type describeImagesOutput struct {
	NextToken    string            `json:"nextToken,omitempty"`
	ImageDetails []imageDetailView `json:"imageDetails"`
}

func toImageDetailView(img Image) imageDetailView {
	// Prefer multi-tag list from DescribeImages annotation; fall back to single tag.
	tags := img.Tags
	if len(tags) == 0 && img.ImageID.ImageTag != "" {
		tags = []string{img.ImageID.ImageTag}
	}
	if tags == nil {
		tags = []string{}
	}

	var pushedAt float64
	if !img.ImagePushedAt.IsZero() {
		pushedAt = float64(img.ImagePushedAt.Unix())
	}

	return imageDetailView{
		ImageDigest:            img.ImageDigest,
		ImageTags:              tags,
		ImagePushedAt:          pushedAt,
		ImageSizeInBytes:       img.ImageSizeInBytes,
		ImageManifestMediaType: img.ImageManifestMediaType,
		ImageStatus:            img.ImageStatus,
		RegistryID:             img.RegistryID,
		RepositoryName:         img.RepositoryName,
	}
}

func (h *Handler) handleDescribeImages(
	ctx context.Context,
	in *describeImagesInput,
) (*describeImagesOutput, error) {
	imgs, err := h.Backend.DescribeImages(ctx, in.RepositoryName, in.ImageIDs)
	if err != nil {
		return nil, err
	}

	// Apply filter.tagStatus when listing all images (not by specific imageIds).
	// AWS DescribeImages supports filter: { tagStatus: "TAGGED" | "UNTAGGED" | "ANY" }.
	if in.Filter != nil && in.Filter.TagStatus != "" && len(in.ImageIDs) == 0 {
		filtered := imgs[:0]
		for _, img := range imgs {
			isTagged := len(img.Tags) > 0
			if passesTagFilter(isTagged, in.Filter.TagStatus) {
				filtered = append(filtered, img)
			}
		}
		imgs = filtered
	}

	// Apply nextToken cursor when paginating without specific imageIds.
	if in.NextToken != "" && len(in.ImageIDs) == 0 {
		start := 0
		for i, img := range imgs {
			if img.ImageDigest == in.NextToken {
				start = i

				break
			}
		}
		imgs = imgs[start:]
	}

	var nextToken string
	if in.MaxResults > 0 && len(in.ImageIDs) == 0 && len(imgs) > in.MaxResults {
		nextToken = imgs[in.MaxResults].ImageDigest
		imgs = imgs[:in.MaxResults]
	}

	details := make([]imageDetailView, 0, len(imgs))
	for _, img := range imgs {
		details = append(details, toImageDetailView(img))
	}

	return &describeImagesOutput{ImageDetails: details, NextToken: nextToken}, nil
}

type listImagesFilter struct {
	TagStatus string `json:"tagStatus,omitempty"`
}

type listImagesInput struct {
	Filter         *listImagesFilter `json:"filter,omitempty"`
	RepositoryName string            `json:"repositoryName"`
	RegistryID     string            `json:"registryId,omitempty"`
	NextToken      string            `json:"nextToken,omitempty"`
	MaxResults     int               `json:"maxResults,omitempty"`
}

type listImagesOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	ImageIDs  []ImageIdentifier `json:"imageIds"`
}

func (h *Handler) handleListImages(
	ctx context.Context,
	in *listImagesInput,
) (*listImagesOutput, error) {
	tagStatusFilter := ""
	if in.Filter != nil {
		tagStatusFilter = in.Filter.TagStatus
	}

	imageIDs, err := h.Backend.ListImages(ctx, in.RepositoryName, tagStatusFilter)
	if err != nil {
		return nil, err
	}

	// Apply nextToken cursor: token is base64(digest:tag) of the first image on this page.
	if in.NextToken != "" {
		decoded, decErr := base64.StdEncoding.DecodeString(in.NextToken)
		if decErr == nil {
			cursorKey := string(decoded)
			start := 0
			for i, id := range imageIDs {
				if id.ImageDigest+":"+id.ImageTag == cursorKey {
					start = i

					break
				}
			}

			imageIDs = imageIDs[start:]
		}
	}

	// Apply maxResults page limit; emit opaque token = base64(digest:tag).
	var nextToken string
	if in.MaxResults > 0 && len(imageIDs) > in.MaxResults {
		next := imageIDs[in.MaxResults]
		nextToken = base64.StdEncoding.EncodeToString(
			[]byte(next.ImageDigest + ":" + next.ImageTag),
		)
		imageIDs = imageIDs[:in.MaxResults]
	}

	return &listImagesOutput{ImageIDs: imageIDs, NextToken: nextToken}, nil
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
	ctx context.Context,
	in *batchGetRepositoryScanningConfigurationInput,
) (*batchGetRepositoryScanningConfigurationOutput, error) {
	configs, failures, err := h.Backend.BatchGetRepositoryScanningConfiguration(
		ctx,
		in.RepositoryNames,
	)
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
	ctx context.Context,
	in *completeLayerUploadInput,
) (*CompleteLayerUploadResult, error) {
	result, err := h.Backend.CompleteLayerUpload(
		ctx,
		in.RepositoryName,
		in.UploadID,
		in.LayerDigests,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type getDownloadURLForLayerInput struct {
	LayerDigest    string `json:"layerDigest"`
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

type getDownloadURLForLayerOutput struct {
	DownloadURL string `json:"downloadUrl"`
	LayerDigest string `json:"layerDigest"`
}

func (h *Handler) handleGetDownloadURLForLayer(
	ctx context.Context,
	in *getDownloadURLForLayerInput,
) (*getDownloadURLForLayerOutput, error) {
	url, err := h.Backend.GetDownloadURLForLayer(ctx, in.RepositoryName, in.LayerDigest)
	if err != nil {
		return nil, err
	}

	return &getDownloadURLForLayerOutput{DownloadURL: url, LayerDigest: in.LayerDigest}, nil
}

type initiateLayerUploadInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

type initiateLayerUploadOutput struct {
	UploadID string `json:"uploadId"`
	PartSize int64  `json:"partSize"`
}

func (h *Handler) handleInitiateLayerUpload(
	ctx context.Context,
	in *initiateLayerUploadInput,
) (*initiateLayerUploadOutput, error) {
	result, err := h.Backend.InitiateLayerUpload(ctx, in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return &initiateLayerUploadOutput{PartSize: result.PartSize, UploadID: result.UploadID}, nil
}

type uploadLayerPartInput struct {
	RepositoryName string `json:"repositoryName"`
	UploadID       string `json:"uploadId"`
	RegistryID     string `json:"registryId,omitempty"`
	LayerPartBlob  []byte `json:"layerPartBlob"`
	PartFirstByte  int64  `json:"partFirstByte"`
	PartLastByte   int64  `json:"partLastByte"`
}

func (h *Handler) handleUploadLayerPart(
	ctx context.Context,
	in *uploadLayerPartInput,
) (*LayerUploadPartResult, error) {
	return h.Backend.UploadLayerPart(
		ctx,
		in.RepositoryName,
		in.UploadID,
		in.PartFirstByte,
		in.PartLastByte,
		in.LayerPartBlob,
	)
}

// createPullThroughCacheRuleInput is the request body for CreatePullThroughCacheRule.
type createPullThroughCacheRuleInput struct {
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	UpstreamRegistry         string `json:"upstreamRegistry,omitempty"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
}

type createPullThroughCacheRuleOutput struct {
	EcrRepositoryPrefix      string `json:"ecrRepositoryPrefix"`
	UpstreamRegistryURL      string `json:"upstreamRegistryUrl"`
	CredentialArn            string `json:"credentialArn,omitempty"`
	CustomRoleArn            string `json:"customRoleArn,omitempty"`
	UpstreamRegistry         string `json:"upstreamRegistry,omitempty"`
	UpstreamRepositoryPrefix string `json:"upstreamRepositoryPrefix,omitempty"`
	RegistryID               string `json:"registryId"`
	CreatedAt                int64  `json:"createdAt"`
	UpdatedAt                int64  `json:"updatedAt,omitempty"`
}

func (h *Handler) handleCreatePullThroughCacheRule(
	ctx context.Context,
	in *createPullThroughCacheRuleInput,
) (*createPullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.CreatePullThroughCacheRule(
		ctx,
		in.EcrRepositoryPrefix,
		in.UpstreamRegistryURL,
		in.CredentialArn,
		in.UpstreamRegistry,
		in.CustomRoleArn,
		in.UpstreamRepositoryPrefix,
	)
	if err != nil {
		return nil, err
	}

	return &createPullThroughCacheRuleOutput{
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		CreatedAt:                rule.CreatedAt.Unix(),
		UpdatedAt:                rule.UpdatedAt.Unix(),
	}, nil
}

type describePullThroughCacheRulesInput struct {
	NextToken             string   `json:"nextToken,omitempty"`
	EcrRepositoryPrefixes []string `json:"ecrRepositoryPrefixes,omitempty"`
	MaxResults            int      `json:"maxResults,omitempty"`
}

type describePullThroughCacheRulesOutput struct {
	NextToken             string                             `json:"nextToken,omitempty"`
	PullThroughCacheRules []createPullThroughCacheRuleOutput `json:"pullThroughCacheRules"`
}

func (h *Handler) handleDescribePullThroughCacheRules(
	ctx context.Context,
	in *describePullThroughCacheRulesInput,
) (*describePullThroughCacheRulesOutput, error) {
	rules, err := h.Backend.DescribePullThroughCacheRules(ctx, in.EcrRepositoryPrefixes)
	if err != nil {
		return nil, err
	}

	// Apply nextToken cursor: token is base64(ecrRepositoryPrefix) of the first rule on this page.
	if in.NextToken != "" && len(in.EcrRepositoryPrefixes) == 0 {
		decoded, decErr := base64.StdEncoding.DecodeString(in.NextToken)
		if decErr == nil {
			cursorPrefix := string(decoded)
			start := 0
			for i, r := range rules {
				if r.EcrRepositoryPrefix == cursorPrefix {
					start = i

					break
				}
			}

			rules = rules[start:]
		}
	}

	// Apply maxResults page limit; emit opaque token = base64(next prefix).
	var nextToken string
	if in.MaxResults > 0 && len(rules) > in.MaxResults {
		nextToken = base64.StdEncoding.EncodeToString(
			[]byte(rules[in.MaxResults].EcrRepositoryPrefix),
		)
		rules = rules[:in.MaxResults]
	}

	out := make([]createPullThroughCacheRuleOutput, 0, len(rules))
	for _, rule := range rules {
		out = append(out, createPullThroughCacheRuleOutput{
			EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
			UpstreamRegistryURL:      rule.UpstreamRegistryURL,
			CredentialArn:            rule.CredentialArn,
			CustomRoleArn:            rule.CustomRoleArn,
			UpstreamRegistry:         rule.UpstreamRegistry,
			UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
			RegistryID:               rule.RegistryID,
			CreatedAt:                rule.CreatedAt.Unix(),
			UpdatedAt:                rule.UpdatedAt.Unix(),
		})
	}

	return &describePullThroughCacheRulesOutput{
		PullThroughCacheRules: out,
		NextToken:             nextToken,
	}, nil
}

type repositoryCreationTemplateInput struct {
	EncryptionConfiguration            *encryptionConfigurationView   `json:"encryptionConfiguration,omitempty"`
	Prefix                             string                         `json:"prefix"`
	Description                        string                         `json:"description,omitempty"`
	ImageTagMutability                 string                         `json:"imageTagMutability,omitempty"`
	RepositoryPolicy                   string                         `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy                    string                         `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn                      string                         `json:"customRoleArn,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
	AppliedFor                         []string                       `json:"appliedFor,omitempty"`
	ResourceTags                       []tagView                      `json:"resourceTags,omitempty"`
}

type createRepositoryCreationTemplateOutput struct {
	Template   *repositoryCreationTemplateView `json:"repositoryCreationTemplate"`
	RegistryID string                          `json:"registryId"`
}

type repositoryCreationTemplateView struct {
	EncryptionConfiguration            *encryptionConfigurationView   `json:"encryptionConfiguration,omitempty"`
	Prefix                             string                         `json:"prefix,omitempty"`
	Description                        string                         `json:"description,omitempty"`
	ImageTagMutability                 string                         `json:"imageTagMutability,omitempty"`
	RepositoryPolicy                   string                         `json:"repositoryPolicy,omitempty"`
	LifecyclePolicy                    string                         `json:"lifecyclePolicy,omitempty"`
	CustomRoleArn                      string                         `json:"customRoleArn,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
	AppliedFor                         []string                       `json:"appliedFor,omitempty"`
	ResourceTags                       []tagView                      `json:"resourceTags,omitempty"`
	CreatedAt                          float64                        `json:"createdAt,omitempty"`
	UpdatedAt                          float64                        `json:"updatedAt,omitempty"`
}

func (h *Handler) handleCreateRepositoryCreationTemplate(
	ctx context.Context,
	in *repositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	req := repositoryCreationTemplateFromInput(in)

	tmpl, err := h.Backend.CreateRepositoryCreationTemplate(ctx, req)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

type deleteRepositoryCreationTemplateInput struct {
	Prefix string `json:"prefix"`
}

func (h *Handler) handleDeleteRepositoryCreationTemplate(
	ctx context.Context,
	in *deleteRepositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	tmpl, err := h.Backend.DeleteRepositoryCreationTemplate(ctx, in.Prefix)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

type describeRepositoryCreationTemplatesInput struct {
	Prefixes []string `json:"prefixes,omitempty"`
}

type describeRepositoryCreationTemplatesOutput struct {
	RegistryID                  string                           `json:"registryId"`
	RepositoryCreationTemplates []repositoryCreationTemplateView `json:"repositoryCreationTemplates"`
}

func (h *Handler) handleDescribeRepositoryCreationTemplates(
	ctx context.Context,
	in *describeRepositoryCreationTemplatesInput,
) (*describeRepositoryCreationTemplatesOutput, error) {
	tmpls, err := h.Backend.DescribeRepositoryCreationTemplates(ctx, in.Prefixes)
	if err != nil {
		return nil, err
	}

	out := make([]repositoryCreationTemplateView, 0, len(tmpls))
	for i := range tmpls {
		out = append(out, *toRepositoryCreationTemplateView(&tmpls[i]))
	}

	return &describeRepositoryCreationTemplatesOutput{
		RegistryID:                  h.Backend.AccountID(),
		RepositoryCreationTemplates: out,
	}, nil
}

func (h *Handler) handleUpdateRepositoryCreationTemplate(
	ctx context.Context,
	in *repositoryCreationTemplateInput,
) (*createRepositoryCreationTemplateOutput, error) {
	tmpl, err := h.Backend.UpdateRepositoryCreationTemplate(
		ctx,
		repositoryCreationTemplateFromInput(in),
	)
	if err != nil {
		return nil, err
	}

	return &createRepositoryCreationTemplateOutput{
		Template: toRepositoryCreationTemplateView(tmpl),
	}, nil
}

func toRepositoryCreationTemplateView(
	in *RepositoryCreationTemplate,
) *repositoryCreationTemplateView {
	if in == nil {
		return nil
	}

	filters := make([]imageTagMutabilityFilterView, 0, len(in.ImageTagMutabilityExclusionFilters))
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, imageTagMutabilityFilterView(filter))
	}

	out := &repositoryCreationTemplateView{
		AppliedFor:                         append([]string(nil), in.AppliedFor...),
		CreatedAt:                          float64(in.CreatedAt.Unix()),
		CustomRoleArn:                      in.CustomRoleArn,
		Description:                        in.Description,
		ImageTagMutability:                 in.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: filters,
		LifecyclePolicy:                    in.LifecyclePolicy,
		Prefix:                             in.Prefix,
		RepositoryPolicy:                   in.RepositoryPolicy,
		ResourceTags:                       toTagViews(in.ResourceTags),
		UpdatedAt:                          float64(in.UpdatedAt.Unix()),
	}
	if in.EncryptionType != "" || in.KMSKey != "" {
		out.EncryptionConfiguration = &encryptionConfigurationView{
			EncryptionType: in.EncryptionType,
			KMSKey:         in.KMSKey,
		}
	}

	return out
}

func toTagViews(tags map[string]string) []tagView {
	keys := sortedTagKeys(tags)

	return toTagViewsForKeys(tags, keys)
}

func toTagViewsForKeys(tags map[string]string, keys []string) []tagView {
	out := make([]tagView, 0, len(keys))
	for _, key := range keys {
		out = append(out, tagView{Key: key, Value: tags[key]})
	}

	return out
}

func repositoryCreationTemplateFromInput(
	in *repositoryCreationTemplateInput,
) *RepositoryCreationTemplate {
	filters := make(
		[]ImageTagMutabilityExclusionFilter,
		0,
		len(in.ImageTagMutabilityExclusionFilters),
	)
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, ImageTagMutabilityExclusionFilter(filter))
	}

	tags := make(map[string]string, len(in.ResourceTags))
	for _, tag := range in.ResourceTags {
		tags[tag.Key] = tag.Value
	}

	req := &RepositoryCreationTemplate{
		Prefix:                             in.Prefix,
		Description:                        in.Description,
		ImageTagMutability:                 in.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: filters,
		RepositoryPolicy:                   in.RepositoryPolicy,
		LifecyclePolicy:                    in.LifecyclePolicy,
		AppliedFor:                         in.AppliedFor,
		CustomRoleArn:                      in.CustomRoleArn,
		ResourceTags:                       tags,
	}
	if in.EncryptionConfiguration != nil {
		req.EncryptionType = in.EncryptionConfiguration.EncryptionType
		req.KMSKey = in.EncryptionConfiguration.KMSKey
	}

	return req
}

// deleteLifecyclePolicyInput is the request body for DeleteLifecyclePolicy.
type deleteLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleDeleteLifecyclePolicy(
	ctx context.Context,
	in *deleteLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.DeleteLifecyclePolicy(ctx, in.RepositoryName)
}

type getLifecyclePolicyInput struct {
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
}

func (h *Handler) handleGetLifecyclePolicy(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.GetLifecyclePolicy(ctx, in.RepositoryName)
}

func (h *Handler) handleGetLifecyclePolicyPreview(
	ctx context.Context,
	in *getLifecyclePolicyInput,
) (*LifecyclePolicyPreviewResult, error) {
	return h.Backend.GetLifecyclePolicyPreview(ctx, in.RepositoryName)
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
	ctx context.Context,
	in *deletePullThroughCacheRuleInput,
) (*deletePullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.DeletePullThroughCacheRule(ctx, in.EcrRepositoryPrefix)
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

type updatePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	CredentialArn       string `json:"credentialArn,omitempty"`
	CustomRoleArn       string `json:"customRoleArn,omitempty"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handleUpdatePullThroughCacheRule(
	ctx context.Context,
	in *updatePullThroughCacheRuleInput,
) (*createPullThroughCacheRuleOutput, error) {
	rule, err := h.Backend.UpdatePullThroughCacheRule(
		ctx,
		in.EcrRepositoryPrefix,
		in.CredentialArn,
		in.CustomRoleArn,
	)
	if err != nil {
		return nil, err
	}

	return &createPullThroughCacheRuleOutput{
		EcrRepositoryPrefix:      rule.EcrRepositoryPrefix,
		UpstreamRegistryURL:      rule.UpstreamRegistryURL,
		CredentialArn:            rule.CredentialArn,
		CustomRoleArn:            rule.CustomRoleArn,
		UpstreamRegistry:         rule.UpstreamRegistry,
		UpstreamRepositoryPrefix: rule.UpstreamRepositoryPrefix,
		RegistryID:               rule.RegistryID,
		CreatedAt:                rule.CreatedAt.Unix(),
		UpdatedAt:                rule.UpdatedAt.Unix(),
	}, nil
}

type validatePullThroughCacheRuleInput struct {
	EcrRepositoryPrefix string `json:"ecrRepositoryPrefix"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handleValidatePullThroughCacheRule(
	ctx context.Context,
	in *validatePullThroughCacheRuleInput,
) (*ValidatePullThroughCacheRuleResult, error) {
	return h.Backend.ValidatePullThroughCacheRule(ctx, in.EcrRepositoryPrefix)
}

// deleteRegistryPolicyInput is the (empty) request body for DeleteRegistryPolicy.
type deleteRegistryPolicyInput struct{}

func (h *Handler) handleDeleteRegistryPolicy(
	ctx context.Context,
	_ *deleteRegistryPolicyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.DeleteRegistryPolicy(ctx)
}

type emptyInput struct{}

func (h *Handler) handleDescribeRegistry(
	ctx context.Context,
	_ *emptyInput,
) (*RegistryDescription, error) {
	return h.Backend.DescribeRegistry(ctx)
}

func (h *Handler) handleGetRegistryPolicy(
	ctx context.Context,
	_ *emptyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.GetRegistryPolicy(ctx)
}

type getRegistryScanningConfigurationOutput struct {
	ScanningConfiguration *RegistryScanningSettings `json:"scanningConfiguration"`
	RegistryID            string                    `json:"registryId"`
}

func (h *Handler) handleGetRegistryScanningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*getRegistryScanningConfigurationOutput, error) {
	settings, err := h.Backend.GetRegistryScanningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &getRegistryScanningConfigurationOutput{ScanningConfiguration: settings}, nil
}

// putLifecyclePolicyInput is the request body for PutLifecyclePolicy.
type putLifecyclePolicyInput struct {
	RepositoryName      string `json:"repositoryName"`
	LifecyclePolicyText string `json:"lifecyclePolicyText"`
	RegistryID          string `json:"registryId,omitempty"`
}

func (h *Handler) handlePutLifecyclePolicy(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*LifecyclePolicyResult, error) {
	return h.Backend.PutLifecyclePolicy(ctx, in.RepositoryName, in.LifecyclePolicyText)
}

func (h *Handler) handleStartLifecyclePolicyPreview(
	ctx context.Context,
	in *putLifecyclePolicyInput,
) (*LifecyclePolicyPreviewResult, error) {
	return h.Backend.StartLifecyclePolicyPreview(ctx, in.RepositoryName, in.LifecyclePolicyText)
}

// putRegistryPolicyInput is the request body for PutRegistryPolicy.
type putRegistryPolicyInput struct {
	PolicyText string `json:"policyText"`
}

func (h *Handler) handlePutRegistryPolicy(
	ctx context.Context,
	in *putRegistryPolicyInput,
) (*RegistryPolicyResult, error) {
	return h.Backend.PutRegistryPolicy(ctx, in.PolicyText)
}

func (h *Handler) handlePutRegistryScanningConfiguration(
	ctx context.Context,
	in *RegistryScanningSettings,
) (*getRegistryScanningConfigurationOutput, error) {
	settings, err := h.Backend.PutRegistryScanningConfiguration(ctx, in)
	if err != nil {
		return nil, err
	}

	return &getRegistryScanningConfigurationOutput{ScanningConfiguration: settings}, nil
}

type replicationConfigurationInput struct {
	ReplicationConfiguration *ReplicationConfig `json:"replicationConfiguration"`
}

func (h *Handler) handlePutReplicationConfiguration(
	ctx context.Context,
	in *replicationConfigurationInput,
) (*replicationConfigurationInput, error) {
	cfg, err := h.Backend.PutReplicationConfiguration(ctx, in.ReplicationConfiguration)
	if err != nil {
		return nil, err
	}

	return &replicationConfigurationInput{ReplicationConfiguration: cfg}, nil
}

type signingConfigurationInput struct {
	SigningConfiguration *SigningSettings `json:"signingConfiguration"`
}

func (h *Handler) handleGetSigningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.GetSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
}

func (h *Handler) handlePutSigningConfiguration(
	ctx context.Context,
	in *signingConfigurationInput,
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.PutSigningConfiguration(ctx, in.SigningConfiguration)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
}

func (h *Handler) handleDeleteSigningConfiguration(
	ctx context.Context,
	_ *emptyInput,
) (*signingConfigurationInput, error) {
	settings, err := h.Backend.DeleteSigningConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	return &signingConfigurationInput{SigningConfiguration: settings}, nil
}

type imageInput struct {
	ImageID        ImageIdentifier `json:"imageId"`
	RepositoryName string          `json:"repositoryName"`
	RegistryID     string          `json:"registryId,omitempty"`
}

type describeImageScanFindingsOutput struct {
	ImageID           ImageIdentifier          `json:"imageId"`
	ImageScanFindings *ImageScanFindingsResult `json:"imageScanFindings"`
	ImageScanStatus   scanStatusView           `json:"imageScanStatus"`
	RegistryID        string                   `json:"registryId"`
	RepositoryName    string                   `json:"repositoryName"`
}

type scanStatusView struct {
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
}

func (h *Handler) handleDescribeImageScanFindings(
	ctx context.Context,
	in *imageInput,
) (*describeImageScanFindingsOutput, error) {
	findings, err := h.Backend.DescribeImageScanFindings(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &describeImageScanFindingsOutput{
		ImageID:           findings.ImageID,
		ImageScanFindings: findings,
		ImageScanStatus: scanStatusView{
			Description: findings.Description,
			Status:      findings.Status,
		},
		RegistryID:     findings.RegistryID,
		RepositoryName: findings.RepositoryName,
	}, nil
}

type startImageScanOutput struct {
	ImageID         ImageIdentifier `json:"imageId"`
	ImageScanStatus scanStatusView  `json:"imageScanStatus"`
	RegistryID      string          `json:"registryId"`
	RepositoryName  string          `json:"repositoryName"`
}

func (h *Handler) handleStartImageScan(
	ctx context.Context,
	in *imageInput,
) (*startImageScanOutput, error) {
	result, err := h.Backend.StartImageScan(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &startImageScanOutput{
		ImageID: result.ImageID,
		ImageScanStatus: scanStatusView{
			Description: result.Description,
			Status:      result.Status,
		},
		RegistryID:     result.RegistryID,
		RepositoryName: result.RepositoryName,
	}, nil
}

type describeImageSigningStatusOutput struct {
	ImageID         ImageIdentifier            `json:"imageId"`
	RegistryID      string                     `json:"registryId,omitempty"`
	RepositoryName  string                     `json:"repositoryName"`
	SigningStatuses []ImageSigningStatusRecord `json:"signingStatuses"`
}

func (h *Handler) handleDescribeImageSigningStatus(
	ctx context.Context,
	in *imageInput,
) (*describeImageSigningStatusOutput, error) {
	result, err := h.Backend.DescribeImageSigningStatus(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &describeImageSigningStatusOutput{
		ImageID:         result.ImageID,
		RegistryID:      result.RegistryID,
		RepositoryName:  result.RepositoryName,
		SigningStatuses: result.SigningStatuses,
	}, nil
}

type describeImageReplicationStatusOutput struct {
	ImageID             ImageIdentifier          `json:"imageId"`
	RepositoryName      string                   `json:"repositoryName"`
	ReplicationStatuses []imageReplicationStatus `json:"replicationStatuses"`
}

type imageReplicationStatus struct {
	Region        string `json:"region,omitempty"`
	RegistryID    string `json:"registryId,omitempty"`
	Status        string `json:"status"`
	FailureCode   string `json:"failureCode,omitempty"`
	FailureReason string `json:"failureReason,omitempty"`
}

func (h *Handler) handleDescribeImageReplicationStatus(
	ctx context.Context,
	in *imageInput,
) (*describeImageReplicationStatusOutput, error) {
	result, err := h.Backend.DescribeImageReplicationStatus(ctx, in.RepositoryName, in.ImageID)
	if err != nil {
		return nil, err
	}

	return &describeImageReplicationStatusOutput{
		ImageID:             result.ImageID,
		ReplicationStatuses: []imageReplicationStatus{{Status: result.ReplicationStatus}},
		RepositoryName:      result.RepositoryName,
	}, nil
}

type putImageInput struct {
	ImageDigest            string `json:"imageDigest,omitempty"`
	ImageManifest          string `json:"imageManifest"`
	ImageManifestMediaType string `json:"imageManifestMediaType,omitempty"`
	ImageTag               string `json:"imageTag,omitempty"`
	RepositoryName         string `json:"repositoryName"`
	RegistryID             string `json:"registryId,omitempty"`
}

type putImageOutput struct {
	Image *Image `json:"image"`
}

func (h *Handler) handlePutImage(ctx context.Context, in *putImageInput) (*putImageOutput, error) {
	img, err := h.Backend.PutImage(ctx, in.RepositoryName, Image{
		ImageDigest:            in.ImageDigest,
		ImageManifest:          in.ImageManifest,
		ImageManifestMediaType: in.ImageManifestMediaType,
		ImageID: ImageIdentifier{
			ImageDigest: in.ImageDigest,
			ImageTag:    in.ImageTag,
		},
		RepositoryName: in.RepositoryName,
		RegistryID:     in.RegistryID,
	})
	if err != nil {
		return nil, err
	}

	return &putImageOutput{Image: img}, nil
}

type putImageScanningConfigurationInput struct {
	RepositoryName             string                         `json:"repositoryName"`
	RegistryID                 string                         `json:"registryId,omitempty"`
	ImageScanningConfiguration imageScanningConfigurationView `json:"imageScanningConfiguration"`
}

type putImageScanningConfigurationOutput struct {
	RegistryID                 string                         `json:"registryId,omitempty"`
	RepositoryName             string                         `json:"repositoryName"`
	ImageScanningConfiguration imageScanningConfigurationView `json:"imageScanningConfiguration"`
}

func (h *Handler) handlePutImageScanningConfiguration(
	ctx context.Context,
	in *putImageScanningConfigurationInput,
) (*putImageScanningConfigurationOutput, error) {
	cfg, err := h.Backend.PutImageScanningConfiguration(
		ctx,
		in.RepositoryName,
		in.ImageScanningConfiguration.ScanOnPush,
	)
	if err != nil {
		return nil, err
	}

	return &putImageScanningConfigurationOutput{
		ImageScanningConfiguration: imageScanningConfigurationView{ScanOnPush: cfg.ScanOnPush},
		RepositoryName:             cfg.RepositoryName,
	}, nil
}

type putImageTagMutabilityInput struct {
	ImageTagMutability                 string                         `json:"imageTagMutability"`
	RepositoryName                     string                         `json:"repositoryName"`
	RegistryID                         string                         `json:"registryId,omitempty"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
}

type putImageTagMutabilityOutput struct {
	ImageTagMutability                 string                         `json:"imageTagMutability"`
	RepositoryName                     string                         `json:"repositoryName"`
	RegistryID                         string                         `json:"registryId"`
	ImageTagMutabilityExclusionFilters []imageTagMutabilityFilterView `json:"imageTagMutabilityExclusionFilters,omitempty"`
}

func (h *Handler) handlePutImageTagMutability(
	ctx context.Context,
	in *putImageTagMutabilityInput,
) (*putImageTagMutabilityOutput, error) {
	filters := make(
		[]ImageTagMutabilityExclusionFilter,
		0,
		len(in.ImageTagMutabilityExclusionFilters),
	)
	for _, filter := range in.ImageTagMutabilityExclusionFilters {
		filters = append(filters, ImageTagMutabilityExclusionFilter(filter))
	}

	repo, err := h.Backend.PutImageTagMutability(
		ctx,
		in.RepositoryName,
		in.ImageTagMutability,
		filters,
	)
	if err != nil {
		return nil, err
	}

	view := toRepositoryView(*repo)

	return &putImageTagMutabilityOutput{
		ImageTagMutability:                 view.ImageTagMutability,
		ImageTagMutabilityExclusionFilters: view.ImageTagMutabilityExclusionFilters,
		RepositoryName:                     view.RepositoryName,
		RegistryID:                         view.RegistryID,
	}, nil
}

type listImageReferrersInput struct {
	RepositoryName string          `json:"repositoryName"`
	SubjectID      ImageIdentifier `json:"subjectId"`
	RegistryID     string          `json:"registryId,omitempty"`
}

type listImageReferrersOutput struct {
	Referrers []ImageReferrer `json:"referrers"`
}

func (h *Handler) handleListImageReferrers(
	ctx context.Context,
	in *listImageReferrersInput,
) (*listImageReferrersOutput, error) {
	referrers, err := h.Backend.ListImageReferrers(ctx, in.RepositoryName, in.SubjectID)
	if err != nil {
		return nil, err
	}

	return &listImageReferrersOutput{Referrers: referrers}, nil
}

type updateImageStorageClassInput struct {
	ImageID            ImageIdentifier `json:"imageId"`
	RepositoryName     string          `json:"repositoryName"`
	TargetStorageClass string          `json:"targetStorageClass"`
	RegistryID         string          `json:"registryId,omitempty"`
}

func (h *Handler) handleUpdateImageStorageClass(
	ctx context.Context,
	in *updateImageStorageClassInput,
) (*ImageStorageClassResult, error) {
	return h.Backend.UpdateImageStorageClass(
		ctx,
		in.RepositoryName,
		in.ImageID,
		in.TargetStorageClass,
	)
}

type accountSettingInput struct {
	Name  string `json:"name"`
	Value string `json:"value,omitempty"`
}

func (h *Handler) handleGetAccountSetting(
	ctx context.Context,
	in *accountSettingInput,
) (*accountSettingInput, error) {
	value, err := h.Backend.GetAccountSetting(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &accountSettingInput{Name: in.Name, Value: value}, nil
}

func (h *Handler) handlePutAccountSetting(
	ctx context.Context,
	in *accountSettingInput,
) (*accountSettingInput, error) {
	value, err := h.Backend.PutAccountSetting(ctx, in.Name, in.Value)
	if err != nil {
		return nil, err
	}

	return &accountSettingInput{Name: in.Name, Value: value}, nil
}

type pullTimeUpdateExclusionInput struct {
	PrincipalArn string `json:"principalArn"`
}

type registerPullTimeUpdateExclusionOutput struct {
	PrincipalArn string `json:"principalArn"`
	CreatedAt    int64  `json:"createdAt,omitempty"`
}

func (h *Handler) handleRegisterPullTimeUpdateExclusion(
	ctx context.Context,
	in *pullTimeUpdateExclusionInput,
) (*registerPullTimeUpdateExclusionOutput, error) {
	exclusion, err := h.Backend.RegisterPullTimeUpdateExclusion(ctx, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &registerPullTimeUpdateExclusionOutput{
		CreatedAt:    exclusion.CreatedAt.Unix(),
		PrincipalArn: exclusion.PrincipalArn,
	}, nil
}

func (h *Handler) handleDeregisterPullTimeUpdateExclusion(
	ctx context.Context,
	in *pullTimeUpdateExclusionInput,
) (*registerPullTimeUpdateExclusionOutput, error) {
	exclusion, err := h.Backend.DeregisterPullTimeUpdateExclusion(ctx, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &registerPullTimeUpdateExclusionOutput{
		CreatedAt:    exclusion.CreatedAt.Unix(),
		PrincipalArn: exclusion.PrincipalArn,
	}, nil
}

type listPullTimeUpdateExclusionsOutput struct {
	PullTimeUpdateExclusions []string `json:"pullTimeUpdateExclusions"`
}

func (h *Handler) handleListPullTimeUpdateExclusions(
	ctx context.Context,
	_ *emptyInput,
) (*listPullTimeUpdateExclusionsOutput, error) {
	exclusions, err := h.Backend.ListPullTimeUpdateExclusions(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(exclusions))
	for _, exclusion := range exclusions {
		out = append(out, exclusion.PrincipalArn)
	}

	return &listPullTimeUpdateExclusionsOutput{PullTimeUpdateExclusions: out}, nil
}

type repositoryPolicyInput struct {
	PolicyText     string `json:"policyText,omitempty"`
	RepositoryName string `json:"repositoryName"`
	RegistryID     string `json:"registryId,omitempty"`
	Force          bool   `json:"force,omitempty"`
}

func (h *Handler) handleGetRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.GetRepositoryPolicy(ctx, in.RepositoryName)
}

func (h *Handler) handleSetRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.SetRepositoryPolicy(ctx, in.RepositoryName, in.PolicyText)
}

func (h *Handler) handleDeleteRepositoryPolicy(
	ctx context.Context,
	in *repositoryPolicyInput,
) (*RepositoryPolicyResult, error) {
	return h.Backend.DeleteRepositoryPolicy(ctx, in.RepositoryName)
}
