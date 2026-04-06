package codeconnections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codeconnectionsMatchPriority = service.PriorityHeaderExact
	ccTargetPrefix               = "CodeConnections_20231201."
	ccContentType                = "application/x-amz-json-1.0"
	ccDefaultPageSize            = 100
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
		"CreateHost",
		"GetHost",
		"DeleteHost",
		"CreateRepositoryLink",
		"GetRepositoryLink",
		"DeleteRepositoryLink",
		"CreateSyncConfiguration",
		"DeleteSyncConfiguration",
		"GetRepositorySyncStatus",
		"GetResourceSyncStatus",
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
		ConnectionArn    string `json:"ConnectionArn"`
		ResourceArn      string `json:"ResourceArn"`
		HostArn          string `json:"HostArn"`
		RepositoryLinkID string `json:"RepositoryLinkId"`
		ResourceName     string `json:"ResourceName"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ConnectionArn != "":
		return req.ConnectionArn
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.HostArn != "":
		return req.HostArn
	case req.RepositoryLinkID != "":
		return req.RepositoryLinkID
	case req.ResourceName != "":
		return req.ResourceName
	default:
		return ""
	}
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
		"CreateConnection":        service.WrapOp(h.handleCreateConnection),
		"GetConnection":           service.WrapOp(h.handleGetConnection),
		"ListConnections":         service.WrapOp(h.handleListConnections),
		"DeleteConnection":        service.WrapOp(h.handleDeleteConnection),
		"TagResource":             service.WrapOp(h.handleTagResource),
		"UntagResource":           service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":     service.WrapOp(h.handleListTagsForResource),
		"CreateHost":              service.WrapOp(h.handleCreateHost),
		"GetHost":                 service.WrapOp(h.handleGetHost),
		"DeleteHost":              service.WrapOp(h.handleDeleteHost),
		"CreateRepositoryLink":    service.WrapOp(h.handleCreateRepositoryLink),
		"GetRepositoryLink":       service.WrapOp(h.handleGetRepositoryLink),
		"DeleteRepositoryLink":    service.WrapOp(h.handleDeleteRepositoryLink),
		"CreateSyncConfiguration": service.WrapOp(h.handleCreateSyncConfiguration),
		"DeleteSyncConfiguration": service.WrapOp(h.handleDeleteSyncConfiguration),
		"GetRepositorySyncStatus": service.WrapOp(h.handleGetRepositorySyncStatus),
		"GetResourceSyncStatus":   service.WrapOp(h.handleGetResourceSyncStatus),
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
	NextToken          *string `json:"NextToken"`
	MaxResults         *int32  `json:"MaxResults"`
	ProviderTypeFilter string  `json:"ProviderTypeFilter"`
}

type listConnectionsOutput struct {
	NextToken   *string          `json:"NextToken,omitempty"`
	Connections []connectionItem `json:"Connections"`
}

func (h *Handler) handleListConnections(_ context.Context, in *listConnectionsInput) (*listConnectionsOutput, error) {
	conns := h.Backend.ListConnections(in.ProviderTypeFilter)

	// Sort for stable pagination.
	sort.Slice(conns, func(i, j int) bool {
		return conns[i].ConnectionName < conns[j].ConnectionName
	})

	all := make([]connectionItem, 0, len(conns))
	for _, conn := range conns {
		all = append(all, connectionItem{
			ConnectionName:   conn.ConnectionName,
			ConnectionArn:    conn.ConnectionArn,
			ProviderType:     conn.ProviderType,
			ConnectionStatus: conn.Status,
			OwnerAccountID:   conn.OwnerAccountID,
		})
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(all, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listConnectionsOutput{Connections: p.Data, NextToken: nextToken}, nil
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

// --- Host handlers ---

type createHostInput struct {
	Name             string `json:"Name"`
	ProviderType     string `json:"ProviderType"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
}

type createHostOutput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleCreateHost(_ context.Context, in *createHostInput) (*createHostOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errValidation)
	}

	if in.ProviderEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", errValidation)
	}

	host, err := h.Backend.CreateHost(in.Name, in.ProviderType, in.ProviderEndpoint)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

type getHostOutput struct {
	Name             string `json:"Name"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	ProviderType     string `json:"ProviderType"`
	Status           string `json:"Status"`
}

func (h *Handler) handleGetHost(_ context.Context, in *getHostInput) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errValidation)
	}

	host, err := h.Backend.GetHost(in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{
		Name:             host.Name,
		ProviderEndpoint: host.ProviderEndpoint,
		ProviderType:     host.ProviderType,
		Status:           host.Status,
	}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleDeleteHost(_ context.Context, in *deleteHostInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteHost(in.HostArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- RepositoryLink handlers ---

type createRepositoryLinkInput struct {
	ConnectionArn    string `json:"ConnectionArn"`
	OwnerID          string `json:"OwnerId"`
	RepositoryName   string `json:"RepositoryName"`
	EncryptionKeyArn string `json:"EncryptionKeyArn"`
}

type repositoryLinkItem struct {
	ConnectionArn     string `json:"ConnectionArn"`
	OwnerID           string `json:"OwnerId"`
	ProviderType      string `json:"ProviderType"`
	RepositoryLinkArn string `json:"RepositoryLinkArn"`
	RepositoryLinkID  string `json:"RepositoryLinkId"`
	RepositoryName    string `json:"RepositoryName"`
	EncryptionKeyArn  string `json:"EncryptionKeyArn,omitempty"`
}

type createRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleCreateRepositoryLink(
	_ context.Context,
	in *createRepositoryLinkInput,
) (*createRepositoryLinkOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errValidation)
	}

	if in.OwnerID == "" {
		return nil, fmt.Errorf("%w: OwnerId is required", errValidation)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: RepositoryName is required", errValidation)
	}

	link, err := h.Backend.CreateRepositoryLink(in.ConnectionArn, in.OwnerID, in.RepositoryName, in.EncryptionKeyArn)
	if err != nil {
		return nil, err
	}

	return &createRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

type getRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
}

type getRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleGetRepositoryLink(
	_ context.Context,
	in *getRepositoryLinkInput,
) (*getRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errValidation)
	}

	link, err := h.Backend.GetRepositoryLink(in.RepositoryLinkID)
	if err != nil {
		return nil, err
	}

	return &getRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

type deleteRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
}

func (h *Handler) handleDeleteRepositoryLink(
	_ context.Context,
	in *deleteRepositoryLinkInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteRepositoryLink(in.RepositoryLinkID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func repositoryLinkToItem(link *RepositoryLink) repositoryLinkItem {
	return repositoryLinkItem{
		ConnectionArn:     link.ConnectionArn,
		OwnerID:           link.OwnerID,
		ProviderType:      link.ProviderType,
		RepositoryLinkArn: link.RepositoryLinkArn,
		RepositoryLinkID:  link.RepositoryLinkID,
		RepositoryName:    link.RepositoryName,
		EncryptionKeyArn:  link.EncryptionKeyArn,
	}
}

// --- SyncConfiguration handlers ---

type createSyncConfigurationInput struct {
	Branch           string `json:"Branch"`
	ConfigFile       string `json:"ConfigFile"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	ResourceName     string `json:"ResourceName"`
	RoleArn          string `json:"RoleArn"`
	SyncType         string `json:"SyncType"`
}

type syncConfigurationItem struct {
	Branch           string `json:"Branch"`
	ConfigFile       string `json:"ConfigFile"`
	OwnerID          string `json:"OwnerId"`
	ProviderType     string `json:"ProviderType"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	RepositoryName   string `json:"RepositoryName"`
	ResourceName     string `json:"ResourceName"`
	RoleArn          string `json:"RoleArn"`
	SyncType         string `json:"SyncType"`
}

type createSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleCreateSyncConfiguration(
	_ context.Context,
	in *createSyncConfigurationInput,
) (*createSyncConfigurationOutput, error) {
	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", errValidation)
	}

	if in.ConfigFile == "" {
		return nil, fmt.Errorf("%w: ConfigFile is required", errValidation)
	}

	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errValidation)
	}

	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errValidation)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errValidation)
	}

	cfg, err := h.Backend.CreateSyncConfiguration(
		in.Branch, in.ConfigFile, in.RepositoryLinkID, in.ResourceName, in.RoleArn, in.SyncType,
	)
	if err != nil {
		return nil, err
	}

	return &createSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type deleteSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

func (h *Handler) handleDeleteSyncConfiguration(
	_ context.Context,
	in *deleteSyncConfigurationInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteSyncConfiguration(in.ResourceName, in.SyncType); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func syncConfigToItem(cfg *SyncConfiguration) syncConfigurationItem {
	return syncConfigurationItem{
		Branch:           cfg.Branch,
		ConfigFile:       cfg.ConfigFile,
		OwnerID:          cfg.OwnerID,
		ProviderType:     cfg.ProviderType,
		RepositoryLinkID: cfg.RepositoryLinkID,
		RepositoryName:   cfg.RepositoryName,
		ResourceName:     cfg.ResourceName,
		RoleArn:          cfg.RoleArn,
		SyncType:         cfg.SyncType,
	}
}

// --- Sync status handlers ---

type getRepositorySyncStatusInput struct {
	Branch           string `json:"Branch"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type syncEventItem struct {
	Event      string `json:"Event"`
	Time       string `json:"Time"`
	Type       string `json:"Type"`
	ExternalID string `json:"ExternalId,omitempty"`
}

type repositorySyncAttemptItem struct {
	StartedAt string          `json:"StartedAt"`
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
}

type getRepositorySyncStatusOutput struct {
	LatestSync repositorySyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetRepositorySyncStatus(
	_ context.Context,
	in *getRepositorySyncStatusInput,
) (*getRepositorySyncStatusOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errValidation)
	}

	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", errValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errValidation)
	}

	status, err := h.Backend.GetRepositorySyncStatus(in.RepositoryLinkID, in.Branch, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := make([]syncEventItem, 0, len(status.Events))
	for _, e := range status.Events {
		events = append(events, syncEventItem{
			Event:      e.Event,
			Time:       e.Time.Format(time.RFC3339),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		})
	}

	return &getRepositorySyncStatusOutput{
		LatestSync: repositorySyncAttemptItem{
			StartedAt: status.StartedAt.Format(time.RFC3339),
			Status:    status.Status,
			Events:    events,
		},
	}, nil
}

type getResourceSyncStatusInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type resourceSyncAttemptItem struct {
	StartedAt string          `json:"StartedAt"`
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
}

type getResourceSyncStatusOutput struct {
	LatestSync resourceSyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetResourceSyncStatus(
	_ context.Context,
	in *getResourceSyncStatusInput,
) (*getResourceSyncStatusOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errValidation)
	}

	status, err := h.Backend.GetResourceSyncStatus(in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := make([]syncEventItem, 0, len(status.Events))
	for _, e := range status.Events {
		events = append(events, syncEventItem{
			Event:      e.Event,
			Time:       e.Time.Format(time.RFC3339),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		})
	}

	return &getResourceSyncStatusOutput{
		LatestSync: resourceSyncAttemptItem{
			StartedAt: status.StartedAt.Format(time.RFC3339),
			Status:    status.Status,
			Events:    events,
		},
	}, nil
}
