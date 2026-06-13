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

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
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

var errUnknownAction = awserr.New("UnknownOperationException", awserr.ErrNotFound)

// Handler is the Echo HTTP handler for AWS CodeConnections operations (JSON 1.0 protocol).
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeConnections handler with a pre-built dispatch table.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnection":              service.WrapOp(h.handleCreateConnection),
		"GetConnection":                 service.WrapOp(h.handleGetConnection),
		"ListConnections":               service.WrapOp(h.handleListConnections),
		"DeleteConnection":              service.WrapOp(h.handleDeleteConnection),
		"TagResource":                   service.WrapOp(h.handleTagResource),
		"UntagResource":                 service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":           service.WrapOp(h.handleListTagsForResource),
		"CreateHost":                    service.WrapOp(h.handleCreateHost),
		"GetHost":                       service.WrapOp(h.handleGetHost),
		"DeleteHost":                    service.WrapOp(h.handleDeleteHost),
		"CreateRepositoryLink":          service.WrapOp(h.handleCreateRepositoryLink),
		"GetRepositoryLink":             service.WrapOp(h.handleGetRepositoryLink),
		"DeleteRepositoryLink":          service.WrapOp(h.handleDeleteRepositoryLink),
		"CreateSyncConfiguration":       service.WrapOp(h.handleCreateSyncConfiguration),
		"DeleteSyncConfiguration":       service.WrapOp(h.handleDeleteSyncConfiguration),
		"GetRepositorySyncStatus":       service.WrapOp(h.handleGetRepositorySyncStatus),
		"GetResourceSyncStatus":         service.WrapOp(h.handleGetResourceSyncStatus),
		"GetSyncBlockerSummary":         service.WrapOp(h.handleGetSyncBlockerSummary),
		"GetSyncConfiguration":          service.WrapOp(h.handleGetSyncConfiguration),
		"ListHosts":                     service.WrapOp(h.handleListHosts),
		"ListRepositoryLinks":           service.WrapOp(h.handleListRepositoryLinks),
		"ListRepositorySyncDefinitions": service.WrapOp(h.handleListRepositorySyncDefinitions),
		"ListSyncConfigurations":        service.WrapOp(h.handleListSyncConfigurations),
		"UpdateHost":                    service.WrapOp(h.handleUpdateHost),
		"UpdateRepositoryLink":          service.WrapOp(h.handleUpdateRepositoryLink),
		"UpdateSyncBlocker":             service.WrapOp(h.handleUpdateSyncBlocker),
		"UpdateSyncConfiguration":       service.WrapOp(h.handleUpdateSyncConfiguration),
	}
}

// Reset clears the backend state (test helper).
func (h *Handler) Reset() {
	h.Backend.Reset()
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
		"GetSyncBlockerSummary",
		"GetSyncConfiguration",
		"ListHosts",
		"ListRepositoryLinks",
		"ListRepositorySyncDefinitions",
		"ListSyncConfigurations",
		"UpdateHost",
		"UpdateRepositoryLink",
		"UpdateSyncBlocker",
		"UpdateSyncConfiguration",
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
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeConnections", ccContentType,
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleEchoError,
		)
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

func (h *Handler) handleEchoError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, statusCode := resolveErrorType(err)

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", ccContentType)

	return c.JSONBlob(statusCode, payload)
}

func resolveErrorType(err error) (string, int) {
	switch {
	case errors.Is(err, ErrNotFound):
		return "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		return "ResourceAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
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

// tagsToSortedArray converts a tag map to a sorted array for deterministic output.
func tagsToSortedArray(m map[string]string) []tag {
	keys := sortedTagKeys(m)
	out := make([]tag, 0, len(keys))

	for _, k := range keys {
		out = append(out, tag{Key: k, Value: m[k]})
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
	HostArn        string `json:"HostArn"`
	ProviderType   string `json:"ProviderType"`
	Tags           []tag  `json:"Tags"`
}

type createConnectionOutput struct {
	ConnectionArn string `json:"ConnectionArn"`
	Tags          []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateConnection(
	ctx context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	conn, err := h.Backend.CreateConnection(
		ctx,
		in.ConnectionName,
		in.ProviderType,
		in.HostArn,
		tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{
		ConnectionArn: conn.ConnectionArn,
		Tags:          tagsToSortedArray(conn.Tags),
	}, nil
}

type getConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type connectionItem struct {
	ConnectionName   string `json:"ConnectionName"`
	ConnectionArn    string `json:"ConnectionArn"`
	HostArn          string `json:"HostArn,omitempty"`
	OwnerAccountID   string `json:"OwnerAccountId"`
	ProviderType     string `json:"ProviderType"`
	ConnectionStatus string `json:"ConnectionStatus"`
	Tags             []tag  `json:"Tags,omitempty"`
}

type getConnectionOutput struct {
	Connection connectionItem `json:"Connection"`
}

func (h *Handler) handleGetConnection(
	ctx context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	conn, err := h.Backend.GetConnection(ctx, in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connToItem(conn)}, nil
}

type listConnectionsInput struct {
	NextToken          *string `json:"NextToken"`
	MaxResults         *int32  `json:"MaxResults"`
	HostArnFilter      string  `json:"HostArnFilter"`
	ProviderTypeFilter string  `json:"ProviderTypeFilter"`
}

type listConnectionsOutput struct {
	NextToken   *string          `json:"NextToken,omitempty"`
	Connections []connectionItem `json:"Connections"`
}

func (h *Handler) handleListConnections(
	ctx context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	conns := h.Backend.ListConnections(ctx, in.ProviderTypeFilter, in.HostArnFilter)

	// Sort for stable pagination.
	sort.Slice(conns, func(i, j int) bool {
		return conns[i].ConnectionName < conns[j].ConnectionName
	})

	all := make([]connectionItem, 0, len(conns))
	for _, conn := range conns {
		all = append(all, connToItem(conn))
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

func (h *Handler) handleDeleteConnection(
	ctx context.Context,
	in *deleteConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteConnection(ctx, in.ConnectionArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func connToItem(conn *Connection) connectionItem {
	return connectionItem{
		ConnectionName:   conn.ConnectionName,
		ConnectionArn:    conn.ConnectionArn,
		ProviderType:     conn.ProviderType,
		ConnectionStatus: conn.Status,
		OwnerAccountID:   conn.OwnerAccountID,
		HostArn:          conn.HostArn,
		Tags:             tagsToSortedArray(conn.Tags),
	}
}

// --- Tag handlers ---

type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []tag  `json:"Tags"`
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*emptyOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
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
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedArray(tags)}, nil
}

// --- Host handlers ---

type createHostInput struct {
	Name             string `json:"Name"`
	ProviderType     string `json:"ProviderType"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	Tags             []tag  `json:"Tags"`
}

type createHostOutput struct {
	HostArn string `json:"HostArn"`
	Tags    []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	ctx context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	host, err := h.Backend.CreateHost(
		ctx,
		in.Name,
		in.ProviderType,
		in.ProviderEndpoint,
		tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn, Tags: tagsToSortedArray(host.Tags)}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

type getHostOutput struct {
	Name             string `json:"Name"`
	HostArn          string `json:"HostArn,omitempty"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	ProviderType     string `json:"ProviderType"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	Tags             []tag  `json:"Tags,omitempty"`
}

func (h *Handler) handleGetHost(ctx context.Context, in *getHostInput) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	host, err := h.Backend.GetHost(ctx, in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{
		Name:             host.Name,
		HostArn:          host.HostArn,
		ProviderEndpoint: host.ProviderEndpoint,
		ProviderType:     host.ProviderType,
		Status:           host.Status,
		StatusMessage:    host.StatusMessage,
		Tags:             tagsToSortedArray(host.Tags),
	}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

func (h *Handler) handleDeleteHost(ctx context.Context, in *deleteHostInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteHost(ctx, in.HostArn); err != nil {
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
	EncryptionKeyArn  string `json:"EncryptionKeyArn,omitempty"`
	OwnerID           string `json:"OwnerId"`
	ProviderType      string `json:"ProviderType"`
	RepositoryLinkArn string `json:"RepositoryLinkArn"`
	RepositoryLinkID  string `json:"RepositoryLinkId"`
	RepositoryName    string `json:"RepositoryName"`
}

type createRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleCreateRepositoryLink(
	ctx context.Context,
	in *createRepositoryLinkInput,
) (*createRepositoryLinkOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", ErrValidation)
	}

	if in.OwnerID == "" {
		return nil, fmt.Errorf("%w: OwnerId is required", ErrValidation)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: RepositoryName is required", ErrValidation)
	}

	link, err := h.Backend.CreateRepositoryLink(
		ctx,
		in.ConnectionArn,
		in.OwnerID,
		in.RepositoryName,
		in.EncryptionKeyArn,
	)
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
	ctx context.Context,
	in *getRepositoryLinkInput,
) (*getRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	link, err := h.Backend.GetRepositoryLink(ctx, in.RepositoryLinkID)
	if err != nil {
		return nil, err
	}

	return &getRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

type deleteRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
}

func (h *Handler) handleDeleteRepositoryLink(
	ctx context.Context,
	in *deleteRepositoryLinkInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteRepositoryLink(ctx, in.RepositoryLinkID); err != nil {
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
	ctx context.Context,
	in *createSyncConfigurationInput,
) (*createSyncConfigurationOutput, error) {
	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", ErrValidation)
	}

	if in.ConfigFile == "" {
		return nil, fmt.Errorf("%w: ConfigFile is required", ErrValidation)
	}

	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.CreateSyncConfiguration(
		ctx,
		in.Branch,
		in.ConfigFile,
		in.RepositoryLinkID,
		in.ResourceName,
		in.RoleArn,
		in.SyncType,
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
	ctx context.Context,
	in *deleteSyncConfigurationInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteSyncConfiguration(ctx, in.ResourceName, in.SyncType); err != nil {
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
	ExternalID string `json:"ExternalId,omitempty"`
	Time       string `json:"Time"`
	Type       string `json:"Type"`
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
	ctx context.Context,
	in *getRepositorySyncStatusInput,
) (*getRepositorySyncStatusOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	status, err := h.Backend.GetRepositorySyncStatus(
		ctx,
		in.RepositoryLinkID,
		in.Branch,
		in.SyncType,
	)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

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
	ctx context.Context,
	in *getResourceSyncStatusInput,
) (*getResourceSyncStatusOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	status, err := h.Backend.GetResourceSyncStatus(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

	return &getResourceSyncStatusOutput{
		LatestSync: resourceSyncAttemptItem{
			StartedAt: status.StartedAt.Format(time.RFC3339),
			Status:    status.Status,
			Events:    events,
		},
	}, nil
}

// buildSyncEventItems converts backend SyncEvents to handler response items.
func buildSyncEventItems(evts []SyncEvent) []syncEventItem {
	out := make([]syncEventItem, 0, len(evts))

	for _, e := range evts {
		out = append(out, syncEventItem{
			Event:      e.Event,
			Time:       e.Time.Format(time.RFC3339),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		})
	}

	return out
}

// --- ListHosts ---

type listHostsInput struct {
	NextToken  *string `json:"NextToken"`
	MaxResults *int32  `json:"MaxResults"`
}

type hostItem struct {
	HostArn          string `json:"HostArn"`
	Name             string `json:"Name"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
	ProviderType     string `json:"ProviderType"`
	Status           string `json:"Status"`
	StatusMessage    string `json:"StatusMessage,omitempty"`
	Tags             []tag  `json:"Tags,omitempty"`
}

type listHostsOutput struct {
	NextToken *string    `json:"NextToken,omitempty"`
	Hosts     []hostItem `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	ctx context.Context,
	in *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts(ctx)
	items := make([]hostItem, len(hosts))

	for i, host := range hosts {
		items[i] = hostItem{
			HostArn:          host.HostArn,
			Name:             host.Name,
			ProviderEndpoint: host.ProviderEndpoint,
			ProviderType:     host.ProviderType,
			Status:           host.Status,
			StatusMessage:    host.StatusMessage,
			Tags:             tagsToSortedArray(host.Tags),
		}
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listHostsOutput{Hosts: p.Data, NextToken: nextToken}, nil
}

// --- UpdateHost ---

type updateHostInput struct {
	HostArn          string `json:"HostArn"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
}

func (h *Handler) handleUpdateHost(ctx context.Context, in *updateHostInput) (*emptyOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", ErrValidation)
	}

	if err := h.Backend.UpdateHost(ctx, in.HostArn, in.ProviderEndpoint); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- ListRepositoryLinks ---

type listRepositoryLinksInput struct {
	NextToken  *string `json:"NextToken"`
	MaxResults *int32  `json:"MaxResults"`
}

type listRepositoryLinksOutput struct {
	NextToken       *string              `json:"NextToken,omitempty"`
	RepositoryLinks []repositoryLinkItem `json:"RepositoryLinks"`
}

func (h *Handler) handleListRepositoryLinks(
	ctx context.Context,
	in *listRepositoryLinksInput,
) (*listRepositoryLinksOutput, error) {
	links := h.Backend.ListRepositoryLinks(ctx)
	items := make([]repositoryLinkItem, len(links))

	for i, link := range links {
		items[i] = repositoryLinkToItem(link)
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listRepositoryLinksOutput{RepositoryLinks: p.Data, NextToken: nextToken}, nil
}

// --- UpdateRepositoryLink ---

type updateRepositoryLinkInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	ConnectionArn    string `json:"ConnectionArn"`
	EncryptionKeyArn string `json:"EncryptionKeyArn"`
}

type updateRepositoryLinkOutput struct {
	RepositoryLinkInfo repositoryLinkItem `json:"RepositoryLinkInfo"`
}

func (h *Handler) handleUpdateRepositoryLink(
	ctx context.Context,
	in *updateRepositoryLinkInput,
) (*updateRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	link, err := h.Backend.UpdateRepositoryLink(
		ctx,
		in.RepositoryLinkID,
		in.ConnectionArn,
		in.EncryptionKeyArn,
	)
	if err != nil {
		return nil, err
	}

	return &updateRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
}

// --- GetSyncConfiguration ---

type getSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type getSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleGetSyncConfiguration(
	ctx context.Context,
	in *getSyncConfigurationInput,
) (*getSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.GetSyncConfiguration(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	return &getSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

// --- ListSyncConfigurations ---

type listSyncConfigurationsInput struct {
	NextToken        *string `json:"NextToken"`
	MaxResults       *int32  `json:"MaxResults"`
	RepositoryLinkID string  `json:"RepositoryLinkId"`
	SyncType         string  `json:"SyncType"`
}

type listSyncConfigurationsOutput struct {
	NextToken          *string                 `json:"NextToken,omitempty"`
	SyncConfigurations []syncConfigurationItem `json:"SyncConfigurations"`
}

func (h *Handler) handleListSyncConfigurations(
	ctx context.Context,
	in *listSyncConfigurationsInput,
) (*listSyncConfigurationsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	cfgs := h.Backend.ListSyncConfigurations(ctx, in.RepositoryLinkID, in.SyncType)
	items := make([]syncConfigurationItem, len(cfgs))

	for i, cfg := range cfgs {
		items[i] = syncConfigToItem(cfg)
	}

	var limit int
	if in.MaxResults != nil && *in.MaxResults > 0 {
		limit = int(*in.MaxResults)
	}

	token := ""
	if in.NextToken != nil {
		token = *in.NextToken
	}

	p := page.New(items, token, limit, ccDefaultPageSize)

	var nextToken *string
	if p.Next != "" {
		nextToken = &p.Next
	}

	return &listSyncConfigurationsOutput{SyncConfigurations: p.Data, NextToken: nextToken}, nil
}

// --- UpdateSyncConfiguration ---

type updateSyncConfigurationInput struct {
	ResourceName     string `json:"ResourceName"`
	SyncType         string `json:"SyncType"`
	Branch           string `json:"Branch"`
	ConfigFile       string `json:"ConfigFile"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	RoleArn          string `json:"RoleArn"`
}

type updateSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleUpdateSyncConfiguration(
	ctx context.Context,
	in *updateSyncConfigurationInput,
) (*updateSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	cfg, err := h.Backend.UpdateSyncConfiguration(
		ctx,
		in.ResourceName,
		in.SyncType,
		in.Branch,
		in.ConfigFile,
		in.RepositoryLinkID,
		in.RoleArn,
	)
	if err != nil {
		return nil, err
	}

	return &updateSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

// --- ListRepositorySyncDefinitions ---

type listRepositorySyncDefinitionsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type repositorySyncDefinitionItem struct {
	Branch    string `json:"Branch"`
	Directory string `json:"Directory"`
	Parent    string `json:"Parent,omitempty"`
	Target    string `json:"Target"`
}

type listRepositorySyncDefinitionsOutput struct {
	RepositorySyncDefinitions []repositorySyncDefinitionItem `json:"RepositorySyncDefinitions"`
}

func (h *Handler) handleListRepositorySyncDefinitions(
	ctx context.Context,
	in *listRepositorySyncDefinitionsInput,
) (*listRepositorySyncDefinitionsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", ErrValidation)
	}

	defs, err := h.Backend.ListRepositorySyncDefinitions(ctx, in.RepositoryLinkID, in.SyncType)
	if err != nil {
		return nil, err
	}

	items := make([]repositorySyncDefinitionItem, len(defs))
	for i, d := range defs {
		items[i] = repositorySyncDefinitionItem(d)
	}

	return &listRepositorySyncDefinitionsOutput{RepositorySyncDefinitions: items}, nil
}

// --- GetSyncBlockerSummary ---

type getSyncBlockerSummaryInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type syncBlockerItem struct {
	ID            string `json:"Id"`
	Type          string `json:"Type"`
	Status        string `json:"Status"`
	CreatedAt     string `json:"CreatedAt"`
	CreatedReason string `json:"CreatedReason"`
}

type syncBlockerSummaryItem struct {
	ResourceName       string            `json:"ResourceName"`
	ParentResourceName string            `json:"ParentResourceName,omitempty"`
	LatestBlockers     []syncBlockerItem `json:"LatestBlockers"`
}

type getSyncBlockerSummaryOutput struct {
	SyncBlockerSummary syncBlockerSummaryItem `json:"SyncBlockerSummary"`
}

func (h *Handler) handleGetSyncBlockerSummary(
	ctx context.Context,
	in *getSyncBlockerSummaryInput,
) (*getSyncBlockerSummaryOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", ErrValidation)
	}

	summary, err := h.Backend.GetSyncBlockerSummary(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	blockers := make([]syncBlockerItem, len(summary.LatestBlockers))
	for i, b := range summary.LatestBlockers {
		blockers[i] = syncBlockerItem{
			ID:            b.ID,
			Type:          b.Type,
			Status:        b.Status,
			CreatedAt:     b.CreatedAt.Format(time.RFC3339),
			CreatedReason: b.CreatedReason,
		}
	}

	return &getSyncBlockerSummaryOutput{
		SyncBlockerSummary: syncBlockerSummaryItem{
			ResourceName:       summary.ResourceName,
			ParentResourceName: summary.ParentResourceName,
			LatestBlockers:     blockers,
		},
	}, nil
}

// --- UpdateSyncBlocker ---

type updateSyncBlockerInput struct {
	ID             string `json:"Id"`
	ResolvedReason string `json:"ResolvedReason"`
	ResourceName   string `json:"ResourceName"`
	SyncType       string `json:"SyncType"`
}

type updateSyncBlockerOutput struct {
	SyncBlockerSummary syncBlockerSummaryItem `json:"SyncBlockerSummary"`
}

func (h *Handler) handleUpdateSyncBlocker(
	ctx context.Context,
	in *updateSyncBlockerInput,
) (*updateSyncBlockerOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrValidation)
	}

	summary, err := h.Backend.UpdateSyncBlocker(ctx, in.ID, in.ResolvedReason)
	if err != nil {
		return nil, err
	}

	return &updateSyncBlockerOutput{
		SyncBlockerSummary: syncBlockerSummaryItem{
			ResourceName:   summary.ResourceName,
			LatestBlockers: []syncBlockerItem{},
		},
	}, nil
}
