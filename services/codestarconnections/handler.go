package codestarconnections

import (
	"context"
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
	codestarTargetPrefix = "CodeStar_connections_20191201."
)

var (
	errUnknownAction  = awserr.New("UnknownOperationException", awserr.ErrNotFound)
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for CodeStar Connections operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler creates a new CodeStar Connections handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears the backend state (test helper).
func (h *Handler) Reset() {
	h.Backend.Reset()
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnection":              service.WrapOp(h.handleCreateConnection),
		"GetConnection":                 service.WrapOp(h.handleGetConnection),
		"ListConnections":               service.WrapOp(h.handleListConnections),
		"DeleteConnection":              service.WrapOp(h.handleDeleteConnection),
		"CreateHost":                    service.WrapOp(h.handleCreateHost),
		"GetHost":                       service.WrapOp(h.handleGetHost),
		"ListHosts":                     service.WrapOp(h.handleListHosts),
		"DeleteHost":                    service.WrapOp(h.handleDeleteHost),
		"UpdateHost":                    service.WrapOp(h.handleUpdateHost),
		"ListTagsForResource":           service.WrapOp(h.handleListTagsForResource),
		"TagResource":                   service.WrapOp(h.handleTagResource),
		"UntagResource":                 service.WrapOp(h.handleUntagResource),
		"CreateRepositoryLink":          service.WrapOp(h.handleCreateRepositoryLink),
		"GetRepositoryLink":             service.WrapOp(h.handleGetRepositoryLink),
		"DeleteRepositoryLink":          service.WrapOp(h.handleDeleteRepositoryLink),
		"ListRepositoryLinks":           service.WrapOp(h.handleListRepositoryLinks),
		"CreateSyncConfiguration":       service.WrapOp(h.handleCreateSyncConfiguration),
		"GetSyncConfiguration":          service.WrapOp(h.handleGetSyncConfiguration),
		"DeleteSyncConfiguration":       service.WrapOp(h.handleDeleteSyncConfiguration),
		"GetRepositorySyncStatus":       service.WrapOp(h.handleGetRepositorySyncStatus),
		"GetResourceSyncStatus":         service.WrapOp(h.handleGetResourceSyncStatus),
		"GetSyncBlockerSummary":         service.WrapOp(h.handleGetSyncBlockerSummary),
		"ListRepositorySyncDefinitions": service.WrapOp(h.handleListRepositorySyncDefinitions),
		"ListSyncConfigurations":        service.WrapOp(h.handleListSyncConfigurations),
		"UpdateRepositoryLink":          service.WrapOp(h.handleUpdateRepositoryLink),
		"UpdateSyncBlocker":             service.WrapOp(h.handleUpdateSyncBlocker),
		"UpdateSyncConfiguration":       service.WrapOp(h.handleUpdateSyncConfiguration),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeStarConnections" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateConnection",
		"GetConnection",
		"ListConnections",
		"DeleteConnection",
		"CreateHost",
		"GetHost",
		"ListHosts",
		"DeleteHost",
		"UpdateHost",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"CreateRepositoryLink",
		"GetRepositoryLink",
		"DeleteRepositoryLink",
		"ListRepositoryLinks",
		"CreateSyncConfiguration",
		"GetSyncConfiguration",
		"DeleteSyncConfiguration",
		"GetRepositorySyncStatus",
		"GetResourceSyncStatus",
		"GetSyncBlockerSummary",
		"ListRepositorySyncDefinitions",
		"ListSyncConfigurations",
		"UpdateRepositoryLink",
		"UpdateSyncBlocker",
		"UpdateSyncConfiguration",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codestar-connections" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches CodeStar Connections requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codestarTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeStar Connections action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, codestarTargetPrefix)
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

// Handler returns the Echo handler function for CodeStar Connections requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeStarConnections", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
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

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "InvalidInputException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrValidation):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ValidationException",
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

// tagEntry is a key-value pair used in the API tag array format.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToSortedArray converts a tag map to a sorted array for deterministic output.
// Returns an empty (non-nil) slice when tags is empty or nil.
func tagsToSortedArray(tags map[string]string) []tagEntry {
	if len(tags) == 0 {
		return []tagEntry{}
	}

	keys := sortedTagKeys(tags)
	result := make([]tagEntry, len(keys))

	for i, k := range keys {
		result[i] = tagEntry{Key: k, Value: tags[k]}
	}

	return result
}

func tagsFromArray(entries []tagEntry) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

// --- Connection operations ---

type createConnectionInput struct {
	ConnectionName string     `json:"ConnectionName"`
	ProviderType   string     `json:"ProviderType"`
	HostArn        string     `json:"HostArn"`
	Tags           []tagEntry `json:"Tags"`
}

type createConnectionOutput struct {
	ConnectionArn string     `json:"ConnectionArn"`
	Tags          []tagEntry `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	conn, err := h.Backend.CreateConnection(
		in.ConnectionName, in.ProviderType, in.HostArn, tagsFromArray(in.Tags),
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

type connectionView struct {
	ConnectionName   string     `json:"ConnectionName"`
	ConnectionArn    string     `json:"ConnectionArn"`
	ConnectionStatus string     `json:"ConnectionStatus"`
	OwnerAccountID   string     `json:"OwnerAccountId"`
	ProviderType     string     `json:"ProviderType"`
	HostArn          string     `json:"HostArn,omitempty"`
	Tags             []tagEntry `json:"Tags,omitempty"`
}

type getConnectionOutput struct {
	Connection connectionView `json:"Connection"`
}

func connectionToView(c *Connection) connectionView {
	return connectionView{
		ConnectionName:   c.ConnectionName,
		ConnectionArn:    c.ConnectionArn,
		ConnectionStatus: c.ConnectionStatus,
		OwnerAccountID:   c.OwnerAccountID,
		ProviderType:     c.ProviderType,
		HostArn:          c.HostArn,
		Tags:             tagsToSortedArray(c.Tags),
	}
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	conn, err := h.Backend.GetConnection(in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connectionToView(conn)}, nil
}

type listConnectionsInput struct {
	ProviderTypeFilter string `json:"ProviderTypeFilter"`
	HostArnFilter      string `json:"HostArnFilter"`
	NextToken          string `json:"NextToken"`
	MaxResults         int32  `json:"MaxResults"`
}

type listConnectionsOutput struct {
	Connections []connectionView `json:"Connections"`
}

func (h *Handler) handleListConnections(
	_ context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	connections := h.Backend.ListConnections(in.ProviderTypeFilter, in.HostArnFilter)

	views := make([]connectionView, len(connections))
	for i, c := range connections {
		views[i] = connectionToView(c)
	}

	return &listConnectionsOutput{Connections: views}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct{}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnection(in.ConnectionArn); err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{}, nil
}

// --- Host operations ---

type createHostInput struct {
	Name             string     `json:"Name"`
	ProviderType     string     `json:"ProviderType"`
	ProviderEndpoint string     `json:"ProviderEndpoint"`
	Tags             []tagEntry `json:"Tags"`
}

type createHostOutput struct {
	HostArn string     `json:"HostArn"`
	Tags    []tagEntry `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	_ context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	host, err := h.Backend.CreateHost(in.Name, in.ProviderType, in.ProviderEndpoint, tagsFromArray(in.Tags))
	if err != nil {
		return nil, err
	}

	return &createHostOutput{HostArn: host.HostArn, Tags: tagsToSortedArray(host.Tags)}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

type hostView struct {
	Name             string     `json:"Name"`
	HostArn          string     `json:"HostArn"`
	ProviderType     string     `json:"ProviderType"`
	ProviderEndpoint string     `json:"ProviderEndpoint"`
	Status           string     `json:"Status"`
	StatusMessage    string     `json:"StatusMessage,omitempty"`
	Tags             []tagEntry `json:"Tags,omitempty"`
}

type getHostOutput struct {
	hostView
}

func hostToView(h *Host) hostView {
	return hostView{
		Name:             h.Name,
		HostArn:          h.HostArn,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		StatusMessage:    h.StatusMessage,
		Tags:             tagsToSortedArray(h.Tags),
	}
}

func (h *Handler) handleGetHost(
	_ context.Context,
	in *getHostInput,
) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	host, err := h.Backend.GetHost(in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{hostToView(host)}, nil
}

type listHostsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listHostsOutput struct {
	Hosts []hostView `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	_ context.Context,
	_ *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts()

	views := make([]hostView, len(hosts))
	for i, host := range hosts {
		views[i] = hostToView(host)
	}

	return &listHostsOutput{Hosts: views}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

type deleteHostOutput struct{}

func (h *Handler) handleDeleteHost(
	_ context.Context,
	in *deleteHostInput,
) (*deleteHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHost(in.HostArn); err != nil {
		return nil, err
	}

	return &deleteHostOutput{}, nil
}

type updateHostInput struct {
	HostArn          string `json:"HostArn"`
	ProviderEndpoint string `json:"ProviderEndpoint"`
}

type updateHostOutput struct{}

func (h *Handler) handleUpdateHost(
	_ context.Context,
	in *updateHostInput,
) (*updateHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateHost(in.HostArn, in.ProviderEndpoint); err != nil {
		return nil, err
	}

	return &updateHostOutput{}, nil
}

// --- Tagging operations ---

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedArray(tags)}, nil
}

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- RepositoryLink operations ---

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
	_ context.Context,
	in *createRepositoryLinkInput,
) (*createRepositoryLinkOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	if in.OwnerID == "" {
		return nil, fmt.Errorf("%w: OwnerId is required", errInvalidRequest)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: RepositoryName is required", errInvalidRequest)
	}

	link, err := h.Backend.CreateRepositoryLink(
		in.ConnectionArn, in.OwnerID, in.RepositoryName, in.EncryptionKeyArn,
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
	_ context.Context,
	in *getRepositoryLinkInput,
) (*getRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
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

type deleteRepositoryLinkOutput struct{}

func (h *Handler) handleDeleteRepositoryLink(
	_ context.Context,
	in *deleteRepositoryLinkInput,
) (*deleteRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRepositoryLink(in.RepositoryLinkID); err != nil {
		return nil, err
	}

	return &deleteRepositoryLinkOutput{}, nil
}

type listRepositoryLinksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type listRepositoryLinksOutput struct {
	RepositoryLinks []repositoryLinkItem `json:"RepositoryLinks"`
}

func (h *Handler) handleListRepositoryLinks(
	_ context.Context,
	_ *listRepositoryLinksInput,
) (*listRepositoryLinksOutput, error) {
	links := h.Backend.ListRepositoryLinks()

	items := make([]repositoryLinkItem, len(links))
	for i, link := range links {
		items[i] = repositoryLinkToItem(link)
	}

	return &listRepositoryLinksOutput{RepositoryLinks: items}, nil
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

// --- SyncConfiguration operations ---

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
		return nil, fmt.Errorf("%w: Branch is required", errInvalidRequest)
	}

	if in.ConfigFile == "" {
		return nil, fmt.Errorf("%w: ConfigFile is required", errInvalidRequest)
	}

	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.CreateSyncConfiguration(
		in.Branch, in.ConfigFile, in.RepositoryLinkID, in.ResourceName, in.RoleArn, in.SyncType,
	)
	if err != nil {
		return nil, err
	}

	return &createSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type getSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type getSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleGetSyncConfiguration(
	_ context.Context,
	in *getSyncConfigurationInput,
) (*getSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetSyncConfiguration(in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	return &getSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}

type deleteSyncConfigurationInput struct {
	ResourceName string `json:"ResourceName"`
	SyncType     string `json:"SyncType"`
}

type deleteSyncConfigurationOutput struct{}

func (h *Handler) handleDeleteSyncConfiguration(
	_ context.Context,
	in *deleteSyncConfigurationInput,
) (*deleteSyncConfigurationOutput, error) {
	if err := h.Backend.DeleteSyncConfiguration(in.ResourceName, in.SyncType); err != nil {
		return nil, err
	}

	return &deleteSyncConfigurationOutput{}, nil
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

// --- Sync status operations ---

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
	_ context.Context,
	in *getRepositorySyncStatusInput,
) (*getRepositorySyncStatusOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if in.Branch == "" {
		return nil, fmt.Errorf("%w: Branch is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	status, err := h.Backend.GetRepositorySyncStatus(in.RepositoryLinkID, in.Branch, in.SyncType)
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
	_ context.Context,
	in *getResourceSyncStatusInput,
) (*getResourceSyncStatusOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	status, err := h.Backend.GetResourceSyncStatus(in.ResourceName, in.SyncType)
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
	_ context.Context,
	in *getSyncBlockerSummaryInput,
) (*getSyncBlockerSummaryOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	summary, err := h.Backend.GetSyncBlockerSummary(in.ResourceName, in.SyncType)
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

func buildSyncEventItems(evts []SyncEvent) []syncEventItem {
	out := make([]syncEventItem, len(evts))

	for i, e := range evts {
		out[i] = syncEventItem{
			Event:      e.Event,
			Time:       e.Time.Format(time.RFC3339),
			Type:       e.Type,
			ExternalID: e.ExternalID,
		}
	}

	return out
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
	_ context.Context,
	in *listRepositorySyncDefinitionsInput,
) (*listRepositorySyncDefinitionsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	defs, err := h.Backend.ListRepositorySyncDefinitions(in.RepositoryLinkID, in.SyncType)
	if err != nil {
		return nil, err
	}

	items := make([]repositorySyncDefinitionItem, len(defs))
	for i, d := range defs {
		items[i] = repositorySyncDefinitionItem(d)
	}

	return &listRepositorySyncDefinitionsOutput{RepositorySyncDefinitions: items}, nil
}

// --- ListSyncConfigurations ---

type listSyncConfigurationsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
	NextToken        string `json:"NextToken"`
	MaxResults       int32  `json:"MaxResults"`
}

type listSyncConfigurationsOutput struct {
	SyncConfigurations []syncConfigurationItem `json:"SyncConfigurations"`
}

func (h *Handler) handleListSyncConfigurations(
	_ context.Context,
	in *listSyncConfigurationsInput,
) (*listSyncConfigurationsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	cfgs := h.Backend.ListSyncConfigurations(in.RepositoryLinkID, in.SyncType)
	items := make([]syncConfigurationItem, len(cfgs))

	for i, cfg := range cfgs {
		items[i] = syncConfigToItem(cfg)
	}

	return &listSyncConfigurationsOutput{SyncConfigurations: items}, nil
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
	_ context.Context,
	in *updateRepositoryLinkInput,
) (*updateRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	link, err := h.Backend.UpdateRepositoryLink(in.RepositoryLinkID, in.ConnectionArn, in.EncryptionKeyArn)
	if err != nil {
		return nil, err
	}

	return &updateRepositoryLinkOutput{RepositoryLinkInfo: repositoryLinkToItem(link)}, nil
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
	_ context.Context,
	in *updateSyncBlockerInput,
) (*updateSyncBlockerOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	summary, err := h.Backend.UpdateSyncBlocker(in.ID, in.ResolvedReason)
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
	_ context.Context,
	in *updateSyncConfigurationInput,
) (*updateSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.UpdateSyncConfiguration(
		in.ResourceName, in.SyncType, in.Branch, in.ConfigFile, in.RepositoryLinkID, in.RoleArn,
	)
	if err != nil {
		return nil, err
	}

	return &updateSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}
