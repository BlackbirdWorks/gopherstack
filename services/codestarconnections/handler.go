package codestarconnections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codestarTargetPrefix = "CodeStar_connections_20191201."
)

var (
	errUnknownAction  = awserr.New("UnknownOperationException", awserr.ErrNotFound)
	errInvalidRequest = errors.New("invalid request")
)

const defaultCSCMaxResults = 100

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

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Without this delegation, cli.go's setupPersistence type-asserts the
// service.Registerable value returned by Provider.Init (this Handler, not
// InMemoryBackend) against a Snapshot/Restore interface -- since Handler
// itself never exposed either method, InMemoryBackend.Snapshot/Restore
// (persistence.go) were dead code and this service was never actually
// persisted, despite implementing the Persistable contract.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
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
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeStarConnections", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
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

const codestarContentType = "application/x-amz-json-1.0"

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	var errType string
	var statusCode int

	switch {
	case errors.Is(err, ErrNotFound):
		errType, statusCode = "ResourceNotFoundException", http.StatusBadRequest
	case errors.Is(err, ErrSyncBlockerNotFound):
		errType, statusCode = "SyncBlockerDoesNotExistException", http.StatusBadRequest
	case errors.Is(err, ErrResourceInUse):
		errType, statusCode = "ConflictException", http.StatusBadRequest
	case errors.Is(err, ErrSyncConfigStillExists):
		errType, statusCode = "SyncConfigurationStillExistsException", http.StatusBadRequest
	case errors.Is(err, ErrResourceAlreadyExists):
		errType, statusCode = "ResourceAlreadyExistsException", http.StatusBadRequest
	case errors.Is(err, ErrAlreadyExists):
		errType, statusCode = "InvalidInputException", http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		errType, statusCode = "ValidationException", http.StatusBadRequest
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		errType, statusCode = "InvalidInputException", http.StatusBadRequest
	default:
		errType, statusCode = "InternalFailure", http.StatusInternalServerError
	}

	payload, marshalErr := json.Marshal(service.JSONErrorResponse{
		Type:    errType,
		Message: err.Error(),
	})
	if marshalErr != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	c.Response().Header().Set("Content-Type", codestarContentType)

	return c.JSONBlob(statusCode, payload)
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
	ctx context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	if in.ConnectionName == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", errInvalidRequest)
	}

	conn, err := h.Backend.CreateConnection(
		ctx, in.ConnectionName, in.ProviderType, in.HostArn, tagsFromArray(in.Tags),
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
	ConnectionName   string `json:"ConnectionName"`
	ConnectionArn    string `json:"ConnectionArn"`
	ConnectionStatus string `json:"ConnectionStatus"`
	OwnerAccountID   string `json:"OwnerAccountId"`
	ProviderType     string `json:"ProviderType"`
	HostArn          string `json:"HostArn,omitempty"`
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
	}
}

func (h *Handler) handleGetConnection(
	ctx context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	conn, err := h.Backend.GetConnection(ctx, in.ConnectionArn)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: connectionToView(conn)}, nil
}

type listConnectionsInput struct {
	ProviderTypeFilter string `json:"ProviderTypeFilter"`
	HostArnFilter      string `json:"HostArnFilter"`
	NextToken          string `json:"NextToken"`
	MaxResults         int    `json:"MaxResults"`
}

type listConnectionsOutput struct {
	NextToken   string           `json:"NextToken,omitempty"`
	Connections []connectionView `json:"Connections"`
}

func (h *Handler) handleListConnections(
	ctx context.Context,
	in *listConnectionsInput,
) (*listConnectionsOutput, error) {
	connections := h.Backend.ListConnections(ctx, in.ProviderTypeFilter, in.HostArnFilter)

	views := make([]connectionView, len(connections))
	for i, c := range connections {
		views[i] = connectionToView(c)
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listConnectionsOutput{Connections: p.Data, NextToken: p.Next}, nil
}

type deleteConnectionInput struct {
	ConnectionArn string `json:"ConnectionArn"`
}

type deleteConnectionOutput struct{}

func (h *Handler) handleDeleteConnection(
	ctx context.Context,
	in *deleteConnectionInput,
) (*deleteConnectionOutput, error) {
	if in.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteConnection(ctx, in.ConnectionArn); err != nil {
		return nil, err
	}

	return &deleteConnectionOutput{}, nil
}

// --- Host operations ---

type vpcConfigurationView struct {
	VpcID            string   `json:"VpcId"`
	TLSCertificate   string   `json:"TlsCertificate,omitempty"`
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

type createHostInput struct {
	Name             string                `json:"Name"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	Tags             []tagEntry            `json:"Tags"`
}

type createHostOutput struct {
	HostArn string     `json:"HostArn"`
	Tags    []tagEntry `json:"Tags,omitempty"`
}

func (h *Handler) handleCreateHost(
	ctx context.Context,
	in *createHostInput,
) (*createHostOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	host, err := h.Backend.CreateHost(
		ctx, in.Name, in.ProviderType, in.ProviderEndpoint,
		vpcConfigFromView(in.VpcConfiguration), tagsFromArray(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createHostOutput{
		HostArn: host.HostArn,
		Tags:    tagsToSortedArray(host.Tags),
	}, nil
}

type getHostInput struct {
	HostArn string `json:"HostArn"`
}

// getHostView is the GetHost response shape — HostArn is NOT included (caller already knows it).
type getHostView struct {
	Name             string                `json:"Name"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	Status           string                `json:"Status"`
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	StatusMessage    string                `json:"StatusMessage,omitempty"`
}

// listHostView is the ListHosts per-item shape — includes HostArn.
type listHostView struct {
	Name             string                `json:"Name"`
	HostArn          string                `json:"HostArn"`
	ProviderType     string                `json:"ProviderType"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
	Status           string                `json:"Status"`
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration,omitempty"`
	StatusMessage    string                `json:"StatusMessage,omitempty"`
}

type getHostOutput struct {
	getHostView
}

func vpcConfigFromView(v *vpcConfigurationView) *VpcConfiguration {
	if v == nil {
		return nil
	}

	return &VpcConfiguration{
		VpcID:            v.VpcID,
		TLSCertificate:   v.TLSCertificate,
		SubnetIDs:        v.SubnetIDs,
		SecurityGroupIDs: v.SecurityGroupIDs,
	}
}

func vpcConfigToView(v *VpcConfiguration) *vpcConfigurationView {
	if v == nil {
		return nil
	}

	return &vpcConfigurationView{
		VpcID:            v.VpcID,
		TLSCertificate:   v.TLSCertificate,
		SubnetIDs:        v.SubnetIDs,
		SecurityGroupIDs: v.SecurityGroupIDs,
	}
}

func hostToGetView(h *Host) getHostView {
	return getHostView{
		Name:             h.Name,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		StatusMessage:    h.StatusMessage,
		VpcConfiguration: vpcConfigToView(h.VpcConfiguration),
	}
}

func hostToListView(h *Host) listHostView {
	return listHostView{
		Name:             h.Name,
		HostArn:          h.HostArn,
		ProviderType:     h.ProviderType,
		ProviderEndpoint: h.ProviderEndpoint,
		Status:           h.Status,
		StatusMessage:    h.StatusMessage,
		VpcConfiguration: vpcConfigToView(h.VpcConfiguration),
	}
}

func (h *Handler) handleGetHost(
	ctx context.Context,
	in *getHostInput,
) (*getHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	host, err := h.Backend.GetHost(ctx, in.HostArn)
	if err != nil {
		return nil, err
	}

	return &getHostOutput{hostToGetView(host)}, nil
}

type listHostsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listHostsOutput struct {
	NextToken string         `json:"NextToken,omitempty"`
	Hosts     []listHostView `json:"Hosts"`
}

func (h *Handler) handleListHosts(
	ctx context.Context,
	in *listHostsInput,
) (*listHostsOutput, error) {
	hosts := h.Backend.ListHosts(ctx)

	views := make([]listHostView, len(hosts))
	for i, host := range hosts {
		views[i] = hostToListView(host)
	}

	p := page.New(views, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listHostsOutput{Hosts: p.Data, NextToken: p.Next}, nil
}

type deleteHostInput struct {
	HostArn string `json:"HostArn"`
}

type deleteHostOutput struct{}

func (h *Handler) handleDeleteHost(
	ctx context.Context,
	in *deleteHostInput,
) (*deleteHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteHost(ctx, in.HostArn); err != nil {
		return nil, err
	}

	return &deleteHostOutput{}, nil
}

type updateHostInput struct {
	VpcConfiguration *vpcConfigurationView `json:"VpcConfiguration"`
	HostArn          string                `json:"HostArn"`
	ProviderEndpoint string                `json:"ProviderEndpoint"`
}

type updateHostOutput struct{}

func (h *Handler) handleUpdateHost(
	ctx context.Context,
	in *updateHostInput,
) (*updateHostOutput, error) {
	if in.HostArn == "" {
		return nil, fmt.Errorf("%w: HostArn is required", errInvalidRequest)
	}

	vpcCfg := vpcConfigFromView(in.VpcConfiguration)
	if err := h.Backend.UpdateHost(ctx, in.HostArn, in.ProviderEndpoint, vpcCfg); err != nil {
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
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
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
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
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
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
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
	ctx context.Context,
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
		ctx, in.ConnectionArn, in.OwnerID, in.RepositoryName, in.EncryptionKeyArn,
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
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
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

type deleteRepositoryLinkOutput struct{}

func (h *Handler) handleDeleteRepositoryLink(
	ctx context.Context,
	in *deleteRepositoryLinkInput,
) (*deleteRepositoryLinkOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRepositoryLink(ctx, in.RepositoryLinkID); err != nil {
		return nil, err
	}

	return &deleteRepositoryLinkOutput{}, nil
}

type listRepositoryLinksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listRepositoryLinksOutput struct {
	NextToken       string               `json:"NextToken,omitempty"`
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

	p := page.New(items, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listRepositoryLinksOutput{RepositoryLinks: p.Data, NextToken: p.Next}, nil
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
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	ResourceName            string `json:"ResourceName"`
	RoleArn                 string `json:"RoleArn"`
	SyncType                string `json:"SyncType"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn"`
}

type syncConfigurationItem struct {
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	OwnerID                 string `json:"OwnerId"`
	ProviderType            string `json:"ProviderType"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	RepositoryName          string `json:"RepositoryName"`
	ResourceName            string `json:"ResourceName"`
	RoleArn                 string `json:"RoleArn"`
	SyncType                string `json:"SyncType"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus,omitempty"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn,omitempty"`
}

type createSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleCreateSyncConfiguration(
	ctx context.Context,
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

	cfg, err := h.Backend.CreateSyncConfigurationFull(
		ctx, in.Branch, in.ConfigFile, in.RepositoryLinkID, in.ResourceName, in.RoleArn, in.SyncType,
		in.PublishDeploymentStatus, in.TriggerResourceUpdateOn,
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
	ctx context.Context,
	in *getSyncConfigurationInput,
) (*getSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetSyncConfiguration(ctx, in.ResourceName, in.SyncType)
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
	ctx context.Context,
	in *deleteSyncConfigurationInput,
) (*deleteSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteSyncConfiguration(ctx, in.ResourceName, in.SyncType); err != nil {
		return nil, err
	}

	return &deleteSyncConfigurationOutput{}, nil
}

func syncConfigToItem(cfg *SyncConfiguration) syncConfigurationItem {
	return syncConfigurationItem{
		Branch:                  cfg.Branch,
		ConfigFile:              cfg.ConfigFile,
		OwnerID:                 cfg.OwnerID,
		ProviderType:            cfg.ProviderType,
		RepositoryLinkID:        cfg.RepositoryLinkID,
		RepositoryName:          cfg.RepositoryName,
		ResourceName:            cfg.ResourceName,
		RoleArn:                 cfg.RoleArn,
		SyncType:                cfg.SyncType,
		PublishDeploymentStatus: cfg.PublishDeploymentStatus,
		TriggerResourceUpdateOn: cfg.TriggerResourceUpdateOn,
	}
}

// --- Sync status operations ---

type getRepositorySyncStatusInput struct {
	Branch           string `json:"Branch"`
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
}

type syncEventItem struct {
	Event      string  `json:"Event"`
	ExternalID string  `json:"ExternalId,omitempty"`
	Type       string  `json:"Type"`
	Time       float64 `json:"Time"`
}

type repositorySyncAttemptItem struct {
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
	StartedAt float64         `json:"StartedAt"`
}

type getRepositorySyncStatusOutput struct {
	LatestSync repositorySyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetRepositorySyncStatus(
	ctx context.Context,
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

	status, err := h.Backend.GetRepositorySyncStatus(ctx, in.RepositoryLinkID, in.Branch, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

	return &getRepositorySyncStatusOutput{
		LatestSync: repositorySyncAttemptItem{
			StartedAt: awstime.Epoch(status.StartedAt),
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
	Status    string          `json:"Status"`
	Events    []syncEventItem `json:"Events"`
	StartedAt float64         `json:"StartedAt"`
}

type getResourceSyncStatusOutput struct {
	LatestSync resourceSyncAttemptItem `json:"LatestSync"`
}

func (h *Handler) handleGetResourceSyncStatus(
	ctx context.Context,
	in *getResourceSyncStatusInput,
) (*getResourceSyncStatusOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	status, err := h.Backend.GetResourceSyncStatus(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	events := buildSyncEventItems(status.Events)

	return &getResourceSyncStatusOutput{
		LatestSync: resourceSyncAttemptItem{
			StartedAt: awstime.Epoch(status.StartedAt),
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
	ID             string  `json:"Id"`
	Type           string  `json:"Type"`
	Status         string  `json:"Status"`
	CreatedReason  string  `json:"CreatedReason"`
	ResolvedReason string  `json:"ResolvedReason,omitempty"`
	CreatedAt      float64 `json:"CreatedAt"`
	ResolvedAt     float64 `json:"ResolvedAt,omitempty"`
}

// syncBlockerToItem converts a backend SyncBlocker to its wire shape.
// CreatedAt/ResolvedAt are epoch-seconds numbers on the wire (see
// awsAwsjson10_deserializeDocumentSyncBlocker in the real SDK), not RFC3339
// strings.
func syncBlockerToItem(b SyncBlocker) syncBlockerItem {
	item := syncBlockerItem{
		ID:            b.ID,
		Type:          b.Type,
		Status:        b.Status,
		CreatedAt:     awstime.Epoch(b.CreatedAt),
		CreatedReason: b.CreatedReason,
	}

	if b.ResolvedAt != nil {
		item.ResolvedAt = awstime.Epoch(*b.ResolvedAt)
		item.ResolvedReason = b.ResolvedReason
	}

	return item
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
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	summary, err := h.Backend.GetSyncBlockerSummary(ctx, in.ResourceName, in.SyncType)
	if err != nil {
		return nil, err
	}

	blockers := make([]syncBlockerItem, len(summary.LatestBlockers))
	for i, b := range summary.LatestBlockers {
		blockers[i] = syncBlockerToItem(b)
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
			Time:       awstime.Epoch(e.Time),
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
	ctx context.Context,
	in *listRepositorySyncDefinitionsInput,
) (*listRepositorySyncDefinitionsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
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

// --- ListSyncConfigurations ---

type listSyncConfigurationsInput struct {
	RepositoryLinkID string `json:"RepositoryLinkId"`
	SyncType         string `json:"SyncType"`
	NextToken        string `json:"NextToken"`
	MaxResults       int    `json:"MaxResults"`
}

type listSyncConfigurationsOutput struct {
	NextToken          string                  `json:"NextToken,omitempty"`
	SyncConfigurations []syncConfigurationItem `json:"SyncConfigurations"`
}

func (h *Handler) handleListSyncConfigurations(
	ctx context.Context,
	in *listSyncConfigurationsInput,
) (*listSyncConfigurationsOutput, error) {
	if in.RepositoryLinkID == "" {
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	cfgs := h.Backend.ListSyncConfigurations(ctx, in.RepositoryLinkID, in.SyncType)
	items := make([]syncConfigurationItem, len(cfgs))

	for i, cfg := range cfgs {
		items[i] = syncConfigToItem(cfg)
	}

	p := page.New(items, in.NextToken, in.MaxResults, defaultCSCMaxResults)

	return &listSyncConfigurationsOutput{SyncConfigurations: p.Data, NextToken: p.Next}, nil
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
		return nil, fmt.Errorf("%w: RepositoryLinkId is required", errInvalidRequest)
	}

	link, err := h.Backend.UpdateRepositoryLink(ctx, in.RepositoryLinkID, in.ConnectionArn, in.EncryptionKeyArn)
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

// updateSyncBlockerOutput is the UpdateSyncBlocker response shape. The real
// operation returns the single updated SyncBlocker object under the
// "SyncBlocker" key -- NOT a "SyncBlockerSummary" list (confirmed against
// aws-sdk-go-v2's awsAwsjson10_deserializeOpDocumentUpdateSyncBlockerOutput,
// which only recognizes ResourceName/ParentResourceName/SyncBlocker).
type updateSyncBlockerOutput struct {
	ResourceName       string          `json:"ResourceName"`
	ParentResourceName string          `json:"ParentResourceName,omitempty"`
	SyncBlocker        syncBlockerItem `json:"SyncBlocker"`
}

func (h *Handler) handleUpdateSyncBlocker(
	ctx context.Context,
	in *updateSyncBlockerInput,
) (*updateSyncBlockerOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	summary, err := h.Backend.UpdateSyncBlocker(ctx, in.ID, in.ResolvedReason)
	if err != nil {
		return nil, err
	}

	// The backend returns every blocker for the owning resource; pick out the
	// one that was just resolved (backend.UpdateSyncBlocker only succeeds when
	// in.ID exists, so it is always present here).
	var resolved SyncBlocker

	for _, b := range summary.LatestBlockers {
		if b.ID == in.ID {
			resolved = b

			break
		}
	}

	return &updateSyncBlockerOutput{
		ResourceName:       summary.ResourceName,
		ParentResourceName: summary.ParentResourceName,
		SyncBlocker:        syncBlockerToItem(resolved),
	}, nil
}

// --- UpdateSyncConfiguration ---

type updateSyncConfigurationInput struct {
	ResourceName            string `json:"ResourceName"`
	SyncType                string `json:"SyncType"`
	Branch                  string `json:"Branch"`
	ConfigFile              string `json:"ConfigFile"`
	RepositoryLinkID        string `json:"RepositoryLinkId"`
	RoleArn                 string `json:"RoleArn"`
	PublishDeploymentStatus string `json:"PublishDeploymentStatus"`
	TriggerResourceUpdateOn string `json:"TriggerResourceUpdateOn"`
}

type updateSyncConfigurationOutput struct {
	SyncConfiguration syncConfigurationItem `json:"SyncConfiguration"`
}

func (h *Handler) handleUpdateSyncConfiguration(
	ctx context.Context,
	in *updateSyncConfigurationInput,
) (*updateSyncConfigurationOutput, error) {
	if in.ResourceName == "" {
		return nil, fmt.Errorf("%w: ResourceName is required", errInvalidRequest)
	}

	if in.SyncType == "" {
		return nil, fmt.Errorf("%w: SyncType is required", errInvalidRequest)
	}

	cfg, err := h.Backend.UpdateSyncConfigurationFull(
		ctx, in.ResourceName, in.SyncType, in.Branch, in.ConfigFile, in.RepositoryLinkID, in.RoleArn,
		in.PublishDeploymentStatus, in.TriggerResourceUpdateOn,
	)
	if err != nil {
		return nil, err
	}

	return &updateSyncConfigurationOutput{SyncConfiguration: syncConfigToItem(cfg)}, nil
}
