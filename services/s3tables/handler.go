package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	s3tablesService       = "s3tables"
	s3tablesMatchPriority = service.PriorityPathVersioned
	segMaintenance        = "maintenance"
)

var (
	errUnknownPath    = errors.New("unknown path")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS S3 Tables API.
type Handler struct {
	Backend   *InMemoryBackend
	AccountID string
	Region    string
}

// NewHandler creates a new S3 Tables handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.accountID,
		Region:    backend.region,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "S3tables" }

// GetSupportedOperations returns the list of supported S3 Tables operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateTableBucket",
		"DeleteTableBucket",
		"GetTableBucket",
		"ListTableBuckets",
		"CreateNamespace",
		"DeleteNamespace",
		"GetNamespace",
		"ListNamespaces",
		"CreateTable",
		"DeleteTable",
		"GetTable",
		"ListTables",
		"RenameTable",
		"UpdateTableMetadataLocation",
		"GetTableBucketMaintenanceConfiguration",
		"PutTableBucketMaintenanceConfiguration",
		"GetTableMaintenanceConfiguration",
		"PutTableMaintenanceConfiguration",
		"GetTableBucketPolicy",
		"PutTableBucketPolicy",
		"DeleteTableBucketPolicy",
		"GetTablePolicy",
		"PutTablePolicy",
		"DeleteTablePolicy",
		"GetTableBucketEncryption",
		"GetTableEncryption",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return s3tablesService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches S3 Tables API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/buckets") ||
			strings.HasPrefix(path, "/namespaces") ||
			strings.HasPrefix(path, "/tables") ||
			strings.HasPrefix(path, "/get-table")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return s3tablesMatchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.routeRequest(c.Request())

	return op
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := rawPathSegments(c.Request())
	if len(segs) > 1 {
		return segs[1]
	}

	return ""
}

// Handler returns the Echo handler function for S3 Tables requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "s3tables: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op, dispatchFn := h.routeRequest(c.Request())
		if dispatchFn == nil {
			return h.handleError(c, fmt.Errorf("%w: %s %s", errUnknownPath, c.Request().Method, c.Request().URL.Path))
		}

		result, dispErr := dispatchFn(ctx, c.Request(), body)
		if dispErr != nil {
			log.ErrorContext(ctx, "s3tables: operation failed", "op", op, "error", dispErr)

			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusNoContent)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

type dispatchFunc func(ctx context.Context, r *http.Request, body []byte) ([]byte, error)

// routeRequest maps HTTP method + path to operation name and dispatch function.
//

func (h *Handler) routeRequest(r *http.Request) (string, dispatchFunc) {
	segs := rawPathSegments(r)
	method := r.Method

	if len(segs) == 0 {
		return "", nil
	}

	switch segs[0] {
	case "buckets":
		return h.routeBuckets(segs, method, r)
	case "namespaces":
		return h.routeNamespaces(segs, method)
	case "tables":
		return h.routeTables(segs, method, r)
	case "get-table":
		if method == http.MethodGet {
			return "GetTable", h.handleGetTable
		}
	}

	return "", nil
}

//nolint:cyclop // routing table is inherently switch-heavy
func (h *Handler) routeBuckets(segs []string, method string, r *http.Request) (string, dispatchFunc) {
	switch len(segs) {
	case 1:
		switch method {
		case http.MethodPut:
			return "CreateTableBucket", h.handleCreateTableBucket
		case http.MethodGet:
			return "ListTableBuckets", h.handleListTableBuckets
		}
	case 2: //nolint:mnd // bucket ARN segment
		switch method {
		case http.MethodGet:
			return "GetTableBucket", h.handleGetTableBucket
		case http.MethodDelete:
			return "DeleteTableBucket", h.handleDeleteTableBucket
		}
	case 3: //nolint:mnd // bucket ARN + sub-resource
		sub := segs[2]
		switch sub {
		case segMaintenance:
			if method == http.MethodGet {
				return "GetTableBucketMaintenanceConfiguration", h.handleGetTableBucketMaintenanceConfiguration
			}
		case "encryption":
			if method == http.MethodGet {
				return "GetTableBucketEncryption", h.handleGetTableBucketEncryption
			}
		case "policy":
			switch method {
			case http.MethodGet:
				return "GetTableBucketPolicy", h.handleGetTableBucketPolicy
			case http.MethodPut:
				return "PutTableBucketPolicy", h.handlePutTableBucketPolicy
			case http.MethodDelete:
				return "DeleteTableBucketPolicy", h.handleDeleteTableBucketPolicy
			}
		}

		_ = r
	case 4: //nolint:mnd // bucket ARN + maintenance + config type
		if segs[2] == segMaintenance && method == http.MethodPut {
			return "PutTableBucketMaintenanceConfiguration", h.handlePutTableBucketMaintenanceConfiguration
		}
	}

	return "", nil
}

func (h *Handler) routeNamespaces(segs []string, method string) (string, dispatchFunc) {
	switch len(segs) {
	case 2: //nolint:mnd // bucket ARN + namespace name prefix
		switch method {
		case http.MethodPut:
			return "CreateNamespace", h.handleCreateNamespace
		case http.MethodGet:
			return "ListNamespaces", h.handleListNamespaces
		}
	case 3: //nolint:mnd // bucket ARN + namespace name
		switch method {
		case http.MethodGet:
			return "GetNamespace", h.handleGetNamespace
		case http.MethodDelete:
			return "DeleteNamespace", h.handleDeleteNamespace
		}
	}

	return "", nil
}

func (h *Handler) routeTables(segs []string, method string, r *http.Request) (string, dispatchFunc) {
	switch len(segs) {
	case 2: //nolint:mnd // bucket ARN prefix (list tables)
		if method == http.MethodGet {
			return "ListTables", h.handleListTables
		}
	case 3: //nolint:mnd // bucket ARN + namespace (create table)
		if method == http.MethodPut {
			return "CreateTable", h.handleCreateTable
		}
	case 4: //nolint:mnd // bucket ARN + namespace + name (delete table)
		if method == http.MethodDelete {
			return "DeleteTable", h.handleDeleteTable
		}
	case 5: //nolint:mnd // bucket ARN + namespace + name + subresource
		return h.routeTableSubResource(segs[4], method, r)
	case 6: //nolint:mnd // bucket ARN + namespace + name + maintenance + type
		if segs[4] == segMaintenance && method == http.MethodPut {
			return "PutTableMaintenanceConfiguration", h.handlePutTableMaintenanceConfiguration
		}
	}

	return "", nil
}

func (h *Handler) routeTableSubResource(sub, method string, _ *http.Request) (string, dispatchFunc) {
	switch sub {
	case "rename":
		if method == http.MethodPut {
			return "RenameTable", h.handleRenameTable
		}
	case "metadata-location":
		if method == http.MethodPut {
			return "UpdateTableMetadataLocation", h.handleUpdateTableMetadataLocation
		}
	case segMaintenance:
		if method == http.MethodGet {
			return "GetTableMaintenanceConfiguration", h.handleGetTableMaintenanceConfiguration
		}
	case "encryption":
		if method == http.MethodGet {
			return "GetTableEncryption", h.handleGetTableEncryption
		}
	case "policy":
		switch method {
		case http.MethodGet:
			return "GetTablePolicy", h.handleGetTablePolicy
		case http.MethodPut:
			return "PutTablePolicy", h.handlePutTablePolicy
		case http.MethodDelete:
			return "DeleteTablePolicy", h.handleDeleteTablePolicy
		}
	}

	return "", nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	status := http.StatusInternalServerError
	errType := "InternalError"
	msg := err.Error()

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status = http.StatusNotFound
		errType = "NotFoundException"
	case errors.Is(err, awserr.ErrConflict):
		status = http.StatusConflict
		errType = "ConflictException"
	case errors.Is(err, errInvalidRequest):
		status = http.StatusBadRequest
		errType = "BadRequestException"
	case errors.Is(err, errUnknownPath):
		status = http.StatusNotFound
		errType = "NotFoundException"
	}

	payload, _ := json.Marshal(map[string]string{
		"message": msg,
	})

	c.Response().Header().Set("x-amzn-errortype", errType)

	return c.JSONBlob(status, payload)
}

// === TableBucket operations ===

// createTableBucketRequest is the request body for CreateTableBucket.
type createTableBucketRequest struct {
	Name string `json:"name"`
}

func (h *Handler) handleCreateTableBucket(ctx context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createTableBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	tb, err := h.Backend.CreateTableBucket(req.Name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: created table bucket", "name", tb.Name, "arn", tb.ARN)

	return json.Marshal(map[string]string{
		"arn": tb.ARN,
	})
}

func (h *Handler) handleGetTableBucket(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket", "arn", tb.ARN)

	return json.Marshal(map[string]any{
		"arn":            tb.ARN,
		"name":           tb.Name,
		"ownerAccountId": tb.OwnerAccountID,
		"createdAt":      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
	})
}

func (h *Handler) handleDeleteTableBucket(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucket(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket", "arn", bucketARN)

	return nil, nil
}

func (h *Handler) handleListTableBuckets(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	_ = r

	list := h.Backend.ListTableBuckets()
	summaries := make([]map[string]any, 0, len(list))

	for _, tb := range list {
		summaries = append(summaries, map[string]any{
			"arn":            tb.ARN,
			"name":           tb.Name,
			"ownerAccountId": tb.OwnerAccountID,
			"createdAt":      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		})
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed table buckets", "count", len(summaries))

	return json.Marshal(map[string]any{
		"tableBuckets": summaries,
	})
}

// === Maintenance configuration operations ===

func (h *Handler) handleGetTableBucketMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	cfg, err := h.Backend.GetTableBucketMaintenanceConfiguration(bucketARN)
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		cfg = make(map[string]any)
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket maintenance configuration", "arn", bucketARN)

	return json.Marshal(map[string]any{
		"tableBucketARN": bucketARN,
		"configuration":  cfg,
	})
}

// putTableBucketMaintenanceRequest is the request body for PutTableBucketMaintenanceConfiguration.
type putTableBucketMaintenanceRequest struct {
	Value map[string]any `json:"value"`
}

func (h *Handler) handlePutTableBucketMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or type", errInvalidRequest)
	}

	bucketARN := segs[1]
	maintenanceType := segs[3]

	var req putTableBucketMaintenanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableBucketMaintenanceConfiguration(bucketARN, maintenanceType, req.Value); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"s3tables: put table bucket maintenance configuration",
		"arn",
		bucketARN,
		"type",
		maintenanceType,
	)

	return nil, nil
}

// === Policy operations ===

// === Encryption operations ===

func (h *Handler) handleGetTableBucketEncryption(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if _, err := h.Backend.GetTableBucket(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket encryption", "arn", bucketARN)

	return nil, awserr.ErrNotFound
}

func (h *Handler) handleGetTableEncryption(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	ns := segs[2]
	name := segs[3]

	if _, err := h.Backend.GetTable(bucketARN, splitNamespace(ns), name); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table encryption", "name", name)

	return json.Marshal(map[string]any{
		"encryptionConfiguration": map[string]string{
			"sseAlgorithm": "AES256",
		},
	})
}

// === Policy operations ===

func (h *Handler) handleGetTableBucketPolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	policy, err := h.Backend.GetTableBucketPolicy(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket policy", "arn", bucketARN)

	return json.Marshal(map[string]string{
		"resourcePolicy": policy,
	})
}

// putTableBucketPolicyRequest is the request body for PutTableBucketPolicy.
type putTableBucketPolicyRequest struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

func (h *Handler) handlePutTableBucketPolicy(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketPolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableBucketPolicy(bucketARN, req.ResourcePolicy); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket policy", "arn", bucketARN)

	return nil, nil
}

func (h *Handler) handleDeleteTableBucketPolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucketPolicy(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket policy", "arn", bucketARN)

	return nil, nil
}

// === Namespace operations ===

// createNamespaceRequest is the request body for CreateNamespace.
type createNamespaceRequest struct {
	Namespace []string `json:"namespace"`
}

func (h *Handler) handleCreateNamespace(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req createNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.Namespace) == 0 {
		return nil, fmt.Errorf("%w: namespace is required", errInvalidRequest)
	}

	ns, err := h.Backend.CreateNamespace(bucketARN, req.Namespace)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: created namespace", "namespace", joinNamespace(ns.Namespace), "bucket", bucketARN)

	return json.Marshal(map[string]any{
		"namespace":      ns.Namespace,
		"tableBucketARN": ns.TableBucketARN,
	})
}

func (h *Handler) handleGetNamespace(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 3 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or namespace", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]

	ns, err := h.Backend.GetNamespace(bucketARN, splitNamespace(nsName))
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got namespace", "namespace", nsName, "bucket", bucketARN)

	return json.Marshal(map[string]any{
		"namespace":      ns.Namespace,
		"createdAt":      ns.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		"createdBy":      ns.CreatedBy,
		"ownerAccountId": ns.OwnerAccountID,
	})
}

func (h *Handler) handleDeleteNamespace(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 3 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or namespace", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]

	if err := h.Backend.DeleteNamespace(bucketARN, splitNamespace(nsName)); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted namespace", "namespace", nsName, "bucket", bucketARN)

	return nil, nil
}

func (h *Handler) handleListNamespaces(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	list, err := h.Backend.ListNamespaces(bucketARN)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(list))

	for _, ns := range list {
		summaries = append(summaries, map[string]any{
			"namespace":      ns.Namespace,
			"createdAt":      ns.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
			"createdBy":      ns.CreatedBy,
			"ownerAccountId": ns.OwnerAccountID,
		})
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed namespaces", "bucket", bucketARN, "count", len(summaries))

	return json.Marshal(map[string]any{
		"namespaces": summaries,
	})
}

// === Table operations ===

// createTableRequest is the request body for CreateTable.
type createTableRequest struct {
	Name   string `json:"name"`
	Format string `json:"format"`
}

func (h *Handler) handleCreateTable(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 3 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN or namespace", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]

	var req createTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if req.Format == "" {
		req.Format = "ICEBERG"
	}

	table, err := h.Backend.CreateTable(bucketARN, splitNamespace(nsName), req.Name, req.Format)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: created table", "name", table.Name, "arn", table.ARN)

	return json.Marshal(map[string]string{
		"tableARN":     table.ARN,
		"versionToken": table.VersionToken,
	})
}

func (h *Handler) handleGetTable(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	bucketARN := q.Get("tableBucketARN")
	nsName := q.Get("namespace")
	name := q.Get("name")

	if bucketARN == "" || nsName == "" || name == "" {
		return nil, fmt.Errorf("%w: tableBucketARN, namespace and name are required", errInvalidRequest)
	}

	table, err := h.Backend.GetTable(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table", "name", table.Name, "arn", table.ARN)

	return json.Marshal(map[string]any{
		"name":              table.Name,
		"namespace":         table.Namespace,
		"tableARN":          table.ARN,
		"tableBucketARN":    table.TableBucketARN,
		"format":            table.Format,
		"type":              "customer",
		"versionToken":      table.VersionToken,
		"metadataLocation":  table.MetadataLocation,
		"warehouseLocation": table.WarehouseLocation,
		"createdAt":         table.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		"modifiedAt":        table.ModifiedAt.Format("2006-01-02T15:04:05.999Z"),
		"createdBy":         table.OwnerAccountID,
		"modifiedBy":        table.OwnerAccountID,
		"ownerAccountId":    table.OwnerAccountID,
	})
}

func (h *Handler) handleDeleteTable(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	if err := h.Backend.DeleteTable(bucketARN, splitNamespace(nsName), name); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table", "name", name, "bucket", bucketARN)

	return nil, nil
}

func (h *Handler) handleListTables(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]
	namespace := r.URL.Query().Get("namespace")

	list, err := h.Backend.ListTables(bucketARN, namespace)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(list))

	for _, t := range list {
		summaries = append(summaries, map[string]any{
			"name":           t.Name,
			"namespace":      t.Namespace,
			"tableARN":       t.ARN,
			"tableBucketARN": t.TableBucketARN,
			"type":           "customer",
			"createdAt":      t.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
			"modifiedAt":     t.ModifiedAt.Format("2006-01-02T15:04:05.999Z"),
		})
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed tables", "bucket", bucketARN, "count", len(summaries))

	return json.Marshal(map[string]any{
		"tables": summaries,
	})
}

// renameTableRequest is the request body for RenameTable.
type renameTableRequest struct {
	NewNamespaceName *string `json:"newNamespaceName"`
	NewName          *string `json:"newName"`
	VersionToken     *string `json:"versionToken"`
}

func (h *Handler) handleRenameTable(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	var req renameTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	newNs := ""
	if req.NewNamespaceName != nil {
		newNs = *req.NewNamespaceName
	}

	newName := ""
	if req.NewName != nil {
		newName = *req.NewName
	}

	if err := h.Backend.RenameTable(bucketARN, splitNamespace(nsName), name, newNs, newName); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: renamed table", "from", name, "to", newName)

	return nil, nil
}

// updateTableMetadataLocationRequest is the request body for UpdateTableMetadataLocation.
type updateTableMetadataLocationRequest struct {
	MetadataLocation string `json:"metadataLocation"`
	VersionToken     string `json:"versionToken"`
}

func (h *Handler) handleUpdateTableMetadataLocation(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	var req updateTableMetadataLocationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	table, err := h.Backend.UpdateTableMetadataLocation(
		bucketARN,
		splitNamespace(nsName),
		name,
		req.MetadataLocation,
		req.VersionToken,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: updated table metadata location", "name", name)

	return json.Marshal(map[string]any{
		"name":             table.Name,
		"tableARN":         table.ARN,
		"tableBucketARN":   table.TableBucketARN,
		"namespace":        table.Namespace,
		"versionToken":     table.VersionToken,
		"metadataLocation": table.MetadataLocation,
	})
}

func (h *Handler) handleGetTableMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	cfg, tableARN, err := h.Backend.GetTableMaintenanceConfiguration(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		cfg = make(map[string]any)
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table maintenance configuration", "name", name)

	return json.Marshal(map[string]any{
		"tableARN":      tableARN,
		"configuration": cfg,
	})
}

// putTableMaintenanceRequest is the request body for PutTableMaintenanceConfiguration.
type putTableMaintenanceRequest struct {
	Value map[string]any `json:"value"`
}

func (h *Handler) handlePutTableMaintenanceConfiguration(
	ctx context.Context,
	r *http.Request,
	body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 6 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace, name or type", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]
	maintenanceType := segs[5]

	var req putTableMaintenanceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableMaintenanceConfiguration(
		bucketARN,
		splitNamespace(nsName),
		name,
		maintenanceType,
		req.Value,
	); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table maintenance configuration", "name", name, "type", maintenanceType)

	return nil, nil
}

func (h *Handler) handleGetTablePolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	policy, err := h.Backend.GetTablePolicy(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table policy", "name", name)

	return json.Marshal(map[string]string{
		"resourcePolicy": policy,
	})
}

// putTablePolicyRequest is the request body for PutTablePolicy.
type putTablePolicyRequest struct {
	ResourcePolicy string `json:"resourcePolicy"`
}

func (h *Handler) handlePutTablePolicy(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	var req putTablePolicyRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTablePolicy(bucketARN, splitNamespace(nsName), name, req.ResourcePolicy); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table policy", "name", name)

	return nil, nil
}

func (h *Handler) handleDeleteTablePolicy(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	if err := h.Backend.DeleteTablePolicy(bucketARN, splitNamespace(nsName), name); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table policy", "name", name)

	return nil, nil
}

// rawPathSegments splits the raw (or decoded) URL path into non-empty segments,
// URL-decoding each segment individually so that encoded slashes in path params
// (e.g. ARNs) are preserved as a single segment.
func rawPathSegments(r *http.Request) []string {
	rawPath := r.URL.RawPath
	if rawPath == "" {
		rawPath = r.URL.Path
	}

	rawPath = strings.TrimPrefix(rawPath, "/")
	parts := strings.Split(rawPath, "/")

	segments := make([]string, 0, len(parts))

	for _, p := range parts {
		if p == "" {
			continue
		}

		decoded, err := url.PathUnescape(p)
		if err != nil {
			decoded = p
		}

		segments = append(segments, decoded)
	}

	return segments
}
