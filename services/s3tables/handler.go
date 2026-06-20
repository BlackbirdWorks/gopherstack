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
	keyArn              = "arn"
	keyName             = "name"
	keyOwnerAccountID   = "ownerAccountId"
	keyCreatedAt        = "createdAt"
	keyTableBucketARN   = "tableBucketARN"
	keyConfiguration    = "configuration"
	keyTableARN         = "tableARN"
	keyStatusField      = "status"
	keyVersionToken     = "versionToken"
	keyMetadataLocation = "metadataLocation"
	keyNamespace        = "namespace"
	keyCreatedBy        = "createdBy"
)

const (
	s3tablesService         = "s3tables"
	s3tablesMatchPriority   = service.PriorityPathVersioned
	segMaintenance          = "maintenance"
	segEncryption           = "encryption"
	segMetrics              = "metrics"
	segStorageClass         = "storage-class"
	segMaintenanceJobStatus = "maintenance-job-status"
	segMetadataLocation     = "metadata-location"
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
		"CreateNamespace",
		"CreateTable",
		"CreateTableBucket",
		"DeleteNamespace",
		"DeleteTable",
		"DeleteTableBucket",
		"DeleteTableBucketEncryption",
		"DeleteTableBucketMetricsConfiguration",
		"DeleteTableBucketPolicy",
		"DeleteTableBucketReplication",
		"DeleteTablePolicy",
		"DeleteTableReplication",
		"GetNamespace",
		"GetTable",
		"GetTableBucket",
		"GetTableBucketEncryption",
		"GetTableBucketMaintenanceConfiguration",
		"GetTableBucketMetricsConfiguration",
		"GetTableBucketPolicy",
		"GetTableBucketReplication",
		"GetTableBucketStorageClass",
		"GetTableEncryption",
		"GetTableMaintenanceConfiguration",
		"GetTableMaintenanceJobStatus",
		"GetTableMetadataLocation",
		"GetTablePolicy",
		"GetTableRecordExpirationConfiguration",
		"GetTableRecordExpirationJobStatus",
		"GetTableReplication",
		"GetTableReplicationStatus",
		"GetTableStorageClass",
		"ListNamespaces",
		"ListTableBuckets",
		"ListTables",
		"ListTagsForResource",
		"PutTableBucketEncryption",
		"PutTableBucketMaintenanceConfiguration",
		"PutTableBucketMetricsConfiguration",
		"PutTableBucketPolicy",
		"PutTableBucketReplication",
		"PutTableBucketStorageClass",
		"PutTableMaintenanceConfiguration",
		"PutTablePolicy",
		"PutTableRecordExpirationConfiguration",
		"PutTableReplication",
		"RenameTable",
		"TagResource",
		"UntagResource",
		"UpdateTableMetadataLocation",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

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
			strings.HasPrefix(path, "/get-table") ||
			strings.HasPrefix(path, "/table-bucket-replication") ||
			strings.HasPrefix(path, "/table-replication") ||
			strings.HasPrefix(path, "/table-record-expiration") ||
			strings.HasPrefix(path, "/replication-status") ||
			strings.HasPrefix(path, "/table-record-expiration-job-status") ||
			strings.HasPrefix(path, "/tag/")
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

	if len(segs) == 0 {
		return ""
	}

	switch segs[0] {
	case "table-bucket-replication", "table-replication", "table-record-expiration",
		"replication-status", "table-record-expiration-job-status":
		q := c.Request().URL.Query()
		if arn := q.Get(keyTableBucketARN); arn != "" {
			return arn
		}

		return q.Get("tableArn")
	case "tag":
		if len(segs) > 1 {
			return segs[1]
		}

		return ""
	}

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
	case "table-bucket-replication":
		return h.routeTableBucketReplication(method, r)
	case "table-replication":
		return h.routeTableReplication(method, r)
	case "table-record-expiration":
		return h.routeTableRecordExpiration(method, r)
	case "replication-status":
		if method == http.MethodGet {
			return "GetTableReplicationStatus", h.handleGetTableReplicationStatus
		}
	case "table-record-expiration-job-status":
		if method == http.MethodGet {
			return "GetTableRecordExpirationJobStatus", h.handleGetTableRecordExpirationJobStatus
		}
	case "tag":
		return h.routeTag(segs, method)
	}

	return "", nil
}

// routeBuckets routes bucket-level operations.
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
		_ = r

		return h.routeBucketSubResource(segs[2], method)
	case 4: //nolint:mnd // bucket ARN + maintenance + config type
		if segs[2] == segMaintenance && method == http.MethodPut {
			return "PutTableBucketMaintenanceConfiguration", h.handlePutTableBucketMaintenanceConfiguration
		}
	}

	return "", nil
}

// routeBucketSubResource routes bucket sub-resource operations.
func (h *Handler) routeBucketSubResource(sub, method string) (string, dispatchFunc) {
	if op, fn := h.routeBucketStorageOps(sub, method); op != "" {
		return op, fn
	}

	return h.routeBucketPolicyOps(sub, method)
}

// routeBucketStorageOps handles bucket maintenance, encryption, and metrics sub-resources.
func (h *Handler) routeBucketStorageOps(sub, method string) (string, dispatchFunc) {
	switch sub {
	case segMaintenance:
		if method == http.MethodGet {
			return "GetTableBucketMaintenanceConfiguration", h.handleGetTableBucketMaintenanceConfiguration
		}
	case segEncryption:
		switch method {
		case http.MethodGet:
			return "GetTableBucketEncryption", h.handleGetTableBucketEncryption
		case http.MethodPut:
			return "PutTableBucketEncryption", h.handlePutTableBucketEncryption
		case http.MethodDelete:
			return "DeleteTableBucketEncryption", h.handleDeleteTableBucketEncryption
		}
	case segMetrics:
		switch method {
		case http.MethodGet:
			return "GetTableBucketMetricsConfiguration", h.handleGetTableBucketMetricsConfiguration
		case http.MethodPut:
			return "PutTableBucketMetricsConfiguration", h.handlePutTableBucketMetricsConfiguration
		case http.MethodDelete:
			return "DeleteTableBucketMetricsConfiguration", h.handleDeleteTableBucketMetricsConfiguration
		}
	}

	return "", nil
}

// routeBucketPolicyOps handles bucket storage class and policy sub-resources.
func (h *Handler) routeBucketPolicyOps(sub, method string) (string, dispatchFunc) {
	switch sub {
	case segStorageClass:
		switch method {
		case http.MethodGet:
			return "GetTableBucketStorageClass", h.handleGetTableBucketStorageClass
		case http.MethodPut:
			return "PutTableBucketStorageClass", h.handlePutTableBucketStorageClass
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

// routeTableSubResource routes table sub-resource operations.
func (h *Handler) routeTableSubResource(sub, method string, _ *http.Request) (string, dispatchFunc) {
	if op, fn := h.routeTableMetaOps(sub, method); op != "" {
		return op, fn
	}

	return h.routeTableConfigOps(sub, method)
}

// routeTableMetaOps handles table rename, metadata location, and maintenance operations.
func (h *Handler) routeTableMetaOps(sub, method string) (string, dispatchFunc) {
	switch sub {
	case "rename":
		if method == http.MethodPut {
			return "RenameTable", h.handleRenameTable
		}
	case segMetadataLocation:
		switch method {
		case http.MethodGet:
			return "GetTableMetadataLocation", h.handleGetTableMetadataLocation
		case http.MethodPut:
			return "UpdateTableMetadataLocation", h.handleUpdateTableMetadataLocation
		}
	case segMaintenance:
		if method == http.MethodGet {
			return "GetTableMaintenanceConfiguration", h.handleGetTableMaintenanceConfiguration
		}
	case segMaintenanceJobStatus:
		if method == http.MethodGet {
			return "GetTableMaintenanceJobStatus", h.handleGetTableMaintenanceJobStatus
		}
	}

	return "", nil
}

// routeTableConfigOps handles table encryption, storage class, and policy operations.
func (h *Handler) routeTableConfigOps(sub, method string) (string, dispatchFunc) {
	switch sub {
	case segEncryption:
		if method == http.MethodGet {
			return "GetTableEncryption", h.handleGetTableEncryption
		}
	case segStorageClass:
		if method == http.MethodGet {
			return "GetTableStorageClass", h.handleGetTableStorageClass
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
	case errors.Is(err, awserr.ErrInvalidParameter):
		status = http.StatusBadRequest
		errType = "BadRequestException"
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
	log.InfoContext(ctx, "s3tables: created table bucket", keyName, tb.Name, keyArn, tb.ARN)

	return json.Marshal(map[string]string{
		keyArn: tb.ARN,
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
	log.InfoContext(ctx, "s3tables: got table bucket", keyArn, tb.ARN)

	return json.Marshal(map[string]any{
		keyArn:            tb.ARN,
		keyName:           tb.Name,
		keyOwnerAccountID: tb.OwnerAccountID,
		keyCreatedAt:      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
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
	log.InfoContext(ctx, "s3tables: deleted table bucket", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleListTableBuckets(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	_ = r

	list := h.Backend.ListTableBuckets()
	summaries := make([]map[string]any, 0, len(list))

	for _, tb := range list {
		summaries = append(summaries, map[string]any{
			keyArn:            tb.ARN,
			keyName:           tb.Name,
			keyOwnerAccountID: tb.OwnerAccountID,
			keyCreatedAt:      tb.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
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
	log.InfoContext(ctx, "s3tables: got table bucket maintenance configuration", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		keyConfiguration:  cfg,
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

	if maintenanceType != maintenanceTypeIcebergUnreferencedFileRemoval {
		return nil, fmt.Errorf("%w: unsupported table bucket maintenance type %q", errInvalidRequest, maintenanceType)
	}

	if req.Value == nil {
		return nil, fmt.Errorf("%w: value is required", errInvalidRequest)
	}

	if err := h.Backend.PutTableBucketMaintenanceConfiguration(bucketARN, maintenanceType, req.Value); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"s3tables: put table bucket maintenance configuration",
		keyArn,
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

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	if tb.Encryption == nil {
		return nil, awserr.ErrNotFound
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket encryption", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		"encryptionConfiguration": tb.Encryption,
	})
}

func (h *Handler) handleDeleteTableBucketEncryption(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.DeleteTableBucketEncryption(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket encryption", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleGetTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if _, err := h.Backend.GetTableBucket(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket metrics configuration", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		keyConfiguration:  map[string]any{},
	})
}

func (h *Handler) handleDeleteTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if _, err := h.Backend.GetTableBucket(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket metrics configuration", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleGetTableBucketStorageClass(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	tb, err := h.Backend.GetTableBucket(bucketARN)
	if err != nil {
		return nil, err
	}

	sc := tb.StorageClass
	if sc == "" {
		sc = storageClassStandard
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket storage class", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		"storageClass":    sc,
	})
}

// === Replication route helpers ===

func (h *Handler) routeTableBucketReplication(method string, r *http.Request) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "GetTableBucketReplication", h.handleGetTableBucketReplication
	case http.MethodPut:
		return "PutTableBucketReplication", h.handlePutTableBucketReplication
	case http.MethodDelete:
		return "DeleteTableBucketReplication", h.handleDeleteTableBucketReplication
	}

	_ = r

	return "", nil
}

func (h *Handler) routeTableReplication(method string, r *http.Request) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "GetTableReplication", h.handleGetTableReplication
	case http.MethodPut:
		return "PutTableReplication", h.handlePutTableReplication
	case http.MethodDelete:
		return "DeleteTableReplication", h.handleDeleteTableReplication
	}

	_ = r

	return "", nil
}

func (h *Handler) routeTableRecordExpiration(method string, r *http.Request) (string, dispatchFunc) {
	switch method {
	case http.MethodGet:
		return "GetTableRecordExpirationConfiguration", h.handleGetTableRecordExpirationConfiguration
	case http.MethodPut:
		return "PutTableRecordExpirationConfiguration", h.handlePutTableRecordExpirationConfiguration
	}

	_ = r

	return "", nil
}

func (h *Handler) handleGetTableBucketReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableBucketReplication(bucketARN)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table bucket replication", keyArn, bucketARN)

	return json.Marshal(map[string]any{
		keyTableBucketARN: bucketARN,
		"destinations":    cfg.Destinations,
	})
}

func (h *Handler) handleDeleteTableBucketReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTableBucketReplication(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table bucket replication", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handleDeleteTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTableReplication(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: deleted table replication", "tableArn", tableArn)

	return nil, nil
}

func (h *Handler) handleGetTableMaintenanceJobStatus(
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

	table, err := h.Backend.GetTable(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table maintenance job status", keyName, name)

	return json.Marshal(map[string]any{
		keyTableARN:    table.ARN,
		keyStatusField: map[string]any{},
	})
}

func (h *Handler) handleGetTableMetadataLocation(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	table, err := h.Backend.GetTable(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table metadata location", keyName, name)

	return json.Marshal(map[string]any{
		keyVersionToken:     table.VersionToken,
		"warehouseLocation": table.WarehouseLocation,
		keyMetadataLocation: table.MetadataLocation,
	})
}

func (h *Handler) handleGetTableRecordExpirationConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableRecordExpirationConfiguration(tableArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration configuration", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		"tableArn":     tableArn,
		keyStatusField: cfg.Status,
	})
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
	log.InfoContext(ctx, "s3tables: got table encryption", keyName, name)

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
	log.InfoContext(ctx, "s3tables: got table bucket policy", keyArn, bucketARN)

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
	log.InfoContext(ctx, "s3tables: put table bucket policy", keyArn, bucketARN)

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
	log.InfoContext(ctx, "s3tables: deleted table bucket policy", keyArn, bucketARN)

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
	log.InfoContext(ctx, "s3tables: created namespace", keyNamespace, joinNamespace(ns.Namespace), "bucket", bucketARN)

	return json.Marshal(map[string]any{
		keyNamespace:      ns.Namespace,
		keyTableBucketARN: ns.TableBucketARN,
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
	log.InfoContext(ctx, "s3tables: got namespace", keyNamespace, nsName, "bucket", bucketARN)

	return json.Marshal(map[string]any{
		keyNamespace:      ns.Namespace,
		keyCreatedAt:      ns.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		keyCreatedBy:      ns.CreatedBy,
		keyOwnerAccountID: ns.OwnerAccountID,
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
	log.InfoContext(ctx, "s3tables: deleted namespace", keyNamespace, nsName, "bucket", bucketARN)

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
			keyNamespace:      ns.Namespace,
			keyCreatedAt:      ns.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
			keyCreatedBy:      ns.CreatedBy,
			keyOwnerAccountID: ns.OwnerAccountID,
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
	log.InfoContext(ctx, "s3tables: created table", keyName, table.Name, keyArn, table.ARN)

	return json.Marshal(map[string]string{
		keyTableARN:     table.ARN,
		keyVersionToken: table.VersionToken,
	})
}

func (h *Handler) handleGetTable(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	bucketARN := q.Get(keyTableBucketARN)
	nsName := q.Get(keyNamespace)
	name := q.Get(keyName)

	if bucketARN == "" || nsName == "" || name == "" {
		return nil, fmt.Errorf("%w: tableBucketARN, namespace and name are required", errInvalidRequest)
	}

	table, err := h.Backend.GetTable(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table", keyName, table.Name, keyArn, table.ARN)

	return json.Marshal(map[string]any{
		keyName:             table.Name,
		keyNamespace:        table.Namespace,
		keyTableARN:         table.ARN,
		keyTableBucketARN:   table.TableBucketARN,
		"format":            table.Format,
		"type":              "customer",
		keyVersionToken:     table.VersionToken,
		keyMetadataLocation: table.MetadataLocation,
		"warehouseLocation": table.WarehouseLocation,
		keyCreatedAt:        table.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
		"modifiedAt":        table.ModifiedAt.Format("2006-01-02T15:04:05.999Z"),
		keyCreatedBy:        table.OwnerAccountID,
		"modifiedBy":        table.OwnerAccountID,
		keyOwnerAccountID:   table.OwnerAccountID,
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
	log.InfoContext(ctx, "s3tables: deleted table", keyName, name, "bucket", bucketARN)

	return nil, nil
}

func (h *Handler) handleListTables(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]
	namespace := r.URL.Query().Get(keyNamespace)

	list, err := h.Backend.ListTables(bucketARN, namespace)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(list))

	for _, t := range list {
		summaries = append(summaries, map[string]any{
			keyName:           t.Name,
			keyNamespace:      t.Namespace,
			keyTableARN:       t.ARN,
			keyTableBucketARN: t.TableBucketARN,
			"type":            "customer",
			keyCreatedAt:      t.CreatedAt.Format("2006-01-02T15:04:05.999Z"),
			"modifiedAt":      t.ModifiedAt.Format("2006-01-02T15:04:05.999Z"),
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

	versionToken := ""
	if req.VersionToken != nil {
		versionToken = *req.VersionToken
	}

	if err := h.Backend.RenameTable(bucketARN, splitNamespace(nsName), name, newNs, newName, versionToken); err != nil {
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

	if req.MetadataLocation == "" || req.VersionToken == "" {
		return nil, fmt.Errorf("%w: metadataLocation and versionToken are required", errInvalidRequest)
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
	log.InfoContext(ctx, "s3tables: updated table metadata location", keyName, name)

	return json.Marshal(map[string]any{
		keyName:             table.Name,
		keyTableARN:         table.ARN,
		keyTableBucketARN:   table.TableBucketARN,
		keyNamespace:        table.Namespace,
		keyVersionToken:     table.VersionToken,
		keyMetadataLocation: table.MetadataLocation,
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
	log.InfoContext(ctx, "s3tables: got table maintenance configuration", keyName, name)

	return json.Marshal(map[string]any{
		keyTableARN:      tableARN,
		keyConfiguration: cfg,
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

	if !validTableMaintenanceType(maintenanceType) {
		return nil, fmt.Errorf("%w: unsupported table maintenance type %q", errInvalidRequest, maintenanceType)
	}

	if req.Value == nil {
		return nil, fmt.Errorf("%w: value is required", errInvalidRequest)
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
	log.InfoContext(ctx, "s3tables: put table maintenance configuration", keyName, name, "type", maintenanceType)

	return nil, nil
}

func validTableMaintenanceType(maintenanceType string) bool {
	return maintenanceType == maintenanceTypeIcebergCompaction ||
		maintenanceType == maintenanceTypeIcebergSnapshotManagement
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
	log.InfoContext(ctx, "s3tables: got table policy", keyName, name)

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
	log.InfoContext(ctx, "s3tables: put table policy", keyName, name)

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
	log.InfoContext(ctx, "s3tables: deleted table policy", keyName, name)

	return nil, nil
}

// === Tag operations ===

// routeTag handles /tag/{resourceArn}.
func (h *Handler) routeTag(segs []string, method string) (string, dispatchFunc) {
	if len(segs) < 2 { //nolint:mnd // need at least tag + resourceArn
		return "", nil
	}

	switch method {
	case http.MethodGet:
		return "ListTagsForResource", h.handleListTagsForResource
	case http.MethodPost:
		return "TagResource", h.handleTagResource
	case http.MethodDelete:
		return "UntagResource", h.handleUntagResource
	}

	return "", nil
}

func (h *Handler) handleListTagsForResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]

	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: listed tags for resource", "resourceArn", resourceArn)

	return json.Marshal(map[string]any{
		"tags": tags,
	})
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.TagResource(resourceArn, req.Tags); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: tagged resource", "resourceArn", resourceArn)

	return nil, nil
}

func (h *Handler) handleUntagResource(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing resourceArn", errInvalidRequest)
	}

	resourceArn := segs[1]
	tagKeys := r.URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: untagged resource", "resourceArn", resourceArn)

	return nil, nil
}

// === New bucket sub-resource handlers ===

// putTableBucketEncryptionRequest is the request body for PutTableBucketEncryption.
type putTableBucketEncryptionRequest struct {
	EncryptionConfiguration map[string]any `json:"encryptionConfiguration"`
}

func (h *Handler) handlePutTableBucketEncryption(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketEncryptionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.PutTableBucketEncryption(bucketARN, req.EncryptionConfiguration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket encryption", keyArn, bucketARN)

	return nil, nil
}

func (h *Handler) handlePutTableBucketMetricsConfiguration(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	if err := h.Backend.PutTableBucketMetricsConfiguration(bucketARN); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket metrics configuration", keyArn, bucketARN)

	return nil, nil
}

// putTableBucketStorageClassRequest is the request body for PutTableBucketStorageClass.
type putTableBucketStorageClassRequest struct {
	StorageClassConfiguration map[string]any `json:"storageClassConfiguration"`
}

func (h *Handler) handlePutTableBucketStorageClass(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 2 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN", errInvalidRequest)
	}

	bucketARN := segs[1]

	var req putTableBucketStorageClassRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	storageClass := storageClassStandard
	if scVal, found := req.StorageClassConfiguration["storageClass"]; found {
		if scStr, isStr := scVal.(string); isStr && scStr != "" {
			storageClass = scStr
		}
	}

	if err := h.Backend.PutTableBucketStorageClass(bucketARN, storageClass); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket storage class", keyArn, bucketARN)

	return nil, nil
}

// === New table-bucket replication handler ===

// putTableBucketReplicationRequest is the request body for PutTableBucketReplication.
type putTableBucketReplicationRequest struct {
	Configuration map[string]any `json:"configuration"`
}

func (h *Handler) handlePutTableBucketReplication(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	bucketARN := r.URL.Query().Get(keyTableBucketARN)
	if bucketARN == "" {
		return nil, fmt.Errorf("%w: tableBucketARN is required", errInvalidRequest)
	}

	var req putTableBucketReplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cfg := &BucketReplicationConfig{
		Destinations: parseBucketReplicationDestinations(req.Configuration),
	}

	if err := h.Backend.PutTableBucketReplication(bucketARN, cfg); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table bucket replication", keyArn, bucketARN)

	return nil, nil
}

// === New table replication handlers ===

func (h *Handler) handleGetTableReplication(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.GetTableReplicationConfig(tableArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table replication", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyConfiguration: cfg,
		keyVersionToken:  "",
	})
}

// putTableReplicationRequest is the request body for PutTableReplication.
type putTableReplicationRequest struct {
	Configuration map[string]any `json:"configuration"`
}

func (h *Handler) handlePutTableReplication(ctx context.Context, r *http.Request, body []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	var req putTableReplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetTableReplicationConfig(tableArn, req.Configuration); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table replication", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyStatusField:  "ACTIVE",
		keyVersionToken: "1",
	})
}

func (h *Handler) handleGetTableReplicationStatus(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table replication status", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		"sourceTableArn": tableArn,
		"destinations":   []any{},
	})
}

// === New table record expiration handlers ===

// putTableRecordExpirationRequest is the request body for PutTableRecordExpirationConfiguration.
type putTableRecordExpirationRequest struct {
	Value map[string]any `json:"value"`
}

func (h *Handler) handlePutTableRecordExpirationConfiguration(
	ctx context.Context,
	r *http.Request,
	body []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	var req putTableRecordExpirationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cfg := &TableRecordExpiryConfig{Status: "DISABLED"}
	if st, ok := req.Value[keyStatusField].(string); ok {
		cfg.Status = st
	}

	if err := h.Backend.PutTableRecordExpirationConfiguration(tableArn, cfg); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: put table record expiration configuration", "tableArn", tableArn)

	return nil, nil
}

func (h *Handler) handleGetTableRecordExpirationJobStatus(
	ctx context.Context,
	r *http.Request,
	_ []byte,
) ([]byte, error) {
	tableArn := r.URL.Query().Get("tableArn")
	if tableArn == "" {
		return nil, fmt.Errorf("%w: tableArn is required", errInvalidRequest)
	}

	if err := h.Backend.ValidateTableExists(tableArn); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table record expiration job status", "tableArn", tableArn)

	return json.Marshal(map[string]any{
		keyStatusField: "SUCCEEDED",
	})
}

// === New table storage-class handler ===

func (h *Handler) handleGetTableStorageClass(ctx context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	if len(segs) < 4 { //nolint:mnd // minimum required segments
		return nil, fmt.Errorf("%w: missing tableBucketARN, namespace or name", errInvalidRequest)
	}

	bucketARN := segs[1]
	nsName := segs[2]
	name := segs[3]

	sc, err := h.Backend.GetTableStorageClass(bucketARN, splitNamespace(nsName), name)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "s3tables: got table storage class", keyName, name)

	return json.Marshal(map[string]any{
		"storageClassConfiguration": map[string]any{
			"storageClass": sc,
		},
	})
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

// parseBucketReplicationDestinations extracts replication destinations from a config map.
func parseBucketReplicationDestinations(cfg map[string]any) []ReplicationDestination {
	destsRaw, ok := cfg["destinations"]
	if !ok {
		return nil
	}

	destSlice, ok := destsRaw.([]any)
	if !ok {
		return nil
	}

	destinations := make([]ReplicationDestination, 0, len(destSlice))

	for _, d := range destSlice {
		dm, isMap := d.(map[string]any)
		if !isMap {
			continue
		}

		dest := ReplicationDestination{}
		if arn, isStr := dm["destinationBucketARN"].(string); isStr {
			dest.DestinationBucketARN = arn
		}

		destinations = append(destinations, dest)
	}

	return destinations
}
