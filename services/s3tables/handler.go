package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyArn               = "arn"
	keyName              = "name"
	keyOwnerAccountID    = "ownerAccountId"
	keyCreatedAt         = "createdAt"
	keyTableBucketARN    = "tableBucketARN"
	keyTableBucketID     = "tableBucketId"
	keyConfiguration     = "configuration"
	keyTableARN          = "tableARN"
	keyStatusField       = "status"
	keyVersionToken      = "versionToken"
	keyMetadataLocation  = "metadataLocation"
	keyNamespace         = "namespace"
	keyCreatedBy         = "createdBy"
	keyNamespaceID       = "namespaceId"
	keyContinuationToken = "continuationToken"
	keyTableArnLower     = "tableArn"
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
	bucketTypeCustomer      = "customer"
	keyType                 = "type"

	// replicationStatusCompleted is the value this emulator returns in the
	// "status" field of PutTableBucketReplicationOutput/PutTableReplicationOutput.
	// The real field is a free-form *string (not a smithy enum -- confirmed
	// via aws-sdk-go-v2/service/s3tables's types.go), and since this backend
	// applies the replication configuration synchronously within the
	// request, "COMPLETED" reflects that the configuration write itself is
	// done (not that cross-bucket data replication has finished, which this
	// in-memory emulator does not perform).
	replicationStatusCompleted = "COMPLETED"

	// replicationStatusCompletedLower is the ReplicationDestinationStatusModel.ReplicationStatus
	// value this emulator reports for each configured replication
	// destination in GetTableReplicationStatusOutput. Unlike
	// replicationStatusCompleted above, this one IS a smithy enum
	// (types.ReplicationStatus) with lowercase values ("pending",
	// "completed", "failed" -- confirmed via enums.go); since this in-memory
	// backend applies replication configuration synchronously and performs
	// no actual cross-bucket data copy, every configured destination is
	// reported as already "completed".
	replicationStatusCompletedLower = "completed"
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

			return h.handleError(c, err)
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
	errType := "InternalServerErrorException"
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

// queryInt parses a query parameter as a positive integer, returning 0 (the
// "unspecified" sentinel page.New treats as defaultLimit) when the
// parameter is absent, empty, or not a valid non-negative integer.
func queryInt(q url.Values, key string) int {
	raw := q.Get(key)
	if raw == "" {
		return 0
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}

	return n
}

// storageClassFromConfig extracts the "storageClass" field from a
// storageClassConfiguration request body map, returning "" when absent or
// not a non-empty string.
func storageClassFromConfig(cfg map[string]any) string {
	scVal, found := cfg["storageClass"]
	if !found {
		return ""
	}

	scStr, isStr := scVal.(string)
	if !isStr {
		return ""
	}

	return scStr
}

// parseReplicationConfiguration extracts the role and rules from a
// replication "configuration" request body map, matching
// TableBucketReplicationConfiguration / TableReplicationConfiguration's
// shared wire shape ({role, rules: [{destinations: [{destinationTableBucketARN}]}]})
// -- both PutTableBucketReplication and PutTableReplication accept the same
// nested shape.
func parseReplicationConfiguration(cfg map[string]any) (string, []ReplicationRule) {
	role, _ := cfg["role"].(string)

	rulesRaw, ok := cfg["rules"]
	if !ok {
		return role, nil
	}

	ruleSlice, ok := rulesRaw.([]any)
	if !ok {
		return role, nil
	}

	rules := make([]ReplicationRule, 0, len(ruleSlice))

	for _, r := range ruleSlice {
		rm, isMap := r.(map[string]any)
		if !isMap {
			continue
		}

		rules = append(rules, ReplicationRule{Destinations: parseReplicationDestinations(rm)})
	}

	return role, rules
}

// parseReplicationDestinations extracts the destinations array from a single
// replication rule map.
func parseReplicationDestinations(rule map[string]any) []ReplicationDestination {
	destsRaw, ok := rule["destinations"]
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
		if arn, isStr := dm["destinationTableBucketARN"].(string); isStr {
			dest.DestinationTableBucketARN = arn
		}

		destinations = append(destinations, dest)
	}

	return destinations
}
