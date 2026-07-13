package timestreamwrite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	targetPrefix    = "Timestream_20181101."
	keyTypeField    = "__type"
	keyMessageField = "message"
)

const (
	// endpointCachePeriodMinutes matches the real AWS DescribeEndpoints response.
	endpointCachePeriodMinutes = 1440
	// defaultTimestreamMaxResults is the default page size when MaxResults is not specified.
	defaultTimestreamMaxResults = 100
)

// AWS API limits enforced by this handler.
const (
	// maxDatabaseNameLen is the maximum length for a Timestream database name per the AWS API.
	maxDatabaseNameLen = 64
	// maxTableNameLen is the maximum length for a Timestream table name per the AWS API.
	maxTableNameLen = 256
	// maxTagKeyLen is the maximum byte length of a tag key per the AWS API.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a tag value per the AWS API.
	maxTagValueLen = 256
	// maxTagsPerResource is the maximum number of tags allowed on a single resource per the AWS API.
	maxTagsPerResource = 200
	// maxRecordsPerRequest is the maximum number of records accepted in a single WriteRecords call
	// per the AWS API.
	maxRecordsPerRequest = 100

	// minMemoryRetentionHours is the minimum allowed value for MemoryStoreRetentionPeriodInHours.
	minMemoryRetentionHours = 1
	// maxMemoryRetentionHours is the maximum allowed value for MemoryStoreRetentionPeriodInHours
	// (approximately one year).
	maxMemoryRetentionHours = 8766
	// minMagneticRetentionDays is the minimum allowed value for MagneticStoreRetentionPeriodInDays.
	minMagneticRetentionDays = 1
	// maxMagneticRetentionDays is the maximum allowed value for MagneticStoreRetentionPeriodInDays
	// (approximately 200 years).
	maxMagneticRetentionDays = 73000
	// maxDimensionsPerRecord is the maximum number of dimensions allowed per record per the AWS API.
	maxDimensionsPerRecord = 128
	// minDatabaseNameLength is the minimum length for a Timestream database name per the AWS API.
	minDatabaseNameLength = 3
)

// resourceNameRE is the allowed character set for Timestream database and table names per the
// AWS API: alphanumeric characters, hyphens, underscores, and dots.
var resourceNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// isValidMeasureValueType reports whether v is a MeasureValueType accepted by the AWS API.
func isValidMeasureValueType(v string) bool {
	switch v {
	case "DOUBLE", "BIGINT", "BOOLEAN", "VARCHAR", "TIMESTAMP", "MULTI":
		return true
	}

	return false
}

// isValidTimeUnit reports whether v is a TimeUnit accepted by the AWS API.
func isValidTimeUnit(v string) bool {
	switch v {
	case "SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS":
		return true
	}

	return false
}

// validateDatabaseName validates a Timestream database name against AWS length and format
// constraints. The name must be 3–64 characters and contain only alphanumeric characters,
// hyphens, underscores, or dots.
func validateDatabaseName(name string) error {
	if len(name) < minDatabaseNameLength {
		return fmt.Errorf(
			"%w: DatabaseName %q must be at least %d characters long",
			errInvalidRequest, name, minDatabaseNameLength,
		)
	}

	if len(name) > maxDatabaseNameLen {
		return fmt.Errorf(
			"%w: DatabaseName %q must be at most %d characters long",
			errInvalidRequest, name, maxDatabaseNameLen,
		)
	}

	if !resourceNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: DatabaseName %q must contain only alphanumeric characters, hyphens, underscores, or dots",
			errInvalidRequest, name,
		)
	}

	return nil
}

// validateTableName validates a Timestream table name against AWS length and format constraints.
// The name must be non-empty, at most 256 characters, and contain only alphanumeric characters,
// hyphens, underscores, or dots.
func validateTableName(name string) error {
	if len(name) > maxTableNameLen {
		return fmt.Errorf(
			"%w: TableName %q must be at most %d characters long",
			errInvalidRequest, name, maxTableNameLen,
		)
	}

	if !resourceNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: TableName %q must contain only alphanumeric characters, hyphens, underscores, or dots",
			errInvalidRequest, name,
		)
	}

	return nil
}

// validateRetentionPropertiesInput validates retention period values against AWS limits.
// A nil input is treated as valid (no retention properties set).
func validateRetentionPropertiesInput(rp *retentionPropertiesInput) error {
	if rp == nil {
		return nil
	}

	if rp.MemoryStoreRetentionPeriodInHours < minMemoryRetentionHours ||
		rp.MemoryStoreRetentionPeriodInHours > maxMemoryRetentionHours {
		return fmt.Errorf(
			"%w: MemoryStoreRetentionPeriodInHours must be between %d and %d, got %d",
			errInvalidRequest,
			minMemoryRetentionHours,
			maxMemoryRetentionHours,
			rp.MemoryStoreRetentionPeriodInHours,
		)
	}

	if rp.MagneticStoreRetentionPeriodInDays < minMagneticRetentionDays ||
		rp.MagneticStoreRetentionPeriodInDays > maxMagneticRetentionDays {
		return fmt.Errorf(
			"%w: MagneticStoreRetentionPeriodInDays must be between %d and %d, got %d",
			errInvalidRequest,
			minMagneticRetentionDays,
			maxMagneticRetentionDays,
			rp.MagneticStoreRetentionPeriodInDays,
		)
	}

	return nil
}

// validateTagInputs validates a tag slice against AWS constraints: max 200 tags per resource,
// keys must be 1–128 characters and non-empty, values must be 0–256 characters.
func validateTagInputs(tags []tagInput) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: too many tags: max %d per resource, got %d",
			errInvalidRequest, maxTagsPerResource, len(tags),
		)
	}

	for _, t := range tags {
		if t.Key == "" {
			return fmt.Errorf("%w: tag key cannot be empty", errInvalidRequest)
		}

		if len(t.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q exceeds maximum length of %d characters",
				errInvalidRequest, t.Key, maxTagKeyLen,
			)
		}

		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d characters",
				errInvalidRequest, t.Key, maxTagValueLen,
			)
		}
	}

	return nil
}

// validateSchemaPartitionKeys validates the CompositePartitionKey slice against AWS rules.
// DIMENSION partition keys must specify a Name field.
// An unknown Type is rejected with a ValidationException.
func validateSchemaPartitionKeys(sc *schemaInput) error {
	if sc == nil {
		return nil
	}

	for i, pk := range sc.CompositePartitionKey {
		switch pk.Type {
		case PartitionKeyTypeDimension:
			if pk.Name == "" {
				return fmt.Errorf(
					"%w: partition key at index %d has Type %q but is missing required Name field",
					errInvalidRequest, i, PartitionKeyTypeDimension,
				)
			}
		case PartitionKeyTypeMeasure:
			// MEASURE partition keys: Name is not applicable and should not be set.
		case "":
			return fmt.Errorf(
				"%w: partition key at index %d is missing required Type field",
				errInvalidRequest, i,
			)
		default:
			return fmt.Errorf(
				"%w: partition key at index %d has unknown Type %q (must be %q or %q)",
				errInvalidRequest, i, pk.Type, PartitionKeyTypeDimension, PartitionKeyTypeMeasure,
			)
		}
	}

	return nil
}

// validateRecord validates an individual WriteRecords record against AWS constraints.
// Validation runs on the merged record (after CommonAttributes is applied).
func validateRecord(r recordInput, idx int) error {
	if r.MeasureName == "" {
		return fmt.Errorf(
			"%w: record[%d] is missing required field MeasureName",
			errInvalidRequest, idx,
		)
	}

	if r.MeasureValueType != "" && !isValidMeasureValueType(r.MeasureValueType) {
		return fmt.Errorf(
			"%w: record[%d] has invalid MeasureValueType %q; valid: DOUBLE, BIGINT, BOOLEAN, VARCHAR, TIMESTAMP, MULTI",
			errInvalidRequest,
			idx,
			r.MeasureValueType,
		)
	}

	if r.TimeUnit != "" && !isValidTimeUnit(r.TimeUnit) {
		return fmt.Errorf(
			"%w: record[%d] has invalid TimeUnit %q; valid values are SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS",
			errInvalidRequest, idx, r.TimeUnit,
		)
	}

	if len(r.Dimensions) > maxDimensionsPerRecord {
		return fmt.Errorf(
			"%w: record[%d] has %d dimensions; maximum allowed is %d",
			errInvalidRequest, idx, len(r.Dimensions), maxDimensionsPerRecord,
		)
	}

	for di, d := range r.Dimensions {
		if d.Name == "" {
			return fmt.Errorf(
				"%w: record[%d] dimension[%d] has empty Name",
				errInvalidRequest, idx, di,
			)
		}

		if d.Value == "" {
			return fmt.Errorf(
				"%w: record[%d] dimension[%d] has empty Value",
				errInvalidRequest, idx, di,
			)
		}
	}

	if r.MeasureValueType == "MULTI" {
		if len(r.MeasureValues) == 0 {
			return fmt.Errorf(
				"%w: record[%d] with MeasureValueType MULTI must have non-empty MeasureValues",
				errInvalidRequest, idx,
			)
		}

		if r.MeasureValue != "" {
			return fmt.Errorf(
				"%w: record[%d] with MeasureValueType MULTI must not set MeasureValue",
				errInvalidRequest, idx,
			)
		}
	}

	// AWS API: "Version must be 1 or greater, or you will receive a ValidationException
	// error." A negative value is unambiguously invalid input regardless of the field
	// being unset (0, defaulted to 1 downstream).
	if r.Version < 0 {
		return fmt.Errorf(
			"%w: record[%d] has Version %d; must be 1 or greater",
			errInvalidRequest, idx, r.Version,
		)
	}

	return nil
}

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for Amazon Timestream Write operations.
type Handler struct {
	Backend      *InMemoryBackend
	ops          map[string]service.JSONOpFunc
	supportedOps map[string]bool
}

// NewHandler creates a new Timestream Write handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()
	supported := h.GetSupportedOperations()
	h.supportedOps = make(map[string]bool, len(supported))

	for _, op := range supported {
		h.supportedOps[op] = true
	}

	return h
}

// Reset clears the backend state and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// buildOps constructs the static dispatch table for JSON operations.
func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateBatchLoadTask":   service.WrapOp(h.handleCreateBatchLoadTask),
		"CreateDatabase":        service.WrapOp(h.handleCreateDatabase),
		"CreateTable":           service.WrapOp(h.handleCreateTable),
		"DeleteDatabase":        service.WrapOp(h.handleDeleteDatabase),
		"DeleteTable":           service.WrapOp(h.handleDeleteTable),
		"DescribeBatchLoadTask": service.WrapOp(h.handleDescribeBatchLoadTask),
		"DescribeDatabase":      service.WrapOp(h.handleDescribeDatabase),
		"DescribeEndpoints":     service.WrapOp(h.handleDescribeEndpoints),
		"DescribeTable":         service.WrapOp(h.handleDescribeTable),
		"ListBatchLoadTasks":    service.WrapOp(h.handleListBatchLoadTasks),
		"ListDatabases":         service.WrapOp(h.handleListDatabases),
		"ListTables":            service.WrapOp(h.handleListTables),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
		"ResumeBatchLoadTask":   service.WrapOp(h.handleResumeBatchLoadTask),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"UpdateDatabase":        service.WrapOp(h.handleUpdateDatabase),
		"UpdateTable":           service.WrapOp(h.handleUpdateTable),
		"WriteRecords":          service.WrapOp(h.handleWriteRecords),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "TimestreamWrite" }

// StartWorker starts the background janitor for Timestream record retention.
func (h *Handler) StartWorker(ctx context.Context) error {
	janitor := NewJanitor(h.Backend)
	go janitor.Run(ctx)

	return nil
}

// GetSupportedOperations returns the list of supported Timestream Write operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBatchLoadTask",
		"CreateDatabase",
		"CreateTable",
		"DeleteDatabase",
		"DeleteTable",
		"DescribeBatchLoadTask",
		"DescribeDatabase",
		"DescribeEndpoints",
		"DescribeTable",
		"ListBatchLoadTasks",
		"ListDatabases",
		"ListTables",
		"ListTagsForResource",
		"ResumeBatchLoadTask",
		"TagResource",
		"UntagResource",
		"UpdateDatabase",
		"UpdateTable",
		"WriteRecords",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "timestreamwrite" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler covers.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches Timestream Write requests.
// It only matches operations explicitly supported by this handler to avoid
// intercepting operations belonging to other Timestream services (e.g. TimestreamQuery)
// that share the same X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		if !strings.HasPrefix(target, targetPrefix) {
			return false
		}

		operation := strings.TrimPrefix(target, targetPrefix)

		return h.supportedOps[operation]
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Timestream Write action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, targetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource returns an empty string (no meaningful resource in request body for routing).
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		if service.IsCBORRequest(c.Request()) {
			return h.handleCBOR(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"TimestreamWrite", "application/x-amz-json-1.0",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) handleCBOR(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	if c.Request().Method != http.MethodPost {
		return c.String(http.StatusMethodNotAllowed, "Method not allowed")
	}

	const targetParts = 2

	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.SplitN(target, ".", targetParts)
	if len(parts) != targetParts || parts[1] == "" {
		return c.String(http.StatusBadRequest, "Missing or invalid X-Amz-Target")
	}
	action := parts[1]

	raw, err := readBodyBytes(c)
	if err != nil {
		log.ErrorContext(ctx, "failed to read CBOR body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	jsonBody, err := service.CBORToJSON(raw)
	if err != nil {
		log.ErrorContext(ctx, "failed to decode CBOR body", "error", err)

		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "SerializationException",
			keyMessageField: "invalid CBOR body: " + err.Error(),
		})
	}

	log.DebugContext(ctx, "TimestreamWrite CBOR request", "action", action)

	jsonResp, reqErr := h.dispatch(ctx, action, jsonBody)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	cborPayload, err := service.JSONToCBOR(jsonResp, nil)
	if err != nil {
		log.ErrorContext(ctx, "failed to encode CBOR response", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(http.StatusOK, service.ContentTypeCBOR, cborPayload)
}

func readBodyBytes(c *echo.Context) ([]byte, error) {
	r := c.Request()
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()

	const maxBody = 10 << 20 // 10 MiB

	return io.ReadAll(io.LimitReader(r.Body, maxBody))
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
	var rejectedErr *RejectedRecordsError

	switch {
	case errors.As(err, &rejectedErr):
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyTypeField:      "RejectedRecordsException",
			keyMessageField:   err.Error(),
			"RejectedRecords": rejectedErr.RejectedRecords,
		})
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrConflict):
		// The awsJson1.0 protocol has no per-exception HTTP status: every client-fault
		// error (including ConflictException) is reported over HTTP 400, and the SDK
		// determines the concrete exception type from the body's __type field, not the
		// status code (see aws-sdk-go-v2/service/timestreamwrite deserializers.go).
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})
	case errors.Is(err, awserr.ErrInvalidParameter) ||
		errors.Is(err, errInvalidRequest) ||
		errors.Is(err, errUnknownAction) ||
		errors.As(err, &syntaxErr) ||
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyTypeField:    "ValidationException",
			keyMessageField: err.Error(),
		})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{
			keyTypeField:    "InternalServerError",
			keyMessageField: err.Error(),
		})
	}
}

// --- input/output types ---

type createDatabaseInput struct {
	DatabaseName string     `json:"DatabaseName"`
	KmsKeyID     string     `json:"KmsKeyId,omitempty"`
	Tags         []tagInput `json:"Tags"`
}

type databaseOutput struct {
	Database databaseView `json:"Database"`
}

type databaseView struct {
	Arn             string  `json:"Arn"`
	DatabaseName    string  `json:"DatabaseName"`
	KmsKeyID        string  `json:"KmsKeyId,omitempty"`
	CreationTime    float64 `json:"CreationTime"`
	LastUpdatedTime float64 `json:"LastUpdatedTime"`
	TableCount      int     `json:"TableCount"`
}

type listDatabasesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listDatabasesOutput struct {
	NextToken string         `json:"NextToken,omitempty"`
	Databases []databaseView `json:"Databases"`
}

type databaseNameInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type updateDatabaseInput struct {
	DatabaseName string `json:"DatabaseName"`
	KmsKeyID     string `json:"KmsKeyId"`
}

type createTableInput struct {
	Schema                       *schemaInput                  `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	Tags                         []tagInput                    `json:"Tags"`
}

type retentionPropertiesInput struct {
	MemoryStoreRetentionPeriodInHours  int64 `json:"MemoryStoreRetentionPeriodInHours"`
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays"`
}

type s3ConfigInput struct {
	BucketName       string `json:"BucketName,omitempty"`
	ObjectKeyPrefix  string `json:"ObjectKeyPrefix,omitempty"`
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KmsKeyID         string `json:"KmsKeyId,omitempty"`
}

type magneticStoreRejectedDataLocationInput struct {
	S3Configuration *s3ConfigInput `json:"S3Configuration,omitempty"`
}

type magneticStoreWritePropsInput struct {
	MagneticStoreRejectedDataLocation *magneticStoreRejectedDataLocationInput `json:"MagneticStoreRejectedDataLocation,omitempty"` //nolint:lll // AWS field name is inherently long
	EnableMagneticStoreWrites         bool                                    `json:"EnableMagneticStoreWrites"`
}

type partitionKeyInput struct {
	Type                string `json:"Type"`
	Name                string `json:"Name,omitempty"`
	EnforcementInRecord string `json:"EnforcementInRecord,omitempty"`
}

type schemaInput struct {
	CompositePartitionKey []partitionKeyInput `json:"CompositePartitionKey,omitempty"`
}

type tableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type updateTableInput struct {
	Schema                       *schemaInput                  `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
}

type tableOutput struct {
	Table tableView `json:"Table"`
}

type partitionKeyView struct {
	Type                string `json:"Type"`
	Name                string `json:"Name,omitempty"`
	EnforcementInRecord string `json:"EnforcementInRecord,omitempty"`
}

type schemaView struct {
	CompositePartitionKey []partitionKeyView `json:"CompositePartitionKey,omitempty"`
}

type tableView struct {
	Schema                       *schemaView                   `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	Arn                          string                        `json:"Arn"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	TableStatus                  string                        `json:"TableStatus"`
	CreationTime                 float64                       `json:"CreationTime"`
	LastUpdatedTime              float64                       `json:"LastUpdatedTime"`
}

type listTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
	NextToken    string `json:"NextToken"`
	MaxResults   int    `json:"MaxResults"`
}

type listTablesOutput struct {
	NextToken string      `json:"NextToken,omitempty"`
	Tables    []tableView `json:"Tables"`
}

type writeRecordsInput struct {
	CommonAttributes *recordInput  `json:"CommonAttributes,omitempty"`
	DatabaseName     string        `json:"DatabaseName"`
	TableName        string        `json:"TableName"`
	Records          []recordInput `json:"Records"`
}

type measureValueInput struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
	Type  string `json:"Type"`
}

type recordInput struct {
	MeasureName      string              `json:"MeasureName"`
	MeasureValue     string              `json:"MeasureValue"`
	MeasureValueType string              `json:"MeasureValueType"`
	Time             string              `json:"Time"`
	TimeUnit         string              `json:"TimeUnit"`
	Dimensions       []dimensionInput    `json:"Dimensions"`
	MeasureValues    []measureValueInput `json:"MeasureValues"`
	Version          int64               `json:"Version"`
}

type dimensionInput struct {
	Name               string `json:"Name"`
	Value              string `json:"Value"`
	DimensionValueType string `json:"DimensionValueType,omitempty"`
}

type writeRecordsOutput struct {
	RecordsIngested struct {
		Total         int32 `json:"Total"`
		MemoryStore   int32 `json:"MemoryStore"`
		MagneticStore int32 `json:"MagneticStore"`
	} `json:"RecordsIngested"`
}

type tagResourceInput struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagInput `json:"Tags"`
}

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []tagInput `json:"Tags"`
}

type describeEndpointsInput struct{}

type endpointOutput struct {
	Endpoints []endpointEntry `json:"Endpoints"`
}

type endpointEntry struct {
	Address              string `json:"Address"`
	CachePeriodInMinutes int64  `json:"CachePeriodInMinutes"`
}

type emptyOutput struct{}

// --- handlers ---

// buildCreateTableInput converts handler-level optional table-property inputs into a
// *CreateTableInput for the backend.  Returns nil when all inputs are nil.
func buildCreateTableInput(
	rp *retentionPropertiesInput,
	mswp *magneticStoreWritePropsInput,
	sc *schemaInput,
) *CreateTableInput {
	if rp == nil && mswp == nil && sc == nil {
		return nil
	}

	inp := &CreateTableInput{}

	if rp != nil {
		inp.RetentionProperties = &RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  rp.MemoryStoreRetentionPeriodInHours,
			MagneticStoreRetentionPeriodInDays: rp.MagneticStoreRetentionPeriodInDays,
		}
	}

	if mswp != nil {
		backendMSWP := &MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: mswp.EnableMagneticStoreWrites,
		}

		if mswp.MagneticStoreRejectedDataLocation != nil {
			loc := mswp.MagneticStoreRejectedDataLocation
			backendLoc := &MagneticStoreRejectedDataLocation{}

			if loc.S3Configuration != nil {
				backendLoc.S3Configuration = &S3Configuration{
					BucketName:       loc.S3Configuration.BucketName,
					ObjectKeyPrefix:  loc.S3Configuration.ObjectKeyPrefix,
					EncryptionOption: loc.S3Configuration.EncryptionOption,
					KmsKeyID:         loc.S3Configuration.KmsKeyID,
				}
			}

			backendMSWP.MagneticStoreRejectedDataLocation = backendLoc
		}

		inp.MagneticStoreWriteProperties = backendMSWP
	}

	if sc != nil && len(sc.CompositePartitionKey) > 0 {
		keys := make([]PartitionKey, 0, len(sc.CompositePartitionKey))
		for _, pk := range sc.CompositePartitionKey {
			keys = append(keys, PartitionKey(pk))
		}

		inp.Schema = &Schema{CompositePartitionKey: keys}
	}

	return inp
}

func toDatabaseView(db *Database) databaseView {
	return databaseView{
		Arn:             db.ARN,
		CreationTime:    float64(db.CreationTime.Unix()),
		DatabaseName:    db.DatabaseName,
		KmsKeyID:        db.KmsKeyID,
		LastUpdatedTime: float64(db.LastUpdatedTime.Unix()),
		TableCount:      db.TableCount,
	}
}

func toTableView(tbl *Table) tableView {
	v := tableView{
		Arn:             tbl.ARN,
		CreationTime:    float64(tbl.CreationTime.Unix()),
		DatabaseName:    tbl.DatabaseName,
		LastUpdatedTime: float64(tbl.LastUpdatedTime.Unix()),
		TableName:       tbl.TableName,
		TableStatus:     tbl.TableStatus,
	}

	if tbl.RetentionProperties != nil {
		v.RetentionProperties = &retentionPropertiesInput{
			MemoryStoreRetentionPeriodInHours:  tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours,
			MagneticStoreRetentionPeriodInDays: tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays,
		}
	}

	if tbl.MagneticStoreWriteProperties != nil {
		mswp := &magneticStoreWritePropsInput{
			EnableMagneticStoreWrites: tbl.MagneticStoreWriteProperties.EnableMagneticStoreWrites,
		}

		if tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation != nil {
			loc := tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation
			inputLoc := &magneticStoreRejectedDataLocationInput{}

			if loc.S3Configuration != nil {
				inputLoc.S3Configuration = &s3ConfigInput{
					BucketName:       loc.S3Configuration.BucketName,
					ObjectKeyPrefix:  loc.S3Configuration.ObjectKeyPrefix,
					EncryptionOption: loc.S3Configuration.EncryptionOption,
					KmsKeyID:         loc.S3Configuration.KmsKeyID,
				}
			}

			mswp.MagneticStoreRejectedDataLocation = inputLoc
		}

		v.MagneticStoreWriteProperties = mswp
	}

	if tbl.Schema != nil && len(tbl.Schema.CompositePartitionKey) > 0 {
		keys := make([]partitionKeyView, 0, len(tbl.Schema.CompositePartitionKey))
		for _, pk := range tbl.Schema.CompositePartitionKey {
			keys = append(keys, partitionKeyView(pk))
		}

		v.Schema = &schemaView{CompositePartitionKey: keys}
	}

	return v
}

func (h *Handler) handleCreateDatabase(
	_ context.Context,
	in *createDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	// Validate database name format and length per AWS API constraints.
	if err := validateDatabaseName(in.DatabaseName); err != nil {
		return nil, err
	}

	// Validate tags per AWS API constraints.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	db, err := h.Backend.CreateDatabase(in.DatabaseName, in.KmsKeyID, tags)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleDescribeDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.DescribeDatabase(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleListDatabases(
	_ context.Context,
	in *listDatabasesInput,
) (*listDatabasesOutput, error) {
	dbs := h.Backend.ListDatabases()
	pg := page.New(dbs, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]databaseView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toDatabaseView(&pg.Data[i]))
	}

	return &listDatabasesOutput{Databases: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleDeleteDatabase(
	_ context.Context,
	in *databaseNameInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	// AWS API requires all tables to be deleted before a database can be deleted.
	tbls, err := h.Backend.ListTables(in.DatabaseName)
	if err != nil {
		// Propagate not-found and other backend errors.
		return nil, err
	}

	if len(tbls) > 0 {
		return nil, fmt.Errorf(
			"%w: database %q cannot be deleted: it still contains %d table(s); "+
				"delete all tables before deleting the database",
			errInvalidRequest, in.DatabaseName, len(tbls),
		)
	}

	err = h.Backend.DeleteDatabase(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateDatabase(
	_ context.Context,
	in *updateDatabaseInput,
) (*databaseOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	db, err := h.Backend.UpdateDatabase(in.DatabaseName, in.KmsKeyID)
	if err != nil {
		return nil, err
	}

	return &databaseOutput{Database: toDatabaseView(db)}, nil
}

func (h *Handler) handleCreateTable(
	_ context.Context,
	in *createTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	// Validate table name format and length per AWS API constraints.
	if err := validateTableName(in.TableName); err != nil {
		return nil, err
	}

	// Validate retention properties ranges per AWS API constraints.
	if err := validateRetentionPropertiesInput(in.RetentionProperties); err != nil {
		return nil, err
	}

	// Validate schema partition key configuration per AWS API constraints.
	if err := validateSchemaPartitionKeys(in.Schema); err != nil {
		return nil, err
	}

	// Validate tags per AWS API constraints.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	inp := buildCreateTableInput(in.RetentionProperties, in.MagneticStoreWriteProperties, in.Schema)

	tbl, err := h.Backend.CreateTable(in.DatabaseName, in.TableName, tags, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleDescribeTable(
	_ context.Context,
	in *tableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tbl, err := h.Backend.DescribeTable(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleListTables(
	_ context.Context,
	in *listTablesInput,
) (*listTablesOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	tbls, err := h.Backend.ListTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	pg := page.New(tbls, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]tableView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toTableView(&pg.Data[i]))
	}

	return &listTablesOutput{Tables: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleDeleteTable(
	_ context.Context,
	in *tableInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTable(in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateTable(
	_ context.Context,
	in *updateTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	// Validate retention properties ranges per AWS API constraints.
	if err := validateRetentionPropertiesInput(in.RetentionProperties); err != nil {
		return nil, err
	}

	// Validate schema partition key configuration per AWS API constraints.
	if err := validateSchemaPartitionKeys(in.Schema); err != nil {
		return nil, err
	}

	var inp *UpdateTableInput
	cti := buildCreateTableInput(in.RetentionProperties, in.MagneticStoreWriteProperties, in.Schema)

	if cti != nil {
		inp = &UpdateTableInput{
			RetentionProperties:          cti.RetentionProperties,
			MagneticStoreWriteProperties: cti.MagneticStoreWriteProperties,
			Schema:                       cti.Schema,
		}
	}

	tbl, err := h.Backend.UpdateTable(in.DatabaseName, in.TableName, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleWriteRecords(
	_ context.Context,
	in *writeRecordsInput,
) (*writeRecordsOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	// AWS API enforces a maximum of 100 records per WriteRecords request.
	if len(in.Records) > maxRecordsPerRequest {
		return nil, fmt.Errorf(
			"%w: WriteRecords accepts at most %d records per request, got %d",
			errInvalidRequest, maxRecordsPerRequest, len(in.Records),
		)
	}

	records := make([]Record, 0, len(in.Records))

	for i, r := range in.Records {
		merged := mergeRecordWithCommon(r, in.CommonAttributes)
		if err := validateRecord(merged, i); err != nil {
			return nil, err
		}

		records = append(records, recordInputToBackend(merged))
	}

	result, err := h.Backend.WriteRecords(in.DatabaseName, in.TableName, records)
	if err != nil {
		return nil, err
	}

	out := &writeRecordsOutput{}
	out.RecordsIngested.Total = result.Total
	out.RecordsIngested.MemoryStore = result.MemoryStore
	out.RecordsIngested.MagneticStore = result.MagneticStore

	return out, nil
}

// mergeRecordWithCommon merges CommonAttributes into a record per AWS semantics:
// record-specific values take priority; common fills in missing fields and dimensions.
func mergeRecordWithCommon(r recordInput, common *recordInput) recordInput {
	if common == nil {
		return r
	}

	merged := r

	// Merge dimensions: start with common, override with record-specific on name conflict.
	dimSet := make(map[string]dimensionInput, len(common.Dimensions)+len(r.Dimensions))
	for _, d := range common.Dimensions {
		dimSet[d.Name] = d
	}

	for _, d := range r.Dimensions {
		dimSet[d.Name] = d
	}

	if len(dimSet) > 0 {
		dims := make([]dimensionInput, 0, len(dimSet))
		for _, d := range dimSet {
			dims = append(dims, d)
		}

		sort.Slice(dims, func(i, j int) bool { return dims[i].Name < dims[j].Name })
		merged.Dimensions = dims
	}

	// Fill scalar fields from common when record does not set them.
	if merged.MeasureName == "" {
		merged.MeasureName = common.MeasureName
	}

	if merged.MeasureValue == "" {
		merged.MeasureValue = common.MeasureValue
	}

	if merged.MeasureValueType == "" {
		merged.MeasureValueType = common.MeasureValueType
	}

	if merged.Time == "" {
		merged.Time = common.Time
	}

	if merged.TimeUnit == "" {
		merged.TimeUnit = common.TimeUnit
	}

	if merged.Version == 0 {
		merged.Version = common.Version
	}

	if len(merged.MeasureValues) == 0 {
		merged.MeasureValues = common.MeasureValues
	}

	return merged
}

// recordInputToBackend converts a handler-level recordInput to the backend Record type.
func recordInputToBackend(r recordInput) Record {
	dims := make([]Dimension, 0, len(r.Dimensions))
	for _, d := range r.Dimensions {
		dims = append(dims, Dimension(d))
	}

	mvs := make([]MeasureValue, 0, len(r.MeasureValues))
	for _, mv := range r.MeasureValues {
		mvs = append(mvs, MeasureValue(mv))
	}

	return Record{
		Dimensions:       dims,
		MeasureName:      r.MeasureName,
		MeasureValue:     r.MeasureValue,
		MeasureValueType: r.MeasureValueType,
		Time:             r.Time,
		TimeUnit:         r.TimeUnit,
		MeasureValues:    mvs,
		Version:          r.Version,
	}
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	// Validate tags per AWS API constraints before storing.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceARN, tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsInput,
) (*listTagsOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tagsMap := h.Backend.ListTagsForResource(in.ResourceARN)
	tags := make([]tagInput, 0, len(tagsMap))

	for k, v := range tagsMap {
		tags = append(tags, tagInput{Key: k, Value: v})
	}

	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })

	return &listTagsOutput{Tags: tags}, nil
}

func (h *Handler) handleDescribeEndpoints(
	_ context.Context,
	_ *describeEndpointsInput,
) (*endpointOutput, error) {
	return &endpointOutput{
		Endpoints: []endpointEntry{
			{Address: "localhost", CachePeriodInMinutes: endpointCachePeriodMinutes},
		},
	}, nil
}

// --- batch load task types ---

type dataSourceS3ConfigInput struct {
	BucketName      string `json:"BucketName"`
	ObjectKeyPrefix string `json:"ObjectKeyPrefix,omitempty"`
	DataFormat      string `json:"DataFormat,omitempty"`
}

type dataSourceConfigInput struct {
	DataSourceS3Configuration *dataSourceS3ConfigInput `json:"DataSourceS3Configuration,omitempty"`
	DataFormat                string                   `json:"DataFormat,omitempty"`
}

type reportConfigInput struct {
	ReportS3Configuration *dataSourceS3ConfigInput `json:"ReportS3Configuration,omitempty"`
}

type createBatchLoadTaskInput struct {
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	ClientToken             string                 `json:"ClientToken"`
}

type createBatchLoadTaskOutput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskIDInput struct {
	TaskID string `json:"TaskId"`
}

type batchLoadTaskDescriptionView struct {
	ResumableUntil          *float64               `json:"ResumableUntil,omitempty"`
	DataSourceConfiguration *dataSourceConfigInput `json:"DataSourceConfiguration,omitempty"`
	ReportConfiguration     *reportConfigInput     `json:"ReportConfiguration,omitempty"`
	ProgressReport          *progressReportView    `json:"ProgressReport,omitempty"`
	TaskID                  string                 `json:"TaskId"`
	TargetDatabaseName      string                 `json:"TargetDatabaseName"`
	TargetTableName         string                 `json:"TargetTableName"`
	TaskStatus              string                 `json:"TaskStatus"`
	ErrorMessage            string                 `json:"ErrorMessage,omitempty"`
	CreationTime            float64                `json:"CreationTime"`
	LastUpdatedTime         float64                `json:"LastUpdatedTime"`
	RecordVersion           int64                  `json:"RecordVersion,omitempty"`
}

type describeBatchLoadTaskOutput struct {
	BatchLoadTaskDescription batchLoadTaskDescriptionView `json:"BatchLoadTaskDescription"`
}

type listBatchLoadTasksInput struct {
	NextToken  string `json:"NextToken"`
	TaskStatus string `json:"TaskStatus"`
	MaxResults int    `json:"MaxResults"`
}

type batchLoadTaskSummaryView struct {
	ResumableUntil  *float64 `json:"ResumableUntil,omitempty"`
	TaskID          string   `json:"TaskId"`
	DatabaseName    string   `json:"DatabaseName"`
	TableName       string   `json:"TableName"`
	TaskStatus      string   `json:"TaskStatus"`
	CreationTime    float64  `json:"CreationTime"`
	LastUpdatedTime float64  `json:"LastUpdatedTime"`
}

type listBatchLoadTasksOutput struct {
	NextToken      string                     `json:"NextToken,omitempty"`
	BatchLoadTasks []batchLoadTaskSummaryView `json:"BatchLoadTasks"`
}

type progressReportView struct {
	BytesMetered            int64 `json:"BytesMetered,omitempty"`
	FileFailures            int64 `json:"FileFailures,omitempty"`
	ParseFailures           int64 `json:"ParseFailures,omitempty"`
	RecordIngestionFailures int64 `json:"RecordIngestionFailures,omitempty"`
	RecordsIngested         int64 `json:"RecordsIngested,omitempty"`
	RecordsProcessed        int64 `json:"RecordsProcessed,omitempty"`
}

func toBatchLoadTaskDescriptionView(task *BatchLoadTask) batchLoadTaskDescriptionView {
	v := batchLoadTaskDescriptionView{
		TaskID:             task.TaskID,
		TargetDatabaseName: task.TargetDatabaseName,
		TargetTableName:    task.TargetTableName,
		TaskStatus:         task.TaskStatus,
		CreationTime:       float64(task.CreationTime.Unix()),
		LastUpdatedTime:    float64(task.LastUpdatedTime.Unix()),
		ErrorMessage:       task.ErrorMessage,
		RecordVersion:      task.RecordVersion,
	}

	if task.ProgressReport != nil {
		v.ProgressReport = &progressReportView{
			BytesMetered:            task.ProgressReport.BytesMetered,
			FileFailures:            task.ProgressReport.FileFailures,
			ParseFailures:           task.ProgressReport.ParseFailures,
			RecordIngestionFailures: task.ProgressReport.RecordIngestionFailures,
			RecordsIngested:         task.ProgressReport.RecordsIngested,
			RecordsProcessed:        task.ProgressReport.RecordsProcessed,
		}
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	if task.DataSourceConfiguration != nil {
		cfg := &dataSourceConfigInput{
			DataFormat: task.DataSourceConfiguration.DataFormat,
		}

		if task.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := task.DataSourceConfiguration.DataSourceS3Configuration
			cfg.DataSourceS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
				DataFormat:      s3.DataFormat,
			}
		}

		v.DataSourceConfiguration = cfg
	}

	if task.ReportConfiguration != nil {
		rpt := &reportConfigInput{}

		if task.ReportConfiguration.ReportS3Configuration != nil {
			s3 := task.ReportConfiguration.ReportS3Configuration
			rpt.ReportS3Configuration = &dataSourceS3ConfigInput{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}

		v.ReportConfiguration = rpt
	}

	return v
}

func toBatchLoadTaskSummaryView(task *BatchLoadTask) batchLoadTaskSummaryView {
	v := batchLoadTaskSummaryView{
		TaskID:          task.TaskID,
		DatabaseName:    task.TargetDatabaseName,
		TableName:       task.TargetTableName,
		TaskStatus:      task.TaskStatus,
		CreationTime:    float64(task.CreationTime.Unix()),
		LastUpdatedTime: float64(task.LastUpdatedTime.Unix()),
	}

	if task.ResumableUntil != nil {
		ts := float64(task.ResumableUntil.Unix())
		v.ResumableUntil = &ts
	}

	return v
}

// tagsFromInput converts a slice of tagInput to a map[string]string.
func tagsFromInput(tags []tagInput) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

func (h *Handler) handleCreateBatchLoadTask(
	_ context.Context,
	in *createBatchLoadTaskInput,
) (*createBatchLoadTaskOutput, error) {
	if in.TargetDatabaseName == "" || in.TargetTableName == "" {
		return nil, fmt.Errorf("%w: TargetDatabaseName and TargetTableName are required", errInvalidRequest)
	}

	if in.DataSourceConfiguration == nil {
		return nil, fmt.Errorf("%w: DataSourceConfiguration is required", errInvalidRequest)
	}

	var dataSourceCfg *DataSourceConfiguration

	if in.DataSourceConfiguration != nil {
		dataSourceCfg = &DataSourceConfiguration{
			DataFormat: in.DataSourceConfiguration.DataFormat,
		}

		if in.DataSourceConfiguration.DataSourceS3Configuration != nil {
			s3 := in.DataSourceConfiguration.DataSourceS3Configuration
			dataSourceCfg.DataSourceS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
				DataFormat:      s3.DataFormat,
			}
		}
	}

	var reportCfg *ReportConfiguration

	if in.ReportConfiguration != nil {
		reportCfg = &ReportConfiguration{}

		if in.ReportConfiguration.ReportS3Configuration != nil {
			s3 := in.ReportConfiguration.ReportS3Configuration
			reportCfg.ReportS3Configuration = &DataSourceS3Configuration{
				BucketName:      s3.BucketName,
				ObjectKeyPrefix: s3.ObjectKeyPrefix,
			}
		}
	}

	task, err := h.Backend.CreateBatchLoadTask(in.TargetDatabaseName, in.TargetTableName, dataSourceCfg, reportCfg)
	if err != nil {
		return nil, err
	}

	return &createBatchLoadTaskOutput{TaskID: task.TaskID}, nil
}

func (h *Handler) handleDescribeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*describeBatchLoadTaskOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	task, err := h.Backend.DescribeBatchLoadTask(in.TaskID)
	if err != nil {
		return nil, err
	}

	return &describeBatchLoadTaskOutput{
		BatchLoadTaskDescription: toBatchLoadTaskDescriptionView(task),
	}, nil
}

func (h *Handler) handleListBatchLoadTasks(
	_ context.Context,
	in *listBatchLoadTasksInput,
) (*listBatchLoadTasksOutput, error) {
	tasks := h.Backend.ListBatchLoadTasks(in.TaskStatus)
	pg := page.New(tasks, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]batchLoadTaskSummaryView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toBatchLoadTaskSummaryView(&pg.Data[i]))
	}

	return &listBatchLoadTasksOutput{BatchLoadTasks: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleResumeBatchLoadTask(
	_ context.Context,
	in *batchLoadTaskIDInput,
) (*emptyOutput, error) {
	if in.TaskID == "" {
		return nil, fmt.Errorf("%w: TaskId is required", errInvalidRequest)
	}

	if err := h.Backend.ResumeBatchLoadTask(in.TaskID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
