package qldb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
	keyTypeField    = "__type"
)

const (
	opCancelJournalKinesisStream         = "CancelJournalKinesisStream"
	opCreateLedger                       = "CreateLedger"
	opDeleteLedger                       = "DeleteLedger"
	opDescribeJournalKinesisStream       = "DescribeJournalKinesisStream"
	opDescribeJournalS3Export            = "DescribeJournalS3Export"
	opDescribeLedger                     = "DescribeLedger"
	opExportJournalToS3                  = "ExportJournalToS3"
	opGetBlock                           = "GetBlock"
	opGetDigest                          = "GetDigest"
	opGetRevision                        = "GetRevision"
	opListJournalKinesisStreamsForLedger = "ListJournalKinesisStreamsForLedger"
	opListJournalS3Exports               = "ListJournalS3Exports"
	opListJournalS3ExportsForLedger      = "ListJournalS3ExportsForLedger"
	opListLedgers                        = "ListLedgers"
	opListTagsForResource                = "ListTagsForResource"
	opTagResource                        = "TagResource"
	opUntagResource                      = "UntagResource"
	opUpdateLedger                       = "UpdateLedger"
)

const (
	qldbService        = "qldb"
	qldbMatchPriority  = 87
	qldbLedgersPath    = "/ledgers"
	qldbTagsPrefix     = "/tags/"
	qldbLedgersSegment = "ledgers"
	// qldbJournalKinesisSegment is the path segment for journal kinesis streams.
	qldbJournalKinesisSegment = "journal-kinesis-streams"
	// qldbJournalS3Segment is the path segment for journal S3 exports.
	qldbJournalS3Segment = "journal-s3-exports"
	// qldbBlockSegment is the path segment for block operations.
	qldbBlockSegment = "block"
	// qldbDigestSegment is the path segment for digest operations.
	qldbDigestSegment = "digest"
	// qldbRevisionSegment is the path segment for revision operations.
	qldbRevisionSegment = "revision"
	// qldbJournalS3ExportsPath is the global journal S3 exports path.
	qldbJournalS3ExportsPath = "/journal-s3-exports"
	// qldbLedgerPathMinSegments is the minimum path segments for /ledgers/{name}: ["ledgers", "{name}"].
	qldbLedgerPathMinSegments = 2
	// qldbLedgerSubPathSegments is the number of path segments for /ledgers/{name}/{sub}: 3.
	qldbLedgerSubPathSegments = 3
	// qldbLedgerSubIDSegments is the number of path segments for /ledgers/{name}/{sub}/{id}: 4.
	qldbLedgerSubIDSegments = 4
	// opUnknown is the operation name returned when no operation matches.
	opUnknown = "Unknown"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
	errMissingARN     = errors.New("missing resource ARN in path")
)

// Handler is the HTTP handler for the QLDB REST API.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new QLDB handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "QLDB" }

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// GetSupportedOperations returns the list of supported QLDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCancelJournalKinesisStream,
		opCreateLedger,
		opDeleteLedger,
		opDescribeJournalKinesisStream,
		opDescribeJournalS3Export,
		opDescribeLedger,
		opExportJournalToS3,
		opGetBlock,
		opGetDigest,
		opGetRevision,
		opListJournalKinesisStreamsForLedger,
		opListJournalS3Exports,
		opListJournalS3ExportsForLedger,
		opListLedgers,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		opUpdateLedger,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return qldbService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches QLDB API requests.
// All path-based matches are gated on the SigV4 service name to prevent
// routing conflicts with other services that share similar REST paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != qldbService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, qldbLedgersPath) ||
			strings.HasPrefix(path, qldbTagsPrefix) ||
			strings.HasPrefix(path, qldbJournalS3ExportsPath)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return qldbMatchPriority }

// ExtractOperation extracts the operation name from the HTTP method and path.
// Tags paths (/tags/*) are matched first, the global /journal-s3-exports path,
// then /ledgers for list/create, /ledgers/{name} for describe/update/delete,
// and /ledgers/{name}/{sub} for sub-resource operations.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if strings.HasPrefix(path, qldbTagsPrefix) {
		return extractTagsOperation(method)
	}

	// Global /journal-s3-exports (no ledger name prefix).
	if path == qldbJournalS3ExportsPath && method == http.MethodGet {
		return opListJournalS3Exports
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) == 0 || segments[0] != qldbLedgersSegment {
		return opUnknown
	}

	return extractLedgerOp(segments, method)
}

// extractTagsOperation returns the operation name for tags path requests.
func extractTagsOperation(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
}

// extractLedgerOp returns the operation name for /ledgers/... path requests.
func extractLedgerOp(segments []string, method string) string {
	switch len(segments) {
	case 1:
		return extractLedgersRootOp(method)
	case qldbLedgerPathMinSegments:
		if segments[1] != "" {
			return extractLedgerByNameOp(method)
		}
	case qldbLedgerSubPathSegments:
		if segments[1] != "" {
			return extractLedgerSubOp(segments[2], method)
		}
	case qldbLedgerSubIDSegments:
		if segments[1] != "" && segments[3] != "" {
			return extractLedgerSubIDOp(segments[2], method)
		}
	}

	return opUnknown
}

func extractLedgersRootOp(method string) string {
	switch method {
	case http.MethodGet:
		return opListLedgers
	case http.MethodPost:
		return opCreateLedger
	}

	return opUnknown
}

func extractLedgerByNameOp(method string) string {
	switch method {
	case http.MethodGet:
		return opDescribeLedger
	case http.MethodPatch:
		return opUpdateLedger
	case http.MethodDelete:
		return opDeleteLedger
	}

	return opUnknown
}

func extractLedgerSubOp(sub, method string) string {
	switch {
	case sub == qldbJournalKinesisSegment && method == http.MethodGet:
		return opListJournalKinesisStreamsForLedger
	case sub == qldbJournalS3Segment && method == http.MethodPost:
		return opExportJournalToS3
	case sub == qldbJournalS3Segment && method == http.MethodGet:
		return opListJournalS3ExportsForLedger
	case sub == qldbBlockSegment && method == http.MethodPost:
		return opGetBlock
	case sub == qldbDigestSegment && method == http.MethodPost:
		return opGetDigest
	case sub == qldbRevisionSegment && method == http.MethodPost:
		return opGetRevision
	}

	return opUnknown
}

func extractLedgerSubIDOp(sub, method string) string {
	switch {
	case sub == qldbJournalKinesisSegment && method == http.MethodGet:
		return opDescribeJournalKinesisStream
	case sub == qldbJournalKinesisSegment && method == http.MethodDelete:
		return opCancelJournalKinesisStream
	case sub == qldbJournalS3Segment && method == http.MethodGet:
		return opDescribeJournalS3Export
	}

	return opUnknown
}

// ExtractResource extracts the ledger name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= qldbLedgerPathMinSegments && segments[0] == qldbLedgersSegment {
		return segments[1]
	}

	return ""
}

// Handler returns the Echo handler function for QLDB requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		query := c.Request().URL.Query()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "qldb: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, path, query, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op, path string, query url.Values, body []byte) ([]byte, error) {
	if res, ok, err := h.dispatchLedgerOps(ctx, op, path, query, body); ok {
		return res, err
	}

	if res, ok, err := h.dispatchJournalOps(ctx, op, path, body); ok {
		return res, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

func (h *Handler) dispatchLedgerOps(
	ctx context.Context, op, path string, query url.Values, body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCreateLedger:
		res, err := h.handleCreateLedger(ctx, body)

		return res, true, err
	case opDescribeLedger:
		res, err := h.handleDescribeLedger(ctx, path)

		return res, true, err
	case opListLedgers:
		res, err := h.handleListLedgers(ctx)

		return res, true, err
	case opUpdateLedger:
		res, err := h.handleUpdateLedger(ctx, path, body)

		return res, true, err
	case opDeleteLedger:
		return nil, true, h.handleDeleteLedger(ctx, path)
	case opTagResource:
		return nil, true, h.handleTagResource(ctx, path, body)
	case opUntagResource:
		return nil, true, h.handleUntagResource(ctx, path, query)
	case opListTagsForResource:
		res, err := h.handleListTagsForResource(ctx, path)

		return res, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchJournalOps(
	ctx context.Context, op, path string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case opCancelJournalKinesisStream:
		res, err := h.handleCancelJournalKinesisStream(ctx, path)

		return res, true, err
	case opDescribeJournalKinesisStream:
		res, err := h.handleDescribeJournalKinesisStream(ctx, path)

		return res, true, err
	case opListJournalKinesisStreamsForLedger:
		res, err := h.handleListJournalKinesisStreamsForLedger(ctx, path)

		return res, true, err
	case opExportJournalToS3:
		res, err := h.handleExportJournalToS3(ctx, path, body)

		return res, true, err
	case opDescribeJournalS3Export:
		res, err := h.handleDescribeJournalS3Export(ctx, path)

		return res, true, err
	case opListJournalS3Exports:
		res, err := h.handleListJournalS3Exports(ctx)

		return res, true, err
	case opListJournalS3ExportsForLedger:
		res, err := h.handleListJournalS3ExportsForLedger(ctx, path)

		return res, true, err
	case opGetBlock:
		res, err := h.handleGetBlock(ctx, path)

		return res, true, err
	case opGetDigest:
		res, err := h.handleGetDigest(ctx, path)

		return res, true, err
	case opGetRevision:
		res, err := h.handleGetRevision(ctx, path)

		return res, true, err
	}

	return nil, false, nil
}

// writeBackendError maps backend errors to HTTP responses.
func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrValidation), errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	case errors.Is(err, ErrDeletionProtection):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourcePreconditionNotMetException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, ErrNotFound), errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists), errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceAlreadyExistsException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	return h.writeBackendError(c, err)
}

// ledgerARN builds an ARN for a QLDB ledger.
func (h *Handler) ledgerARN(name string) string {
	return arn.Build("qldb", h.Region, h.AccountID, "ledger/"+name)
}

// epochSeconds converts a [time.Time] to Unix epoch seconds as float64,
// as required by the AWS REST-JSON protocol for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

type createLedgerRequest struct {
	Tags              map[string]string `json:"Tags"`
	Name              string            `json:"Name"`
	PermissionsMode   string            `json:"PermissionsMode"`
	DeletionProtected bool              `json:"DeletionProtection"`
}

type ledgerResponse struct {
	Tags              map[string]string `json:"Tags,omitempty"`
	Arn               string            `json:"Arn"`
	Name              string            `json:"Name"`
	State             string            `json:"State"`
	PermissionsMode   string            `json:"PermissionsMode"`
	CreationDateTime  float64           `json:"CreationDateTime"`
	DeletionProtected bool              `json:"DeletionProtection"`
}

func toLedgerResponse(l *Ledger) ledgerResponse {
	return ledgerResponse{
		Arn:               l.ARN,
		Name:              l.Name,
		State:             l.State,
		PermissionsMode:   l.PermissionsMode,
		DeletionProtected: l.DeletionProtected,
		CreationDateTime:  epochSeconds(l.CreationDateTime),
		Tags:              l.Tags,
	}
}

func (h *Handler) handleCreateLedger(_ context.Context, body []byte) ([]byte, error) {
	var req createLedgerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in request body", errInvalidRequest)
	}

	l, err := h.Backend.CreateLedger(req.Name, req.PermissionsMode, req.DeletionProtected, req.Tags)
	if err != nil {
		return nil, err
	}

	// Ensure the ARN in the response matches the canonical ARN for this handler.
	l.ARN = h.ledgerARN(l.Name)

	return json.Marshal(toLedgerResponse(l))
}

func (h *Handler) handleDescribeLedger(_ context.Context, path string) ([]byte, error) {
	name := extractLedgerName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	l, err := h.Backend.GetLedger(name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toLedgerResponse(l))
}

type ledgerSummary struct {
	Arn              string  `json:"Arn"`
	Name             string  `json:"Name"`
	State            string  `json:"State"`
	CreationDateTime float64 `json:"CreationDateTime"`
}

type listLedgersResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Ledgers   []ledgerSummary `json:"Ledgers"`
}

func (h *Handler) handleListLedgers(_ context.Context) ([]byte, error) {
	ledgers := h.Backend.ListLedgers()
	items := make([]ledgerSummary, 0, len(ledgers))

	for _, l := range ledgers {
		items = append(items, ledgerSummary{
			Arn:              l.ARN,
			Name:             l.Name,
			State:            l.State,
			CreationDateTime: epochSeconds(l.CreationDateTime),
		})
	}

	return json.Marshal(listLedgersResponse{Ledgers: items})
}

type updateLedgerRequest struct {
	DeletionProtected bool `json:"DeletionProtection"`
}

func (h *Handler) handleUpdateLedger(_ context.Context, path string, body []byte) ([]byte, error) {
	name := extractLedgerName(path)
	if name == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	var req updateLedgerRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	l, err := h.Backend.UpdateLedger(name, req.DeletionProtected)
	if err != nil {
		return nil, err
	}

	return json.Marshal(toLedgerResponse(l))
}

func (h *Handler) handleDeleteLedger(_ context.Context, path string) error {
	name := extractLedgerName(path)
	if name == "" {
		return fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	return h.Backend.DeleteLedger(name)
}

type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, path string, body []byte) error {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	var req tagResourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, unmarshalErr)
	}

	return h.Backend.TagResource(resourceARN, req.Tags)
}

func (h *Handler) handleUntagResource(_ context.Context, path string, query url.Values) error {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return h.Backend.UntagResource(resourceARN, query["tagKeys"])
}

type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, path string) ([]byte, error) {
	resourceARN, err := extractTagsARN(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(listTagsResponse{Tags: tags})
}

// extractLedgerName extracts the ledger name from a /ledgers/{name} path.
func extractLedgerName(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) >= qldbLedgerPathMinSegments && segments[0] == qldbLedgersSegment {
		return segments[1]
	}

	return ""
}

// extractLedgerSubSegments extracts ledger name and sub-resource segments from a path.
// For /ledgers/{name}/{sub}/{id} returns (name, sub, id).
func extractLedgerSubSegments(path string) (string, string, string) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) < qldbLedgerSubPathSegments || segments[0] != qldbLedgersSegment {
		return "", "", ""
	}

	if len(segments) >= qldbLedgerSubIDSegments {
		return segments[1], segments[2], segments[3]
	}

	return segments[1], segments[2], ""
}

// journalKinesisStreamResponse is the JSON response for a journal kinesis stream.
type journalKinesisStreamResponse struct {
	ExclusiveEndTime     *float64              `json:"ExclusiveEndTime,omitempty"`
	InclusiveStartTime   *float64              `json:"InclusiveStartTime,omitempty"`
	Arn                  string                `json:"Arn"`
	LedgerName           string                `json:"LedgerName"`
	RoleArn              string                `json:"RoleArn"`
	Status               string                `json:"Status"`
	StreamID             string                `json:"StreamId"`
	StreamName           string                `json:"StreamName"`
	KinesisConfiguration kinesisConfigResponse `json:"KinesisConfiguration"`
	CreationTime         float64               `json:"CreationTime"`
}

type kinesisConfigResponse struct {
	StreamArn          string `json:"StreamArn"`
	AggregationEnabled bool   `json:"AggregationEnabled"`
}

func toJournalKinesisStreamResponse(s *JournalKinesisStream) journalKinesisStreamResponse {
	r := journalKinesisStreamResponse{
		Arn:          s.ARN,
		LedgerName:   s.LedgerName,
		RoleArn:      s.RoleArn,
		Status:       s.Status,
		StreamID:     s.StreamID,
		StreamName:   s.StreamName,
		CreationTime: epochSeconds(s.CreationTime),
		KinesisConfiguration: kinesisConfigResponse{
			StreamArn:          s.KinesisConfig.StreamArn,
			AggregationEnabled: s.KinesisConfig.AggregationEnabled,
		},
	}

	if s.ExclusiveEndTime != nil {
		v := epochSeconds(*s.ExclusiveEndTime)
		r.ExclusiveEndTime = &v
	}

	if s.InclusiveStartTime != nil {
		v := epochSeconds(*s.InclusiveStartTime)
		r.InclusiveStartTime = &v
	}

	return r
}

type cancelJournalKinesisStreamResponse struct {
	StreamID string `json:"StreamId"`
}

func (h *Handler) handleCancelJournalKinesisStream(_ context.Context, path string) ([]byte, error) {
	ledgerName, sub, streamID := extractLedgerSubSegments(path)
	if ledgerName == "" || sub != qldbJournalKinesisSegment || streamID == "" {
		return nil, fmt.Errorf("%w: invalid path for CancelJournalKinesisStream", errInvalidRequest)
	}

	s, err := h.Backend.CancelJournalKinesisStream(ledgerName, streamID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(cancelJournalKinesisStreamResponse{StreamID: s.StreamID})
}

type describeJournalKinesisStreamResponse struct {
	Stream journalKinesisStreamResponse `json:"Stream"`
}

func (h *Handler) handleDescribeJournalKinesisStream(_ context.Context, path string) ([]byte, error) {
	ledgerName, sub, streamID := extractLedgerSubSegments(path)
	if ledgerName == "" || sub != qldbJournalKinesisSegment || streamID == "" {
		return nil, fmt.Errorf("%w: invalid path for DescribeJournalKinesisStream", errInvalidRequest)
	}

	s, err := h.Backend.GetJournalKinesisStream(ledgerName, streamID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describeJournalKinesisStreamResponse{Stream: toJournalKinesisStreamResponse(s)})
}

type listJournalKinesisStreamsResponse struct {
	NextToken string                         `json:"NextToken,omitempty"`
	Streams   []journalKinesisStreamResponse `json:"Streams"`
}

func (h *Handler) handleListJournalKinesisStreamsForLedger(_ context.Context, path string) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	streams, err := h.Backend.ListJournalKinesisStreamsForLedger(ledgerName)
	if err != nil {
		return nil, err
	}

	items := make([]journalKinesisStreamResponse, 0, len(streams))
	for _, s := range streams {
		items = append(items, toJournalKinesisStreamResponse(s))
	}

	return json.Marshal(listJournalKinesisStreamsResponse{Streams: items})
}

type s3EncryptionConfig struct {
	ObjectEncryptionType string `json:"ObjectEncryptionType"`
	KmsKeyArn            string `json:"KmsKeyArn,omitempty"`
}

type s3ExportConfigRequest struct {
	Bucket                  string             `json:"Bucket"`
	Prefix                  string             `json:"Prefix"`
	EncryptionConfiguration s3EncryptionConfig `json:"EncryptionConfiguration"`
}

type exportJournalToS3Request struct {
	S3ExportConfiguration s3ExportConfigRequest `json:"S3ExportConfiguration"`
	ExclusiveEndTime      string                `json:"ExclusiveEndTime"`
	InclusiveStartTime    string                `json:"InclusiveStartTime"`
	RoleArn               string                `json:"RoleArn"`
	OutputFormat          string                `json:"OutputFormat,omitempty"`
}

type exportJournalToS3Response struct {
	ExportID string `json:"ExportId"`
}

func (h *Handler) handleExportJournalToS3(_ context.Context, path string, body []byte) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	var req exportJournalToS3Request
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.RoleArn == "" {
		return nil, fmt.Errorf("%w: missing RoleArn in request body", errInvalidRequest)
	}

	inclusiveStart, parseErr := time.Parse(time.RFC3339, req.InclusiveStartTime)
	if parseErr != nil {
		return nil, fmt.Errorf("%w: invalid InclusiveStartTime: %w", errInvalidRequest, parseErr)
	}

	exclusiveEnd, parseErr := time.Parse(time.RFC3339, req.ExclusiveEndTime)
	if parseErr != nil {
		return nil, fmt.Errorf("%w: invalid ExclusiveEndTime: %w", errInvalidRequest, parseErr)
	}

	encCfg := req.S3ExportConfiguration.EncryptionConfiguration
	s3Config := S3ExportConfiguration{
		Bucket: req.S3ExportConfiguration.Bucket,
		Prefix: req.S3ExportConfiguration.Prefix,
	}
	s3Config.EncryptionConfiguration.ObjectEncryptionType = encCfg.ObjectEncryptionType
	s3Config.EncryptionConfiguration.KmsKeyArn = encCfg.KmsKeyArn

	e, err := h.Backend.ExportJournalToS3(
		ledgerName,
		req.RoleArn,
		s3Config,
		inclusiveStart,
		exclusiveEnd,
		req.OutputFormat,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(exportJournalToS3Response{ExportID: e.ExportID})
}

// journalS3ExportDescResponse is the JSON representation of a journal S3 export description.
type journalS3ExportDescResponse struct {
	S3ExportConfiguration s3ExportConfigResponse `json:"S3ExportConfiguration"`
	ExportID              string                 `json:"ExportId"`
	LedgerName            string                 `json:"LedgerName"`
	RoleArn               string                 `json:"RoleArn"`
	Status                string                 `json:"Status"`
	OutputFormat          string                 `json:"OutputFormat,omitempty"`
	ExclusiveEndTime      float64                `json:"ExclusiveEndTime"`
	ExportCreationTime    float64                `json:"ExportCreationTime"`
	InclusiveStartTime    float64                `json:"InclusiveStartTime"`
}

type s3ExportConfigResponse struct {
	EncryptionConfiguration s3EncryptionConfig `json:"EncryptionConfiguration"`
	Bucket                  string             `json:"Bucket"`
	Prefix                  string             `json:"Prefix"`
}

func toJournalS3ExportResponse(e *JournalS3Export) journalS3ExportDescResponse {
	return journalS3ExportDescResponse{
		ExportID:           e.ExportID,
		LedgerName:         e.LedgerName,
		RoleArn:            e.RoleArn,
		Status:             e.Status,
		OutputFormat:       e.OutputFormat,
		ExclusiveEndTime:   epochSeconds(e.ExclusiveEndTime),
		ExportCreationTime: epochSeconds(e.ExportCreationTime),
		InclusiveStartTime: epochSeconds(e.InclusiveStartTime),
		S3ExportConfiguration: s3ExportConfigResponse{
			Bucket: e.S3Config.Bucket,
			Prefix: e.S3Config.Prefix,
			EncryptionConfiguration: s3EncryptionConfig{
				ObjectEncryptionType: e.S3Config.EncryptionConfiguration.ObjectEncryptionType,
				KmsKeyArn:            e.S3Config.EncryptionConfiguration.KmsKeyArn,
			},
		},
	}
}

type describeJournalS3ExportResponse struct {
	ExportDescription journalS3ExportDescResponse `json:"ExportDescription"`
}

func (h *Handler) handleDescribeJournalS3Export(_ context.Context, path string) ([]byte, error) {
	ledgerName, sub, exportID := extractLedgerSubSegments(path)
	if ledgerName == "" || sub != qldbJournalS3Segment || exportID == "" {
		return nil, fmt.Errorf("%w: invalid path for DescribeJournalS3Export", errInvalidRequest)
	}

	e, err := h.Backend.GetJournalS3Export(ledgerName, exportID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(describeJournalS3ExportResponse{ExportDescription: toJournalS3ExportResponse(e)})
}

type listJournalS3ExportsResponse struct {
	NextToken        string                        `json:"NextToken,omitempty"`
	JournalS3Exports []journalS3ExportDescResponse `json:"JournalS3Exports"`
}

func (h *Handler) handleListJournalS3Exports(_ context.Context) ([]byte, error) {
	exports := h.Backend.ListJournalS3Exports()
	items := make([]journalS3ExportDescResponse, 0, len(exports))

	for _, e := range exports {
		items = append(items, toJournalS3ExportResponse(e))
	}

	return json.Marshal(listJournalS3ExportsResponse{JournalS3Exports: items})
}

func (h *Handler) handleListJournalS3ExportsForLedger(_ context.Context, path string) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	exports, err := h.Backend.ListJournalS3ExportsForLedger(ledgerName)
	if err != nil {
		return nil, err
	}

	items := make([]journalS3ExportDescResponse, 0, len(exports))

	for _, e := range exports {
		items = append(items, toJournalS3ExportResponse(e))
	}

	return json.Marshal(listJournalS3ExportsResponse{JournalS3Exports: items})
}

// valueHolder wraps Ion text values in QLDB responses.
type valueHolder struct {
	IonText string `json:"IonText"`
}

type getBlockResponse struct {
	Block *valueHolder `json:"Block"`
	Proof *valueHolder `json:"Proof,omitempty"`
}

func (h *Handler) handleGetBlock(_ context.Context, path string) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	blockIon, proofIon, err := h.Backend.GetBlock(ledgerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getBlockResponse{
		Block: &valueHolder{IonText: blockIon},
		Proof: &valueHolder{IonText: proofIon},
	})
}

type getDigestResponse struct {
	DigestTipAddress *valueHolder `json:"DigestTipAddress"`
	Digest           []byte       `json:"Digest"`
}

func (h *Handler) handleGetDigest(_ context.Context, path string) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	digest, tipIon, err := h.Backend.GetDigest(ledgerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getDigestResponse{
		Digest:           digest,
		DigestTipAddress: &valueHolder{IonText: tipIon},
	})
}

type getRevisionResponse struct {
	Revision *valueHolder `json:"Revision"`
	Proof    *valueHolder `json:"Proof,omitempty"`
}

func (h *Handler) handleGetRevision(_ context.Context, path string) ([]byte, error) {
	ledgerName := extractLedgerName(path)
	if ledgerName == "" {
		return nil, fmt.Errorf("%w: missing ledger name in path", errInvalidRequest)
	}

	revisionIon, proofIon, err := h.Backend.GetRevision(ledgerName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(getRevisionResponse{
		Revision: &valueHolder{IonText: revisionIon},
		Proof:    &valueHolder{IonText: proofIon},
	})
}

// extractTagsARN extracts and URL-decodes the ARN from a /tags/{arn} path.
func extractTagsARN(path string) (string, error) {
	rawARN := strings.TrimPrefix(path, qldbTagsPrefix)
	if rawARN == "" {
		return "", errMissingARN
	}

	decoded, err := url.PathUnescape(rawARN)
	if err != nil {
		return "", fmt.Errorf("invalid ARN encoding: %w", err)
	}

	return decoded, nil
}
