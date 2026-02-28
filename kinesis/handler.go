package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Handler is the Echo HTTP handler for Kinesis operations.
type Handler struct {
	Backend       StorageBackend
	Logger        *slog.Logger
	tags          map[string]map[string]string
	DefaultRegion string
	AccountID     string
	tagsMu        sync.RWMutex
}

// NewHandler creates a new Kinesis Handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	maps.Copy(result, h.tags[resourceID])

	return result
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "Kinesis"
}

// GetSupportedOperations returns the list of supported Kinesis operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStream",
		"DeleteStream",
		"DescribeStream",
		"DescribeStreamSummary",
		"ListStreams",
		"PutRecord",
		"PutRecords",
		"GetShardIterator",
		"GetRecords",
		"ListShards",
		"AddTagsToStream",
		"RemoveTagsFromStream",
		"ListTagsForStream",
	}
}

// kinesisTargetPrefix is the X-Amz-Target prefix used by the AWS Kinesis SDK.
const kinesisTargetPrefix = "Kinesis_20131202."

// RouteMatcher returns a function that matches incoming Kinesis requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisTargetPrefix)
	}
}

const kinesisMatchPriority = 75

// MatchPriority returns the routing priority for the Kinesis handler.
func (h *Handler) MatchPriority() int {
	return kinesisMatchPriority
}

// ExtractOperation extracts the Kinesis action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, kinesisTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the stream name from the JSON request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		StreamName string `json:"StreamName"`
	}

	if err = json.Unmarshal(body, &req); err != nil {
		return ""
	}

	return req.StreamName
}

// Handler returns the Echo handler function for Kinesis operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read Kinesis request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		action := strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisTargetPrefix)
		requestID := uuid.New().String()

		if action == "" {
			h.writeError(c.Response(), ErrUnknownAction, requestID)

			return nil
		}

		log.DebugContext(ctx, "Kinesis request", "action", action)
		h.dispatch(ctx, c.Response(), c.Request(), body, action, requestID)

		return nil
	}
}

type kinesisDispatchFn func(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, requestID string)

func (h *Handler) kinesisDispatchTable() map[string]kinesisDispatchFn {
	return map[string]kinesisDispatchFn{
		"CreateStream":          h.handleCreateStream,
		"DeleteStream":          h.handleDeleteStream,
		"DescribeStream":        h.handleDescribeStream,
		"DescribeStreamSummary": h.handleDescribeStreamSummary,
		"ListStreams":           h.handleListStreams,
		"PutRecord":             h.handlePutRecord,
		"PutRecords":            h.handlePutRecords,
		"GetShardIterator":      h.handleGetShardIterator,
		"GetRecords":            h.handleGetRecords,
		"ListShards":            h.handleListShards,
		"AddTagsToStream":       h.handleAddTagsToStream,
		"RemoveTagsFromStream":  h.handleRemoveTagsFromStream,
		"ListTagsForStream":     h.handleListTagsForStream,
	}
}

// dispatch routes the action to the appropriate handler method.
func (h *Handler) dispatch(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	action, requestID string,
) {
	fn, ok := h.kinesisDispatchTable()[action]
	if !ok {
		h.writeError(w, ErrUnknownAction, requestID)

		return
	}

	fn(ctx, w, r, body, requestID)
}

// --- JSON request/response types ---

type jsonCreateStreamReq struct {
	StreamName string `json:"StreamName"`
	ShardCount int    `json:"ShardCount"`
}

type jsonDeleteStreamReq struct {
	StreamName string `json:"StreamName"`
}

type jsonDescribeStreamReq struct {
	StreamName string `json:"StreamName"`
}

type jsonListStreamsReq struct {
	NextToken string `json:"NextToken"`
	Limit     int    `json:"Limit"`
}

type jsonPutRecordReq struct {
	StreamName   string `json:"StreamName"`
	PartitionKey string `json:"PartitionKey"`
	Data         []byte `json:"Data"`
}

type jsonPutRecordEntry struct {
	PartitionKey string `json:"PartitionKey"`
	Data         []byte `json:"Data"`
}

type jsonPutRecordsReq struct {
	StreamName string               `json:"StreamName"`
	Records    []jsonPutRecordEntry `json:"Records"`
}

type jsonGetShardIteratorReq struct {
	StreamName             string `json:"StreamName"`
	ShardID                string `json:"ShardId"`
	ShardIteratorType      string `json:"ShardIteratorType"`
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
}

type jsonGetRecordsReq struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

type jsonListShardsReq struct {
	StreamName string `json:"StreamName"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type jsonShardDescription struct {
	ShardID             string           `json:"ShardId"`
	HashKeyRange        jsonHashKeyRange `json:"HashKeyRange"`
	SequenceNumberRange jsonSeqNumRange  `json:"SequenceNumberRange"`
}

type jsonHashKeyRange struct {
	StartingHashKey string `json:"StartingHashKey"`
	EndingHashKey   string `json:"EndingHashKey"`
}

type jsonSeqNumRange struct {
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

type jsonStreamDescriptionSummary struct {
	StreamName           string `json:"StreamName"`
	StreamARN            string `json:"StreamARN"`
	StreamStatus         string `json:"StreamStatus"`
	RetentionPeriodHours int    `json:"RetentionPeriodHours"`
	OpenShardCount       int    `json:"OpenShardCount"`
}

type jsonStreamDescription struct {
	StreamName           string                 `json:"StreamName"`
	StreamARN            string                 `json:"StreamARN"`
	StreamStatus         string                 `json:"StreamStatus"`
	Shards               []jsonShardDescription `json:"Shards"`
	RetentionPeriodHours int                    `json:"RetentionPeriodHours"`
	HasMoreShards        bool                   `json:"HasMoreShards"`
}

type jsonDescribeStreamResp struct {
	StreamDescription jsonStreamDescription `json:"StreamDescription"`
}

type jsonDescribeStreamSummaryResp struct {
	StreamDescriptionSummary jsonStreamDescriptionSummary `json:"StreamDescriptionSummary"`
}

type jsonListStreamsResp struct {
	NextToken      string   `json:"NextToken,omitempty"`
	StreamNames    []string `json:"StreamNames"`
	HasMoreStreams bool     `json:"HasMoreStreams"`
}

type jsonPutRecordResp struct {
	ShardID        string `json:"ShardId"`
	SequenceNumber string `json:"SequenceNumber"`
}

type jsonPutRecordsResultEntry struct {
	ShardID        string `json:"ShardId,omitempty"`
	SequenceNumber string `json:"SequenceNumber,omitempty"`
	ErrorCode      string `json:"ErrorCode,omitempty"`
	ErrorMessage   string `json:"ErrorMessage,omitempty"`
}

type jsonPutRecordsResp struct {
	Records           []jsonPutRecordsResultEntry `json:"Records"`
	FailedRecordCount int                         `json:"FailedRecordCount"`
}

type jsonGetShardIteratorResp struct {
	ShardIterator string `json:"ShardIterator"`
}

type jsonRecord struct {
	PartitionKey                string  `json:"PartitionKey"`
	SequenceNumber              string  `json:"SequenceNumber"`
	Data                        []byte  `json:"Data"`
	ApproximateArrivalTimestamp float64 `json:"ApproximateArrivalTimestamp"`
}

type jsonGetRecordsResp struct {
	NextShardIterator  string       `json:"NextShardIterator"`
	Records            []jsonRecord `json:"Records"`
	MillisBehindLatest int64        `json:"MillisBehindLatest"`
}

type jsonListShardsResp struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Shards    []jsonShardDescription `json:"Shards"`
}

type jsonKinesisError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// --- handler methods ---

func (h *Handler) handleCreateStream(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonCreateStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	region := httputil.ExtractRegionFromRequest(r, h.DefaultRegion)

	err := h.Backend.CreateStream(&CreateStreamInput{
		StreamName: req.StreamName,
		ShardCount: req.ShardCount,
		Region:     region,
		AccountID:  h.AccountID,
	})
	if err != nil {
		if !errors.Is(err, ErrStreamAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateStream failed", "error", err)
		}

		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleDeleteStream(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonDeleteStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	if err := h.Backend.DeleteStream(&DeleteStreamInput{StreamName: req.StreamName}); err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleDescribeStream(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonDescribeStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	shards := make([]jsonShardDescription, len(out.Shards))
	for i, s := range out.Shards {
		shards[i] = jsonShardDescription{
			ShardID: s.ShardID,
			HashKeyRange: jsonHashKeyRange{
				StartingHashKey: s.HashKeyRangeStart,
				EndingHashKey:   s.HashKeyRangeEnd,
			},
			SequenceNumberRange: jsonSeqNumRange{
				StartingSequenceNumber: s.SequenceNumberRangeStart,
				EndingSequenceNumber:   s.SequenceNumberRangeEnd,
			},
		}
	}

	resp := jsonDescribeStreamResp{
		StreamDescription: jsonStreamDescription{
			StreamName:           out.StreamName,
			StreamARN:            out.StreamARN,
			StreamStatus:         out.StreamStatus,
			RetentionPeriodHours: out.RetentionPeriodHours,
			Shards:               shards,
			HasMoreShards:        false,
		},
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) handleDescribeStreamSummary(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonDescribeStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	resp := jsonDescribeStreamSummaryResp{
		StreamDescriptionSummary: jsonStreamDescriptionSummary{
			StreamName:           out.StreamName,
			StreamARN:            out.StreamARN,
			StreamStatus:         out.StreamStatus,
			RetentionPeriodHours: out.RetentionPeriodHours,
			OpenShardCount:       len(out.Shards),
		},
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, resp)
}

func (h *Handler) handleListStreams(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonListStreamsReq
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListStreams(&ListStreamsInput{
		Limit:     req.Limit,
		NextToken: req.NextToken,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	names := out.StreamNames
	if names == nil {
		names = []string{}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonListStreamsResp{
		StreamNames:    names,
		HasMoreStreams: out.HasMoreStreams,
	})
}

func (h *Handler) handlePutRecord(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonPutRecordReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.PutRecord(&PutRecordInput{
		StreamName:   req.StreamName,
		PartitionKey: req.PartitionKey,
		Data:         req.Data,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonPutRecordResp{
		ShardID:        out.ShardID,
		SequenceNumber: out.SequenceNumber,
	})
}

func (h *Handler) handlePutRecords(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonPutRecordsReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	entries := make([]PutRecordsEntry, len(req.Records))
	for i, r := range req.Records {
		entries[i] = PutRecordsEntry{
			PartitionKey: r.PartitionKey,
			Data:         r.Data,
		}
	}

	out, err := h.Backend.PutRecords(&PutRecordsInput{
		StreamName: req.StreamName,
		Records:    entries,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	results := make([]jsonPutRecordsResultEntry, len(out.Records))
	for i, r := range out.Records {
		results[i] = jsonPutRecordsResultEntry(r)
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonPutRecordsResp{
		Records:           results,
		FailedRecordCount: out.FailedRecordCount,
	})
}

func (h *Handler) handleGetShardIterator(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonGetShardIteratorReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.GetShardIterator(&GetShardIteratorInput{
		StreamName:             req.StreamName,
		ShardID:                req.ShardID,
		ShardIteratorType:      req.ShardIteratorType,
		StartingSequenceNumber: req.StartingSequenceNumber,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonGetShardIteratorResp{
		ShardIterator: out.ShardIterator,
	})
}

func (h *Handler) handleGetRecords(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonGetRecordsReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.GetRecords(&GetRecordsInput{
		ShardIterator: req.ShardIterator,
		Limit:         req.Limit,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	records := make([]jsonRecord, len(out.Records))
	for i, r := range out.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisToSeconds,
		}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonGetRecordsResp{
		Records:            records,
		NextShardIterator:  out.NextShardIterator,
		MillisBehindLatest: out.MillisBehindLatest,
	})
}

func (h *Handler) handleListShards(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req jsonListShardsReq
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}

	out, err := h.Backend.ListShards(&ListShardsInput{
		StreamName: req.StreamName,
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		h.writeError(w, err, requestID)

		return
	}

	shards := make([]jsonShardDescription, len(out.Shards))
	for i, s := range out.Shards {
		shards[i] = jsonShardDescription{
			ShardID: s.ShardID,
			HashKeyRange: jsonHashKeyRange{
				StartingHashKey: s.HashKeyRangeStart,
				EndingHashKey:   s.HashKeyRangeEnd,
			},
			SequenceNumberRange: jsonSeqNumRange{
				StartingSequenceNumber: s.SequenceNumberRangeStart,
				EndingSequenceNumber:   s.SequenceNumberRangeEnd,
			},
		}
	}

	httputil.WriteJSON(h.Logger, w, http.StatusOK, jsonListShardsResp{Shards: shards})
}

// writeError writes a JSON Kinesis error response.
func (h *Handler) writeError(w http.ResponseWriter, err error, _ string) {
	errType, message, status := errorDetails(err)
	httputil.WriteJSON(h.Logger, w, status, jsonKinesisError{
		Type:    errType,
		Message: message,
	})
}

// errorDetails maps an error to its Kinesis JSON error type, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	switch {
	case errors.Is(err, ErrStreamNotFound):
		return "ResourceNotFoundException",
			"Stream not found.",
			http.StatusBadRequest
	case errors.Is(err, ErrStreamAlreadyExists):
		return "ResourceInUseException",
			"A stream with this name already exists.",
			http.StatusBadRequest
	case errors.Is(err, ErrInvalidArgument):
		return "InvalidArgumentException",
			"Invalid argument.",
			http.StatusBadRequest
	case errors.Is(err, ErrShardIteratorExpired):
		return "ExpiredIteratorException",
			"The shard iterator has expired.",
			http.StatusBadRequest
	case errors.Is(err, ErrUnknownAction):
		return "UnknownOperationException",
			"The requested operation is not recognized.",
			http.StatusBadRequest
	default:
		return "InternalFailure",
			"An internal error occurred.",
			http.StatusInternalServerError
	}
}

func (h *Handler) handleAddTagsToStream(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req struct {
		Tags       map[string]string `json:"Tags"`
		StreamName string            `json:"StreamName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}
	h.setTags(req.StreamName, req.Tags)
	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleRemoveTagsFromStream(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req struct {
		StreamName string   `json:"StreamName"`
		TagKeys    []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}
	h.removeTags(req.StreamName, req.TagKeys)
	httputil.WriteJSON(h.Logger, w, http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForStream(
	_ context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	body []byte,
	requestID string,
) {
	var req struct {
		StreamName string `json:"StreamName"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrInvalidArgument, requestID)

		return
	}
	tags := h.getTags(req.StreamName)
	type kinesisTag struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}
	tagList := make([]kinesisTag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, kinesisTag{Key: k, Value: v})
	}
	httputil.WriteJSON(h.Logger, w, http.StatusOK, map[string]any{
		"Tags":        tagList,
		"HasMoreTags": false,
	})
}
