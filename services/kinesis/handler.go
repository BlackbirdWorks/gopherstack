package kinesis

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Handler is the Echo HTTP handler for Kinesis operations.
type Handler struct {
	Backend       StorageBackend
	tags          map[string]*svcTags.Tags
	tagsMu        *lockmetrics.RWMutex
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Kinesis Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("kinesis.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("kinesis." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
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
		"RegisterStreamConsumer",
		"DescribeStreamConsumer",
		"ListStreamConsumers",
		"DeregisterStreamConsumer",
		"SubscribeToShard",
		"UpdateShardCount",
		"EnableEnhancedMonitoring",
		"DisableEnhancedMonitoring",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kinesis" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Kinesis instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// kinesisTargetPrefix is the X-Amz-Target prefix used by the AWS Kinesis SDK.
const kinesisTargetPrefix = "Kinesis_20131202."

// RouteMatcher returns a function that matches incoming Kinesis requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisTargetPrefix)
	}
}

// MatchPriority returns the routing priority for the Kinesis handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityTargetPrefixed
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

type extractStreamNameInput struct {
	StreamName string `json:"StreamName"`
}

// ExtractResource extracts the stream name from the JSON request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req extractStreamNameInput

	if err = json.Unmarshal(body, &req); err != nil {
		return ""
	}

	return req.StreamName
}

// Handler returns the Echo handler function for Kinesis operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// SubscribeToShard uses the AWS event-stream binary protocol and must be
		// dispatched before the normal JSON target handler.
		if c.Request().Header.Get("X-Amz-Target") == kinesisTargetPrefix+"SubscribeToShard" {
			return h.handleSubscribeToShardHTTP(c)
		}

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Kinesis", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.kinesisRoute(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type kinesisDispatchFn func(ctx context.Context, r *http.Request, body []byte) (any, error)

func (h *Handler) kinesisDispatchTable() map[string]kinesisDispatchFn {
	return map[string]kinesisDispatchFn{
		"CreateStream":                  h.handleCreateStream,
		"DeleteStream":                  h.handleDeleteStream,
		"DescribeStream":                h.handleDescribeStream,
		"DescribeStreamSummary":         h.handleDescribeStreamSummary,
		"ListStreams":                   h.handleListStreams,
		"PutRecord":                     h.handlePutRecord,
		"PutRecords":                    h.handlePutRecords,
		"GetShardIterator":              h.handleGetShardIterator,
		"GetRecords":                    h.handleGetRecords,
		"ListShards":                    h.handleListShards,
		"AddTagsToStream":               h.handleAddTagsToStream,
		"RemoveTagsFromStream":          h.handleRemoveTagsFromStream,
		"ListTagsForStream":             h.handleListTagsForStream,
		"IncreaseStreamRetentionPeriod": h.handleIncreaseStreamRetentionPeriod,
		"DecreaseStreamRetentionPeriod": h.handleDecreaseStreamRetentionPeriod,
		"DescribeLimits":                h.handleDescribeLimits,
		"RegisterStreamConsumer":        h.handleRegisterStreamConsumer,
		"DescribeStreamConsumer":        h.handleDescribeStreamConsumer,
		"ListStreamConsumers":           h.handleListStreamConsumers,
		"DeregisterStreamConsumer":      h.handleDeregisterStreamConsumer,
		"UpdateShardCount":              h.handleUpdateShardCount,
		"EnableEnhancedMonitoring":      h.handleEnableEnhancedMonitoring,
		"DisableEnhancedMonitoring":     h.handleDisableEnhancedMonitoring,
	}
}

// kinesisRoute dispatches a Kinesis action to the appropriate handler method.
func (h *Handler) kinesisRoute(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	fn, ok := h.kinesisDispatchTable()[action]
	if !ok {
		return nil, ErrUnknownAction
	}

	result, err := fn(ctx, r, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

// handleError writes a Kinesis error response using the standard error details mapping.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	errType, message, status := errorDetails(err)

	return c.JSON(status, jsonKinesisError{Type: errType, Message: message})
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
	StreamName             string  `json:"StreamName"`
	ShardID                string  `json:"ShardId"`
	ShardIteratorType      string  `json:"ShardIteratorType"`
	StartingSequenceNumber string  `json:"StartingSequenceNumber"`
	Timestamp              float64 `json:"Timestamp"`
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

type listTagsForStreamOutput struct {
	Tags        []svcTags.KV `json:"Tags"`
	HasMoreTags bool         `json:"HasMoreTags"`
}

type describeLimitsOutput struct {
	ShardLimit     int `json:"ShardLimit"`
	OpenShardCount int `json:"OpenShardCount"`
}

// --- handler methods ---

func (h *Handler) handleCreateStream(
	ctx context.Context,
	r *http.Request,
	body []byte,
) (any, error) {
	var req jsonCreateStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

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

		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDeleteStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeleteStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeleteStream(&DeleteStreamInput{StreamName: req.StreamName}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDescribeStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDescribeStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName})
	if err != nil {
		return nil, err
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

	return jsonDescribeStreamResp{
		StreamDescription: jsonStreamDescription{
			StreamName:           out.StreamName,
			StreamARN:            out.StreamARN,
			StreamStatus:         out.StreamStatus,
			RetentionPeriodHours: out.RetentionPeriodHours,
			Shards:               shards,
			HasMoreShards:        false,
		},
	}, nil
}

func (h *Handler) handleDescribeStreamSummary(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDescribeStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName})
	if err != nil {
		return nil, err
	}

	return jsonDescribeStreamSummaryResp{
		StreamDescriptionSummary: jsonStreamDescriptionSummary{
			StreamName:           out.StreamName,
			StreamARN:            out.StreamARN,
			StreamStatus:         out.StreamStatus,
			RetentionPeriodHours: out.RetentionPeriodHours,
			OpenShardCount:       len(out.Shards),
		},
	}, nil
}

func (h *Handler) handleListStreams(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListStreamsReq
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListStreams(&ListStreamsInput{
		Limit:     req.Limit,
		NextToken: req.NextToken,
	})
	if err != nil {
		return nil, err
	}

	names := out.StreamNames
	if names == nil {
		names = []string{}
	}

	return jsonListStreamsResp{
		StreamNames:    names,
		HasMoreStreams: out.HasMoreStreams,
	}, nil
}

func (h *Handler) handlePutRecord(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonPutRecordReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.PutRecord(&PutRecordInput{
		StreamName:   req.StreamName,
		PartitionKey: req.PartitionKey,
		Data:         req.Data,
	})
	if err != nil {
		return nil, err
	}

	return jsonPutRecordResp{
		ShardID:        out.ShardID,
		SequenceNumber: out.SequenceNumber,
	}, nil
}

func (h *Handler) handlePutRecords(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonPutRecordsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
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
		return nil, err
	}

	results := make([]jsonPutRecordsResultEntry, len(out.Records))
	for i, r := range out.Records {
		results[i] = jsonPutRecordsResultEntry(r)
	}

	return jsonPutRecordsResp{
		Records:           results,
		FailedRecordCount: out.FailedRecordCount,
	}, nil
}

func (h *Handler) handleGetShardIterator(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetShardIteratorReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.GetShardIterator(&GetShardIteratorInput{
		StreamName:             req.StreamName,
		ShardID:                req.ShardID,
		ShardIteratorType:      req.ShardIteratorType,
		StartingSequenceNumber: req.StartingSequenceNumber,
		Timestamp:              time.UnixMilli(int64(req.Timestamp * millisPerSecond)),
	})
	if err != nil {
		return nil, err
	}

	return jsonGetShardIteratorResp{
		ShardIterator: out.ShardIterator,
	}, nil
}

func (h *Handler) handleGetRecords(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetRecordsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.GetRecords(&GetRecordsInput{
		ShardIterator: req.ShardIterator,
		Limit:         req.Limit,
	})
	if err != nil {
		return nil, err
	}

	records := make([]jsonRecord, len(out.Records))
	for i, r := range out.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisPerSecond,
		}
	}

	return jsonGetRecordsResp{
		Records:            records,
		NextShardIterator:  out.NextShardIterator,
		MillisBehindLatest: out.MillisBehindLatest,
	}, nil
}

func (h *Handler) handleListShards(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListShardsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.ListShards(&ListShardsInput{
		StreamName: req.StreamName,
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
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

	return jsonListShardsResp{Shards: shards}, nil
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
	case errors.Is(err, ErrConsumerNotFound):
		return "ResourceNotFoundException",
			"Consumer not found.",
			http.StatusBadRequest
	case errors.Is(err, ErrConsumerAlreadyExists):
		return "ResourceInUseException",
			"A consumer with this name already exists.",
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
		return "InternalFailureException",
			"An internal error occurred.",
			http.StatusInternalServerError
	}
}

type handleAddTagsToStreamInput struct {
	Tags       *svcTags.Tags `json:"Tags"`
	StreamName string        `json:"StreamName"`
}

func (h *Handler) handleAddTagsToStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req handleAddTagsToStreamInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	var kv map[string]string
	if req.Tags != nil {
		kv = req.Tags.Clone()
	}
	h.setTags(req.StreamName, kv)

	return struct{}{}, nil
}

type handleRemoveTagsFromStreamInput struct {
	StreamName string   `json:"StreamName"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTagsFromStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req handleRemoveTagsFromStreamInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	h.removeTags(req.StreamName, req.TagKeys)

	return struct{}{}, nil
}

func (h *Handler) handleListTagsForStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req extractStreamNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	tags := h.getTags(req.StreamName)
	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	return &listTagsForStreamOutput{
		Tags:        tagList,
		HasMoreTags: false,
	}, nil
}

func (h *Handler) handleIncreaseStreamRetentionPeriod(
	_ context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleDecreaseStreamRetentionPeriod(
	_ context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return struct{}{}, nil
}

const kinesisDefaultShardLimit = 500

func (h *Handler) handleDescribeLimits(
	_ context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return &describeLimitsOutput{
		OpenShardCount: 0,
		ShardLimit:     kinesisDefaultShardLimit,
	}, nil
}

// --- Consumer JSON types ---

type jsonRegisterStreamConsumerReq struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerName string `json:"ConsumerName"`
}

type jsonConsumer struct {
	ConsumerName              string  `json:"ConsumerName"`
	ConsumerARN               string  `json:"ConsumerARN"`
	ConsumerStatus            string  `json:"ConsumerStatus"`
	StreamARN                 string  `json:"StreamARN"`
	ConsumerCreationTimestamp float64 `json:"ConsumerCreationTimestamp"`
}

type jsonRegisterStreamConsumerResp struct {
	Consumer jsonConsumer `json:"Consumer"`
}

type jsonDescribeStreamConsumerReq struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerARN  string `json:"ConsumerARN"`
	ConsumerName string `json:"ConsumerName"`
}

type jsonDescribeStreamConsumerResp struct {
	ConsumerDescription jsonConsumer `json:"ConsumerDescription"`
}

type jsonListStreamConsumersReq struct {
	StreamARN  string `json:"StreamARN"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type jsonListStreamConsumersResp struct {
	NextToken string         `json:"NextToken,omitempty"`
	Consumers []jsonConsumer `json:"Consumers"`
}

type jsonDeregisterStreamConsumerReq struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerARN  string `json:"ConsumerARN"`
	ConsumerName string `json:"ConsumerName"`
}

type jsonStartingPosition struct {
	Type           string  `json:"Type"`
	SequenceNumber string  `json:"SequenceNumber,omitempty"`
	Timestamp      float64 `json:"Timestamp,omitempty"`
}

type jsonSubscribeToShardReq struct {
	ConsumerARN      string               `json:"ConsumerARN"`
	ShardID          string               `json:"ShardId"`
	StartingPosition jsonStartingPosition `json:"StartingPosition"`
}

type jsonSubscribeToShardEvent struct {
	ContinuationSequenceNumber string       `json:"ContinuationSequenceNumber"`
	Records                    []jsonRecord `json:"Records"`
	MillisBehindLatest         int64        `json:"MillisBehindLatest"`
}

type jsonUpdateShardCountReq struct {
	StreamName       string `json:"StreamName"`
	ScalingType      string `json:"ScalingType"`
	TargetShardCount int    `json:"TargetShardCount"`
}

type jsonUpdateShardCountResp struct {
	StreamName        string `json:"StreamName"`
	CurrentShardCount int    `json:"CurrentShardCount"`
	TargetShardCount  int    `json:"TargetShardCount"`
}

type jsonEnhancedMonitoringReq struct {
	StreamName        string   `json:"StreamName"`
	ShardLevelMetrics []string `json:"ShardLevelMetrics"`
}

type jsonEnhancedMonitoringResp struct {
	StreamName               string   `json:"StreamName"`
	CurrentShardLevelMetrics []string `json:"CurrentShardLevelMetrics"`
	DesiredShardLevelMetrics []string `json:"DesiredShardLevelMetrics"`
}

// toJSONConsumer converts a Consumer to its JSON representation.
func toJSONConsumer(c Consumer) jsonConsumer {
	return jsonConsumer{
		ConsumerName:              c.ConsumerName,
		ConsumerARN:               c.ConsumerARN,
		ConsumerStatus:            c.ConsumerStatus,
		ConsumerCreationTimestamp: float64(c.ConsumerCreationTimestamp.UnixMilli()) / millisPerSecond,
		StreamARN:                 c.StreamARN,
	}
}

func (h *Handler) handleRegisterStreamConsumer(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRegisterStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.RegisterStreamConsumer(&RegisterStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerName: req.ConsumerName,
	})
	if err != nil {
		return nil, err
	}

	return jsonRegisterStreamConsumerResp{Consumer: toJSONConsumer(out.Consumer)}, nil
}

func (h *Handler) handleDescribeStreamConsumer(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDescribeStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DescribeStreamConsumer(&DescribeStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerARN:  req.ConsumerARN,
		ConsumerName: req.ConsumerName,
	})
	if err != nil {
		return nil, err
	}

	return jsonDescribeStreamConsumerResp{ConsumerDescription: toJSONConsumer(out.ConsumerDescription)}, nil
}

func (h *Handler) handleListStreamConsumers(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListStreamConsumersReq
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListStreamConsumers(&ListStreamConsumersInput{
		StreamARN:  req.StreamARN,
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	consumers := make([]jsonConsumer, len(out.Consumers))
	for i, c := range out.Consumers {
		consumers[i] = toJSONConsumer(c)
	}

	return jsonListStreamConsumersResp{Consumers: consumers, NextToken: out.NextToken}, nil
}

func (h *Handler) handleDeregisterStreamConsumer(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeregisterStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeregisterStreamConsumer(&DeregisterStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerARN:  req.ConsumerARN,
		ConsumerName: req.ConsumerName,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUpdateShardCount(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateShardCountReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.UpdateShardCount(&UpdateShardCountInput{
		StreamName:       req.StreamName,
		TargetShardCount: req.TargetShardCount,
		ScalingType:      req.ScalingType,
	})
	if err != nil {
		return nil, err
	}

	return jsonUpdateShardCountResp{
		StreamName:        out.StreamName,
		CurrentShardCount: out.CurrentShardCount,
		TargetShardCount:  out.TargetShardCount,
	}, nil
}

func (h *Handler) handleEnableEnhancedMonitoring(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEnhancedMonitoringReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.EnableEnhancedMonitoring(&EnableEnhancedMonitoringInput{
		StreamName:        req.StreamName,
		ShardLevelMetrics: req.ShardLevelMetrics,
	})
	if err != nil {
		return nil, err
	}

	return jsonEnhancedMonitoringResp{
		StreamName:               out.StreamName,
		CurrentShardLevelMetrics: out.CurrentShardLevelMetrics,
		DesiredShardLevelMetrics: out.DesiredShardLevelMetrics,
	}, nil
}

func (h *Handler) handleDisableEnhancedMonitoring(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEnhancedMonitoringReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DisableEnhancedMonitoring(&DisableEnhancedMonitoringInput{
		StreamName:        req.StreamName,
		ShardLevelMetrics: req.ShardLevelMetrics,
	})
	if err != nil {
		return nil, err
	}

	return jsonEnhancedMonitoringResp{
		StreamName:               out.StreamName,
		CurrentShardLevelMetrics: out.CurrentShardLevelMetrics,
		DesiredShardLevelMetrics: out.DesiredShardLevelMetrics,
	}, nil
}

// --- AWS Event Stream encoding for SubscribeToShard ---

// eventStreamHeaderValueTypeString is the AWS event stream type byte for string values.
const eventStreamHeaderValueTypeString = 7

// eventStreamPreludeLen is the number of bytes in an event stream prelude.
const eventStreamPreludeLen = 12

// eventStreamHeaderValueLenBytes is the number of bytes used to encode a header value length.
const eventStreamHeaderValueLenBytes = 2

// eventStreamMsgCRCLen is the number of bytes used for the message CRC field.
const eventStreamMsgCRCLen = 4

// buildEventStreamHeaders encodes the given slice of header name/value pairs as AWS
// event stream binary headers. Headers are encoded in the order provided in the slice.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	var buf bytes.Buffer

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		buf.WriteByte(byte(len(name))) //nolint:gosec // header name bounded by AWS event stream protocol
		buf.WriteString(name)
		buf.WriteByte(eventStreamHeaderValueTypeString)
		vlen := make([]byte, eventStreamHeaderValueLenBytes)
		//nolint:gosec // header value length fits in uint16 by AWS event stream protocol definition
		binary.BigEndian.PutUint16(vlen, uint16(len(value)))
		buf.Write(vlen)
		buf.WriteString(value)
	}

	return buf.Bytes()
}

// encodeEventStreamMsg encodes a single AWS event stream binary message.
// Format: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
func encodeEventStreamMsg(hdrs [][2]string, payload []byte) []byte {
	hdrBytes := buildEventStreamHeaders(hdrs)
	headerLen := len(hdrBytes)
	payloadLen := len(payload)
	// prelude (12 bytes) + headers + payload + message CRC (4 bytes)
	totalLen := eventStreamPreludeLen + headerLen + payloadLen + eventStreamMsgCRCLen

	buf := make([]byte, totalLen)
	//nolint:gosec // totalLen is bounded by AWS event stream protocol constraints
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	//nolint:gosec // headerLen is bounded by AWS event stream protocol constraints
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen))

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:eventStreamPreludeLen], preludeCRC)

	copy(buf[eventStreamPreludeLen:eventStreamPreludeLen+headerLen], hdrBytes)
	copy(buf[eventStreamPreludeLen+headerLen:eventStreamPreludeLen+headerLen+payloadLen], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0 : eventStreamPreludeLen+headerLen+payloadLen])
	binary.BigEndian.PutUint32(buf[eventStreamPreludeLen+headerLen+payloadLen:], msgCRC)

	return buf
}

// handleSubscribeToShardHTTP handles the SubscribeToShard operation using the AWS event stream
// binary protocol. It delivers all currently available records as a single SubscribeToShardEvent
// and then ends the stream. This is a simplified, polling-based mock.
func (h *Handler) handleSubscribeToShardHTTP(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "SubscribeToShard: failed to read body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var req jsonSubscribeToShardReq
	if err = json.Unmarshal(body, &req); err != nil {
		return h.handleError(ctx, c, "SubscribeToShard", ErrInvalidArgument)
	}

	sp := StartingPosition{
		Type:           req.StartingPosition.Type,
		SequenceNumber: req.StartingPosition.SequenceNumber,
	}

	if req.StartingPosition.Timestamp != 0 {
		ts := time.UnixMilli(int64(req.StartingPosition.Timestamp * millisPerSecond))
		sp.Timestamp = &ts
	}

	out, err := h.Backend.SubscribeToShard(&SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: sp,
	})
	if err != nil {
		return h.handleError(ctx, c, "SubscribeToShard", err)
	}

	records := make([]jsonRecord, len(out.Event.Records))
	for i, r := range out.Event.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisPerSecond,
		}
	}

	eventPayload, err := json.Marshal(jsonSubscribeToShardEvent{
		Records:                    records,
		ContinuationSequenceNumber: out.Event.ContinuationSequenceNumber,
		MillisBehindLatest:         out.Event.MillisBehindLatest,
	})
	if err != nil {
		log.ErrorContext(ctx, "SubscribeToShard: failed to marshal event payload", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	// The AWS SDK event stream middleware blocks until it receives an "initial-response"
	// message. This must be written before any event messages or the SDK and the
	// readEventStream goroutine will deadlock: the middleware waits on initialResponse
	// while the goroutine waits for someone to drain the stream channel.
	// The payload is an empty JSON object because SubscribeToShardOutput has no fields.
	initialResponseMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "initial-response"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, []byte("{}"))

	eventMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "SubscribeToShardEvent"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, eventPayload)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)

	if _, err = c.Response().Write(initialResponseMsg); err != nil {
		return err
	}

	_, err = c.Response().Write(eventMsg)

	return err
}
