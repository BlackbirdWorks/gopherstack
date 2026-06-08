package kinesis

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"maps"
	"math"
	"net/http"
	"slices"
	"sort"
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
	janitor       *Janitor
	tags          map[string]*svcTags.Tags
	tagsMu        *lockmetrics.RWMutex
	ops           map[string]kinesisDispatchFn
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Kinesis Handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("kinesis.tags"),
	}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}
		// Wire the cleanup callback so that when a stream is purged from the backend
		// the handler-level tag registry for that stream is also closed and removed.
		mem.OnStreamPurged = func(streamName string) {
			h.tagsMu.Lock("OnStreamPurged")
			if t := h.tags[streamName]; t != nil {
				t.Close()
			}
			delete(h.tags, streamName)
			h.tagsMu.Unlock()
		}
		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if one is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
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
		"TagResource",
		"UntagResource",
		"IncreaseStreamRetentionPeriod",
		"DecreaseStreamRetentionPeriod",
		"RegisterStreamConsumer",
		"DescribeStreamConsumer",
		"ListStreamConsumers",
		"DeregisterStreamConsumer",
		"SubscribeToShard",
		"UpdateShardCount",
		"EnableEnhancedMonitoring",
		"DisableEnhancedMonitoring",
		"DescribeLimits",
		"DescribeAccountSettings",
		"UpdateAccountSettings",
		"UpdateMaxRecordSize",
		"UpdateStreamWarmThroughput",
		"MergeShards",
		"SplitShard",
		"StartStreamEncryption",
		"StopStreamEncryption",
		"DeleteResourcePolicy",
		"GetResourcePolicy",
		"PutResourcePolicy",
		"ListTagsForResource",
		"UpdateStreamMode",
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

func (h *Handler) buildOps() map[string]kinesisDispatchFn {
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
		"TagResource":                   h.handleTagResource,
		"UntagResource":                 h.handleUntagResource,
		"IncreaseStreamRetentionPeriod": h.handleIncreaseStreamRetentionPeriod,
		"DecreaseStreamRetentionPeriod": h.handleDecreaseStreamRetentionPeriod,
		"DescribeLimits":                h.handleDescribeLimits,
		"DescribeAccountSettings":       h.handleDescribeAccountSettings,
		"UpdateAccountSettings":         h.handleUpdateAccountSettings,
		"UpdateMaxRecordSize":           h.handleUpdateMaxRecordSize,
		"UpdateStreamWarmThroughput":    h.handleUpdateStreamWarmThroughput,
		"MergeShards":                   h.handleMergeShards,
		"SplitShard":                    h.handleSplitShard,
		"StartStreamEncryption":         h.handleStartStreamEncryption,
		"StopStreamEncryption":          h.handleStopStreamEncryption,
		"DeleteResourcePolicy":          h.handleDeleteResourcePolicy,
		"GetResourcePolicy":             h.handleGetResourcePolicy,
		"PutResourcePolicy":             h.handlePutResourcePolicy,
		"ListTagsForResource":           h.handleListTagsForResource,
		"UpdateStreamMode":              h.handleUpdateStreamMode,
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
	fn, ok := h.ops[action]
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

type jsonStreamModeDetails struct {
	StreamMode string `json:"StreamMode"`
}

type jsonCreateStreamReq struct {
	StreamModeDetails *jsonStreamModeDetails `json:"StreamModeDetails,omitempty"`
	Tags              map[string]string      `json:"Tags,omitempty"`
	StreamName        string                 `json:"StreamName"`
	ShardCount        int                    `json:"ShardCount"`
}

type jsonDeleteStreamReq struct {
	StreamName string `json:"StreamName"`
	StreamARN  string `json:"StreamARN"`
}

type jsonDescribeStreamReq struct {
	StreamARN  string `json:"StreamARN"`
	StreamName string `json:"StreamName"`
}

type jsonListStreamsReq struct {
	NextToken                string `json:"NextToken"`
	ExclusiveStartStreamName string `json:"ExclusiveStartStreamName"`
	Limit                    int    `json:"Limit"`
}

type jsonPutRecordReq struct {
	StreamName      string `json:"StreamName"`
	StreamARN       string `json:"StreamARN"`
	PartitionKey    string `json:"PartitionKey"`
	ExplicitHashKey string `json:"ExplicitHashKey,omitempty"`
	Data            []byte `json:"Data"`
}

type jsonPutRecordEntry struct {
	PartitionKey    string `json:"PartitionKey"`
	ExplicitHashKey string `json:"ExplicitHashKey,omitempty"`
	Data            []byte `json:"Data"`
}

type jsonPutRecordsReq struct {
	StreamName string               `json:"StreamName"`
	StreamARN  string               `json:"StreamARN"`
	Records    []jsonPutRecordEntry `json:"Records"`
}

type jsonGetShardIteratorReq struct {
	StreamName             string  `json:"StreamName"`
	StreamARN              string  `json:"StreamARN"`
	ShardID                string  `json:"ShardId"`
	ShardIteratorType      string  `json:"ShardIteratorType"`
	StartingSequenceNumber string  `json:"StartingSequenceNumber"`
	Timestamp              float64 `json:"Timestamp"`
}

type jsonGetRecordsReq struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

type jsonShardFilter struct {
	ShardID   string  `json:"ShardId"`
	Type      string  `json:"Type"`
	Timestamp float64 `json:"Timestamp"`
}

type jsonListShardsReq struct {
	ShardFilter           *jsonShardFilter `json:"ShardFilter,omitempty"`
	StreamName            string           `json:"StreamName"`
	NextToken             string           `json:"NextToken"`
	ExclusiveStartShardID string           `json:"ExclusiveStartShardId,omitempty"`
	MaxResults            int              `json:"MaxResults"`
}

type jsonShardDescription struct {
	ShardID               string           `json:"ShardId"`
	ParentShardID         string           `json:"ParentShardId,omitempty"`
	AdjacentParentShardID string           `json:"AdjacentParentShardId,omitempty"`
	HashKeyRange          jsonHashKeyRange `json:"HashKeyRange"`
	SequenceNumberRange   jsonSeqNumRange  `json:"SequenceNumberRange"`
}

type jsonHashKeyRange struct {
	StartingHashKey string `json:"StartingHashKey"`
	EndingHashKey   string `json:"EndingHashKey"`
}

type jsonSeqNumRange struct {
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

type jsonEnhancedMonitoringEntry struct {
	ShardLevelMetrics []string `json:"ShardLevelMetrics"`
}

type jsonStreamDescriptionSummary struct {
	StreamModeDetails       *jsonStreamModeDetails        `json:"StreamModeDetails,omitempty"`
	StreamName              string                        `json:"StreamName"`
	StreamARN               string                        `json:"StreamARN"`
	StreamStatus            string                        `json:"StreamStatus"`
	EncryptionType          string                        `json:"EncryptionType,omitempty"`
	KeyID                   string                        `json:"KeyId,omitempty"`
	EnhancedMonitoring      []jsonEnhancedMonitoringEntry `json:"EnhancedMonitoring"`
	StreamCreationTimestamp float64                       `json:"StreamCreationTimestamp,omitempty"`
	RetentionPeriodHours    int                           `json:"RetentionPeriodHours"`
	OpenShardCount          int                           `json:"OpenShardCount"`
	// ConsumerCount is the number of registered enhanced fan-out consumers.
	ConsumerCount int `json:"ConsumerCount"`
}

type jsonStreamDescription struct {
	StreamModeDetails       *jsonStreamModeDetails        `json:"StreamModeDetails,omitempty"`
	StreamName              string                        `json:"StreamName"`
	StreamARN               string                        `json:"StreamARN"`
	StreamStatus            string                        `json:"StreamStatus"`
	EncryptionType          string                        `json:"EncryptionType,omitempty"`
	KeyID                   string                        `json:"KeyId,omitempty"`
	Shards                  []jsonShardDescription        `json:"Shards"`
	EnhancedMonitoring      []jsonEnhancedMonitoringEntry `json:"EnhancedMonitoring"`
	StreamCreationTimestamp float64                       `json:"StreamCreationTimestamp,omitempty"`
	RetentionPeriodHours    int                           `json:"RetentionPeriodHours"`
	HasMoreShards           bool                          `json:"HasMoreShards"`
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
	EncryptionType string `json:"EncryptionType,omitempty"`
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

type jsonRetentionPeriodReq struct {
	StreamName           string `json:"StreamName"`
	RetentionPeriodHours int    `json:"RetentionPeriodHours"`
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

	var streamMode string
	if req.StreamModeDetails != nil {
		streamMode = req.StreamModeDetails.StreamMode
	}

	shardCount := req.ShardCount
	// ON_DEMAND streams do not require a ShardCount — AWS ignores it and manages
	// capacity automatically. PROVISIONED streams require ShardCount >= 1.
	if streamMode != streamModeOnDemand && (shardCount <= 0 || shardCount > maxShardCount) {
		return nil, ErrInvalidArgument
	}

	err := h.Backend.CreateStream(&CreateStreamInput{
		StreamName: req.StreamName,
		ShardCount: shardCount,
		Region:     region,
		AccountID:  h.AccountID,
		StreamMode: streamMode,
	})
	if err != nil {
		if !errors.Is(err, ErrStreamAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateStream failed", "error", err)
		}

		return nil, err
	}

	if len(req.Tags) > 0 {
		h.setTags(req.StreamName, req.Tags)
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

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	if err := h.Backend.DeleteStream(&DeleteStreamInput{StreamName: streamName}); err != nil {
		return nil, err
	}

	// Clean up handler-level tags to prevent resource/metric leaks.
	h.tagsMu.Lock("handleDeleteStream")
	if t := h.tags[streamName]; t != nil {
		t.Close()
	}

	delete(h.tags, streamName)
	h.tagsMu.Unlock()

	return struct{}{}, nil
}

// enhancedMonitoringEntries converts a flat slice of shard-level metric names into
// the nested AWS JSON format expected by the Kinesis API.
func enhancedMonitoringEntries(metrics []string) []jsonEnhancedMonitoringEntry {
	if len(metrics) == 0 {
		return nil
	}

	return []jsonEnhancedMonitoringEntry{{ShardLevelMetrics: metrics}}
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

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: streamName})
	if err != nil {
		return nil, err
	}

	shards := make([]jsonShardDescription, 0, len(out.Shards))
	for _, s := range out.Shards {
		shards = append(shards, jsonShardDescription{
			ShardID:               s.ShardID,
			ParentShardID:         s.ParentShardID,
			AdjacentParentShardID: s.AdjacentParentShardID,
			HashKeyRange: jsonHashKeyRange{
				StartingHashKey: s.HashKeyRangeStart,
				EndingHashKey:   s.HashKeyRangeEnd,
			},
			SequenceNumberRange: jsonSeqNumRange{
				StartingSequenceNumber: s.SequenceNumberRangeStart,
				EndingSequenceNumber:   s.SequenceNumberRangeEnd,
			},
		})
	}

	return jsonDescribeStreamResp{
		StreamDescription: jsonStreamDescription{
			StreamName:              out.StreamName,
			StreamARN:               out.StreamARN,
			StreamStatus:            out.StreamStatus,
			RetentionPeriodHours:    out.RetentionPeriodHours,
			Shards:                  shards,
			HasMoreShards:           false,
			EncryptionType:          out.EncryptionType,
			KeyID:                   out.KeyID,
			EnhancedMonitoring:      enhancedMonitoringEntries(out.EnhancedMonitoring),
			StreamCreationTimestamp: float64(out.StreamCreationTimestamp.Unix()),
			StreamModeDetails:       &jsonStreamModeDetails{StreamMode: out.StreamMode},
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

	summaryStreamName := req.StreamName
	if summaryStreamName == "" && req.StreamARN != "" {
		summaryStreamName = streamNameFromARN(req.StreamARN)
	}

	out, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: summaryStreamName})
	if err != nil {
		return nil, err
	}

	openCount := 0
	for _, s := range out.Shards {
		if !s.Closed {
			openCount++
		}
	}

	// Fetch the live consumer count.
	consumerList, _ := h.Backend.ListStreamConsumers(&ListStreamConsumersInput{StreamARN: out.StreamARN})
	consumerCount := 0
	if consumerList != nil {
		consumerCount = len(consumerList.Consumers)
	}

	return jsonDescribeStreamSummaryResp{
		StreamDescriptionSummary: jsonStreamDescriptionSummary{
			StreamName:              out.StreamName,
			StreamARN:               out.StreamARN,
			StreamStatus:            out.StreamStatus,
			RetentionPeriodHours:    out.RetentionPeriodHours,
			OpenShardCount:          openCount,
			ConsumerCount:           consumerCount,
			EncryptionType:          out.EncryptionType,
			KeyID:                   out.KeyID,
			EnhancedMonitoring:      enhancedMonitoringEntries(out.EnhancedMonitoring),
			StreamCreationTimestamp: float64(out.StreamCreationTimestamp.Unix()),
			StreamModeDetails:       &jsonStreamModeDetails{StreamMode: out.StreamMode},
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
		Limit:                    req.Limit,
		NextToken:                req.NextToken,
		ExclusiveStartStreamName: req.ExclusiveStartStreamName,
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
		NextToken:      out.NextToken,
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

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	out, err := h.Backend.PutRecord(&PutRecordInput{
		StreamName:      streamName,
		PartitionKey:    req.PartitionKey,
		ExplicitHashKey: req.ExplicitHashKey,
		Data:            req.Data,
	})
	if err != nil {
		return nil, err
	}

	return jsonPutRecordResp{
		ShardID:        out.ShardID,
		SequenceNumber: out.SequenceNumber,
		EncryptionType: out.EncryptionType,
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

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	numRecords := len(req.Records)
	const maxPutRecords = 500
	if numRecords > maxPutRecords {
		numRecords = maxPutRecords
	}
	entries := make([]PutRecordsEntry, numRecords)
	for i, r := range req.Records[:numRecords] {
		entries[i] = PutRecordsEntry(r)
	}

	out, err := h.Backend.PutRecords(&PutRecordsInput{
		StreamName: streamName,
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

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	out, err := h.Backend.GetShardIterator(&GetShardIteratorInput{
		StreamName:             streamName,
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

	numRecords := len(out.Records)
	const maxGetRecords = 10000
	if numRecords > maxGetRecords {
		numRecords = maxGetRecords
	}
	records := make([]jsonRecord, numRecords)
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

	// AWS rejects requests where NextToken is combined with StreamName,
	// ExclusiveStartShardID, or ShardFilter — the token already encodes stream context.
	if req.NextToken != "" && (req.StreamName != "" || req.ExclusiveStartShardID != "" || req.ShardFilter != nil) {
		return nil, ErrValidation
	}

	// Decode the opaque NextToken to extract embedded stream name and shard cursor.
	// Our token format is "streamName|shardId", base64-free since neither component
	// contains "|".  When StreamName is present (first page), no decoding is needed.
	streamName := req.StreamName
	backendNextToken := ""
	if req.NextToken != "" {
		const tokenParts = 2
		parts := strings.SplitN(req.NextToken, "|", tokenParts)
		if len(parts) == tokenParts {
			streamName = parts[0]
			backendNextToken = parts[1]
		} else {
			// Legacy token: plain shard ID (pre-encoding).  Callers on first page
			// always provide StreamName, so this path only fires for stale tokens.
			backendNextToken = req.NextToken
		}
	}

	var shardFilterType, shardFilterShardID, shardFilterStr string
	if req.ShardFilter != nil {
		shardFilterType = req.ShardFilter.Type
		shardFilterShardID = req.ShardFilter.ShardID
		if shardFilterType != "AT_SHARD_ID" {
			shardFilterStr = shardFilterType
		}
	}
	out, err := h.Backend.ListShards(&ListShardsInput{
		StreamName:            streamName,
		NextToken:             backendNextToken,
		MaxResults:            req.MaxResults,
		ExclusiveStartShardID: req.ExclusiveStartShardID,
		ShardFilter:           shardFilterStr,
		ShardFilterType:       shardFilterType,
		ShardFilterShardID:    shardFilterShardID,
	})
	if err != nil {
		return nil, err
	}

	shards := make([]jsonShardDescription, 0, len(out.Shards))
	for _, s := range out.Shards {
		// The backend already applies ShardFilter; no additional filtering here.
		shards = append(shards, jsonShardDescription{
			ShardID:               s.ShardID,
			ParentShardID:         s.ParentShardID,
			AdjacentParentShardID: s.AdjacentParentShardID,
			HashKeyRange: jsonHashKeyRange{
				StartingHashKey: s.HashKeyRangeStart,
				EndingHashKey:   s.HashKeyRangeEnd,
			},
			SequenceNumberRange: jsonSeqNumRange{
				StartingSequenceNumber: s.SequenceNumberRangeStart,
				EndingSequenceNumber:   s.SequenceNumberRangeEnd,
			},
		})
	}

	// Encode stream name into the NextToken so callers can paginate without re-specifying StreamName.
	encodedNextToken := ""
	if out.NextToken != "" {
		encodedNextToken = streamName + "|" + out.NextToken
	}

	return jsonListShardsResp{Shards: shards, NextToken: encodedNextToken}, nil
}

// errTypeResourceNotFound is the Kinesis error type string for resource not found errors.
const errTypeResourceNotFound = "ResourceNotFoundException"

// errorDetails maps an error to its Kinesis JSON error type, message, and HTTP status.
func errorDetails(err error) (string, string, int) {
	switch {
	case errors.Is(err, ErrStreamNotFound):
		return errTypeResourceNotFound,
			"Stream not found.",
			http.StatusBadRequest
	case errors.Is(err, ErrStreamAlreadyExists):
		return "ResourceInUseException",
			"A stream with this name already exists.",
			http.StatusBadRequest
	case errors.Is(err, ErrConsumerNotFound):
		return errTypeResourceNotFound,
			"Consumer not found.",
			http.StatusBadRequest
	case errors.Is(err, ErrConsumerAlreadyExists):
		return "ResourceInUseException",
			"A consumer with this name already exists.",
			http.StatusBadRequest
	case errors.Is(err, ErrResourcePolicyNotFound):
		return errTypeResourceNotFound,
			"Resource policy not found.",
			http.StatusBadRequest
	case errors.Is(err, ErrProvisionedThroughputExceeded):
		return "ProvisionedThroughputExceededException",
			"Rate exceeded for shard.",
			http.StatusBadRequest
	case errors.Is(err, ErrTagLimitExceeded):
		return "LimitExceededException",
			"Tag limit exceeded. A stream can have at most 50 tags.",
			http.StatusBadRequest
	case errors.Is(err, ErrLimitExceeded):
		return "LimitExceededException",
			"Limit exceeded.",
			http.StatusBadRequest
	case errors.Is(err, ErrInvalidArgument):
		return "InvalidArgumentException",
			"Invalid argument.",
			http.StatusBadRequest
	case errors.Is(err, ErrValidation):
		return "ValidationException", err.Error(), http.StatusBadRequest
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

const (
	// maxTagKeyLen is the maximum byte length of a Kinesis tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a Kinesis tag value.
	maxTagValueLen = 256
)

// validateTagKVs checks that all tag keys are 1-128 bytes and values are 0-256 bytes.
func validateTagKVs(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return ErrInvalidArgument
		}
		if len(v) > maxTagValueLen {
			return ErrInvalidArgument
		}
	}

	return nil
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

	if _, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName}); err != nil {
		return nil, err
	}

	var kv map[string]string
	if req.Tags != nil {
		kv = req.Tags.Clone()
	}

	if err := validateTagKVs(kv); err != nil {
		return nil, err
	}

	existing := h.getTags(req.StreamName)
	merged := make(map[string]string, len(existing))
	maps.Copy(merged, existing)
	maps.Copy(merged, kv)
	if len(merged) > maxTagsPerStream {
		return nil, ErrTagLimitExceeded
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

	if _, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName}); err != nil {
		return nil, err
	}

	h.removeTags(req.StreamName, req.TagKeys)

	return struct{}{}, nil
}

type listTagsForStreamReq struct {
	ExclusiveStartTagKey string `json:"ExclusiveStartTagKey"`
	StreamName           string `json:"StreamName"`
	Limit                int    `json:"Limit"`
}

func (h *Handler) handleListTagsForStream(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req listTagsForStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if _, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: req.StreamName}); err != nil {
		return nil, err
	}

	tagsMap := h.getTags(req.StreamName)

	keys := make([]string, 0, len(tagsMap))
	for k := range tagsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	startIdx := 0
	if req.ExclusiveStartTagKey != "" {
		for startIdx < len(keys) && keys[startIdx] <= req.ExclusiveStartTagKey {
			startIdx++
		}
	}

	const (
		defaultTagPageSize = 10
		maxTagPageSize     = 50
	)
	limit := defaultTagPageSize
	if req.Limit >= 1 && req.Limit <= maxTagPageSize {
		limit = req.Limit
	}

	tagList := make([]svcTags.KV, 0, limit)
	for i := startIdx; i < len(keys) && len(tagList) < limit; i++ {
		tagList = append(tagList, svcTags.KV{Key: keys[i], Value: tagsMap[keys[i]]})
	}

	hasMore := startIdx+len(tagList) < len(keys)

	return &listTagsForStreamOutput{
		Tags:        tagList,
		HasMoreTags: hasMore,
	}, nil
}

func (h *Handler) handleIncreaseStreamRetentionPeriod(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRetentionPeriodReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.IncreaseStreamRetentionPeriod(&IncreaseStreamRetentionPeriodInput{
		StreamName:           req.StreamName,
		RetentionPeriodHours: req.RetentionPeriodHours,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDecreaseStreamRetentionPeriod(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRetentionPeriodReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DecreaseStreamRetentionPeriod(&DecreaseStreamRetentionPeriodInput{
		StreamName:           req.StreamName,
		RetentionPeriodHours: req.RetentionPeriodHours,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDescribeLimits(
	_ context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return &describeLimitsOutput{
		OpenShardCount: h.Backend.CountOpenShards(),
		ShardLimit:     kinesisDefaultShardLimit,
	}, nil
}

// --- JSON types for new operations ---

type jsonDescribeAccountSettingsResp struct {
	ShardLimit               int `json:"ShardLimit"`
	OnDemandStreamCount      int `json:"OnDemandStreamCount"`
	OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
}

type jsonMergeShardsReq struct {
	StreamName           string `json:"StreamName"`
	StreamARN            string `json:"StreamARN"`
	ShardToMerge         string `json:"ShardToMerge"`
	AdjacentShardToMerge string `json:"AdjacentShardToMerge"`
}

type jsonSplitShardReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	ShardToSplit       string `json:"ShardToSplit"`
	NewStartingHashKey string `json:"NewStartingHashKey"`
}

type jsonEncryptionReq struct {
	StreamName     string `json:"StreamName"`
	StreamARN      string `json:"StreamARN"`
	EncryptionType string `json:"EncryptionType"`
	KeyID          string `json:"KeyId"`
}

type jsonResourceARNReq struct {
	ResourceARN string `json:"ResourceARN"`
}

type jsonPutResourcePolicyReq struct {
	ResourceARN string `json:"ResourceARN"`
	Policy      string `json:"Policy"`
}

type jsonGetResourcePolicyResp struct {
	Policy string `json:"Policy"`
}

type jsonListTagsForResourceResp struct {
	Tags []svcTags.KV `json:"Tags"`
}

// --- Handler methods for new operations ---

func (h *Handler) handleDescribeAccountSettings(
	_ context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	out, err := h.Backend.DescribeAccountSettings()
	if err != nil {
		return nil, err
	}

	return jsonDescribeAccountSettingsResp{
		ShardLimit:               out.ShardLimit,
		OnDemandStreamCount:      out.OnDemandStreamCount,
		OnDemandStreamCountLimit: out.OnDemandStreamCountLimit,
	}, nil
}

func (h *Handler) handleMergeShards(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonMergeShardsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.MergeShards(&MergeShardsInput{
		StreamName:           req.StreamName,
		StreamARN:            req.StreamARN,
		ShardToMerge:         req.ShardToMerge,
		AdjacentShardToMerge: req.AdjacentShardToMerge,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSplitShard(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonSplitShardReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.SplitShard(&SplitShardInput{
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		ShardToSplit:       req.ShardToSplit,
		NewStartingHashKey: req.NewStartingHashKey,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleStartStreamEncryption(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEncryptionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.StartStreamEncryption(&StartStreamEncryptionInput{
		StreamName:     req.StreamName,
		StreamARN:      req.StreamARN,
		EncryptionType: req.EncryptionType,
		KeyID:          req.KeyID,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleStopStreamEncryption(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEncryptionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.StopStreamEncryption(&StopStreamEncryptionInput{
		StreamName:     req.StreamName,
		StreamARN:      req.StreamARN,
		EncryptionType: req.EncryptionType,
		KeyID:          req.KeyID,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handlePutResourcePolicy(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonPutResourcePolicyReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.PutResourcePolicy(&PutResourcePolicyInput{
		ResourceARN: req.ResourceARN,
		Policy:      req.Policy,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleGetResourcePolicy(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.GetResourcePolicy(&GetResourcePolicyInput{
		ResourceARN: req.ResourceARN,
	})
	if err != nil {
		return nil, err
	}

	return jsonGetResourcePolicyResp{Policy: out.Policy}, nil
}

func (h *Handler) handleDeleteResourcePolicy(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeleteResourcePolicy(&DeleteResourcePolicyInput{
		ResourceARN: req.ResourceARN,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonResourceARNReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName := streamNameFromARN(req.ResourceARN)

	// Validate the stream exists before returning tags.
	if _, err := h.Backend.DescribeStream(&DescribeStreamInput{StreamName: streamName}); err != nil {
		return nil, err
	}

	tags := h.getTags(streamName)
	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}
	slices.SortFunc(tagList, func(a, b svcTags.KV) int {
		return cmp.Compare(a.Key, b.Key)
	})

	return jsonListTagsForResourceResp{Tags: tagList}, nil
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
		nameLen := len(name)
		if nameLen > math.MaxUint8 {
			continue
		}

		buf.WriteByte(byte(nameLen))
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
	// Guard against integer overflow when calculating totalLen.
	totalLen := uint64(eventStreamPreludeLen) + uint64(headerLen) + uint64(payloadLen) + uint64(eventStreamMsgCRCLen)
	if totalLen > math.MaxInt32 {
		return nil
	}

	buf := make([]byte, totalLen)
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

// subscribeToShardStreamDuration is how long a SubscribeToShard stream stays open (~5 min).
const subscribeToShardStreamDuration = 5 * time.Minute

// subscribeToShardPollInterval is the poll interval between record checks.
const subscribeToShardPollInterval = 200 * time.Millisecond

// subscribeToShardMaxIdlePolls is the number of consecutive empty polls before the stream
// is closed gracefully.  AWS clients re-subscribe after a stream closes, so closing on
// idle is safe.  Keeping this small (3 × 200 ms = 600 ms) ensures tests complete quickly.
const subscribeToShardMaxIdlePolls = 3

// handleSubscribeToShardHTTP handles the SubscribeToShard operation using the AWS event stream
// binary protocol. It keeps the response stream open for up to 5 minutes, pushing records as
// they arrive via periodic polling with chunked flushing.
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

	// Validate consumer/shard before opening the stream.
	if _, err = h.Backend.SubscribeToShard(&SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: sp,
	}); err != nil {
		return h.handleError(ctx, c, "SubscribeToShard", err)
	}

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)

	flusher, canFlush := c.Response().(http.Flusher)

	// Send initial-response so the SDK event-stream middleware unblocks.
	initialMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "initial-response"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, []byte("{}"))
	if _, writeErr := c.Response().Write(initialMsg); writeErr != nil {
		return writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	deadline := time.Now().Add(subscribeToShardStreamDuration)
	ticker := time.NewTicker(subscribeToShardPollInterval)
	defer ticker.Stop()

	curSP := sp
	idlePolls := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}

			if stop, next := h.advanceShardCursor(req, curSP, c.Response(), flusher, canFlush, &idlePolls); stop {
				return nil
			} else if next != nil {
				curSP = *next
			}
		}
	}
}

// advanceShardCursor calls pollSubscribeToShardTick and returns (stop=true, nil) when the
// stream should close, or (false, nextSP) when it should continue (nextSP may be nil).
func (h *Handler) advanceShardCursor(
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	idlePolls *int,
) (bool, *StartingPosition) {
	done, next, tickErr := h.pollSubscribeToShardTick(req, curSP, w, flusher, canFlush, idlePolls)
	if tickErr != nil || done {
		return true, nil
	}

	return false, next
}

// pollSubscribeToShardTick performs one poll tick for handleSubscribeToShardHTTP.
// Returns (true, nil, err) when the stream should close (poll error or idle limit reached),
// (false, nextSP, nil) when records were delivered (nextSP non-nil means cursor advanced),
// and (false, nil, err) on a write error.
func (h *Handler) pollSubscribeToShardTick(
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	idlePolls *int,
) (bool, *StartingPosition, error) {
	out, pollErr := h.Backend.SubscribeToShard(&SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: curSP,
	})
	if pollErr != nil {
		return true, nil, pollErr
	}

	if len(out.Event.Records) == 0 {
		*idlePolls++
		if *idlePolls >= subscribeToShardMaxIdlePolls {
			return true, nil, nil
		}

		return false, nil, nil
	}
	*idlePolls = 0

	records := make([]jsonRecord, len(out.Event.Records))
	for i, r := range out.Event.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisPerSecond,
		}
	}

	eventPayload, marshalErr := json.Marshal(jsonSubscribeToShardEvent{
		Records:                    records,
		ContinuationSequenceNumber: out.Event.ContinuationSequenceNumber,
		MillisBehindLatest:         out.Event.MillisBehindLatest,
	})
	if marshalErr != nil {
		return false, nil, marshalErr
	}

	eventMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "SubscribeToShardEvent"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, eventPayload)

	if _, writeErr := w.Write(eventMsg); writeErr != nil {
		return false, nil, writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	if out.Event.ContinuationSequenceNumber != "" {
		sp := StartingPosition{
			Type:           iteratorTypeAfterSequenceNumber,
			SequenceNumber: out.Event.ContinuationSequenceNumber,
		}

		return false, &sp, nil
	}

	return false, nil, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(resetter); ok {
		r.Reset()
	}

	// Close and discard handler-level tag registries to prevent metric leaks.
	h.tagsMu.Lock("Reset")
	for _, t := range h.tags {
		if t != nil {
			t.Close()
		}
	}

	h.tags = make(map[string]*svcTags.Tags)
	h.tagsMu.Unlock()
}

// Purge implements service.Purgeable by removing all Kinesis streams older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if p, ok := h.Backend.(purger); ok {
		p.Purge(ctx, cutoff)
	}
}

func (h *Handler) handleUpdateStreamMode(_ context.Context, _ *http.Request, body []byte) (any, error) {
	var req struct {
		StreamModeDetails *jsonStreamModeDetails `json:"StreamModeDetails"`
		StreamARN         string                 `json:"StreamARN"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	if req.StreamModeDetails == nil {
		return nil, ErrInvalidArgument
	}

	return struct{}{}, h.Backend.UpdateStreamMode(&UpdateStreamModeInput{
		StreamARN:         req.StreamARN,
		StreamModeDetails: StreamModeDetails{StreamMode: req.StreamModeDetails.StreamMode},
	})
}

// --- Handler functions for TagResource, UntagResource, and new account/stream operations ---

type jsonTagResourceReq struct {
	Tags        map[string]string `json:"Tags"`
	ResourceARN string            `json:"ResourceARN"`
}

type jsonUntagResourceReq struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleTagResource(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonTagResourceReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := validateTagKVs(req.Tags); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(&TagResourceInput{
		ResourceARN: req.ResourceARN,
		Tags:        req.Tags,
	}); err != nil {
		return nil, err
	}

	// Mirror into the handler-level tag store for ListTagsForStream compatibility.
	streamName := streamNameFromARN(req.ResourceARN)
	if streamName != "" {
		h.setTags(streamName, req.Tags)
	}

	return struct{}{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUntagResourceReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UntagResource(&UntagResourceInput{
		ResourceARN: req.ResourceARN,
		TagKeys:     req.TagKeys,
	}); err != nil {
		return nil, err
	}

	// Mirror removal into the handler-level tag store.
	streamName := streamNameFromARN(req.ResourceARN)
	if streamName != "" {
		h.removeTags(streamName, req.TagKeys)
	}

	return struct{}{}, nil
}

type jsonUpdateAccountSettingsReq struct {
	OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
}

func (h *Handler) handleUpdateAccountSettings(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateAccountSettingsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UpdateAccountSettings(&UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: req.OnDemandStreamCountLimit,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

type jsonUpdateMaxRecordSizeReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	MaxRecordSizeBytes int    `json:"MaxRecordSizeBytes"`
}

func (h *Handler) handleUpdateMaxRecordSize(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateMaxRecordSizeReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UpdateMaxRecordSize(&UpdateMaxRecordSizeInput{
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		MaxRecordSizeBytes: req.MaxRecordSizeBytes,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

type jsonUpdateStreamWarmThroughputReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	WriteCapacityUnits int64  `json:"WriteCapacityUnits"`
	ReadCapacityUnits  int64  `json:"ReadCapacityUnits"`
}

func (h *Handler) handleUpdateStreamWarmThroughput(
	_ context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateStreamWarmThroughputReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UpdateStreamWarmThroughput(&UpdateStreamWarmThroughputInput{
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		WriteCapacityUnits: req.WriteCapacityUnits,
		ReadCapacityUnits:  req.ReadCapacityUnits,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
