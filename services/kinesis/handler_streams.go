package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type jsonStreamModeDetails struct {
	StreamMode string `json:"StreamMode"`
}

type jsonCreateStreamReq struct {
	StreamModeDetails   *jsonStreamModeDetails `json:"StreamModeDetails,omitempty"`
	Tags                map[string]string      `json:"Tags,omitempty"`
	StreamName          string                 `json:"StreamName"`
	ShardCount          int                    `json:"ShardCount"`
	MaxRecordSizeInKiB  int                    `json:"MaxRecordSizeInKiB,omitempty"`
	WarmThroughputMiBps int                    `json:"WarmThroughputMiBps,omitempty"`
}

type jsonDeleteStreamReq struct {
	StreamName              string `json:"StreamName"`
	StreamARN               string `json:"StreamARN"`
	EnforceConsumerDeletion bool   `json:"EnforceConsumerDeletion"`
}

type jsonDescribeStreamReq struct {
	StreamARN  string `json:"StreamARN"`
	StreamName string `json:"StreamName"`
	// ExclusiveStartShardID and Limit paginate the Shards list (DescribeStream
	// only; DescribeStreamSummary ignores them, matching the AWS request shape).
	ExclusiveStartShardID string `json:"ExclusiveStartShardId"`
	Limit                 int    `json:"Limit"`
}

type jsonListStreamsReq struct {
	NextToken                string `json:"NextToken"`
	ExclusiveStartStreamName string `json:"ExclusiveStartStreamName"`
	Limit                    int    `json:"Limit"`
}

type jsonEnhancedMonitoringEntry struct {
	ShardLevelMetrics []string `json:"ShardLevelMetrics"`
}

type jsonStreamDescriptionSummary struct {
	StreamModeDetails       *jsonStreamModeDetails        `json:"StreamModeDetails,omitempty"`
	WarmThroughput          *jsonWarmThroughputObject     `json:"WarmThroughput,omitempty"`
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
	// MaxRecordSizeInKiB mirrors Stream.MaxRecordSizeBytes (UpdateMaxRecordSize's
	// wire unit is KiB, not bytes -- see UpdateMaxRecordSizeInput).
	MaxRecordSizeInKiB int `json:"MaxRecordSizeInKiB"`
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

func (h *Handler) handleCreateStream(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonCreateStreamReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if !streamNameRe.MatchString(req.StreamName) {
		return nil, ErrValidation
	}

	// Validate tags before mutating any state: AWS rejects the whole CreateStream
	// call (no stream created) when the inline Tags fail key/value length or
	// count constraints.
	if err := validateTagKVs(req.Tags); err != nil {
		return nil, err
	}
	if len(req.Tags) > maxTagsPerStream {
		return nil, ErrTagLimitExceeded
	}

	region := getRegion(ctx, h.defaultRegion())

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

	err := h.Backend.CreateStream(ctx, &CreateStreamInput{
		StreamName:          req.StreamName,
		ShardCount:          shardCount,
		Region:              region,
		AccountID:           h.AccountID,
		StreamMode:          streamMode,
		MaxRecordSizeInKiB:  req.MaxRecordSizeInKiB,
		WarmThroughputMiBps: req.WarmThroughputMiBps,
	})
	if err != nil {
		if !errors.Is(err, ErrStreamAlreadyExists) {
			logger.Load(ctx).WarnContext(ctx, "CreateStream failed", "error", err)
		}

		return nil, err
	}

	if len(req.Tags) > 0 {
		// Persist tags on the backend's stream.Tags (the field that actually
		// participates in Snapshot/Restore) rather than a handler-local map, so
		// tags applied at creation time survive a persistence round-trip.
		if out, dErr := h.Backend.DescribeStream(ctx, &DescribeStreamInput{StreamName: req.StreamName}); dErr == nil {
			if tErr := h.Backend.TagResource(ctx, &TagResourceInput{
				ResourceARN: out.StreamARN,
				Tags:        req.Tags,
			}); tErr != nil {
				logger.Load(ctx).WarnContext(ctx, "CreateStream: failed to apply inline tags", "error", tErr)
			}
		}
	}

	return struct{}{}, nil
}

func (h *Handler) handleDeleteStream(
	ctx context.Context,
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

	// When the request addresses the stream by ARN, route to the ARN's region;
	// otherwise use the region carried on ctx.
	region := getRegion(ctx, h.defaultRegion())
	if req.StreamARN != "" {
		region = regionFromARNOrCtx(ctx, req.StreamARN, h.defaultRegion())
	}
	regionCtx := contextWithRegion(ctx, region)

	if err := h.Backend.DeleteStream(regionCtx, &DeleteStreamInput{
		StreamName:              streamName,
		EnforceConsumerDeletion: req.EnforceConsumerDeletion,
	}); err != nil {
		return nil, err
	}

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
	ctx context.Context,
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

	out, err := h.Backend.DescribeStream(ctx, &DescribeStreamInput{
		StreamName:            streamName,
		ExclusiveStartShardID: req.ExclusiveStartShardID,
		Limit:                 req.Limit,
	})
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
			HasMoreShards:           out.HasMoreShards,
			EncryptionType:          out.EncryptionType,
			KeyID:                   out.KeyID,
			EnhancedMonitoring:      enhancedMonitoringEntries(out.EnhancedMonitoring),
			StreamCreationTimestamp: float64(out.StreamCreationTimestamp.Unix()),
			StreamModeDetails:       &jsonStreamModeDetails{StreamMode: out.StreamMode},
		},
	}, nil
}

func (h *Handler) handleDescribeStreamSummary(
	ctx context.Context,
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

	out, err := h.Backend.DescribeStream(ctx, &DescribeStreamInput{StreamName: summaryStreamName})
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
	consumerList, _ := h.Backend.ListStreamConsumers(ctx, &ListStreamConsumersInput{StreamARN: out.StreamARN})
	consumerCount := 0
	if consumerList != nil {
		consumerCount = len(consumerList.Consumers)
	}

	maxRecordSizeBytes := out.MaxRecordSizeBytes
	if maxRecordSizeBytes <= 0 {
		maxRecordSizeBytes = defaultMaxRecordSizeBytes
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
			MaxRecordSizeInKiB:      maxRecordSizeBytes / bytesPerKiB,
			// Applied synchronously (no UPDATING transient-state model), so
			// Current and Target always match -- see UpdateStreamWarmThroughput.
			WarmThroughput: &jsonWarmThroughputObject{
				CurrentMiBps: out.WarmThroughputMiBps,
				TargetMiBps:  out.WarmThroughputMiBps,
			},
		},
	}, nil
}

func (h *Handler) handleListStreams(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListStreamsReq
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListStreams(ctx, &ListStreamsInput{
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
