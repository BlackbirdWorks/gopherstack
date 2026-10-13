package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type jsonShardFilter struct {
	Timestamp *float64 `json:"Timestamp,omitempty"`
	ShardID   string   `json:"ShardId"`
	Type      string   `json:"Type"`
}

type jsonListShardsReq struct {
	ShardFilter           *jsonShardFilter `json:"ShardFilter,omitempty"`
	StreamCreationTime    *float64         `json:"StreamCreationTimestamp,omitempty"`
	MaxResults            *int             `json:"MaxResults"`
	StreamName            string           `json:"StreamName"`
	StreamARN             string           `json:"StreamARN"`
	NextToken             string           `json:"NextToken"`
	ExclusiveStartShardID string           `json:"ExclusiveStartShardId,omitempty"`
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

type jsonListShardsResp struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Shards    []jsonShardDescription `json:"Shards"`
}

const (
	minListShardsMaxResults = 1
	maxListShardsMaxResults = 10000
)

func epochSecondsToTime(sec *float64) *time.Time {
	if sec == nil {
		return nil
	}

	ts := time.UnixMilli(int64(*sec * millisPerSecond))

	return &ts
}

func (h *Handler) handleListShards(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListShardsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if req.NextToken != "" &&
		(req.StreamName != "" || req.StreamARN != "" || req.ExclusiveStartShardID != "" || req.ShardFilter != nil ||
			req.StreamCreationTime != nil) {
		return nil, ErrInvalidArgument
	}

	if req.MaxResults != nil &&
		(*req.MaxResults < minListShardsMaxResults || *req.MaxResults > maxListShardsMaxResults) {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	var maxResults int
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}

	var shardFilterType, shardFilterShardID string
	var shardFilterTimestamp *time.Time
	if req.ShardFilter != nil {
		shardFilterType = req.ShardFilter.Type
		shardFilterShardID = req.ShardFilter.ShardID
		shardFilterTimestamp = epochSecondsToTime(req.ShardFilter.Timestamp)
	}
	streamCreationTime := epochSecondsToTime(req.StreamCreationTime)

	out, err := h.Backend.ListShards(ctx, &ListShardsInput{
		StreamCreationTime:    streamCreationTime,
		StreamName:            streamName,
		NextToken:             req.NextToken,
		MaxResults:            maxResults,
		ExclusiveStartShardID: req.ExclusiveStartShardID,
		ShardFilter:           shardFilterType,
		ShardFilterType:       shardFilterType,
		ShardFilterShardID:    shardFilterShardID,
		ShardFilterTimestamp:  shardFilterTimestamp,
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

	return jsonListShardsResp{Shards: shards, NextToken: out.NextToken}, nil
}
