package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type jsonShardFilter struct {
	Timestamp *float64 `json:"Timestamp,omitempty"`
	ShardID   string   `json:"ShardId"`
	Type      string   `json:"Type"`
}

type jsonListShardsReq struct {
	ShardFilter           *jsonShardFilter `json:"ShardFilter,omitempty"`
	StreamName            string           `json:"StreamName"`
	StreamARN             string           `json:"StreamARN"`
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

type jsonListShardsResp struct {
	NextToken string                 `json:"NextToken,omitempty"`
	Shards    []jsonShardDescription `json:"Shards"`
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

	// AWS rejects requests where NextToken is combined with StreamName,
	// StreamARN, ExclusiveStartShardID, or ShardFilter — the token already
	// encodes stream context.
	if req.NextToken != "" &&
		(req.StreamName != "" || req.StreamARN != "" || req.ExclusiveStartShardID != "" || req.ShardFilter != nil) {
		return nil, ErrValidation
	}

	// Decode the opaque NextToken to extract embedded stream name and shard cursor.
	// Our token format is "streamName|shardId", base64-free since neither component
	// contains "|".  When StreamName is present (first page), no decoding is needed.
	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())
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

	var shardFilterType, shardFilterShardID string
	var shardFilterTimestamp *time.Time
	if req.ShardFilter != nil {
		shardFilterType = req.ShardFilter.Type
		shardFilterShardID = req.ShardFilter.ShardID
		if req.ShardFilter.Timestamp != nil {
			ts := time.UnixMilli(int64(*req.ShardFilter.Timestamp * millisPerSecond))
			shardFilterTimestamp = &ts
		}
	}
	out, err := h.Backend.ListShards(ctx, &ListShardsInput{
		StreamName:            streamName,
		NextToken:             backendNextToken,
		MaxResults:            req.MaxResults,
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

	// Encode stream name into the NextToken so callers can paginate without re-specifying StreamName.
	encodedNextToken := ""
	if out.NextToken != "" {
		encodedNextToken = streamName + "|" + out.NextToken
	}

	return jsonListShardsResp{Shards: shards, NextToken: encodedNextToken}, nil
}
