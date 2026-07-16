package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

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

type jsonGetRecordsReq struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
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

func (h *Handler) handlePutRecord(
	ctx context.Context,
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

	out, err := h.Backend.PutRecord(ctx, &PutRecordInput{
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
	ctx context.Context,
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

	out, err := h.Backend.PutRecords(ctx, &PutRecordsInput{
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

func (h *Handler) handleGetRecords(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetRecordsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.GetRecords(ctx, &GetRecordsInput{
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
