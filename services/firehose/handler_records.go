package firehose

import (
	"context"
	"encoding/base64"
)

// firehoseRecord holds the base64-encoded data for a single Firehose record.
type firehoseRecord struct {
	Data string `json:"Data"`
}

type handlePutRecordInput struct {
	DeliveryStreamName string         `json:"DeliveryStreamName"`
	Record             firehoseRecord `json:"Record"`
}

type putRecordOutput struct {
	RecordID  string `json:"RecordId"`
	Encrypted bool   `json:"Encrypted,omitempty"`
}

func (h *Handler) handlePutRecord(ctx context.Context, in *handlePutRecordInput) (*putRecordOutput, error) {
	data, err := base64.StdEncoding.DecodeString(in.Record.Data)
	if err != nil {
		data = []byte(in.Record.Data)
	}

	if putErr := h.Backend.PutRecord(ctx, in.DeliveryStreamName, data); putErr != nil {
		return nil, putErr
	}

	encrypted := h.Backend.IsStreamEncrypted(ctx, in.DeliveryStreamName)

	return &putRecordOutput{RecordID: newRecordID(ctx), Encrypted: encrypted}, nil
}

type handlePutRecordBatchInput struct {
	DeliveryStreamName string           `json:"DeliveryStreamName"`
	Records            []firehoseRecord `json:"Records"`
}

// putRecordBatchEntry holds the per-record response from PutRecordBatch.
// On success RecordId is populated; on failure ErrorCode and ErrorMessage are set.
type putRecordBatchEntry struct {
	RecordID     string `json:"RecordId,omitempty"`
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

type putRecordBatchOutput struct {
	RequestResponses []putRecordBatchEntry `json:"RequestResponses"`
	FailedPutCount   int                   `json:"FailedPutCount"`
	Encrypted        bool                  `json:"Encrypted,omitempty"`
}

func (h *Handler) handlePutRecordBatch(
	ctx context.Context,
	in *handlePutRecordBatchInput,
) (*putRecordBatchOutput, error) {
	records := make([][]byte, 0, len(in.Records))
	for _, r := range in.Records {
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			data = []byte(r.Data)
		}

		records = append(records, data)
	}

	failedCount, err := h.Backend.PutRecordBatch(ctx, in.DeliveryStreamName, records)
	if err != nil {
		return nil, err
	}

	responses := make([]putRecordBatchEntry, len(records))
	for i := range records {
		responses[i] = putRecordBatchEntry{RecordID: newRecordID(ctx)}
	}

	return &putRecordBatchOutput{
		FailedPutCount:   failedCount,
		RequestResponses: responses,
		Encrypted:        h.Backend.IsStreamEncrypted(ctx, in.DeliveryStreamName),
	}, nil
}
