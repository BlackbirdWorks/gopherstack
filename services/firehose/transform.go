package firehose

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// lambdaTransformEvent is the event sent to a Lambda transformation function.
type lambdaTransformEvent struct {
	InvocationID      string                  `json:"invocationId"`
	DeliveryStreamARN string                  `json:"deliveryStreamArn"`
	Region            string                  `json:"region"`
	Records           []lambdaTransformRecord `json:"records"`
}

// lambdaTransformRecord is a single record in a Lambda transform event.
type lambdaTransformRecord struct {
	RecordID                    string `json:"recordId"`
	Data                        string `json:"data"`
	ApproximateArrivalTimestamp int64  `json:"approximateArrivalTimestamp"`
}

// lambdaTransformResponse is the response from a Lambda transformation function.
type lambdaTransformResponse struct {
	Records []lambdaTransformResponseRecord `json:"records"`
}

// lambdaTransformResponseRecord is a single record in a Lambda transform response.
type lambdaTransformResponseRecord struct {
	RecordID string `json:"recordId"`
	Result   string `json:"result"`
	Data     string `json:"data"`
}

// buildLambdaTransformPayload builds the JSON payload for a Lambda transformation invocation.
func buildLambdaTransformPayload(records [][]byte, streamARN, region string) []byte {
	now := time.Now().UnixMilli()

	event := lambdaTransformEvent{
		InvocationID:      uuid.NewString(),
		DeliveryStreamARN: streamARN,
		Region:            region,
		Records:           make([]lambdaTransformRecord, len(records)),
	}

	for i, rec := range records {
		event.Records[i] = lambdaTransformRecord{
			RecordID:                    uuid.NewString(),
			Data:                        base64.StdEncoding.EncodeToString(rec),
			ApproximateArrivalTimestamp: now,
		}
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return []byte("{}")
	}

	return payload
}

// parseLambdaTransformResponse parses the Lambda response and returns only "Ok" records.
func parseLambdaTransformResponse(result []byte) [][]byte {
	var resp lambdaTransformResponse
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil
	}

	out := make([][]byte, 0, len(resp.Records))

	for _, rec := range resp.Records {
		if rec.Result != "Ok" {
			continue
		}

		data, err := base64.StdEncoding.DecodeString(rec.Data)
		if err != nil {
			continue
		}

		out = append(out, data)
	}

	return out
}
