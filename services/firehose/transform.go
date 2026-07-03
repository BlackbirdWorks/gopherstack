package firehose

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
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

// transformOutcome is the result of a Lambda transformation: records marked "Ok"
// are delivered downstream, while records marked "ProcessingFailed" are routed to
// the S3 error/backup destination. Records marked "Dropped" are intentionally
// discarded and appear in neither slice.
type transformOutcome struct {
	Ok     [][]byte
	Failed [][]byte
}

// buildLambdaTransformPayload builds the JSON payload for a Lambda transformation
// invocation. It returns the marshaled event and a map from the deterministic
// record ID to the original record bytes so that "ProcessingFailed" records can be
// recovered (and routed to the error destination) even when the Lambda response
// omits their data.
func buildLambdaTransformPayload(records [][]byte, streamARN, region string) ([]byte, map[string][]byte) {
	now := time.Now().UnixMilli()

	event := lambdaTransformEvent{
		InvocationID:      uuid.NewString(),
		DeliveryStreamARN: streamARN,
		Region:            region,
		Records:           make([]lambdaTransformRecord, len(records)),
	}

	idToOriginal := make(map[string][]byte, len(records))

	for i, rec := range records {
		recordID := strconv.Itoa(i)
		idToOriginal[recordID] = rec
		event.Records[i] = lambdaTransformRecord{
			RecordID:                    recordID,
			Data:                        base64.StdEncoding.EncodeToString(rec),
			ApproximateArrivalTimestamp: now,
		}
	}

	payload, err := json.Marshal(event)
	if err != nil {
		// If we can't marshal the payload, return nil so the caller
		// can propagate the failure and route the records to the error output.
		return nil, nil
	}

	return payload, idToOriginal
}

// parseLambdaTransformResponse parses the Lambda response, separating "Ok" records
// (to be delivered) from "ProcessingFailed" records (to be routed to the S3 error
// destination). "Dropped" records are discarded. idToOriginal maps record IDs back
// to their source bytes so failed records carry their original payload even when the
// Lambda omits the data field. A record whose data cannot be decoded is treated as a
// processing failure so it is not silently lost.
func parseLambdaTransformResponse(result []byte, idToOriginal map[string][]byte) (transformOutcome, bool) {
	var resp lambdaTransformResponse
	if err := json.Unmarshal(result, &resp); err != nil {
		return transformOutcome{}, false
	}

	out := transformOutcome{
		Ok:     make([][]byte, 0, len(resp.Records)),
		Failed: make([][]byte, 0),
	}

	for _, rec := range resp.Records {
		switch rec.Result {
		case "Ok":
			data, err := base64.StdEncoding.DecodeString(rec.Data)
			if err != nil {
				out.Failed = append(out.Failed, originalOrDecoded(rec, idToOriginal))

				continue
			}

			out.Ok = append(out.Ok, data)
		case "Dropped":
			// Intentionally discarded by the transform function.
			continue
		default:
			// "ProcessingFailed" (and any unknown result) is routed to the error output.
			out.Failed = append(out.Failed, originalOrDecoded(rec, idToOriginal))
		}
	}

	return out, true
}

// originalOrDecoded returns the original source bytes for a record ID when known,
// falling back to decoding the response data field.
func originalOrDecoded(rec lambdaTransformResponseRecord, idToOriginal map[string][]byte) []byte {
	if orig, ok := idToOriginal[rec.RecordID]; ok && len(orig) > 0 {
		return orig
	}

	if data, err := base64.StdEncoding.DecodeString(rec.Data); err == nil && len(data) > 0 {
		return data
	}

	return nil
}
