package sqs

import (
	"net/http"
	"net/url"
	"strconv"
)

// parseQueryChangeBatchEntries parses ChangeMessageVisibilityBatch entries from Query params.
func parseQueryChangeBatchEntries(vals url.Values) []ChangeMessageVisibilityBatchRequestEntry {
	var entries []ChangeMessageVisibilityBatchRequestEntry

	const prefix = "ChangeMessageVisibilityBatchRequestEntry"

	for i := 1; i <= maxParseIterations; i++ {
		id := numberedParam(vals, prefix, i, "Id")
		if id == "" {
			break
		}

		vt, _ := strconv.Atoi(numberedParam(vals, prefix, i, "VisibilityTimeout"))
		entries = append(entries, ChangeMessageVisibilityBatchRequestEntry{
			ID:                id,
			ReceiptHandle:     numberedParam(vals, prefix, i, "ReceiptHandle"),
			VisibilityTimeout: vt,
		})
	}

	return entries
}

func (h *Handler) queryChangeMessageVisibility(vals url.Values, region string) ([]byte, int, *queryError) {
	vt, _ := strconv.Atoi(vals.Get("VisibilityTimeout"))

	if err := h.Backend.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
		QueueURL:          vals.Get("QueueUrl"),
		Region:            region,
		ReceiptHandle:     vals.Get("ReceiptHandle"),
		VisibilityTimeout: vt,
	}); err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ChangeMessageVisibilityResponse{
		Xmlns:            sqsNamespace,
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}

func (h *Handler) queryChangeMessageVisibilityBatch(vals url.Values, region string) ([]byte, int, *queryError) {
	entries := parseQueryChangeBatchEntries(vals)

	out, err := h.Backend.ChangeMessageVisibilityBatch(&ChangeMessageVisibilityBatchInput{
		QueueURL: vals.Get("QueueUrl"),
		Region:   region,
		Entries:  entries,
	})
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	resp := ChangeMessageVisibilityBatchResponse{
		Xmlns: sqsNamespace,
		Result: ChangeMessageVisibilityBatchResult{
			Successful: out.Successful,
			Failed:     out.Failed,
		},
		ResponseMetadata: XMLResponseMetadata{RequestID: queryRequestID},
	}

	b, err := marshalXML(resp)
	if err != nil {
		return nil, 0, buildQueryError(err)
	}

	return b, http.StatusOK, nil
}
