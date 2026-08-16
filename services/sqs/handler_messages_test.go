package sqs_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestHandlerActions_SendMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "my-queue")

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello from handler",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MessageID        string `json:"MessageId"`
		MD5OfMessageBody string `json:"MD5OfMessageBody"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.MessageID)
	assert.NotEmpty(t, resp.MD5OfMessageBody)
}

func TestHandlerActions_ReceiveMessage(t *testing.T) {
	t.Parallel()

	t.Run("standard", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "my-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		rec := doRequest(t, h, "ReceiveMessage", map[string]any{
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": 1,
			"WaitTimeSeconds":     0,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Messages []struct {
				Body string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "hello", resp.Messages[0].Body)
	})

	t.Run("with visibility timeout", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "vt-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		rec := doRequest(t, h, "ReceiveMessage", map[string]any{
			"QueueUrl":          queueURL,
			"VisibilityTimeout": 30,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Messages []struct {
				Body string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Messages, 1)
	})
}

func TestHandlerActions_ReceiveMessageAttributeNamesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		messageAttrNames []string
		wantAttrNames    []string
		wantAttrNamesLen int
	}{
		{
			name:             "no_requested_message_attributes_returns_empty_map",
			messageAttrNames: nil,
			wantAttrNamesLen: 0,
		},
		{
			name:             "explicit_attribute_name_filters_results",
			messageAttrNames: []string{"AttrA"},
			wantAttrNames:    []string{"AttrA"},
			wantAttrNamesLen: 1,
		},
		{
			name:             "all_sentinel_returns_all_attributes",
			messageAttrNames: []string{"All"},
			wantAttrNames:    []string{"AttrA", "AttrB", "Other"},
			wantAttrNamesLen: 3,
		},
		{
			name:             "prefix_wildcard_returns_matching_attributes",
			messageAttrNames: []string{"Attr.*"},
			wantAttrNames:    []string{"AttrA", "AttrB"},
			wantAttrNamesLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "msg-attr-filter-queue")
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "hello",
				"MessageAttributes": map[string]any{
					"AttrA": map[string]any{"DataType": "String", "StringValue": "A"},
					"AttrB": map[string]any{"DataType": "String", "StringValue": "B"},
					"Other": map[string]any{"DataType": "String", "StringValue": "X"},
				},
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":              queueURL,
				"MaxNumberOfMessages":   1,
				"MessageAttributeNames": tt.messageAttrNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Messages []struct {
					MessageAttributes map[string]map[string]any `json:"MessageAttributes"`
				} `json:"Messages"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Messages, 1)
			assert.Len(t, resp.Messages[0].MessageAttributes, tt.wantAttrNamesLen)
			for _, name := range tt.wantAttrNames {
				assert.Contains(t, resp.Messages[0].MessageAttributes, name)
			}
		})
	}
}

// sendForMD5 sends a message carrying exactly attrs and returns the
// MD5OfMessageAttributes the backend computed for that attribute set. It is
// used as the oracle for the digest a ReceiveMessage should report when only
// that subset is requested.
func sendForMD5(t *testing.T, h *sqs.Handler, queueURL string, attrs map[string]any) string {
	t.Helper()

	body := map[string]any{"QueueUrl": queueURL, "MessageBody": "x"}
	if len(attrs) > 0 {
		body["MessageAttributes"] = attrs
	}

	rec := doRequest(t, h, "SendMessage", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MD5OfMessageAttributes string `json:"MD5OfMessageAttributes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.MD5OfMessageAttributes
}

// TestHandlerActions_ReceiveMessageMD5OverSubset verifies that ReceiveMessage
// recomputes MD5OfMessageAttributes over only the attributes returned to the
// consumer (AWS behaviour) rather than echoing the send-time digest computed
// over the full attribute set. SDKs verify this checksum against the returned
// attributes, so a stale full-set digest would fail client-side validation.
func TestHandlerActions_ReceiveMessageMD5OverSubset(t *testing.T) {
	t.Parallel()

	allAttrs := map[string]any{
		"AttrA": map[string]any{"DataType": "String", "StringValue": "A"},
		"AttrB": map[string]any{"DataType": "String", "StringValue": "B"},
		"Other": map[string]any{"DataType": "String", "StringValue": "X"},
	}

	tests := []struct {
		// oracleAttrs is the exact subset the consumer should receive; the
		// expected MD5 is the digest a SendMessage of just these would produce.
		oracleAttrs      map[string]any
		name             string
		messageAttrNames []string
		wantEmptyMD5     bool
	}{
		{
			name:             "subset_one_attribute",
			messageAttrNames: []string{"AttrA"},
			oracleAttrs: map[string]any{
				"AttrA": map[string]any{"DataType": "String", "StringValue": "A"},
			},
		},
		{
			name:             "prefix_subset",
			messageAttrNames: []string{"Attr.*"},
			oracleAttrs: map[string]any{
				"AttrA": map[string]any{"DataType": "String", "StringValue": "A"},
				"AttrB": map[string]any{"DataType": "String", "StringValue": "B"},
			},
		},
		{
			name:             "all_returns_full_set_digest",
			messageAttrNames: []string{"All"},
			oracleAttrs:      allAttrs,
		},
		{
			name:             "no_attributes_requested_yields_empty_md5",
			messageAttrNames: nil,
			wantEmptyMD5:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "md5-subset-queue")

			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":          queueURL,
				"MessageBody":       "hello",
				"MessageAttributes": allAttrs,
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":              queueURL,
				"MaxNumberOfMessages":   1,
				"MessageAttributeNames": tt.messageAttrNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Messages []struct {
					MessageAttributes      map[string]map[string]any `json:"MessageAttributes"`
					MD5OfMessageAttributes string                    `json:"MD5OfMessageAttributes"`
				} `json:"Messages"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Messages, 1)

			if tt.wantEmptyMD5 {
				assert.Empty(t, resp.Messages[0].MD5OfMessageAttributes)
				assert.Empty(t, resp.Messages[0].MessageAttributes)

				return
			}

			// Oracle: a fresh send carrying only the returned subset must
			// produce the identical digest the receive reports.
			want := sendForMD5(t, h, queueURL, tt.oracleAttrs)
			require.NotEmpty(t, want)
			assert.Equal(t, want, resp.Messages[0].MD5OfMessageAttributes)
			assert.Len(t, resp.Messages[0].MessageAttributes, len(tt.oracleAttrs))
		})
	}
}

func TestHandlerActions_DeleteMessage(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "my-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": 1,
		})

		var recvResp struct {
			Messages []struct {
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
		require.Len(t, recvResp.Messages, 1)

		receipt := recvResp.Messages[0].ReceiptHandle

		rec := doRequest(t, h, "DeleteMessage", map[string]any{
			"QueueUrl":      queueURL,
			"ReceiptHandle": receipt,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "del-msg-queue")

		rec := doRequest(t, h, "DeleteMessage", map[string]any{
			"QueueUrl":      queueURL,
			"ReceiptHandle": "invalid-receipt",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandlerActions_SendMessageBatch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "batch-queue")

		rec := doRequest(t, h, "SendMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries": []map[string]any{
				{"Id": "msg1", "MessageBody": "hello1"},
				{"Id": "msg2", "MessageBody": "hello2"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Successful []struct {
				ID string `json:"Id"`
			} `json:"Successful"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Successful, 2)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, "SendMessageBatch", map[string]any{
			"QueueUrl": "http://localhost/000000000000/noqueue",
			"Entries":  []map[string]any{{"Id": "msg1", "MessageBody": "hello"}},
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp jsonErr
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", errResp.Type)
	})

	t.Run("empty entries", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "empty-batch-queue")

		rec := doRequest(t, h, "SendMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries":  []map[string]any{},
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp jsonErr
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "com.amazonaws.sqs#EmptyBatchRequest", errResp.Type)
	})

	t.Run("too many entries", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "toomany-batch-queue")

		entries := make([]map[string]any, 10)
		for i := range 10 {
			entries[i] = map[string]any{
				"Id":          fmt.Sprintf("msg%d", i+1),
				"MessageBody": "body",
			}
		}

		rec := doRequest(t, h, "SendMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries":  entries,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Successful []struct {
				ID string `json:"Id"`
			} `json:"Successful"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Successful, 10)
	})
}

func TestHandlerActions_DeleteMessageBatch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "del-batch-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})

		var recvResp struct {
			Messages []struct {
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
		require.Len(t, recvResp.Messages, 1)

		receipt := recvResp.Messages[0].ReceiptHandle

		rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": receipt}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Successful []struct {
				ID string `json:"Id"`
			} `json:"Successful"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Successful, 1)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
			"QueueUrl": "http://localhost/000000000000/noqueue",
			"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "some-receipt"}},
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp jsonErr
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", errResp.Type)
	})

	t.Run("failed entry", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "del-fail-queue")

		rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "invalid-receipt"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Failed []struct {
				ID string `json:"Id"`
			} `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("empty entries", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "empty-del-batch-queue")

		rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
			"QueueUrl": queueURL,
			"Entries":  []map[string]any{},
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp jsonErr
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "com.amazonaws.sqs#EmptyBatchRequest", errResp.Type)
	})
}
