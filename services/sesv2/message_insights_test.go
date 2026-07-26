package sesv2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestBatchGetMetricData tests the BatchGetMetricData operation.
func TestBatchGetMetricData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queries   []sesv2.MetricDataQuery
		wantCount int
		wantErr   bool
	}{
		{
			name:      "single_query",
			queries:   []sesv2.MetricDataQuery{{ID: "q1", Namespace: "VDM", Metric: "SEND"}},
			wantCount: 1,
		},
		{
			name:      "multiple_queries",
			queries:   []sesv2.MetricDataQuery{{ID: "q1"}, {ID: "q2"}, {ID: "q3"}},
			wantCount: 3,
		},
		{
			name:      "empty_queries",
			queries:   []sesv2.MetricDataQuery{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			results, err := backend.BatchGetMetricData(tt.queries)

			require.NoError(t, err)
			assert.Len(t, results, tt.wantCount)

			for i, r := range results {
				assert.Equal(t, tt.queries[i].ID, r.ID)
				assert.NotEmpty(t, r.Timestamps, "BatchGetMetricData should return synthetic data")
				assert.NotEmpty(t, r.Values, "BatchGetMetricData should return synthetic data")
			}
		})
	}
}

// TestBatchGetMetricDataHTTP tests BatchGetMetricData via HTTP.
func TestBatchGetMetricDataHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{
		"Queries": []map[string]any{
			{"Id": "q1", "Namespace": "VDM", "Metric": "SEND"},
		},
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/metrics/batch", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBatchGetMetricDataHTTPBadJSON tests bad JSON for BatchGetMetricData.
func TestBatchGetMetricDataHTTPBadJSON(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v2/email/metrics/batch",
		bytes.NewReader([]byte("not-json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetMessageInsights tests the GetMessageInsights operation, served as
// GET /v2/email/insights/{MessageId}. gopherstack's message history isn't a
// real delivery pipeline, so unlike other reachable-but-synthetic
// deliverability ops, GetMessageInsights genuinely round-trips against a
// message that was actually sent through this backend.
func TestGetMessageInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	doRequest(t, h, http.MethodPost, "/v2/email/identities",
		map[string]any{"EmailIdentity": "sender@example.com"})

	sendRec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", map[string]any{
		"FromEmailAddress": "sender@example.com",
		"Destination":      map[string]any{"ToAddresses": []string{"rcpt@example.com"}},
		"Content": map[string]any{
			"Simple": map[string]any{
				"Subject": map[string]any{"Data": "Hello"},
				"Body":    map[string]any{"Text": map[string]any{"Data": "body"}},
			},
		},
	})
	require.Equal(t, http.StatusOK, sendRec.Code)

	var sendResp map[string]any
	require.NoError(t, json.Unmarshal(sendRec.Body.Bytes(), &sendResp))
	messageID, _ := sendResp["MessageId"].(string)
	require.NotEmpty(t, messageID)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/insights/"+messageID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, messageID, out["MessageId"])
	assert.Equal(t, "sender@example.com", out["FromEmailAddress"])

	insights, ok := out["Insights"].([]any)
	require.True(t, ok)
	require.Len(t, insights, 1)

	entry, ok := insights[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "rcpt@example.com", entry["Destination"])
}

// TestGetMessageInsights_NotFound verifies NotFoundException for an unknown MessageId.
func TestGetMessageInsights_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/insights/does-not-exist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestBatchGetMetricDataSendSDKRoundTrip verifies BatchGetMetricData derives
// a real SEND count from gopherstack's own send history (rather than always
// returning a zero-valued placeholder) for the Metric/dimension combination
// it has real backing data for, via the real aws-sdk-go-v2 client so the
// StartDate/EndDate epoch-seconds JSON-body encoding is exercised by the
// genuine SDK serializer/deserializer.
func TestBatchGetMetricDataSendSDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dimensions map[string]string
		name       string
		wantCount  int64
	}{
		{
			name:      "unfiltered_send_count",
			wantCount: 2,
		},
		{
			name:       "email_identity_dimension_filters",
			dimensions: map[string]string{"EMAIL_IDENTITY": "metrics@example.com"},
			wantCount:  2,
		},
		{
			name:       "unsupported_dimension_falls_back_to_zero",
			dimensions: map[string]string{"CONFIGURATION_SET": "cs1"},
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newSESv2SDKClient(t, h)

			_, err := client.CreateEmailIdentity(t.Context(), &sesv2sdk.CreateEmailIdentityInput{
				EmailIdentity: aws.String("metrics@example.com"),
			})
			require.NoError(t, err)

			for range 2 {
				_, sendErr := client.SendEmail(t.Context(), &sesv2sdk.SendEmailInput{
					FromEmailAddress: aws.String("metrics@example.com"),
					Destination:      &sesv2types.Destination{ToAddresses: []string{"rcpt@example.com"}},
					Content: &sesv2types.EmailContent{
						Simple: &sesv2types.Message{
							Subject: &sesv2types.Content{Data: aws.String("s")},
							Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
						},
					},
				})
				require.NoError(t, sendErr)
			}

			out, err := client.BatchGetMetricData(t.Context(), &sesv2sdk.BatchGetMetricDataInput{
				Queries: []sesv2types.BatchGetMetricDataQuery{
					{
						Id:         aws.String("q1"),
						Namespace:  sesv2types.MetricNamespaceVdm,
						Metric:     sesv2types.MetricSend,
						StartDate:  aws.Time(time.Now().Add(-time.Hour)),
						EndDate:    aws.Time(time.Now().Add(time.Hour)),
						Dimensions: tt.dimensions,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, out.Results, 1)

			result := out.Results[0]
			assert.Equal(t, "q1", aws.ToString(result.Id))
			require.NotEmpty(t, result.Values)

			var total int64
			for _, v := range result.Values {
				total += v
			}

			assert.Equal(t, tt.wantCount, total)
		})
	}
}

// TestBatchGetMetricDataSynthetic verifies BatchGetMetricData returns synthetic data
// (previously returned empty arrays).
func TestBatchGetMetricDataSynthetic(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	queries := []sesv2.MetricDataQuery{
		{ID: "q1", Namespace: "ses.send", Metric: "Send"},
		{ID: "q2", Namespace: "ses.send", Metric: "Bounce"},
	}

	results, err := backend.BatchGetMetricData(queries)
	require.NoError(t, err)
	require.Len(t, results, 2)

	for _, r := range results {
		assert.NotEmpty(t, r.Timestamps, "query %s: expected synthetic timestamps", r.ID)
		assert.NotEmpty(t, r.Values, "query %s: expected synthetic values", r.ID)
	}
}
