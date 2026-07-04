package timestreamquery_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateScheduledQueryRequiresNotificationAndErrorReport verifies
// that CreateScheduledQuery rejects requests missing NotificationConfiguration
// or ErrorReportConfiguration. Real AWS enforces both as required fields;
// the emulator previously accepted them as optional and stored empty values.
func TestParity_CreateScheduledQueryRequiresNotificationAndErrorReport(t *testing.T) {
	t.Parallel()

	fullBody := map[string]any{
		"Name":                           "parity-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123456789012:role/role",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	}

	tests := []struct {
		name     string
		omitKey  string
		wantCode int
	}{
		{
			name:     "missing_notification_configuration",
			omitKey:  "NotificationConfiguration",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_error_report_configuration",
			omitKey:  "ErrorReportConfiguration",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "all_required_fields_present",
			omitKey:  "",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := make(map[string]any, len(fullBody))
			maps.Copy(body, fullBody)

			if tt.omitKey != "" {
				delete(body, tt.omitKey)
			}

			rec := doRequest(t, h, "CreateScheduledQuery", body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateScheduledQuery status for case %q", tt.name)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.NotEmpty(t, errResp["__type"], "error response must include __type")
			}
		})
	}
}
