package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_AssociateKmsKey_Idempotent(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	const kmsKey = "arn:aws:kms:us-east-1:123:key/mykey"

	// Associate once.
	rec := doLogsRequest(t, h, e, "AssociateKmsKey",
		`{"logGroupName":"/my/group","kmsKeyId":"`+kmsKey+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Associate again (idempotent update).
	rec = doLogsRequest(t, h, e, "AssociateKmsKey",
		`{"logGroupName":"/my/group","kmsKeyId":"`+kmsKey+`"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_CreateLogGroup_EmptyName(t *testing.T) {
	t.Parallel()

	rec := makeLogsRequest(t, "CreateLogGroup", `{"logGroupName":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_AssociateKmsKey_RequiresIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "neither_log_group_nor_resource_id",
			body:     `{"kmsKeyId":"arn:aws:kms:us-east-1:123:key/k"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_kms_key_id",
			body:     `{"logGroupName":"/my/group","kmsKeyId":""}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "log_group_only_ok",
			body:     `{"logGroupName":"/my/group","kmsKeyId":"arn:aws:kms:us-east-1:123:key/k"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "resource_identifier_only_ok",
			body: `{"resourceIdentifier":"arn:aws:logs:us-east-1:123:query-definition:def",` +
				`"kmsKeyId":"arn:aws:kms:us-east-1:123:key/k"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, "AssociateKmsKey", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_LogGroupDeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutLogGroupDeletionProtection/Enable",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier":        "/aws/lambda/fn",
				"deletionProtectionEnabled": true,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutLogGroupDeletionProtection/Disable",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier":        "/aws/lambda/fn",
				"deletionProtectionEnabled": false,
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutLogGroupDeletionProtection/EmptyIdentifier",
			action: "PutLogGroupDeletionProtection",
			body: map[string]any{
				"logGroupIdentifier":        "",
				"deletionProtectionEnabled": true,
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_PutLogGroupDeletionProtection_WireName pins the request member
// name to the botocore model (logs/2014-03-28): deletionProtectionEnabled,
// not deletionProtected. A client sending the real wire name must actually
// flip backend state, not just get a 200.
func TestHandler_PutLogGroupDeletionProtection_WireName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "enable", enabled: true},
		{name: "disable", enabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			handler := cloudwatchlogs.NewHandler(backend)

			const lg = "/aws/lambda/wire-name-fn"
			require.NoError(t, backend.SetLogGroupDeletionProtection(lg, !tt.enabled))

			body, err := json.Marshal(map[string]any{
				"logGroupIdentifier":        lg,
				"deletionProtectionEnabled": tt.enabled,
			})
			require.NoError(t, err)

			rec := doLogsRequest(t, handler, e, "PutLogGroupDeletionProtection", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.enabled, backend.IsLogGroupDeletionProtected(lg))
		})
	}
}

func TestHandler_DescribeLogGroupsPagination(t *testing.T) {
	t.Parallel()

	e := echo.New()

	h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

	for i := range 5 {
		doLogsRequest(t, h, e, "CreateLogGroup",
			`{"logGroupName":"/group/`+string(rune('a'+i))+`"}`)
	}

	rec := doLogsRequest(t, h, e, "DescribeLogGroups", `{"limit":2}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutRetentionPolicy_ZeroDays_ReturnsInvalidParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		retentionInDays int
	}{
		{name: "zero_days_rejected", retentionInDays: 0},
		{name: "negative_days_rejected", retentionInDays: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)

			body, _ := json.Marshal(map[string]any{
				"logGroupName":    "ret-grp",
				"retentionInDays": tt.retentionInDays,
			})
			rec := doLogsRequest(t, h, e, "PutRetentionPolicy", string(body))

			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"retentionInDays=%d must return 400 InvalidParameterException", tt.retentionInDays)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidParameterException", errResp.Type,
				"retentionInDays=%d must return InvalidParameterException", tt.retentionInDays)
		})
	}
}

func TestHandler_LogGroupOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name:     "CreateLogGroup",
			action:   "CreateLogGroup",
			body:     map[string]any{"logGroupName": "/my/group"},
			wantCode: http.StatusOK,
		},
		{
			name: "CreateLogGroup/AlreadyExists",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"dup"}`)
			},
			action:   "CreateLogGroup",
			body:     map[string]any{"logGroupName": "dup"},
			wantCode: http.StatusConflict,
		},
		{
			name: "DeleteLogGroup",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"to-delete"}`)
			},
			action:   "DeleteLogGroup",
			body:     map[string]any{"logGroupName": "to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteLogGroup/NotFound",
			action:   "DeleteLogGroup",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "DescribeLogGroups",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)
			},
			action:        "DescribeLogGroups",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "logGroups",
			wantListLen:   2,
		},
		{
			name: "DescribeLogGroups/WithPrefix",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/prod/app"}`)
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"/dev/app"}`)
			},
			action:        "DescribeLogGroups",
			body:          map[string]any{"logGroupNamePrefix": "/prod"},
			wantCode:      http.StatusOK,
			wantListField: "logGroups",
			wantListLen:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}

func TestHandler_RetentionPolicyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name: "PutRetentionPolicy",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)
			},
			action:   "PutRetentionPolicy",
			body:     map[string]any{"logGroupName": "ret-grp", "retentionInDays": 30},
			wantCode: http.StatusOK,
		},
		{
			name: "DeleteRetentionPolicy",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)
				doLogsRequest(t, h, e, "PutRetentionPolicy",
					`{"logGroupName":"ret-grp","retentionInDays":30}`)
			},
			action:   "DeleteRetentionPolicy",
			body:     map[string]any{"logGroupName": "ret-grp"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}

func TestHandler_AssociateKmsKeyOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// AssociateKmsKey
		{
			name:   "AssociateKmsKey/LogGroup",
			action: "AssociateKmsKey",
			body: map[string]any{
				"logGroupName": "/my/group",
				"kmsKeyId":     "arn:aws:kms:us-east-1:123:key/abc",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateKmsKey/ResourceIdentifier",
			action: "AssociateKmsKey",
			body: map[string]any{
				"resourceIdentifier": "arn:aws:logs:us-east-1:123:query-result:*",
				"kmsKeyId":           "arn:aws:kms:us-east-1:123:key/abc",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "AssociateKmsKey/MissingKmsKeyId",
			action:   "AssociateKmsKey",
			body:     map[string]any{"logGroupName": "/my/group"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}

func TestHandler_AggregateLogGroupSummariesEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			name:          "ListAggregateLogGroupSummaries/ReturnsEmpty",
			action:        "ListAggregateLogGroupSummaries",
			body:          map[string]any{},
			wantCode:      http.StatusOK,
			wantListField: "logGroupSummaries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}
