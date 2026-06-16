package macie2_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestMacie2_Accuracy_FrequencyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		freq      string
		wantError string
		wantCode  int
	}{
		{
			name:     "EnableMacie with FIFTEEN_MINUTES succeeds",
			freq:     "FIFTEEN_MINUTES",
			wantCode: http.StatusOK,
		},
		{
			name:     "EnableMacie with ONE_HOUR succeeds",
			freq:     "ONE_HOUR",
			wantCode: http.StatusOK,
		},
		{
			name:     "EnableMacie with SIX_HOURS succeeds",
			freq:     "SIX_HOURS",
			wantCode: http.StatusOK,
		},
		{
			name:      "EnableMacie with invalid frequency returns 400",
			freq:      "HOURLY",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:      "EnableMacie with lowercase frequency returns 400",
			freq:      "one_hour",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/macie",
				map[string]string{"findingPublishingFrequency": tt.freq})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestMacie2_Accuracy_UpdateSessionNotEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		freq      string
		wantError string
		wantCode  int
		enabled   bool
	}{
		{
			name:      "UpdateMacieSession when not enabled returns 403",
			enabled:   false,
			freq:      "ONE_HOUR",
			wantCode:  http.StatusForbidden,
			wantError: "AccessDeniedException",
		},
		{
			name:     "UpdateMacieSession when enabled succeeds",
			enabled:  true,
			freq:     "ONE_HOUR",
			wantCode: http.StatusOK,
		},
		{
			name:      "UpdateMacieSession with invalid frequency returns 400",
			enabled:   true,
			freq:      "DAILY",
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.enabled {
				doRequest(t, h, http.MethodPost, "/macie", nil)
			}

			rec := doRequest(t, h, http.MethodPatch, "/macie",
				map[string]string{"findingPublishingFrequency": tt.freq})

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestMacie2_Accuracy_CustomDataIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		setup     func(h *macie2.Handler) string
		pathFn    func(id string) string
		name      string
		method    string
		wantError string
		wantCode  int
	}{
		{
			name:   "CreateCustomDataIdentifier with invalid regex returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":  "bad-regex",
				"regex": `[invalid`,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance > 300 returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "too-far",
				"regex":                `\d+`,
				"maximumMatchDistance": 301,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance 0 returns 400",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "zero-dist",
				"regex":                `\d+`,
				"maximumMatchDistance": 0,
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name:   "CreateCustomDataIdentifier with maxMatchDistance 300 succeeds",
			method: http.MethodPost,
			pathFn: func(_ string) string { return "/custom-data-identifiers" },
			body: map[string]any{
				"name":                 "max-dist",
				"regex":                `\d+`,
				"maximumMatchDistance": 300,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "GetCustomDataIdentifier on deleted CDI returns 404",
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "to-delete",
					"regex": `\d+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id := resp["customDataIdentifierId"]

				doRequest(t, h, http.MethodDelete, "/custom-data-identifiers/"+id, nil)

				return id
			},
			method:    http.MethodGet,
			pathFn:    func(id string) string { return "/custom-data-identifiers/" + id },
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			id := ""
			if tt.setup != nil {
				id = tt.setup(h)
			}

			path := tt.pathFn(id)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestMacie2_Accuracy_TagOperations(t *testing.T) {
	t.Parallel()

	const unknownARN = "arn:aws:macie2:us-east-1:000000000000:allow-list/nonexistent-id"

	tests := []struct {
		body      any
		setup     func(h *macie2.Handler) string
		pathFn    func(arn string) string
		name      string
		method    string
		wantError string
		wantCode  int
	}{
		{
			name:      "TagResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodPost,
			body:      map[string]any{"tags": map[string]string{"k": "v"}},
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "UntagResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodDelete,
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "ListTagsForResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodGet,
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name: "TagResource with key exceeding 128 chars returns 400",
			setup: func(h *macie2.Handler) string {
				return createTestAllowListARN(t, h)
			},
			method: http.MethodPost,
			pathFn: func(arn string) string { return "/tags/" + arn },
			body: map[string]any{
				"tags": map[string]string{
					strings.Repeat("k", 129): "val",
				},
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name: "TagResource with value exceeding 256 chars returns 400",
			setup: func(h *macie2.Handler) string {
				return createTestAllowListARN(t, h)
			},
			method: http.MethodPost,
			pathFn: func(arn string) string { return "/tags/" + arn },
			body: map[string]any{
				"tags": map[string]string{
					"key": strings.Repeat("v", 257),
				},
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name: "TagResource on deleted custom-data-identifier returns 404",
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "temp-cdi",
					"regex": `\d+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id := resp["customDataIdentifierId"]
				doRequest(t, h, http.MethodDelete, "/custom-data-identifiers/"+id, nil)

				return "arn:aws:macie2:us-east-1:000000000000:custom-data-identifier/" + id
			},
			method:    http.MethodPost,
			pathFn:    func(arn string) string { return "/tags/" + arn },
			body:      map[string]any{"tags": map[string]string{"k": "v"}},
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := ""
			if tt.setup != nil {
				arn = tt.setup(h)
			}

			path := tt.pathFn(arn)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}
