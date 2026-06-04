package inspector2_test

// Audit batch-2: AWS-accuracy gaps fixed.
//
// Gaps addressed:
//   1. TagResource: no key/value length or count validation (AWS: key 1-128, value 0-256, max 50)
//   2. CreateFilter: invalid action accepted (AWS: only NONE or SUPPRESS)
//   3. UpdateFilter: invalid action accepted (AWS: only NONE or SUPPRESS)
//   4. TagResource via CreateFilter tags: same tag key/value limits apply

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- tag validation on TagResource ---

func TestAudit2_TagResource_KeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     map[string]string{"env": "prod", "team": "security"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key at limit accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over limit rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at limit accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over limit rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			filterARN := auditCreateFilter(t, h, "tag-validation-"+tc.name)

			rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
				"tags": tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestAudit2_TagResource_CountLimit(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	filterARN := auditCreateFilter(t, h, "tag-count-filter")

	bulk := make(map[string]string, 50)
	for i := range 50 {
		bulk[strings.Repeat("k", 1)+string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}

	rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{"tags": bulk})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
		"tags": map[string]string{"overflow-key": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- filter action validation ---

func TestAudit2_CreateFilter_ActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "NONE accepted",
			action:   "NONE",
			wantCode: http.StatusOK,
		},
		{
			name:     "SUPPRESS accepted",
			action:   "SUPPRESS",
			wantCode: http.StatusOK,
		},
		{
			name:     "empty action accepted (defaults to NONE)",
			action:   "",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid action rejected",
			action:   "ALLOW",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "lowercase action rejected",
			action:   "none",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)

			body := map[string]any{"name": "filter-" + tc.name}
			if tc.action != "" {
				body["action"] = tc.action
			}

			rec := auditDo(t, h, http.MethodPost, "/filters/create", body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestAudit2_UpdateFilter_ActionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "SUPPRESS accepted",
			action:   "SUPPRESS",
			wantCode: http.StatusOK,
		},
		{
			name:     "NONE accepted",
			action:   "NONE",
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid action rejected",
			action:   "DENY",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)
			filterARN := auditCreateFilter(t, h, "update-action-"+tc.name)

			rec := auditDo(t, h, http.MethodPost, "/filters/update", map[string]any{
				"filterArn": filterARN,
				"action":    tc.action,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// --- tag validation on CreateFilter ---

func TestAudit2_CreateFilter_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags in create accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty tag key in create rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key over limit in create rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value over limit in create rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t)

			rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
				"name":   "create-tag-" + tc.name,
				"action": "NONE",
				"tags":   tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// --- error type accuracy ---

func TestAudit2_ErrorType_TagResource_InvalidKey(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	filterARN := auditCreateFilter(t, h, "err-type-filter")

	rec := auditDo(t, h, http.MethodPost, "/tags/"+filterARN, map[string]any{
		"tags": map[string]string{"": "value"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, inspector2UnmarshalError(t, rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

func TestAudit2_ErrorType_CreateFilter_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
		"name":   "bad-action-filter",
		"action": "INVALID",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, inspector2UnmarshalError(t, rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

func inspector2UnmarshalError(t *testing.T, data []byte, v any) error {
	t.Helper()

	return json.Unmarshal(data, v)
}
