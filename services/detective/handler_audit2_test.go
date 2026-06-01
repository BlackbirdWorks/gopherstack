package detective_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func TestDetective_TagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags",
			tags:     map[string]string{"env": "prod", "team": "infra"},
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

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			require.Equal(t, http.StatusOK, createRec.Code)

			rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{"Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_TagResourceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *detective.Handler, graphArn string)
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tag unknown resource returns 404",
			tags:     map[string]string{"k": "v"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "tag key over limit returns 400",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value over limit returns 400",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	unknownARN := "arn:aws:detective:us-east-1:000000000000:graph:doesnotexist"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := unknownARN
			if tc.setup != nil {
				rec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
				tc.setup(h, arn)
			}

			rec := doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{"Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_TagCountLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, jsonUnmarshal(t, createRec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	bulk := make(map[string]string, 50)
	for i := range 50 {
		bulk[strings.Repeat("k", 1)+string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
	}
	rec := doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{"Tags": bulk})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, "/tags/"+graphArn, map[string]any{"Tags": map[string]string{"new-key": "v"}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDetective_CreateMembersValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accounts []any
		wantCode int
	}{
		{
			name: "valid accounts accepted",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "non-numeric account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "abc123456789", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "short account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "12345", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "long account ID rejected",
			accounts: []any{
				map[string]any{"AccountId": "1234567890123", "EmailAddress": "a@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid email rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "notanemail"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "email missing local part rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "@example.com"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "email missing domain rejected",
			accounts: []any{
				map[string]any{"AccountId": "111111111111", "EmailAddress": "user@"},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, jsonUnmarshal(t, createRec.Body.Bytes(), &createResp))
			graphArn := createResp["GraphArn"].(string)

			rec := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
				"GraphArn": graphArn,
				"Accounts": tc.accounts,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestDetective_CreateMembersBatchLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/graph", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, jsonUnmarshal(t, createRec.Body.Bytes(), &createResp))
	graphArn := createResp["GraphArn"].(string)

	accounts := make([]any, 51)
	for i := range 51 {
		accounts[i] = map[string]any{
			"AccountId":    fmt.Sprintf("%012d", i+1),
			"EmailAddress": "a@example.com",
		}
	}

	rec := doRequest(t, h, http.MethodPost, "/graph/members", map[string]any{
		"GraphArn": graphArn,
		"Accounts": accounts,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func jsonUnmarshal(t *testing.T, data []byte, v any) error {
	t.Helper()

	return json.Unmarshal(data, v)
}
