package mediastore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CorsPolicy(t *testing.T) {
	t.Parallel()

	corsBody := map[string]any{
		"ContainerName": "cors-test",
		"CorsPolicy": []any{
			map[string]any{
				"AllowedOrigins": []any{"https://example.com"},
				"AllowedHeaders": []any{"*"},
			},
		},
	}

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
		deleted    bool
	}{
		{
			name:       "put cors policy",
			op:         "PutCorsPolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get cors policy",
			op:         "GetCorsPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete cors policy",
			op:         "DeleteCorsPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get cors policy after delete returns not found",
			op:         "GetCorsPolicy",
			withPolicy: true,
			deleted:    true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "cors-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutCorsPolicy", corsBody)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tt.deleted {
				delRec := doRequest(t, h, "DeleteCorsPolicy", map[string]any{"ContainerName": "cors-test"})
				require.Equal(t, http.StatusOK, delRec.Code)
			}

			var body map[string]any
			if tt.op == "PutCorsPolicy" {
				body = corsBody
			} else {
				body = map[string]any{"ContainerName": "cors-test"}
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_PutCorsPolicy_Validation verifies CORS rule validation. Moved
// (and de-prefixed) from the former parity_audit1_test.go's
// TestParity_PutCorsPolicy_Validation.
func TestHandler_PutCorsPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		corsPolicy any
		name       string
		wantStatus int
	}{
		{
			name: "valid_rule",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{"https://example.com"},
					"AllowedHeaders": []any{"Authorization"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_allowed_origins",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{},
					"AllowedHeaders": []any{"*"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_allowed_headers",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{"https://example.com"},
					"AllowedHeaders": []any{},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// CorsPolicy list caps at 100 rules (MediaStore API model
			// CorsPolicy shape `max` trait); the real SDK's client-side
			// validator never checks the rule count, so it must be enforced
			// server-side.
			name:       "too_many_rules_101",
			corsPolicy: makeCorsRules(101),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exactly_100_rules_allowed",
			corsPolicy: makeCorsRules(100),
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestContainer(t, h, "cors-validation")

			rec := doRequest(t, h, "PutCorsPolicy", map[string]any{
				"ContainerName": "cors-validation",
				"CorsPolicy":    tt.corsPolicy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// makeCorsRules builds n structurally-valid CORS rules for rule-count-limit
// test cases.
func makeCorsRules(n int) []any {
	rules := make([]any, n)
	for i := range rules {
		rules[i] = map[string]any{
			"AllowedOrigins": []any{"https://example.com"},
			"AllowedHeaders": []any{"*"},
		}
	}

	return rules
}
