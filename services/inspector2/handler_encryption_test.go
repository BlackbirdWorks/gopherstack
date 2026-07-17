package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptionKeyLifecycle(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Get returns default key",
			steps: []step{
				{
					name:   "get default",
					method: http.MethodGet,
					path:   "/encryptionkey/get?resourceType=EC2&scanType=NETWORK",
					body:   nil,
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "AWS_OWNED_KEY", resp["kmsKeyId"])
					},
				},
			},
		},
		{
			name: "Update/Reset cycle",
			steps: []step{
				{
					name:   "update key",
					method: http.MethodPut,
					path:   "/encryptionkey/update",
					body: map[string]any{
						"kmsKeyId":     "arn:aws:kms:us-east-1:123456789012:key/abc",
						"resourceType": "EC2",
						"scanType":     "NETWORK",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "reset key",
					method: http.MethodPut,
					path:   "/encryptionkey/reset",
					body: map[string]any{
						"resourceType": "EC2",
						"scanType":     "NETWORK",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}
