package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodeBuild_ResourcePolicy covers Put, Get, Delete.
func TestCodeBuild_ResourcePolicy(t *testing.T) {
	t.Parallel()

	const resArn = "arn:aws:codebuild:us-east-1:000000000000:report-group/my-rg"
	const policy = `{"Version":"2012-10-17"}`

	t.Run("put_and_get", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		putRec := doRequest(t, h, "PutResourcePolicy", map[string]any{
			"resourceArn": resArn,
			"policy":      policy,
		})
		require.Equal(t, http.StatusOK, putRec.Code)

		var putOut struct {
			ResourceArn string `json:"resourceArn"`
		}
		require.NoError(t, json.NewDecoder(putRec.Body).Decode(&putOut))
		assert.Equal(t, resArn, putOut.ResourceArn)

		getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resArn})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			Policy string `json:"policy"`
		}
		require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
		assert.JSONEq(t, policy, getOut.Policy)
	})

	t.Run("get_missing_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "GetResourcePolicy", map[string]any{
			"resourceArn": "arn:aws:codebuild:us-east-1:000000000000:report-group/ghost",
		})
		// AWS returns ResourceNotFoundException when no policy is set.
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("delete_then_get_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "PutResourcePolicy", map[string]any{
			"resourceArn": resArn,
			"policy":      policy,
		})

		del1 := doRequest(t, h, "DeleteResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusOK, del1.Code)

		del2 := doRequest(t, h, "DeleteResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusOK, del2.Code)

		// Policy should now be gone — AWS returns ResourceNotFoundException.
		getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusBadRequest, getRec.Code)
	})

	t.Run("put_missing_fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "PutResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("get_missing_resource_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "GetResourcePolicy", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_ResourcePolicy_PutGetRoundTrip tests PutResourcePolicy, GetResourcePolicy round-trip.
func TestHandler_ResourcePolicy_PutGetRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		policy      string
		wantPolicy  string
		wantGet     int
	}{
		{
			name:        "put_and_get_policy",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/my-proj",
			policy:      `{"Version":"2012-10-17","Statement":[]}`,
			wantGet:     http.StatusOK,
			wantPolicy:  `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			putRec := doRequest(t, h, "PutResourcePolicy", map[string]any{
				"resourceArn": tt.resourceArn,
				"policy":      tt.policy,
			})
			require.Equal(t, http.StatusOK, putRec.Code)

			getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": tt.resourceArn})
			assert.Equal(t, tt.wantGet, getRec.Code)

			if tt.wantGet == http.StatusOK {
				var out struct {
					Policy string `json:"policy"`
				}
				require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
				assert.JSONEq(t, tt.wantPolicy, out.Policy)
			}
		})
	}
}
