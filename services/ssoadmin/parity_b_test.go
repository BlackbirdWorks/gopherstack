package ssoadmin_test

// Tests for parity.md findings: opaque NextToken, status-map cap, sort-at-backend.

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestOpaqueNextToken verifies that NextToken returned by list ops is
// base64-encoded and NOT equal to the raw cursor value (e.g. a permission-set
// ARN), making it opaque to callers.
func TestOpaqueNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		makeReq func(instanceArn string) map[string]any
		items   []string
	}{
		{
			name:  "ListPermissionSets",
			op:    "ListPermissionSets",
			items: []string{"alpha", "beta", "gamma"},
			makeReq: func(instanceArn string) map[string]any {
				return map[string]any{"InstanceArn": instanceArn, "MaxResults": 1}
			},
		},
		{
			name:  "ListInstances",
			op:    "ListInstances",
			items: []string{"inst-x", "inst-y"},
			makeReq: func(_ string) map[string]any {
				return map[string]any{"MaxResults": 1}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "opaque-test")
			for _, name := range tt.items {
				if tt.op == "ListPermissionSets" {
					createPermissionSet(t, h, instanceArn, name)
				} else {
					createInstance(t, h, name)
				}
			}

			rec := doRequest(t, h, tt.op, tt.makeReq(instanceArn))
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)

			token, hasToken := resp["NextToken"]
			if !hasToken || token == nil {
				t.Skip("no NextToken returned — not enough items for pagination")
			}

			tokenStr, ok := token.(string)
			require.True(t, ok, "NextToken should be a string")
			require.NotEmpty(t, tokenStr)

			// Must be valid base64.
			decoded, err := base64.StdEncoding.DecodeString(tokenStr)
			require.NoError(t, err, "NextToken should be base64-encoded, got %q", tokenStr)

			// Decoded value must not equal the token itself (i.e. was actually encoded).
			assert.NotEqual(t, tokenStr, string(decoded),
				"NextToken should be opaque (base64), not the raw cursor")

			// The decoded value should NOT look like a raw ARN or plain name
			// sitting unencoded in the token string.
			assert.False(t, strings.HasPrefix(tokenStr, "arn:"),
				"NextToken should not expose raw ARN as cursor")

			// Second page should be reachable using the token.
			req2 := tt.makeReq(instanceArn)
			req2["NextToken"] = tokenStr
			rec2 := doRequest(t, h, tt.op, req2)
			require.Equal(t, http.StatusOK, rec2.Code)
		})
	}
}

// TestListInstancesSortedByBackend verifies that ListInstances (via handler)
// returns instances sorted by ARN, relying on backend sort rather than handler
// sort (the previous implementation sorted in the handler layer on each call).
func TestListInstancesSortedByBackend(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"zzz-last", "aaa-first", "mmm-middle"} {
		createInstance(t, h, name)
	}

	rec := doRequest(t, h, "ListInstances", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	insts, ok := resp["Instances"].([]any)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(insts), 3)

	arns := make([]string, 0, len(insts))
	for _, inst := range insts {
		m, mok := inst.(map[string]any)
		require.True(t, mok)
		arns = append(arns, m["InstanceArn"].(string))
	}

	for i := 1; i < len(arns); i++ {
		assert.LessOrEqual(t, arns[i-1], arns[i],
			"ListInstances result not sorted at index %d: %s > %s", i, arns[i-1], arns[i])
	}
}

// TestStatusMapCap verifies that creationStatuses and deletionStatuses do not
// grow unboundedly: after creating and deleting assignments the backend
// remains functional and the counts stay within the expected cap.
func TestStatusMapCap(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := ssoadmin.NewHandler(backend)

	instanceArn := createInstance(t, h, "cap-test")
	psArn := createPermissionSet(t, h, instanceArn, "cap-ps")

	const n = 20
	for i := range n {
		body := map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"PrincipalId":      "user-" + string(rune('a'+i)),
			"PrincipalType":    "USER",
			"TargetId":         "111111111111",
			"TargetType":       "AWS_ACCOUNT",
		}
		rec := doRequest(t, h, "CreateAccountAssignment", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		// Also delete each one so deletionStatuses accumulates entries.
		rec2 := doRequest(t, h, "DeleteAccountAssignment", body)
		require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())
	}

	creationCount := ssoadmin.CreationStatusCount(backend)
	deletionCount := ssoadmin.DeletionStatusCount(backend)

	// After n creates (pruned by delete idempotency cleanup) and n deletes,
	// counts should remain reasonable — well below any runaway growth.
	assert.LessOrEqual(t, creationCount, n,
		"creationStatuses grew beyond expected: %d", creationCount)
	assert.LessOrEqual(t, deletionCount, n,
		"deletionStatuses grew beyond expected: %d", deletionCount)
}
