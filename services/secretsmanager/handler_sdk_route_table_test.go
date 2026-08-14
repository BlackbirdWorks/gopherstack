package secretsmanager_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Secrets
// Manager operation, extracted from secretsmanager@v1.44.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("secretsmanager.<Op>")
// and always request.Request.Method = "POST" against path "/" --
// Secrets Manager is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one header. ExtractOperation and Handler() both derive
// the action the same way (split on "."), so the class of bug this table
// can catch is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case -- Secrets Manager is case-sensitive JSON-RPC),
// not a route-template mismatch.
//
// This table covers all 23 real Secrets Manager ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(), 23/23) as
// of secretsmanager@v1.44.4 -- confirmed by diffing the dispatch-table keys
// against this exact list, zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("secretsmanager.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchGetSecretValue", "secretsmanager.BatchGetSecretValue"},
		{"CancelRotateSecret", "secretsmanager.CancelRotateSecret"},
		{"CreateSecret", "secretsmanager.CreateSecret"},
		{"DeleteResourcePolicy", "secretsmanager.DeleteResourcePolicy"},
		{"DeleteSecret", "secretsmanager.DeleteSecret"},
		{"DescribeSecret", "secretsmanager.DescribeSecret"},
		{"GetRandomPassword", "secretsmanager.GetRandomPassword"},
		{"GetResourcePolicy", "secretsmanager.GetResourcePolicy"},
		{"GetSecretValue", "secretsmanager.GetSecretValue"},
		{"ListSecrets", "secretsmanager.ListSecrets"},
		{"ListSecretVersionIds", "secretsmanager.ListSecretVersionIds"},
		{"PutResourcePolicy", "secretsmanager.PutResourcePolicy"},
		{"PutSecretValue", "secretsmanager.PutSecretValue"},
		{"RemoveRegionsFromReplication", "secretsmanager.RemoveRegionsFromReplication"},
		{"ReplicateSecretToRegions", "secretsmanager.ReplicateSecretToRegions"},
		{"RestoreSecret", "secretsmanager.RestoreSecret"},
		{"RotateSecret", "secretsmanager.RotateSecret"},
		{"StopReplicationToReplica", "secretsmanager.StopReplicationToReplica"},
		{"TagResource", "secretsmanager.TagResource"},
		{"UntagResource", "secretsmanager.UntagResource"},
		{"UpdateSecret", "secretsmanager.UpdateSecret"},
		{"UpdateSecretVersionStage", "secretsmanager.UpdateSecretVersionStage"},
		{"ValidateResourcePolicy", "secretsmanager.ValidateResourcePolicy"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Secrets Manager
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel that a dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := secretsmanager.NewHandler(secretsmanager.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
