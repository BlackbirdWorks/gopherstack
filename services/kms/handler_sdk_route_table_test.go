package kms_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real KMS
// operation, extracted from kms@v1.55.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("TrentService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- KMS is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same
// way (split on "."), so the class of bug this table can catch is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- KMS is case-sensitive JSON-RPC), not a route-template
// mismatch. "TrentService" is KMS's real, historical internal service
// name -- not a gopherstack placeholder -- confirmed directly in the
// pinned serializer.
//
// This table covers all 54 real KMS ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 54/54) as of kms@v1.55.4 --
// confirmed by diffing the dispatch-table keys against this exact list,
// zero mismatches either direction.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("TrentService.` and pulling the suffix
// after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CancelKeyDeletion", "TrentService.CancelKeyDeletion"},
		{"ConnectCustomKeyStore", "TrentService.ConnectCustomKeyStore"},
		{"CreateAlias", "TrentService.CreateAlias"},
		{"CreateCustomKeyStore", "TrentService.CreateCustomKeyStore"},
		{"CreateGrant", "TrentService.CreateGrant"},
		{"CreateKey", "TrentService.CreateKey"},
		{"Decrypt", "TrentService.Decrypt"},
		{"DeleteAlias", "TrentService.DeleteAlias"},
		{"DeleteCustomKeyStore", "TrentService.DeleteCustomKeyStore"},
		{"DeleteImportedKeyMaterial", "TrentService.DeleteImportedKeyMaterial"},
		{"DeriveSharedSecret", "TrentService.DeriveSharedSecret"},
		{"DescribeCustomKeyStores", "TrentService.DescribeCustomKeyStores"},
		{"DescribeKey", "TrentService.DescribeKey"},
		{"DisableKey", "TrentService.DisableKey"},
		{"DisableKeyRotation", "TrentService.DisableKeyRotation"},
		{"DisconnectCustomKeyStore", "TrentService.DisconnectCustomKeyStore"},
		{"EnableKey", "TrentService.EnableKey"},
		{"EnableKeyRotation", "TrentService.EnableKeyRotation"},
		{"Encrypt", "TrentService.Encrypt"},
		{"GenerateDataKey", "TrentService.GenerateDataKey"},
		{"GenerateDataKeyPair", "TrentService.GenerateDataKeyPair"},
		{"GenerateDataKeyPairWithoutPlaintext", "TrentService.GenerateDataKeyPairWithoutPlaintext"},
		{"GenerateDataKeyWithoutPlaintext", "TrentService.GenerateDataKeyWithoutPlaintext"},
		{"GenerateMac", "TrentService.GenerateMac"},
		{"GenerateRandom", "TrentService.GenerateRandom"},
		{"GetKeyLastUsage", "TrentService.GetKeyLastUsage"},
		{"GetKeyPolicy", "TrentService.GetKeyPolicy"},
		{"GetKeyRotationStatus", "TrentService.GetKeyRotationStatus"},
		{"GetParametersForImport", "TrentService.GetParametersForImport"},
		{"GetPublicKey", "TrentService.GetPublicKey"},
		{"ImportKeyMaterial", "TrentService.ImportKeyMaterial"},
		{"ListAliases", "TrentService.ListAliases"},
		{"ListGrants", "TrentService.ListGrants"},
		{"ListKeyPolicies", "TrentService.ListKeyPolicies"},
		{"ListKeyRotations", "TrentService.ListKeyRotations"},
		{"ListKeys", "TrentService.ListKeys"},
		{"ListResourceTags", "TrentService.ListResourceTags"},
		{"ListRetirableGrants", "TrentService.ListRetirableGrants"},
		{"PutKeyPolicy", "TrentService.PutKeyPolicy"},
		{"ReEncrypt", "TrentService.ReEncrypt"},
		{"ReplicateKey", "TrentService.ReplicateKey"},
		{"RetireGrant", "TrentService.RetireGrant"},
		{"RevokeGrant", "TrentService.RevokeGrant"},
		{"RotateKeyOnDemand", "TrentService.RotateKeyOnDemand"},
		{"ScheduleKeyDeletion", "TrentService.ScheduleKeyDeletion"},
		{"Sign", "TrentService.Sign"},
		{"TagResource", "TrentService.TagResource"},
		{"UntagResource", "TrentService.UntagResource"},
		{"UpdateAlias", "TrentService.UpdateAlias"},
		{"UpdateCustomKeyStore", "TrentService.UpdateCustomKeyStore"},
		{"UpdateKeyDescription", "TrentService.UpdateKeyDescription"},
		{"UpdatePrimaryRegion", "TrentService.UpdatePrimaryRegion"},
		{"Verify", "TrentService.Verify"},
		{"VerifyMac", "TrentService.VerifyMac"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real KMS operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := kms.NewHandler(kms.NewInMemoryBackend())
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
