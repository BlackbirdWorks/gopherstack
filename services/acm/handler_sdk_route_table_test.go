package acm_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real ACM
// operation, extracted from acm@v1.43.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CertificateManager.<Op>")
// and always POSTs to "/" -- ACM is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. Note the target has NO version
// suffix ("CertificateManager.", not "CertificateManager_YYYYMMDD.") --
// like textract, route53resolver and swf, a version-stamped target is a
// convention, not a rule.
//
// ExtractOperation (TrimPrefix on "CertificateManager.") and Handler() (via
// pkgs/service.HandleTarget, which independently splits the target on "."
// and takes parts[1]) both resolve to the identical action string for every
// case here since no ACM op name itself contains a dot, and HandleTarget
// dispatches through acmDispatchTable, a flat map. So the class of bug this
// table catches is a dispatch-table key that doesn't exactly match the real
// op name (typo, wrong case), not a route-template or splitting mismatch.
//
// This table covers all 39 real ACM ops (acm@v1.43.4) -- confirmed by
// diffing both GetSupportedOperations() (a hand-written literal) and
// acmDispatchTable's keys against this exact list: zero mismatches in
// either direction, no dead or excluded keys. The two diffs are genuinely
// independent -- GetSupportedOperations is a separately maintained literal,
// not built by ranging over acmDispatchTable.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CertificateManager.` and pulling the
// suffix after the dot.
func sdkRouteCases() []string {
	return []string{
		"AddTagsToCertificate",
		"CreateAcmeDomainValidation",
		"CreateAcmeEndpoint",
		"CreateAcmeExternalAccountBinding",
		"DeleteAcmeDomainValidation",
		"DeleteAcmeEndpoint",
		"DeleteAcmeExternalAccountBinding",
		"DeleteCertificate",
		"DescribeAcmeAccount",
		"DescribeAcmeDomainValidation",
		"DescribeAcmeEndpoint",
		"DescribeAcmeExternalAccountBinding",
		"DescribeCertificate",
		"ExportCertificate",
		"GetAccountConfiguration",
		"GetAcmeExternalAccountBindingCredentials",
		"GetCertificate",
		"ImportCertificate",
		"ListAcmeAccounts",
		"ListAcmeDomainValidations",
		"ListAcmeEndpoints",
		"ListAcmeExternalAccountBindings",
		"ListCertificates",
		"ListTagsForCertificate",
		"ListTagsForResource",
		"PutAccountConfiguration",
		"RemoveTagsFromCertificate",
		"RenewCertificate",
		"RequestCertificate",
		"ResendValidationEmail",
		"RevokeAcmeAccount",
		"RevokeAcmeExternalAccountBinding",
		"RevokeCertificate",
		"SearchCertificates",
		"TagResource",
		"UntagResource",
		"UpdateAcmeDomainValidation",
		"UpdateAcmeEndpoint",
		"UpdateCertificateOptions",
	}
}

// TestExtractOperation_SDKRouteTable drives every real ACM operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss branch (handler.go's
// dispatchJSON returning errUnknownACMAction, whose sole production call
// site maps it to wire code "InvalidAction" in handleError). Grepped
// handler.go: "InvalidAction" is written in exactly that one place, not
// shared with any entry in acmErrorCodeTable, so asserting on the wire
// type is safe here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := acm.NewHandler(acm.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "CertificateManager."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
