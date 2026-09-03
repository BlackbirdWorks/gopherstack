package acmpca_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real ACM PCA
// operation, extracted from acmpca@v1.50.0 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("ACMPrivateCA.<Op>")
// and always POSTs to "/" -- ACM PCA is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. The target prefix is
// "ACMPrivateCA.", distinct from plain ACM's "CertificateManager." even
// though both are certificate services -- checked against acmpca's own
// pinned SDK rather than assumed shared with acm, per the task's caution
// about the two APIs' overlapping op names.
//
// ExtractOperation (TrimPrefix on "ACMPrivateCA.") and Handler() (via
// pkgs/service.HandleTarget splitting on "." and taking parts[1], then
// dispatchJSON's three-deep chained switch: dispatchJSON ->
// dispatchCertAndTagOps -> dispatchPermissionAndAuditOps, each falling
// through its own default to the next) both resolve to the identical
// action string, so the class of bug this table catches is a case label
// that doesn't exactly match the real op name (typo, wrong case), not a
// route-template or splitting mismatch.
//
// This table covers all 23 real ACM PCA ops (acmpca@v1.50.0) -- confirmed
// by diffing both GetSupportedOperations() (a hand-written literal) and
// every `case "X":` label across all three chained switches against this
// exact list: zero mismatches in either direction, no dead or excluded
// keys. The two diffs are genuinely independent -- GetSupportedOperations
// is a separately maintained literal, not built by ranging over the
// dispatch chain.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("ACMPrivateCA.` and pulling the suffix
// after the dot.
func sdkRouteCases() []string {
	return []string{
		"CreateCertificateAuthority",
		"CreateCertificateAuthorityAuditReport",
		"CreatePermission",
		"DeleteCertificateAuthority",
		"DeletePermission",
		"DeletePolicy",
		"DescribeCertificateAuthority",
		"DescribeCertificateAuthorityAuditReport",
		"GetCertificate",
		"GetCertificateAuthorityCertificate",
		"GetCertificateAuthorityCsr",
		"GetPolicy",
		"ImportCertificateAuthorityCertificate",
		"IssueCertificate",
		"ListCertificateAuthorities",
		"ListPermissions",
		"ListTags",
		"PutPolicy",
		"RestoreCertificateAuthority",
		"RevokeCertificate",
		"TagCertificateAuthority",
		"UntagCertificateAuthority",
		"UpdateCertificateAuthority",
	}
}

// TestExtractOperation_SDKRouteTable drives every real ACM PCA operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss branch (the innermost switch's
// default case, returning errUnknownACMPCAAction, mapped by handleError to
// wire code "InvalidAction"). Grepped handler.go: "InvalidAction" is
// written in exactly that one place -- handleOpError's switch covers a
// disjoint set of sentinels (ResourceNotFoundException, InvalidArgsException,
// InvalidArnException, InvalidRequestException, InvalidPolicyException,
// MalformedCertificateException, MalformedCSRException, InvalidStateException,
// PermissionAlreadyExistsException, TooManyTagsException, InternalFailure)
// none of which reuse that code -- so asserting on the wire type is safe
// here.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			h := acmpca.NewHandler(acmpca.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "ACMPrivateCA."+op)
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
