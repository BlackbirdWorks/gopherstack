package sts_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// sdkRouteCases is the authoritative Action value for every real STS
// operation, extracted from sts@v1.45.4 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets body.Key("Action").String("<Op>")
// and always POSTs to "/" -- STS is AWS Query/XML (services/_PROTOCOLS.md), so
// unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one form field. ExtractOperation and Handler()
// both read the Action value from the request body (ExtractOperation via a
// hand-rolled parseFormValues, Handler()'s dispatch via r.FormValue), so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 11 real STS ops (sts@v1.45.4) -- confirmed by
// diffing both GetSupportedOperations() and the actual dispatch() switch's
// case list against this exact list: zero mismatches in either direction, no
// dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AssumeRole",
		"AssumeRoleWithSAML",
		"AssumeRoleWithWebIdentity",
		"AssumeRoot",
		"DecodeAuthorizationMessage",
		"GetAccessKeyInfo",
		"GetCallerIdentity",
		"GetDelegatedAccessToken",
		"GetFederationToken",
		"GetSessionToken",
		"GetWebIdentityToken",
	}
}

// TestExtractOperation_SDKRouteTable drives every real STS operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "InvalidAction" sentinel (ErrInvalidAction,
// handler.go's dispatch() default case) that a dispatch-table key mismatch
// would produce. ErrInvalidAction has exactly one production call site and
// "InvalidAction" is not reused by any other error path in this service
// (grepped) -- the only sibling that maps to the same code, ErrMissingAction,
// fires solely when the Action field is entirely absent, which never happens
// here since every case drives a real Action value -- so asserting on the
// wire code is safe here, unlike workmail/transfer, where the dispatch-miss
// sentinel shares its wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := sts.NewHandler(sts.NewInMemoryBackend())

			e := echo.New()
			body := "Action=" + op + "&Version=2011-06-15"
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
