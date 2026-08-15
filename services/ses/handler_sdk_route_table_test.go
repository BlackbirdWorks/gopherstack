package ses_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// sdkRouteCases is the authoritative Action value for every real SES
// (v1) operation, extracted from ses@v1.37.4 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- SES v1 is
// AWS Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// form field. ExtractOperation and Handler() both read r.Form.Get("Action")
// after r.ParseForm(), so the class of bug this table catches is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case), not a route-template mismatch.
//
// This table covers all 71 real SES v1 ops (ses@v1.37.4) -- confirmed by
// diffing both GetSupportedOperations() and the actual dispatch logic
// against this exact list: zero mismatches in either direction, no dead or
// excluded keys.
//
// The extraction trap (gopherstack-n1mb: dms's literal string keys,
// organizations's bare if) fired here in a third idiom: SES dispatches
// through an eight-deep chain of switch statements (dispatch ->
// dispatchExtended -> dispatchNewOps -> dispatchRefinedOps ->
// dispatchMissingOps -> {dispatchIdentityOps ->
// dispatchIdentitySetVerifyOps, dispatchSendMissingOps,
// dispatchConfigReceiptOps}), each falling through its own "default" to the
// next helper rather than one flat switch or map. Every case in every link
// of the chain was extracted (not just the first one reached) and totalled
// 71 -- matching the real op count exactly, so no case was dropped by
// stopping at an intermediate helper's default.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"CloneReceiptRuleSet",
		"CreateConfigurationSet",
		"CreateConfigurationSetEventDestination",
		"CreateConfigurationSetTrackingOptions",
		"CreateCustomVerificationEmailTemplate",
		"CreateReceiptFilter",
		"CreateReceiptRule",
		"CreateReceiptRuleSet",
		"CreateTemplate",
		"DeleteConfigurationSet",
		"DeleteConfigurationSetEventDestination",
		"DeleteConfigurationSetTrackingOptions",
		"DeleteCustomVerificationEmailTemplate",
		"DeleteIdentity",
		"DeleteIdentityPolicy",
		"DeleteReceiptFilter",
		"DeleteReceiptRule",
		"DeleteReceiptRuleSet",
		"DeleteTemplate",
		"DeleteVerifiedEmailAddress",
		"DescribeActiveReceiptRuleSet",
		"DescribeConfigurationSet",
		"DescribeReceiptRule",
		"DescribeReceiptRuleSet",
		"GetAccountSendingEnabled",
		"GetCustomVerificationEmailTemplate",
		"GetIdentityDkimAttributes",
		"GetIdentityMailFromDomainAttributes",
		"GetIdentityNotificationAttributes",
		"GetIdentityPolicies",
		"GetIdentityVerificationAttributes",
		"GetSendQuota",
		"GetSendStatistics",
		"GetTemplate",
		"ListConfigurationSets",
		"ListCustomVerificationEmailTemplates",
		"ListIdentities",
		"ListIdentityPolicies",
		"ListReceiptFilters",
		"ListReceiptRuleSets",
		"ListTemplates",
		"ListVerifiedEmailAddresses",
		"PutConfigurationSetDeliveryOptions",
		"PutIdentityPolicy",
		"ReorderReceiptRuleSet",
		"SendBounce",
		"SendBulkTemplatedEmail",
		"SendCustomVerificationEmail",
		"SendEmail",
		"SendRawEmail",
		"SendTemplatedEmail",
		"SetActiveReceiptRuleSet",
		"SetIdentityDkimEnabled",
		"SetIdentityFeedbackForwardingEnabled",
		"SetIdentityHeadersInNotificationsEnabled",
		"SetIdentityMailFromDomain",
		"SetIdentityNotificationTopic",
		"SetReceiptRulePosition",
		"TestRenderTemplate",
		"UpdateAccountSendingEnabled",
		"UpdateConfigurationSetEventDestination",
		"UpdateConfigurationSetReputationMetricsEnabled",
		"UpdateConfigurationSetSendingEnabled",
		"UpdateConfigurationSetTrackingOptions",
		"UpdateCustomVerificationEmailTemplate",
		"UpdateReceiptRule",
		"UpdateTemplate",
		"VerifyDomainDkim",
		"VerifyDomainIdentity",
		"VerifyEmailAddress",
		"VerifyEmailIdentity",
	}
}

// TestExtractOperation_SDKRouteTable drives every real SES operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "InvalidAction" sentinel (errUnknownSESAction)
// that a dispatch-table key mismatch would produce. errUnknownSESAction has
// three return sites in handler.go, but all three are the terminal "default"
// of the same dispatch chain described above -- functionally one dispatch-miss
// path, only reached after every real case in every link has failed to
// match. Handler() maps errUnknownSESAction to "InvalidAction" via a single
// explicit check ahead of the general error mapper (sesErrorCode), and
// sesErrorCode itself never produces "InvalidAction" for any other error
// (grepped), so asserting on the wire code is safe here, unlike
// workmail/transfer, where the dispatch-miss sentinel shares its wire type
// with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())

			e := echo.New()
			body := "Action=" + op + "&Version=2010-12-01"
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
