package elasticbeanstalk_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

// sdkRouteCases is the authoritative Action value for every real Elastic
// Beanstalk operation, extracted from elasticbeanstalk@v1.37.4 serializers.go:
// each op's awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- Elastic
// Beanstalk is AWS Query/XML (services/_PROTOCOLS.md), so unlike a
// REST-family service there is no path template to get wrong: dispatch is
// entirely by this one form field. ExtractOperation and Handler() both read
// r.Form.Get("Action") after r.ParseForm(), so the class of bug this table
// catches is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case), not a route-template mismatch.
//
// This table covers all 47 real Elastic Beanstalk ops (elasticbeanstalk@v1.37.4)
// -- confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() dispatch map's 47 keys against this exact list: zero mismatches
// in either direction, no dead or excluded keys.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AbortEnvironmentUpdate",
		"ApplyEnvironmentManagedAction",
		"AssociateEnvironmentOperationsRole",
		"CheckDNSAvailability",
		"ComposeEnvironments",
		"CreateApplication",
		"CreateApplicationVersion",
		"CreateConfigurationTemplate",
		"CreateEnvironment",
		"CreatePlatformVersion",
		"CreateStorageLocation",
		"DeleteApplication",
		"DeleteApplicationVersion",
		"DeleteConfigurationTemplate",
		"DeleteEnvironmentConfiguration",
		"DeletePlatformVersion",
		"DescribeAccountAttributes",
		"DescribeApplications",
		"DescribeApplicationVersions",
		"DescribeConfigurationOptions",
		"DescribeConfigurationSettings",
		"DescribeEnvironmentHealth",
		"DescribeEnvironmentManagedActionHistory",
		"DescribeEnvironmentManagedActions",
		"DescribeEnvironmentResources",
		"DescribeEnvironments",
		"DescribeEvents",
		"DescribeInstancesHealth",
		"DescribePlatformVersion",
		"DisassociateEnvironmentOperationsRole",
		"ListAvailableSolutionStacks",
		"ListPlatformBranches",
		"ListPlatformVersions",
		"ListTagsForResource",
		"RebuildEnvironment",
		"RequestEnvironmentInfo",
		"RestartAppServer",
		"RetrieveEnvironmentInfo",
		"SwapEnvironmentCNAMEs",
		"TerminateEnvironment",
		"UpdateApplication",
		"UpdateApplicationResourceLifecycle",
		"UpdateApplicationVersion",
		"UpdateConfigurationTemplate",
		"UpdateEnvironment",
		"UpdateTagsForResource",
		"ValidateConfigurationSettings",
	}
}

// TestExtractOperation_SDKRouteTable drives every real Elastic Beanstalk
// operation's authoritative Action value through ExtractOperation and
// Handler(), asserting the form field resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel (ErrUnknownAction, handler.go's dispatch() single production
// call site) that a dispatch-table key mismatch would produce.
// ErrUnknownAction is a distinct package-level sentinel from its siblings
// (ErrNotFound, ErrResourceNotFound, ErrAlreadyExists, ErrInvalidParameter,
// ErrValidation) -- each is its own awserr.New instance, so errors.Is only
// matches ErrUnknownAction to itself even though several siblings share the
// same underlying awserr.ErrInvalidParameter category -- and
// "UnknownOperationException" is not reused by any other entry in
// handleOpError's mapping table (grepped), so asserting on the wire code is
// safe here, unlike workmail/transfer, where the dispatch-miss sentinel
// shares its wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			h := elasticbeanstalk.NewHandler(b)

			e := echo.New()
			body := "Action=" + op + "&Version=2010-12-01"
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
