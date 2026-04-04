package appsync_test

import (
	"testing"

	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// appsync client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := appsync.NewInMemoryBackend("000000000000", "us-east-1", "")
	h := appsync.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &appsyncsdk.Client{}, h.GetSupportedOperations(), []string{
		"DeleteApi",
		"DeleteApiCache",
		"DeleteApiKey",
		"DeleteChannelNamespace",
		"DeleteDomainName",
		"DeleteFunction",
		"DeleteType",
		"DisassociateApi",
		"DisassociateMergedGraphqlApi",
		"DisassociateSourceGraphqlApi",
		"EvaluateCode",
		"EvaluateMappingTemplate",
		"FlushApiCache",
		"GetApi",
		"GetApiAssociation",
		"GetApiCache",
		"GetChannelNamespace",
		"GetDataSourceIntrospection",
		"GetDomainName",
		"GetFunction",
		"GetGraphqlApiEnvironmentVariables",
		"GetSourceApiAssociation",
		"GetType",
		"ListApiKeys",
		"ListApis",
		"ListChannelNamespaces",
		"ListDomainNames",
		"ListFunctions",
		"ListResolversByFunction",
		"ListSourceApiAssociations",
		"ListTagsForResource",
		"ListTypes",
		"ListTypesByAssociation",
		"PutGraphqlApiEnvironmentVariables",
		"StartDataSourceIntrospection",
		"StartSchemaMerge",
		"TagResource",
		"UntagResource",
		"UpdateApi",
		"UpdateApiCache",
		"UpdateApiKey",
		"UpdateChannelNamespace",
		"UpdateDataSource",
		"UpdateDomainName",
		"UpdateFunction",
		"UpdateGraphqlApi",
		"UpdateResolver",
		"UpdateSourceApiAssociation",
		"UpdateType",
	})
}
