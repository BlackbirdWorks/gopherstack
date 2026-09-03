package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestListFunctions_SDKRoundTrip_StageFilter drives the real SDK client with Stage set and
// asserts the excluded stage's function is absent. Before the fix, handleListFunctions ignored
// the Stage query parameter (a real ListFunctionsInput member, cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpHttpBindingsListFunctionsInput binds it to the query
// string) and always returned every function regardless of stage.
func TestListFunctions_SDKRoundTrip_StageFilter(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	_, err := backend.CreateFunction(
		"dev-fn",
		"dev",
		"cloudfront-js-2.0",
		"function handler(e){return e.request;}",
		nil,
	)
	require.NoError(t, err)

	_, err = backend.CreateFunction(
		"live-fn",
		"live",
		"cloudfront-js-2.0",
		"function handler(e){return e.request;}",
		nil,
	)
	require.NoError(t, err)

	_, err = backend.PublishFunction("live-fn")
	require.NoError(t, err)

	out, err := client.ListFunctions(t.Context(), &cfsdk.ListFunctionsInput{
		Stage: types.FunctionStageLive,
	})
	require.NoError(t, err)
	require.NotNil(t, out.FunctionList)

	names := make([]string, 0, len(out.FunctionList.Items))
	for _, fn := range out.FunctionList.Items {
		names = append(names, aws.ToString(fn.Name))
	}

	require.Equal(t, []string{"live-fn"}, names)
}

// TestListConnectionFunctions_SDKRoundTrip_StageFilter drives the real SDK client with Stage
// set. ListConnectionFunctions carries Stage in the XML request body, not the query string
// (cloudfront@v1.67.4 serializers.go: awsRestxml_serializeOpHttpBindingsListConnectionFunctionsInput
// returns nil), unlike its sibling ListFunctions. Before the fix, handleListConnectionFunctions
// never read the body at all and always returned every connection function.
func TestListConnectionFunctions_SDKRoundTrip_StageFilter(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	_, err := backend.CreateConnectionFunction("dev-cfn", "dev")
	require.NoError(t, err)

	_, err = backend.CreateConnectionFunction("live-cfn", "live")
	require.NoError(t, err)

	_, err = backend.PublishConnectionFunction("live-cfn")
	require.NoError(t, err)

	out, err := client.ListConnectionFunctions(t.Context(), &cfsdk.ListConnectionFunctionsInput{
		Stage: types.FunctionStageLive,
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.ConnectionFunctions))
	for _, fn := range out.ConnectionFunctions {
		names = append(names, aws.ToString(fn.Name))
	}

	require.Equal(t, []string{"live-cfn"}, names)
}

// TestListConnectionGroups_SDKRoundTrip_AssociationFilter drives the real SDK client with
// AssociationFilter.AnycastIpListId set. Before the fix, handleListConnectionGroups never read
// the request body and always returned every connection group regardless of the filter.
func TestListConnectionGroups_SDKRoundTrip_AssociationFilter(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	_, err := backend.CreateConnectionGroupWithConfig("cg-matched", "matched", "anycast-abc", true, true, nil)
	require.NoError(t, err)

	_, err = backend.CreateConnectionGroupWithConfig("cg-other", "other", "anycast-xyz", true, true, nil)
	require.NoError(t, err)

	out, err := client.ListConnectionGroups(t.Context(), &cfsdk.ListConnectionGroupsInput{
		AssociationFilter: &types.ConnectionGroupAssociationFilter{
			AnycastIpListId: aws.String("anycast-abc"),
		},
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.ConnectionGroups))
	for _, cg := range out.ConnectionGroups {
		names = append(names, aws.ToString(cg.Name))
	}

	require.Equal(t, []string{"cg-matched"}, names)
}

// TestListKeyValueStores_SDKRoundTrip_StatusFilter drives the real SDK client with Status set
// to a value no store carries (the emulator provisions stores synchronously, so every store is
// always READY). Before the fix, handleListKeyValueStores ignored the Status query parameter (a
// real ListKeyValueStoresInput member, cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpHttpBindingsListKeyValueStoresInput binds it to the query string) and
// always returned every store regardless of Status.
func TestListKeyValueStores_SDKRoundTrip_StatusFilter(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	_, err := backend.CreateKeyValueStore("kvs-one", "comment", nil)
	require.NoError(t, err)

	out, err := client.ListKeyValueStores(t.Context(), &cfsdk.ListKeyValueStoresInput{
		Status: aws.String("PROVISIONING"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.KeyValueStoreList)
	require.Empty(t, out.KeyValueStoreList.Items)

	outReady, err := client.ListKeyValueStores(t.Context(), &cfsdk.ListKeyValueStoresInput{
		Status: aws.String("READY"),
	})
	require.NoError(t, err)
	require.Len(t, outReady.KeyValueStoreList.Items, 1)
}

// TestListDistributionTenants_SDKRoundTrip_AssociationFilter drives the real SDK client with
// AssociationFilter.DistributionId and .ConnectionGroupId set. Before the fix,
// handleListDistributionTenants never read the request body at all and always returned every
// tenant regardless of the filter.
func TestListDistributionTenants_SDKRoundTrip_AssociationFilter(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	distA, err := backend.CreateDistribution("ref-a", "dist-a", true, nil)
	require.NoError(t, err)

	distB, err := backend.CreateDistribution("ref-b", "dist-b", true, nil)
	require.NoError(t, err)

	tenantA, err := backend.CreateDistributionTenant(distA.ID, "tenant-a", []string{"a.example.com"}, nil)
	require.NoError(t, err)

	_, err = backend.CreateDistributionTenant(distB.ID, "tenant-b", []string{"b.example.com"}, nil)
	require.NoError(t, err)

	_, err = backend.UpdateDistributionTenant(tenantA.ID, cloudfront.DistributionTenantUpdate{
		ConnectionGroupID: "cg-a",
	})
	require.NoError(t, err)

	byDist, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{
		AssociationFilter: &types.DistributionTenantAssociationFilter{
			DistributionId: aws.String(distA.ID),
		},
	})
	require.NoError(t, err)
	require.Len(t, byDist.DistributionTenantList, 1)
	require.Equal(t, "tenant-a", aws.ToString(byDist.DistributionTenantList[0].Name))

	byGroup, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{
		AssociationFilter: &types.DistributionTenantAssociationFilter{
			ConnectionGroupId: aws.String("cg-a"),
		},
	})
	require.NoError(t, err)
	require.Len(t, byGroup.DistributionTenantList, 1)
	require.Equal(t, "tenant-a", aws.ToString(byGroup.DistributionTenantList[0].Name))
}
