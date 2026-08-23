package cloudfront_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestListInvalidations_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListInvalidations and asserts the pages are disjoint. Before the fix,
// handleListInvalidations ignored Marker/MaxItems (both real ListInvalidationsInput
// members, cloudfront@v1.67.4 api_op_ListInvalidations.go) and always returned every
// invalidation in one unbounded page with a hardcoded IsTruncated=false and no
// NextMarker.
func TestListInvalidations_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	dist, err := backend.CreateDistribution("ref-pg-inv", "pg-inv-dist", true, nil)
	require.NoError(t, err)

	const total = 25

	for i := range total {
		_, createErr := backend.CreateInvalidation(
			dist.ID,
			fmt.Sprintf("caller-ref-%02d", i),
			[]string{"/*"},
		)
		require.NoError(t, createErr)
	}

	page1, err := client.ListInvalidations(t.Context(), &cfsdk.ListInvalidationsInput{
		DistributionId: aws.String(dist.ID),
		MaxItems:       aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotNil(t, page1.InvalidationList)
	require.Len(t, page1.InvalidationList.Items, 10)
	require.NotNil(t, page1.InvalidationList.NextMarker)

	page2, err := client.ListInvalidations(t.Context(), &cfsdk.ListInvalidationsInput{
		DistributionId: aws.String(dist.ID),
		MaxItems:       aws.Int32(10),
		Marker:         page1.InvalidationList.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.InvalidationList.Items, 10)

	seen := make(map[string]bool, 20)
	for _, inv := range page1.InvalidationList.Items {
		seen[aws.ToString(inv.Id)] = true
	}

	for _, inv := range page2.InvalidationList.Items {
		assert.False(t, seen[aws.ToString(inv.Id)], "page 2 repeated invalidation %s from page 1", aws.ToString(inv.Id))
		seen[aws.ToString(inv.Id)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListInvalidationsForDistributionTenant_SDKRoundTrip_Pagination drives the real SDK
// client across two pages of ListInvalidationsForDistributionTenant and asserts the
// pages are disjoint. Before the fix, handleListInvalidationsForTenant ignored
// Marker/MaxItems and always returned every tenant invalidation in one unbounded page.
func TestListInvalidationsForDistributionTenant_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	tenant, err := backend.CreateDistributionTenant(
		"dist-pg-tenant", "pg-tenant", []string{"pg-tenant.example.com"}, nil,
	)
	require.NoError(t, err)

	const total = 25

	for range total {
		_, createErr := backend.CreateInvalidationForTenant(tenant.ID, []string{"/*"})
		require.NoError(t, createErr)
	}

	page1, err := client.ListInvalidationsForDistributionTenant(
		t.Context(),
		&cfsdk.ListInvalidationsForDistributionTenantInput{
			Id:       aws.String(tenant.ID),
			MaxItems: aws.Int32(10),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, page1.InvalidationList)
	require.Len(t, page1.InvalidationList.Items, 10)
	require.NotNil(t, page1.InvalidationList.NextMarker)

	page2, err := client.ListInvalidationsForDistributionTenant(
		t.Context(),
		&cfsdk.ListInvalidationsForDistributionTenantInput{
			Id:       aws.String(tenant.ID),
			MaxItems: aws.Int32(10),
			Marker:   page1.InvalidationList.NextMarker,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.InvalidationList.Items, 10)

	seen := make(map[string]bool, 20)
	for _, inv := range page1.InvalidationList.Items {
		seen[aws.ToString(inv.Id)] = true
	}

	for _, inv := range page2.InvalidationList.Items {
		assert.False(t, seen[aws.ToString(inv.Id)],
			"page 2 repeated tenant invalidation %s from page 1", aws.ToString(inv.Id))
		seen[aws.ToString(inv.Id)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListFunctions_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListFunctions and asserts the pages are disjoint. Before the fix,
// handleListFunctions ignored Marker/MaxItems and always returned every function in
// one unbounded page with no NextMarker.
func TestListFunctions_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	h := cloudfront.NewHandler(cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"))
	client := newTestCloudFrontClient(t, h)

	const total = 25

	for i := range total {
		_, err := client.CreateFunction(t.Context(), &cfsdk.CreateFunctionInput{
			Name:         aws.String(fmt.Sprintf("pg-func-%02d", i)),
			FunctionCode: []byte("function handler(event) { return event.request; }"),
			FunctionConfig: &types.FunctionConfig{
				Comment: aws.String("pagination test"),
				Runtime: types.FunctionRuntimeCloudfrontJs20,
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListFunctions(t.Context(), &cfsdk.ListFunctionsInput{
		MaxItems: aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotNil(t, page1.FunctionList)
	require.Len(t, page1.FunctionList.Items, 10)
	require.NotNil(t, page1.FunctionList.NextMarker)

	page2, err := client.ListFunctions(t.Context(), &cfsdk.ListFunctionsInput{
		MaxItems: aws.Int32(10),
		Marker:   page1.FunctionList.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.FunctionList.Items, 10)

	seen := make(map[string]bool, 20)
	for _, fn := range page1.FunctionList.Items {
		seen[aws.ToString(fn.Name)] = true
	}

	for _, fn := range page2.FunctionList.Items {
		assert.False(t, seen[aws.ToString(fn.Name)], "page 2 repeated function %s from page 1", aws.ToString(fn.Name))
		seen[aws.ToString(fn.Name)] = true
	}

	assert.Len(t, seen, 20)
}
