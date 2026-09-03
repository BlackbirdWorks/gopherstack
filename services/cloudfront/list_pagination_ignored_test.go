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

// TestListDistributionTenants_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListDistributionTenants and asserts the pages are disjoint. Before the fix,
// handleListDistributionTenants never read the request body (Marker/MaxItems, like
// AssociationFilter, travel there -- cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpHttpBindingsListDistributionTenantsInput returns nil) and always
// returned every tenant in one unbounded page.
func TestListDistributionTenants_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	const total = 25

	for i := range total {
		dist, err := backend.CreateDistribution(
			fmt.Sprintf("ref-pg-tenant-%02d", i),
			fmt.Sprintf("pg-tenant-dist-%02d", i),
			true,
			nil,
		)
		require.NoError(t, err)

		_, err = backend.CreateDistributionTenant(
			dist.ID, fmt.Sprintf("pg-tenant-%02d", i), []string{fmt.Sprintf("pg-tenant-%02d.example.com", i)}, nil,
		)
		require.NoError(t, err)
	}

	page1, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{
		MaxItems: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.DistributionTenantList, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListDistributionTenants(t.Context(), &cfsdk.ListDistributionTenantsInput{
		MaxItems: aws.Int32(10),
		Marker:   page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.DistributionTenantList, 10)

	seen := make(map[string]bool, 20)
	for _, tn := range page1.DistributionTenantList {
		seen[aws.ToString(tn.Id)] = true
	}

	for _, tn := range page2.DistributionTenantList {
		assert.False(t, seen[aws.ToString(tn.Id)], "page 2 repeated tenant %s from page 1", aws.ToString(tn.Id))
		seen[aws.ToString(tn.Id)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListConnectionGroups_SDKRoundTrip_Pagination drives the real SDK client across two pages
// of ListConnectionGroups and asserts the pages are disjoint. Before the fix,
// handleListConnectionGroups never read the request body and always returned every connection
// group in one unbounded response with no NextMarker.
func TestListConnectionGroups_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	const total = 25

	for i := range total {
		_, err := backend.CreateConnectionGroup(fmt.Sprintf("pg-cg-%02d", i), "pagination test")
		require.NoError(t, err)
	}

	page1, err := client.ListConnectionGroups(t.Context(), &cfsdk.ListConnectionGroupsInput{
		MaxItems: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.ConnectionGroups, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListConnectionGroups(t.Context(), &cfsdk.ListConnectionGroupsInput{
		MaxItems: aws.Int32(10),
		Marker:   page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.ConnectionGroups, 10)

	seen := make(map[string]bool, 20)
	for _, cg := range page1.ConnectionGroups {
		seen[aws.ToString(cg.Id)] = true
	}

	for _, cg := range page2.ConnectionGroups {
		assert.False(
			t,
			seen[aws.ToString(cg.Id)],
			"page 2 repeated connection group %s from page 1",
			aws.ToString(cg.Id),
		)
		seen[aws.ToString(cg.Id)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListKeyValueStores_SDKRoundTrip_Pagination drives the real SDK client across two pages of
// ListKeyValueStores and asserts the pages are disjoint. Before the fix,
// handleListKeyValueStores ignored Marker/MaxItems (both real ListKeyValueStoresInput members,
// query-bound per cloudfront@v1.67.4 serializers.go) and always returned every store in one
// unbounded page.
func TestListKeyValueStores_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := cloudfront.NewHandler(backend)
	client := newTestCloudFrontClient(t, h)

	const total = 25

	for i := range total {
		_, err := backend.CreateKeyValueStore(fmt.Sprintf("pg-kvs-%02d", i), "pagination test", nil)
		require.NoError(t, err)
	}

	page1, err := client.ListKeyValueStores(t.Context(), &cfsdk.ListKeyValueStoresInput{
		MaxItems: aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotNil(t, page1.KeyValueStoreList)
	require.Len(t, page1.KeyValueStoreList.Items, 10)
	require.NotNil(t, page1.KeyValueStoreList.NextMarker)

	page2, err := client.ListKeyValueStores(t.Context(), &cfsdk.ListKeyValueStoresInput{
		MaxItems: aws.Int32(10),
		Marker:   page1.KeyValueStoreList.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.KeyValueStoreList.Items, 10)

	seen := make(map[string]bool, 20)
	for _, kvs := range page1.KeyValueStoreList.Items {
		seen[aws.ToString(kvs.Name)] = true
	}

	for _, kvs := range page2.KeyValueStoreList.Items {
		assert.False(
			t,
			seen[aws.ToString(kvs.Name)],
			"page 2 repeated key value store %s from page 1",
			aws.ToString(kvs.Name),
		)
		seen[aws.ToString(kvs.Name)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListConnectionFunctions_DuplicateNames_NoDropAcrossPages proves
// handleListConnectionFunctions loses records at a page boundary when several
// connection functions share a Name. CreateConnectionFunctionWithCode documents that
// "AWS allows multiple connection functions to share the same Name -- they are keyed
// and uniqued by ID, not by name" (connection.go), yet ListConnectionFunctions sorts
// solely by Name and paginateByMarkerValue's cursor is `getID(item) <= marker`: once a
// tie group of same-named functions straddles a MaxItems boundary, the members left
// out of page 1 share the exact marker value emitted for page 1's last item, so page
// 2's cutoff silently discards the rest of the group forever -- deterministically, not
// just under map-iteration luck, so this is looped only for extra confidence.
func TestListConnectionFunctions_DuplicateNames_NoDropAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
		h := cloudfront.NewHandler(backend)
		client := newTestCloudFrontClient(t, h)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for range dupCount {
			fn, err := backend.CreateConnectionFunction("dup-fn-name", "pagination tie test")
			require.NoError(t, err)
			created[fn.ID] = true
		}

		seen := make(map[string]bool, dupCount)

		marker := (*string)(nil)
		for range dupCount + 1 {
			out, err := client.ListConnectionFunctions(t.Context(), &cfsdk.ListConnectionFunctionsInput{
				MaxItems: aws.Int32(2),
				Marker:   marker,
			})
			require.NoError(t, err)

			for _, fn := range out.ConnectionFunctions {
				seen[aws.ToString(fn.Id)] = true
			}

			if out.NextMarker == nil {
				break
			}

			marker = out.NextMarker
		}

		assert.Equal(t, created, seen, "paged ListConnectionFunctions dropped same-named functions across pages")
	}
}
