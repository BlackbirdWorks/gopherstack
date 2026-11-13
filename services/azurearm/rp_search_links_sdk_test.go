package azurearm_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/search/armsearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const searchLinkTarget = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg1/providers/" +
	"Microsoft.Storage/storageAccounts/acct"

func sharedLink() armsearch.SharedPrivateLinkResource {
	return armsearch.SharedPrivateLinkResource{Properties: &armsearch.SharedPrivateLinkResourceProperties{
		GroupID: new("blob"), PrivateLinkResourceID: new(searchLinkTarget), RequestMessage: new("please approve"),
	}}
}

func TestSearchSDK_Operations(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	pager := s.factory.NewOperationsClient().NewListPager(nil)

	var names []string

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, op := range page.Value {
			names = append(names, *op.Name)
		}
	}

	assert.Len(t, names, 50)
	assert.Contains(t, names, "Microsoft.Search/searchServices/sharedPrivateLinkResources/write")
	assert.Contains(t, names, "Microsoft.Search/searchServices/indexes/documents/read")
}

func TestSearchSDK_SharedPrivateLinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, s *searchStack, c *armsearch.SharedPrivateLinkResourcesClient)
		name  string
	}{
		{
			name: "create get list delete",
			check: func(t *testing.T, _ *searchStack, c *armsearch.SharedPrivateLinkResourcesClient) {
				t.Helper()

				poller, err := c.BeginCreateOrUpdate(t.Context(), searchRG, "svc", "link1", sharedLink(), nil, nil)
				require.NoError(t, err)

				created, err := poller.PollUntilDone(t.Context(), nil)
				require.NoError(t, err)
				assert.Equal(t, armsearch.SharedPrivateLinkResourceStatusPending, *created.Properties.Status)
				assert.Equal(t, "blob", *created.Properties.GroupID)

				got, err := c.Get(t.Context(), searchRG, "svc", "link1", nil, nil)
				require.NoError(t, err)
				assert.Equal(t, searchLinkTarget, *got.Properties.PrivateLinkResourceID)

				pager := c.NewListByServicePager(searchRG, "svc", nil, nil)
				page, err := pager.NextPage(t.Context())
				require.NoError(t, err)
				require.Len(t, page.Value, 1)

				del, err := c.BeginDelete(t.Context(), searchRG, "svc", "link1", nil, nil)
				require.NoError(t, err)
				_, err = del.PollUntilDone(t.Context(), nil)
				require.NoError(t, err)

				_, err = c.Get(t.Context(), searchRG, "svc", "link1", nil, nil)
				require.Error(t, err)
			},
		},
		{
			name: "missing group refused",
			check: func(t *testing.T, _ *searchStack, c *armsearch.SharedPrivateLinkResourcesClient) {
				t.Helper()

				bad := sharedLink()
				bad.Properties.GroupID = nil
				_, err := c.BeginCreateOrUpdate(t.Context(), searchRG, "svc", "bad", bad, nil, nil)
				require.Error(t, err)
			},
		},
		{
			name: "unknown service",
			check: func(t *testing.T, _ *searchStack, c *armsearch.SharedPrivateLinkResourcesClient) {
				t.Helper()

				_, err := c.BeginCreateOrUpdate(t.Context(), searchRG, "nosuch", "l", sharedLink(), nil, nil)
				require.Error(t, err)
			},
		},
		{
			name: "delete missing",
			check: func(t *testing.T, _ *searchStack, c *armsearch.SharedPrivateLinkResourcesClient) {
				t.Helper()

				_, err := c.BeginDelete(t.Context(), searchRG, "svc", "nope", nil, nil)
				require.Error(t, err)
			},
		},
		{
			name: "survive service update and restore",
			check: func(t *testing.T, s *searchStack, c *armsearch.SharedPrivateLinkResourcesClient) {
				t.Helper()

				poller, err := c.BeginCreateOrUpdate(t.Context(), searchRG, "svc", "keep", sharedLink(), nil, nil)
				require.NoError(t, err)
				_, err = poller.PollUntilDone(t.Context(), nil)
				require.NoError(t, err)

				s.create(t, "svc", basicService(armsearch.SKUNameStandard))

				svc, err := s.factory.NewServicesClient().Get(t.Context(), searchRG, "svc", nil, nil)
				require.NoError(t, err)
				require.Len(t, svc.Properties.SharedPrivateLinkResources, 1)

				fresh := newSearchStack(t)
				require.NoError(t, fresh.handler.Restore(t.Context(), s.handler.Snapshot(t.Context())))

				got, err := fresh.factory.NewSharedPrivateLinkResourcesClient().
					Get(t.Context(), searchRG, "svc", "keep", nil, nil)
				require.NoError(t, err)
				assert.Equal(t, "blob", *got.Properties.GroupID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSearchStack(t)
			s.create(t, "svc", basicService(armsearch.SKUNameStandard))
			tt.check(t, s, s.factory.NewSharedPrivateLinkResourcesClient())
		})
	}
}
