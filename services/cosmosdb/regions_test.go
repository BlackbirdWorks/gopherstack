package cosmosdb_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func locationNames(t *testing.T, v any) []string {
	t.Helper()

	list, _ := v.([]any)
	out := make([]string, 0, len(list))

	for _, e := range list {
		m, _ := e.(map[string]any)
		name, _ := m["name"].(string)
		out = append(out, name)
	}

	return out
}

func TestRaw_AccountRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantWritable []string
		wantReadable []string
		regions      cosmosdb.AccountRegions
		wantMulti    bool
	}{
		{
			name:         "default",
			wantWritable: []string{"South Central US"}, wantReadable: []string{"South Central US"},
		},
		{
			name: "single_writer",
			regions: cosmosdb.AccountRegions{Regions: []cosmosdb.AccountRegion{
				{Name: "West US", FailoverPriority: 1}, {Name: "East US", FailoverPriority: 0},
			}},
			wantWritable: []string{"East US"}, wantReadable: []string{"East US", "West US"},
		},
		{
			name: "failed_over",
			regions: cosmosdb.AccountRegions{Regions: []cosmosdb.AccountRegion{
				{Name: "East US", FailoverPriority: 1}, {Name: "West US", FailoverPriority: 0},
			}},
			wantWritable: []string{"West US"}, wantReadable: []string{"West US", "East US"},
		},
		{
			name: "multi_write",
			regions: cosmosdb.AccountRegions{MultiWrite: true, Regions: []cosmosdb.AccountRegion{
				{Name: "East US"}, {Name: "West US", FailoverPriority: 1}, {Name: "east us", FailoverPriority: 2},
			}},
			wantWritable: []string{"East US", "West US"}, wantReadable: []string{"East US", "West US"}, wantMulti: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newLiveServer(t)
			s.backend.SetAccountRegions(tt.regions)

			body := s.do(http.MethodGet, "/", nil, "").object(t)
			assert.Equal(t, tt.wantWritable, locationNames(t, body["writableLocations"]))
			assert.Equal(t, tt.wantReadable, locationNames(t, body["readableLocations"]))
			assert.Equal(t, tt.wantMulti, body["enableMultipleWriteLocations"])
		})
	}
}

func TestSDK_MultiRegionAccountServesOneStore(t *testing.T) {
	t.Parallel()

	s := newLiveServer(t)
	s.backend.SetAccountRegions(cosmosdb.AccountRegions{MultiWrite: true, Regions: []cosmosdb.AccountRegion{
		{Name: "East US"}, {Name: "West US", FailoverPriority: 1},
	}})

	cred, err := azcosmos.NewKeyCredential(cosmosdb.DefaultMasterKey)
	require.NoError(t, err)

	client, err := azcosmos.NewClientWithKey(s.base+"/", cred, &azcosmos.ClientOptions{
		PreferredRegions: []string{"West US"}, EnableContentResponseOnWrite: true,
		Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	c, err := client.NewContainer("d", "c")
	require.NoError(t, err)

	_, err = c.CreateItem(t.Context(), azcosmos.NewPartitionKeyString("a"), []byte(`{"id":"1","pk":"a"}`), nil)
	require.NoError(t, err)

	read, err := c.ReadItem(t.Context(), azcosmos.NewPartitionKeyString("a"), "1", nil)
	require.NoError(t, err)
	assert.Contains(t, string(read.Value), `"id":"1"`)
}

func TestAccountRegions_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cosmosdb.NewInMemoryBackend()
	b.SetAccountRegions(cosmosdb.AccountRegions{MultiWrite: true, Regions: []cosmosdb.AccountRegion{
		{Name: "West US", FailoverPriority: 1}, {Name: "East US"},
	}})

	snap := b.Snapshot(context.Background())

	restored := cosmosdb.NewInMemoryBackend()
	require.NoError(t, restored.Restore(context.Background(), snap))
	assert.Equal(t, b.AccountRegions(), restored.AccountRegions())

	restored.Reset()
	assert.Equal(t, "South Central US", restored.AccountRegions().Regions[0].Name)
}
