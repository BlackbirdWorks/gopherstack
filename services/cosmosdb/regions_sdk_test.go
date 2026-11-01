package cosmosdb_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func (e *regionEnv) sdkContainer(preferred ...string) *azcosmos.ContainerClient {
	e.t.Helper()

	cred, err := azcosmos.NewKeyCredential(cosmosdb.DefaultMasterKey)
	require.NoError(e.t, err)

	client, err := azcosmos.NewClientWithKey(e.base+"/", cred, &azcosmos.ClientOptions{
		PreferredRegions:             preferred,
		EnableContentResponseOnWrite: true,
		Retry:                        policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(e.t, err)

	c, err := client.NewContainer("d", "c")
	require.NoError(e.t, err)

	return c
}

func TestSDK_PreferredRegionReadsFromItsOwnReplica(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preferred []string
		wantErr   int
	}{
		{name: "write region", preferred: []string{eastUS}},
		{name: "default order reads from the first region", preferred: nil},
		{name: "read region lags behind the write", preferred: []string{westUS}, wantErr: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newRegionEnv(t, twoRegions(5000))
			c := e.sdkContainer(tt.preferred...)

			_, err := c.UpsertItem(t.Context(), azcosmos.NewPartitionKeyString("a"),
				[]byte(`{"id":"sdk","pk":"a"}`), nil)
			require.NoError(t, err, "writes reach the write region whatever the read preference")

			_, err = c.ReadItem(t.Context(), azcosmos.NewPartitionKeyString("a"), "sdk", nil)
			if tt.wantErr != 0 {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, responseStatus(t, err))

				e.clock.advance(6 * time.Second)

				_, err = c.ReadItem(t.Context(), azcosmos.NewPartitionKeyString("a"), "sdk", nil)
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_WriteFailsOverWhenTheWriteRegionGoesOffline(t *testing.T) {
	t.Parallel()

	e := newRegionEnv(t, twoRegions(0))
	c := e.sdkContainer(eastUS, westUS)
	pk := azcosmos.NewPartitionKeyString("a")

	_, err := c.UpsertItem(t.Context(), pk, []byte(`{"id":"before","pk":"a"}`), nil)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, e.admin(http.MethodPost, "regions/East%20US/offline", "").status)

	_, err = c.UpsertItem(t.Context(), pk, []byte(`{"id":"after","pk":"a"}`), nil)
	require.NoError(t, err, "the SDK refreshes the account and retries the write in the new write region")

	got := e.getAt(westUS, "after", nil)
	assert.Equal(t, http.StatusOK, got.status)

	read, err := c.ReadItem(t.Context(), pk, "before", nil)
	require.NoError(t, err, "reads move to the surviving region")
	assert.NotEmpty(t, read.Value)
}
