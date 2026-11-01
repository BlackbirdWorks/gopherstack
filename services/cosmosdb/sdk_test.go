package cosmosdb_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// sdkClient starts a real listener (optionally enforcing master-key auth) and returns an azcosmos client for it.
func sdkClient(t *testing.T, validate bool, key string) *azcosmos.Client {
	t.Helper()

	h := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
	h.Port = 0
	h.ValidateAuth = validate

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	cred, err := azcosmos.NewKeyCredential(key)
	require.NoError(t, err)

	client, err := azcosmos.NewClientWithKey(fmt.Sprintf("http://127.0.0.1:%d/", h.Port), cred, &azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
		Retry:                        policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return client
}

func TestSDK_MasterKeyAuthEnforced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "correct key", key: cosmosdb.DefaultMasterKey},
		{name: "wrong key", key: "d3JvbmcgbWFzdGVyIGtleSB3cm9uZyBtYXN0ZXIga2V5IHdyb25nIG1hc3RlciBrZXk=", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := sdkClient(t, true, tt.key)

			_, err := client.CreateDatabase(t.Context(), azcosmos.DatabaseProperties{ID: "d"}, nil)
			if tt.wantErr {
				var respErr *azcore.ResponseError
				require.ErrorAs(t, err, &respErr)
				assert.Equal(t, http.StatusUnauthorized, respErr.StatusCode)

				return
			}

			require.NoError(t, err)

			db, err := client.NewDatabase("d")
			require.NoError(t, err)

			_, err = db.CreateContainer(t.Context(), azcosmos.ContainerProperties{
				ID:                     "c",
				PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
			}, nil)
			require.NoError(t, err)

			c, err := client.NewContainer("d", "c")
			require.NoError(t, err)

			resp, err := c.CreateItem(
				t.Context(),
				azcosmos.NewPartitionKeyString("a"),
				[]byte(`{"id":"1","pk":"a"}`),
				nil,
			)
			require.NoError(t, err)
			assert.InDelta(t, 6.1, float64(resp.RequestCharge), 0.01)
		})
	}
}

func TestSDK_QueriesPagingTTLAndHierarchicalKeys(t *testing.T) {
	t.Parallel()

	client := sdkClient(t, true, cosmosdb.DefaultMasterKey)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "d"}, nil)
	require.NoError(t, err)

	db, err := client.NewDatabase("d")
	require.NoError(t, err)

	ttl := int32(-1)
	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: "c",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/tenant", "/user"}, Kind: azcosmos.PartitionKeyKindMultiHash, Version: 2,
		},
		DefaultTimeToLive: &ttl,
	}, nil)
	require.NoError(t, err)

	c, err := client.NewContainer("d", "c")
	require.NoError(t, err)

	props, err := c.Read(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, props.ContainerProperties.DefaultTimeToLive)
	assert.Equal(t, int32(-1), *props.ContainerProperties.DefaultTimeToLive)
	assert.Equal(t, azcosmos.PartitionKeyKindMultiHash, props.ContainerProperties.PartitionKeyDefinition.Kind)

	for i := range 5 {
		tenant := "t1"
		if i >= 3 {
			tenant = "t2"
		}

		body, mErr := json.Marshal(map[string]any{
			"id": fmt.Sprintf("i%d", i), "tenant": tenant, "user": "u", "n": i, "tags": []string{"x", strconv.Itoa(i)},
		})
		require.NoError(t, mErr)

		_, err = c.CreateItem(ctx, azcosmos.NewPartitionKeyString(tenant).AppendString("u"), body, nil)
		require.NoError(t, err)
	}

	read, err := c.ReadItem(ctx, azcosmos.NewPartitionKeyString("t2").AppendString("u"), "i4", nil)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, float64(read.RequestCharge), 0.01)

	collect := func(query string, pk azcosmos.PartitionKey, pageSize int32) (int, [][]byte) {
		pager := c.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PageSizeHint: pageSize})

		var (
			pages int
			items [][]byte
		)

		for pager.More() {
			page, pErr := pager.NextPage(ctx)
			require.NoError(t, pErr)

			pages++

			items = append(items, page.Items...)
		}

		return pages, items
	}

	pages, items := collect("SELECT c.id FROM c ORDER BY c.n", azcosmos.NewPartitionKey(), 2)
	assert.Equal(t, 3, pages, "5 items at page size 2")
	assert.Len(t, items, 5)
	assert.JSONEq(t, `{"id":"i0"}`, string(items[0]))
	assert.JSONEq(t, `{"id":"i4"}`, string(items[4]))

	_, items = collect("SELECT VALUE COUNT(1) FROM c", azcosmos.NewPartitionKey(), 0)
	require.Len(t, items, 1)
	assert.JSONEq(t, "5", string(items[0]))

	_, items = collect("SELECT VALUE COUNT(1) FROM c", azcosmos.NewPartitionKeyString("t1"), 0)
	require.Len(t, items, 1)
	assert.JSONEq(t, "3", string(items[0]), "prefix partition key scopes the query")

	_, items = collect(
		"SELECT c.id FROM c WHERE ARRAY_CONTAINS(c.tags, '2') OR STARTSWITH(c.id, 'i4')",
		azcosmos.NewPartitionKey(), 0,
	)
	assert.Len(t, items, 2)

	// Hierarchical keys require all components for point operations.
	_, err = c.ReadItem(ctx, azcosmos.NewPartitionKeyString("t1"), "i0", nil)
	require.Error(t, err)

	if respErr, ok := errors.AsType[*azcore.ResponseError](err); ok {
		assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
	}

	dbPager := client.NewQueryDatabasesPager("SELECT * FROM d", nil)

	var dbCount int

	for dbPager.More() {
		page, pErr := dbPager.NextPage(ctx)
		require.NoError(t, pErr)

		dbCount += len(page.Databases)
	}

	assert.Equal(t, 1, dbCount)
}
