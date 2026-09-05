package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cosmosDBDevMasterKey is the real Cosmos DB Local Emulator's published
// well-known master key, which gopherstack accepts as its default identity
// (see services/cosmosdb/settings.go's DefaultMasterKey and AZURE.md
// sections 3/5) so that unmodified Azure SDKs pointed at this server work
// out of the box.
const cosmosDBDevMasterKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

// createCosmosDBClient returns an azure-sdk-for-go Cosmos DB client pointed
// at the shared test container's dedicated Cosmos DB port (see
// cosmosDBEndpoint in main_test.go). Skips the calling test if that port
// could not be determined (mirrors createAzureTableServiceClient).
//
// EnableContentResponseOnWrite is set true so Create/Replace/Upsert Item
// responses carry the document body back (the SDK default is false, which
// would otherwise make every write's Value empty and this test unable to
// assert on it).
func createCosmosDBClient(t *testing.T) *azcosmos.Client {
	t.Helper()

	if cosmosDBEndpoint == "" {
		t.Skip("Cosmos DB endpoint not available (mapped port could not be determined)")
	}

	cred, err := azcosmos.NewKeyCredential(cosmosDBDevMasterKey)
	require.NoError(t, err, "unable to build KeyCredential")

	client, err := azcosmos.NewClientWithKey(cosmosDBEndpoint, cred, &azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
	})
	require.NoError(t, err, "unable to construct Cosmos DB client")

	return client
}

func TestIntegration_CosmosDB_DatabaseContainerDocumentLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCosmosDBClient(t)
	ctx := t.Context()

	dbName := "testdb" + uuid.NewString()[:8]

	// CreateDatabase.
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: dbName}, nil)
	require.NoError(t, err)

	dbClient, err := client.NewDatabase(dbName)
	require.NoError(t, err)

	// CreateContainer with a partition-key-path declaration.
	containerName := "testcoll"
	_, err = dbClient.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: containerName,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/pk"},
		},
	}, nil)
	require.NoError(t, err)

	containerClient, err := client.NewContainer(dbName, containerName)
	require.NoError(t, err)

	// CreateItem.
	type testDoc struct {
		ID    string `json:"id"`
		PK    string `json:"pk"`
		Value int    `json:"value"`
	}

	doc := testDoc{ID: "doc1", PK: "partA", Value: 42}
	body, err := json.Marshal(doc)
	require.NoError(t, err)

	pk := azcosmos.NewPartitionKeyString("partA")

	createResp, err := containerClient.CreateItem(ctx, pk, body, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, createResp.ETag)

	// ReadItem.
	readResp, err := containerClient.ReadItem(ctx, pk, "doc1", nil)
	require.NoError(t, err)

	var gotDoc testDoc
	require.NoError(t, json.Unmarshal(readResp.Value, &gotDoc))
	assert.Equal(t, doc, gotDoc)

	// Query via SQL.
	queryPager := containerClient.NewQueryItemsPager(
		"SELECT * FROM c WHERE c.value = @v",
		pk,
		&azcosmos.QueryOptions{QueryParameters: []azcosmos.QueryParameter{{Name: "@v", Value: 42}}},
	)

	found := false

	for queryPager.More() {
		page, pageErr := queryPager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, item := range page.Items {
			var qd testDoc
			require.NoError(t, json.Unmarshal(item, &qd))

			if qd.ID == "doc1" {
				found = true
			}
		}
	}

	assert.True(t, found, "query should return the inserted document")

	// UpsertItem (replaces existing doc1).
	upserted := testDoc{ID: "doc1", PK: "partA", Value: 100}
	upsertBody, err := json.Marshal(upserted)
	require.NoError(t, err)

	_, err = containerClient.UpsertItem(ctx, pk, upsertBody, nil)
	require.NoError(t, err)

	readResp, err = containerClient.ReadItem(ctx, pk, "doc1", nil)
	require.NoError(t, err)

	var afterUpsert testDoc
	require.NoError(t, json.Unmarshal(readResp.Value, &afterUpsert))
	assert.Equal(t, 100, afterUpsert.Value)

	// DeleteItem.
	_, err = containerClient.DeleteItem(ctx, pk, "doc1", nil)
	require.NoError(t, err)

	_, err = containerClient.ReadItem(ctx, pk, "doc1", nil)
	require.Error(t, err, "reading a deleted item should fail")

	// DeleteContainer, then DeleteDatabase.
	_, err = containerClient.Delete(ctx, nil)
	require.NoError(t, err)

	_, err = dbClient.Delete(ctx, nil)
	require.NoError(t, err)
}
