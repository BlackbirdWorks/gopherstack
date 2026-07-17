package textract_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

// TestInMemoryBackend_DeleteAdapter_CascadesVersions ensures that deleting an
// adapter also removes all its versions.
func TestInMemoryBackend_DeleteAdapter_CascadesVersions(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	adapter, err := b.CreateAdapter(context.Background(), "cascade-test", "", "DISABLED", []string{"FORMS"}, nil)
	require.NoError(t, err)

	_, err = b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	_, err = b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	require.Equal(t, 2, textract.AdapterVersionCount(b))

	err = b.DeleteAdapter(context.Background(), adapter.AdapterID)
	require.NoError(t, err)

	assert.Equal(t, 0, textract.AdapterCount(b))
	assert.Equal(t, 0, textract.AdapterVersionCount(b))
}

// TestInMemoryBackend_AddAdapterInternal tests the AddAdapterInternal seed helper.
func TestInMemoryBackend_AddAdapterInternal(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	a := &textract.Adapter{
		AdapterID:    "seeded-id",
		AdapterName:  "seeded-adapter",
		AutoUpdate:   "DISABLED",
		CreationTime: time.Now(),
		FeatureTypes: []string{"TABLES"},
		Tags:         map[string]string{},
	}

	textract.AddAdapterInternal(b, a)

	assert.Equal(t, 1, textract.AdapterCount(b))

	fetched, err := b.GetAdapter(context.Background(), "seeded-id")
	require.NoError(t, err)
	assert.Equal(t, "seeded-adapter", fetched.AdapterName)
}

// TestInMemoryBackend_CreateAdapter_CloneTagsNilInput tests that cloneAdapter
// returns non-nil tags even when the input adapter has nil tags.
func TestInMemoryBackend_CreateAdapter_CloneTagsNilInput(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	// Create adapter without tags.
	adapter, err := b.CreateAdapter(context.Background(), "no-tags", "", "DISABLED", []string{"FORMS"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, adapter.Tags)
}

// TestInMemoryBackend_PersistenceWithAdapters tests Snapshot/Restore with
// adapters and adapter versions.
func TestInMemoryBackend_PersistenceWithAdapters(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")

	adapter, err := b.CreateAdapter(
		context.Background(),
		"persist-adapter",
		"desc",
		"ENABLED",
		[]string{"FORMS"},
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	av, err := b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := textract.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, textract.AdapterCount(b2))
	assert.Equal(t, 1, textract.AdapterVersionCount(b2))

	fetchedAdapter, err := b2.GetAdapter(context.Background(), adapter.AdapterID)
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", fetchedAdapter.AutoUpdate)
	assert.Equal(t, "v", fetchedAdapter.Tags["k"])

	fetchedAV, err := b2.GetAdapterVersion(context.Background(), adapter.AdapterID, av.AdapterVersion)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", fetchedAV.Status)
}
