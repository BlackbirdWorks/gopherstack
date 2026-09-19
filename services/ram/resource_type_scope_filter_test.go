package ram_test

import (
	"testing"

	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	"github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// TestListResourceTypes_ResourceRegionScopeFilter proves ResourceRegionScope
// (ram@v1.39.4 api_op_ListResourceTypes.go:46-58: ALL default, GLOBAL, REGIONAL) is
// read and applied, not silently dropped: GLOBAL narrows the static catalogue down to
// its one documented global entry (ssm-contacts:Contact).
func TestListResourceTypes_ResourceRegionScopeFilter(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient(t, h)

	t.Run("global scope narrows the catalogue", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListResourceTypes(t.Context(), &ramsdk.ListResourceTypesInput{
			ResourceRegionScope: types.ResourceRegionScopeFilterGlobal,
		})
		require.NoError(t, err)
		require.NotEmpty(t, out.ResourceTypes)

		for _, rt := range out.ResourceTypes {
			require.NotNil(t, rt.ResourceRegionScope)
			assert.Equal(t, "GLOBAL", string(rt.ResourceRegionScope))
		}
	})

	t.Run("all scope returns the full catalogue", func(t *testing.T) {
		t.Parallel()

		unfiltered, err := client.ListResourceTypes(t.Context(), &ramsdk.ListResourceTypesInput{})
		require.NoError(t, err)

		all, err := client.ListResourceTypes(t.Context(), &ramsdk.ListResourceTypesInput{
			ResourceRegionScope: types.ResourceRegionScopeFilterAll,
		})
		require.NoError(t, err)
		assert.Len(t, all.ResourceTypes, len(unfiltered.ResourceTypes))
	})
}
