package dataplane_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
)

// TestAttrListIDBoundsDictionary verifies the attribute-list dictionary stops
// growing once it reaches attrListMaxCap and degrades to the empty-list id,
// preventing unbounded memory growth from a pathological client.
func TestAttrListIDBoundsDictionary(t *testing.T) {
	t.Parallel()

	srv := dataplane.NewAttrListServerForTest()
	maxCap := dataplane.AttrListMaxCapForTest()

	// Fill the dictionary exactly to capacity with distinct attribute sets.
	for i := range maxCap {
		id := srv.AllocID([]string{"attr" + strconv.Itoa(i)})
		require.NotEqual(t, dataplane.EmptyAttributeListIDForTest(), id)
	}
	assert.Equal(t, maxCap, srv.DictLen())

	// One more distinct set must be refused and fall back to the empty-list id
	// without growing the dictionary.
	overflow := srv.AllocID([]string{"overflow"})
	assert.Equal(t, dataplane.EmptyAttributeListIDForTest(), overflow)
	assert.Equal(t, maxCap, srv.DictLen())

	// Previously-registered ids remain resolvable (no eviction).
	assert.Equal(t, []string{"attr0"}, srv.Names(srv.AllocID([]string{"attr0"})))
}
