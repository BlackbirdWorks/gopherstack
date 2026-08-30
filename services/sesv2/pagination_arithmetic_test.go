package sesv2_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestListTenantsPaginationBoundaryWalk exercises check 1 (boundary walk)
// against ListTenants, whose underlying paginateMaps helper does a linear
// equality scan for the cursor.
func TestListTenantsPaginationBoundaryWalk(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()

	const n = 7 // does not divide page size 3
	want := make(map[string]bool, n)
	for i := range n {
		name := fmt.Sprintf("tenant-%02d", i)
		_, err := b.CreateTenant(name, nil)
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, n)
	nextToken := ""

	for range n + 2 {
		page, next, err := b.ListTenants(nextToken, 3)
		require.NoError(t, err)

		for _, tn := range page {
			assert.Falsef(t, got[tn.TenantName], "tenant %s returned twice across pages", tn.TenantName)
			got[tn.TenantName] = true
		}

		if next == "" {
			break
		}
		nextToken = next
	}

	assert.Equal(t, want, got, "concatenation of every page must reproduce the collection exactly")
}

// TestListTenantsPaginationStaleCursor exercises check 7 (Class B: infinite
// loop via equality-matched cursor defaulting to zero on a miss).
func TestListTenantsPaginationStaleCursor(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()

	for i := range 5 {
		_, err := b.CreateTenant(fmt.Sprintf("tenant-%02d", i), nil)
		require.NoError(t, err)
	}

	// A cursor naming a tenant that sorts after every remaining item and was
	// never created (equivalent to "since deleted").
	page, _, err := b.ListTenants("tenant-99", 2)
	require.NoError(t, err)

	for _, tn := range page {
		assert.NotEqual(t, "tenant-00", tn.TenantName,
			"stale cursor must not reset pagination to the first item of the collection")
	}
}

// TestListTenantsPaginationFinalPageAndEmpty covers checks 2, 3, and 4.
func TestListTenantsPaginationFinalPageAndEmpty(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()

	page, next, err := b.ListTenants("", 10)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Empty(t, next, "empty collection must not emit a cursor")

	for i := range 3 {
		_, cerr := b.CreateTenant(fmt.Sprintf("tenant-%02d", i), nil)
		require.NoError(t, cerr)
	}

	page, next, err = b.ListTenants("", 10)
	require.NoError(t, err)
	assert.Len(t, page, 3, "collection smaller than one page must return everything")
	assert.Empty(t, next, "must not emit a cursor when nothing remains")
}
