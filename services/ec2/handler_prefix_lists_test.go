package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedPrefixList(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var plID string

	t.Run("create prefix list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		pl, err := b.CreateManagedPrefixList("my-list", "IPv4", 10)
		require.NoError(t, err)
		assert.NotEmpty(t, pl.PrefixListID)
		assert.Equal(t, "my-list", pl.PrefixListName)
		assert.Equal(t, "IPv4", pl.AddressFamily)
		assert.Equal(t, "create-complete", pl.State)
		assert.Equal(t, int64(1), pl.Version)
		plID = pl.PrefixListID
	})

	t.Run("describe returns created list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribeManagedPrefixLists([]string{plID})
		require.Len(t, lists, 1)
		assert.Equal(t, "my-list", lists[0].PrefixListName)
	})

	t.Run("modify add entries", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.ModifyManagedPrefixList(plID, []ec2.PrefixListEntry{
			{Cidr: "10.0.0.0/8", Description: "private"},
		}, nil)
		require.NoError(t, err)
	})

	t.Run("get entries returns added", func(t *testing.T) { //nolint:paralleltest // existing issue.
		entries, err := b.GetManagedPrefixListEntries(plID)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "10.0.0.0/8", entries[0].Cidr)
	})

	t.Run("restore version", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.RestoreManagedPrefixListVersion(plID, 1))
		lists := b.DescribeManagedPrefixLists([]string{plID})
		require.Len(t, lists, 1)
		assert.Equal(t, "restore-complete", lists[0].State)
	})

	t.Run("delete prefix list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteManagedPrefixList(plID))
		lists := b.DescribeManagedPrefixLists(nil)
		assert.Empty(t, lists)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteManagedPrefixList("pl-nonexistent"))
	})

	t.Run("create with empty name returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateManagedPrefixList("", "IPv4", 10)
		require.Error(t, err)
	})
}

// ---- ClientVpnEndpoint ----

// TestManagedPrefixList_FullCycle tests full CRUD and entry management.
func TestManagedPrefixList_FullCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		listName      string
		addressFamily string
		maxEntries    int
	}{
		{name: "ipv4_list", listName: "my-ipv4-list", addressFamily: "IPv4", maxEntries: 10},
		{name: "ipv6_list", listName: "my-ipv6-list", addressFamily: "IPv6", maxEntries: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			pl, err := b.CreateManagedPrefixList(tt.listName, tt.addressFamily, tt.maxEntries)
			require.NoError(t, err)
			assert.Contains(t, pl.PrefixListID, "pl-")
			assert.Equal(t, tt.listName, pl.PrefixListName)
			assert.Equal(t, tt.addressFamily, pl.AddressFamily)
			assert.Equal(t, "create-complete", pl.State)
			assert.Equal(t, int64(1), pl.Version)

			// add entries
			require.NoError(t, b.ModifyManagedPrefixList(pl.PrefixListID, []ec2.PrefixListEntry{
				{Cidr: "10.0.0.0/8", Description: "rfc1918-a"},
				{Cidr: "172.16.0.0/12", Description: "rfc1918-b"},
			}, nil))

			entries, err := b.GetManagedPrefixListEntries(pl.PrefixListID)
			require.NoError(t, err)
			assert.Len(t, entries, 2)

			// remove one entry
			require.NoError(t, b.ModifyManagedPrefixList(
				pl.PrefixListID, nil, []ec2.PrefixListEntry{{Cidr: "10.0.0.0/8"}},
			))

			entries2, err := b.GetManagedPrefixListEntries(pl.PrefixListID)
			require.NoError(t, err)
			assert.Len(t, entries2, 1)
			assert.Equal(t, "172.16.0.0/12", entries2[0].Cidr)

			// delete the prefix list
			require.NoError(t, b.DeleteManagedPrefixList(pl.PrefixListID))
			lists := b.DescribeManagedPrefixLists([]string{pl.PrefixListID})
			assert.Empty(t, lists)
		})
	}
}

// TestManagedPrefixList_ViaHandler verifies handler response shape.

// TestManagedPrefixList_ViaHandler verifies handler response shape.
func TestManagedPrefixList_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":         {"CreateManagedPrefixList"},
		"PrefixListName": {"test-list"},
		"AddressFamily":  {"IPv4"},
		"MaxEntries":     {"20"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<prefixListId>pl-")
	assert.Contains(t, resp, "<prefixListName>test-list</prefixListName>")
	assert.Contains(t, resp, "<state>create-complete</state>")
	assert.Contains(t, resp, "<CreateManagedPrefixListResponse")
}

// TestManagedPrefixList_EmptyNameError verifies validation.

// TestManagedPrefixList_EmptyNameError verifies validation.
func TestManagedPrefixList_EmptyNameError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        {"CreateManagedPrefixList"},
		"AddressFamily": {"IPv4"},
		"MaxEntries":    {"10"},
	})
	require.Error(t, err, "empty PrefixListName must return error")
}

// ============================================================================
// VerifiedAccess accuracy
// ============================================================================

// TestVerifiedAccess_InstanceCRUD verifies full instance lifecycle.
