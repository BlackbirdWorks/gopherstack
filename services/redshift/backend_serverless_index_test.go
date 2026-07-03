package redshift_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestServerlessNamespaceIndex_OrderedAndConsistent verifies that ListNamespaces
// returns names in sorted order regardless of insertion order, and that the
// sorted index tracks creates and deletes consistently with the store.
func TestServerlessNamespaceIndex_OrderedAndConsistent(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	// Insert out of order.
	inserted := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for _, name := range inserted {
		_, err := b.CreateNamespace(name, "admin", "dev", "", nil, nil)
		require.NoError(t, err)
	}

	require.Equal(t, len(inserted), redshift.ServerlessIndexLen(b, "namespace"))

	got := listNamespaceNames(t, b)
	assert.True(t, sort.StringsAreSorted(got), "namespaces not sorted: %v", got)
	assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta", "echo"}, got)

	// Delete a middle element; order and index stay consistent.
	_, err := b.DeleteNamespace("charlie")
	require.NoError(t, err)

	require.Equal(t, len(inserted)-1, redshift.ServerlessIndexLen(b, "namespace"))

	got = listNamespaceNames(t, b)
	assert.Equal(t, []string{"alpha", "bravo", "delta", "echo"}, got)
}

// TestServerlessNamespaceIndex_Pagination verifies pagination still yields the
// full sorted set across pages after the index refactor.
func TestServerlessNamespaceIndex_Pagination(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	const total = 25
	for i := range total {
		_, err := b.CreateNamespace(fmt.Sprintf("ns-%02d", i), "admin", "dev", "", nil, nil)
		require.NoError(t, err)
	}

	var (
		all   []string
		token string
	)

	for {
		page, next := b.ListNamespaces(10, token)
		for _, ns := range page {
			all = append(all, ns.NamespaceName)
		}

		if next == "" {
			break
		}

		token = next
	}

	require.Len(t, all, total)
	assert.True(t, sort.StringsAreSorted(all), "paged results not globally sorted")
}

// TestServerlessIndex_ResetAndReset verifies Reset clears every serverless index.
func TestServerlessIndex_Reset(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateNamespace("ns", "admin", "dev", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateWorkgroup("wg", "ns", 32, nil, nil)
	require.NoError(t, err)

	require.Equal(t, 1, redshift.ServerlessIndexLen(b, "namespace"))
	require.Equal(t, 1, redshift.ServerlessIndexLen(b, "workgroup"))

	b.Reset()

	assert.Equal(t, 0, redshift.ServerlessIndexLen(b, "namespace"))
	assert.Equal(t, 0, redshift.ServerlessIndexLen(b, "workgroup"))
}

// TestServerlessIndex_RebuiltOnRestore verifies the sorted indexes are rebuilt
// after a Snapshot/Restore round trip so list ordering survives persistence.
func TestServerlessIndex_RebuiltOnRestore(t *testing.T) {
	t.Parallel()

	src := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	for _, name := range []string{"zeta", "alpha", "mike"} {
		_, err := src.CreateNamespace(name, "admin", "dev", "", nil, nil)
		require.NoError(t, err)
	}

	ctx := t.Context()
	data := src.Snapshot(ctx)
	require.NotEmpty(t, data)

	dst := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, dst.Restore(ctx, data))

	require.Equal(t, 3, redshift.ServerlessIndexLen(dst, "namespace"))

	got := listNamespaceNames(t, dst)
	assert.Equal(t, []string{"alpha", "mike", "zeta"}, got)
}

// TestServerlessSnapshotIndex_OrderedWithFilter verifies snapshot list results
// are sorted via the index and the namespace filter is preserved.
func TestServerlessSnapshotIndex_OrderedWithFilter(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateNamespace("ns-a", "admin", "dev", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateNamespace("ns-b", "admin", "dev", "", nil, nil)
	require.NoError(t, err)

	// Create snapshots out of order across two namespaces.
	seed := []struct{ snap, ns string }{
		{"snap-3", "ns-a"},
		{"snap-1", "ns-b"},
		{"snap-2", "ns-a"},
		{"snap-0", "ns-b"},
	}
	for _, s := range seed {
		_, cerr := b.CreateServerlessSnapshot(s.snap, s.ns)
		require.NoError(t, cerr)
	}

	require.Equal(t, len(seed), redshift.ServerlessIndexLen(b, "snapshot"))

	// Unfiltered: globally sorted.
	all, _ := b.ListServerlessSnapshots("", 0, "")
	allNames := make([]string, 0, len(all))
	for _, s := range all {
		allNames = append(allNames, s.SnapshotName)
	}

	assert.Equal(t, []string{"snap-0", "snap-1", "snap-2", "snap-3"}, allNames)

	// Filtered by namespace: still sorted, only matching entries.
	nsA, _ := b.ListServerlessSnapshots("ns-a", 0, "")
	nsANames := make([]string, 0, len(nsA))
	for _, s := range nsA {
		nsANames = append(nsANames, s.SnapshotName)
	}

	assert.Equal(t, []string{"snap-2", "snap-3"}, nsANames)

	// Delete keeps index consistent.
	_, err = b.DeleteServerlessSnapshot("snap-2")
	require.NoError(t, err)
	assert.Equal(t, len(seed)-1, redshift.ServerlessIndexLen(b, "snapshot"))
}

// TestServerlessUsageLimitIndex_OrderedWithFilter verifies usage-limit list
// results stay sorted via the index and honour the resourceArn filter.
func TestServerlessUsageLimitIndex_OrderedWithFilter(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	arnA := "arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/a"
	arnB := "arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/b"

	for i := range 6 {
		target := arnA
		if i%2 == 0 {
			target = arnB
		}

		_, err := b.CreateServerlessUsageLimit(target, "serverless-compute", "monthly", "log", int64(10+i))
		require.NoError(t, err)
	}

	require.Equal(t, 6, redshift.ServerlessIndexLen(b, "usagelimit"))

	all, _ := b.ListServerlessUsageLimits("", 0, "")
	ids := make([]string, 0, len(all))
	for _, ul := range all {
		ids = append(ids, ul.UsageLimitID)
	}

	assert.True(t, sort.StringsAreSorted(ids), "usage limit ids not sorted: %v", ids)

	filtered, _ := b.ListServerlessUsageLimits(arnA, 0, "")
	for _, ul := range filtered {
		assert.Equal(t, arnA, ul.ResourceArn)
	}

	assert.Len(t, filtered, 3)
}

// TestServerlessScheduledActionIndex_OrderedWithFilter verifies scheduled-action
// list results stay sorted via the index and honour the namespace filter.
func TestServerlessScheduledActionIndex_OrderedWithFilter(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	start := time.Now()
	end := start.Add(time.Hour)

	seed := []struct{ name, ns string }{
		{"act-c", "ns-1"},
		{"act-a", "ns-2"},
		{"act-b", "ns-1"},
	}
	for _, s := range seed {
		_, err := b.CreateServerlessScheduledAction(s.name, s.ns, "rate(1 hour)", "pause", start, end)
		require.NoError(t, err)
	}

	require.Equal(t, len(seed), redshift.ServerlessIndexLen(b, "scheduledaction"))

	all, _ := b.ListServerlessScheduledActions("", 0, "")
	names := make([]string, 0, len(all))
	for _, sa := range all {
		names = append(names, sa.ScheduledActionName)
	}

	assert.Equal(t, []string{"act-a", "act-b", "act-c"}, names)

	ns1, _ := b.ListServerlessScheduledActions("ns-1", 0, "")
	ns1Names := make([]string, 0, len(ns1))
	for _, sa := range ns1 {
		ns1Names = append(ns1Names, sa.ScheduledActionName)
	}

	assert.Equal(t, []string{"act-b", "act-c"}, ns1Names)
}

func listNamespaceNames(t *testing.T, b *redshift.InMemoryBackend) []string {
	t.Helper()

	page, _ := b.ListNamespaces(0, "")
	names := make([]string, 0, len(page))

	for _, ns := range page {
		names = append(names, ns.NamespaceName)
	}

	return names
}
