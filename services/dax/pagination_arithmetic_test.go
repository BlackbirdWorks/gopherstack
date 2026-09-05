package dax_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

func namedClusters(names ...string) []*dax.Cluster {
	out := make([]*dax.Cluster, 0, len(names))
	for _, n := range names {
		out = append(out, &dax.Cluster{ClusterName: n})
	}

	return out
}

func clusterNames(cs []*dax.Cluster) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.ClusterName)
	}

	return out
}

func TestPaginateClusters_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")

	names := make([]string, 0, 17)
	for i := range 17 {
		names = append(names, string(rune('a'+i)))
	}

	all := namedClusters(names...)

	var collected []string

	token := ""
	for {
		page, next := dax.PaginateClustersForTest(b, all, 4, token)
		collected = append(collected, clusterNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateClusters_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")
	all := namedClusters("a", "b", "c", "d")

	page1, tok1 := dax.PaginateClustersForTest(b, all, 2, "")
	require.Equal(t, []string{"a", "b"}, clusterNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := dax.PaginateClustersForTest(b, all, 2, tok1)
	require.Equal(t, []string{"c", "d"}, clusterNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateClusters_SinglePage(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")
	all := namedClusters("a", "b")

	page, tok := dax.PaginateClustersForTest(b, all, 10, "")
	require.Equal(t, []string{"a", "b"}, clusterNames(page))
	assert.Empty(t, tok)
}

func TestPaginateClusters_Empty(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")

	page, tok := dax.PaginateClustersForTest(b, nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateClusters_StaleCursorAfterDeletion demonstrates that when the
// cluster named by nextToken has since been deleted, pagination must resume
// at the next remaining cluster in sorted order -- not silently restart from
// the beginning, which would hand the caller clusters it already consumed.
func TestPaginateClusters_StaleCursorAfterDeletion(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")
	all := namedClusters("a", "b", "c", "d", "e")

	page1, tok := dax.PaginateClustersForTest(b, all, 2, "")
	require.Equal(t, []string{"a", "b"}, clusterNames(page1))
	require.Equal(t, "c", tok)

	// "c" is deleted between calls.
	remaining := namedClusters("a", "b", "d", "e")

	page2, tok2 := dax.PaginateClustersForTest(b, remaining, 2, tok)
	assert.Equal(t, []string{"d", "e"}, clusterNames(page2),
		"must resume after the deleted cursor, not restart from the beginning")
	assert.Empty(t, tok2)
}

func TestPaginateClusters_CursorPastEnd(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")
	all := namedClusters("a", "b", "c")

	page, tok := dax.PaginateClustersForTest(b, all, 10, "z")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateListStrings_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 13)
	for i := range 13 {
		names = append(names, string(rune('a'+i)))
	}

	var collected []string

	token := ""
	for {
		page, next := dax.PaginateListStringsForTest(names, 4, token)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateListStrings_StaleCursorAfterDeletion(t *testing.T) {
	t.Parallel()

	all := []string{"a", "b", "c", "d", "e"}

	page1, tok := dax.PaginateListStringsForTest(all, 2, "")
	require.Equal(t, []string{"a", "b"}, page1)
	require.Equal(t, "c", tok)

	remaining := []string{"a", "b", "d", "e"}

	page2, tok2 := dax.PaginateListStringsForTest(remaining, 2, tok)
	assert.Equal(t, []string{"d", "e"}, page2,
		"must resume after the deleted cursor, not restart from the beginning")
	assert.Empty(t, tok2)
}

func TestPaginateListStrings_Empty(t *testing.T) {
	t.Parallel()

	page, tok := dax.PaginateListStringsForTest(nil, 10, "")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func namedParameters(names ...string) []*dax.Parameter {
	out := make([]*dax.Parameter, 0, len(names))
	for _, n := range names {
		out = append(out, &dax.Parameter{ParameterName: n})
	}

	return out
}

func parameterNames(ps []*dax.Parameter) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.ParameterName)
	}

	return out
}

// TestPaginateParameters_BoundaryWalk covers paginateParameters, which --
// unlike paginateClusters/paginateList above -- uses a plain decimal-index
// cursor (strconv.Atoi), not a value/name lookup, so it was not exposed to
// the exact-match-reset-to-zero bug fixed in those two.
func TestPaginateParameters_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 15)
	for i := range 15 {
		names = append(names, string(rune('a'+i)))
	}

	all := namedParameters(names...)

	var collected []string

	token := ""
	for {
		page, next := dax.PaginateParametersForTest(all, 4, token)
		collected = append(collected, parameterNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateParameters_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := namedParameters("a", "b", "c", "d")

	page1, tok1 := dax.PaginateParametersForTest(all, 2, "")
	require.Equal(t, []string{"a", "b"}, parameterNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := dax.PaginateParametersForTest(all, 2, tok1)
	require.Equal(t, []string{"c", "d"}, parameterNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateParameters_SinglePageAndEmpty(t *testing.T) {
	t.Parallel()

	all := namedParameters("a", "b")
	page, tok := dax.PaginateParametersForTest(all, 10, "")
	require.Equal(t, []string{"a", "b"}, parameterNames(page))
	assert.Empty(t, tok)

	page2, tok2 := dax.PaginateParametersForTest(nil, 10, "")
	assert.Empty(t, page2)
	assert.Empty(t, tok2)
}

func TestPaginateParameters_CursorPastEnd(t *testing.T) {
	t.Parallel()

	all := namedParameters("a", "b", "c")

	page, tok := dax.PaginateParametersForTest(all, 10, "100")
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestDescribeEvents_BoundaryWalk(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")

	const n = 17
	for i := range n {
		dax.EmitEventForTest(b, "cluster-a", "CLUSTER", strconv.Itoa(i))
	}

	var collected []string

	token := ""
	for {
		page, next, err := b.DescribeEvents("", "", nil, nil, 4, token)
		require.NoError(t, err)

		for _, ev := range page {
			collected = append(collected, ev.Message)
		}

		if next == "" {
			break
		}

		token = next
	}

	want := make([]string, n)
	for i := range want {
		want[i] = strconv.Itoa(i)
	}

	require.Equal(t, want, collected)
}

func TestDescribeEvents_CursorPastEnd(t *testing.T) {
	t.Parallel()

	b := dax.NewInMemoryBackend("123456789012", "us-east-1")
	for i := range 3 {
		dax.EmitEventForTest(b, "cluster-a", "CLUSTER", strconv.Itoa(i))
	}

	page, tok, err := b.DescribeEvents("", "", nil, nil, 10, "100")
	require.NoError(t, err)
	assert.Empty(t, page, "a token past the end must not restart pagination from the beginning")
	assert.Empty(t, tok)
}

func TestPaginateParameters_MalformedTokenResetsToStart(t *testing.T) {
	t.Parallel()

	all := namedParameters("a", "b", "c")

	page, _ := dax.PaginateParametersForTest(all, 2, "not-a-number")
	assert.Equal(t, []string{"a", "b"}, parameterNames(page))
}
