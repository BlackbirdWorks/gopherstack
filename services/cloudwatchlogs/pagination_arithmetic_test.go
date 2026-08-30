package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func streamsNamed(names ...string) []cloudwatchlogs.LogStream {
	out := make([]cloudwatchlogs.LogStream, 0, len(names))
	for _, n := range names {
		out = append(out, cloudwatchlogs.LogStream{LogStreamName: n})
	}

	return out
}

func streamNames(s []cloudwatchlogs.LogStream) []string {
	out := make([]string, 0, len(s))
	for _, x := range s {
		out = append(out, x.LogStreamName)
	}

	return out
}

func groupsNamed(names ...string) []cloudwatchlogs.LogGroup {
	out := make([]cloudwatchlogs.LogGroup, 0, len(names))
	for _, n := range names {
		out = append(out, cloudwatchlogs.LogGroup{LogGroupName: n})
	}

	return out
}

func groupNames(g []cloudwatchlogs.LogGroup) []string {
	out := make([]string, 0, len(g))
	for _, x := range g {
		out = append(out, x.LogGroupName)
	}

	return out
}

// TestPaginateStreams_BoundaryWalk and its sibling TestPaginateGroups_BoundaryWalk
// verify the shared offset/index cursor arithmetic (parseNextToken/encodeNextToken
// backing paginateStreams and paginateGroups) that is duplicated, byte-for-byte
// identical, across roughly 19 other backend List/Describe methods in this
// package (anomaly detectors, account policies, deliveries, destinations,
// export/import tasks, query definitions, S3-table-integration sources,
// syslog configurations, lookup tables, resource policies, metric filters,
// queries, scheduled queries + their run history, subscription filters, plus
// two handler-level query-results/log-groups pagers). All were read and
// confirmed to share this exact start/end/token computation.
func TestPaginateStreams_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 19)
	for i := range 19 {
		names = append(names, string(rune('a'+i)))
	}

	all := streamsNamed(names...)

	var collected []string

	token := ""
	for {
		page, next := cloudwatchlogs.PaginateStreamsForTest(all, token, 4)
		collected = append(collected, streamNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateStreams_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := streamsNamed("a", "b", "c", "d")

	page1, tok1 := cloudwatchlogs.PaginateStreamsForTest(all, "", 2)
	require.Equal(t, []string{"a", "b"}, streamNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := cloudwatchlogs.PaginateStreamsForTest(all, tok1, 2)
	require.Equal(t, []string{"c", "d"}, streamNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateStreams_SinglePageAndEmpty(t *testing.T) {
	t.Parallel()

	all := streamsNamed("a", "b")
	page, tok := cloudwatchlogs.PaginateStreamsForTest(all, "", 10)
	require.Equal(t, []string{"a", "b"}, streamNames(page))
	assert.Empty(t, tok)

	page2, tok2 := cloudwatchlogs.PaginateStreamsForTest(nil, "", 10)
	assert.Empty(t, page2)
	assert.Empty(t, tok2)
}

func TestPaginateStreams_CursorPastEnd(t *testing.T) {
	t.Parallel()

	all := streamsNamed("a", "b", "c")
	staleToken := cloudwatchlogs.EncodeNextTokenForTest(100)

	page, tok := cloudwatchlogs.PaginateStreamsForTest(all, staleToken, 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginateGroups_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 17)
	for i := range 17 {
		names = append(names, string(rune('a'+i)))
	}

	all := groupsNamed(names...)

	var collected []string

	token := ""
	for {
		page, next := cloudwatchlogs.PaginateGroupsForTest(all, token, 4)
		collected = append(collected, groupNames(page)...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, names, collected)
}

func TestPaginateGroups_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := groupsNamed("a", "b", "c", "d")

	page1, tok1 := cloudwatchlogs.PaginateGroupsForTest(all, "", 2)
	require.Equal(t, []string{"a", "b"}, groupNames(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := cloudwatchlogs.PaginateGroupsForTest(all, tok1, 2)
	require.Equal(t, []string{"c", "d"}, groupNames(page2))
	assert.Empty(t, tok2)
}

func TestPaginateGroups_CursorPastEnd(t *testing.T) {
	t.Parallel()

	all := groupsNamed("a", "b", "c")
	staleToken := cloudwatchlogs.EncodeNextTokenForTest(100)

	page, tok := cloudwatchlogs.PaginateGroupsForTest(all, staleToken, 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestEncodeParseNextToken_RoundTrip(t *testing.T) {
	t.Parallel()

	for _, idx := range []int{0, 1, 42, 9999} {
		tok := cloudwatchlogs.EncodeNextTokenForTest(idx)
		got := cloudwatchlogs.ParseNextTokenForTest(tok)
		assert.Equal(t, idx, got)
	}
}

// TestParseNextToken_LegacyPlainDecimalFallback documents the documented
// backward-compatibility path: parseNextToken accepts a bare decimal string
// (not base64) for cursors minted before the base64 encoding was
// introduced, falling back gracefully rather than resetting to 0.
func TestParseNextToken_LegacyPlainDecimalFallback(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 42, cloudwatchlogs.ParseNextTokenForTest("42"))
	assert.Equal(t, 0, cloudwatchlogs.ParseNextTokenForTest("-1"), "negative offsets are invalid, must reset to start")
	assert.Equal(t, 0, cloudwatchlogs.ParseNextTokenForTest("garbage"))
	assert.Equal(t, 0, cloudwatchlogs.ParseNextTokenForTest(""))
}
