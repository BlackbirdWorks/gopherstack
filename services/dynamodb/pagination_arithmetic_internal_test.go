package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// TestFindStartIndex_BoundaryWalk verifies that walking a sorted name list in
// pages of K, where K does not divide N, and concatenating every page
// reproduces the original list exactly.
func TestFindStartIndex_BoundaryWalk(t *testing.T) {
	t.Parallel()

	names := make([]string, 0, 27)
	for i := range 27 {
		names = append(names, string(rune('a'+i)))
	}

	const pageSize = 5

	var collected []string

	cursor := ""
	for {
		start := 0
		if cursor != "" {
			idx, found := findStartIndex(names, cursor)
			if !found {
				break
			}

			start = idx
		}

		page := names[start:]

		var last string
		if len(page) > pageSize {
			last = page[pageSize-1]
			page = page[:pageSize]
		}

		collected = append(collected, page...)

		if last == "" {
			break
		}

		cursor = last
	}

	require.Equal(t, names, collected)
}

// TestFindStartIndex_DeletionTolerant confirms findStartIndex resumes
// correctly even when the exact cursor name no longer exists in the list
// (e.g. the table it named was dropped) -- it finds the first remaining name
// strictly greater than the cursor, rather than restarting or erroring.
func TestFindStartIndex_DeletionTolerant(t *testing.T) {
	t.Parallel()

	remaining := []string{"a", "b", "d", "e"}

	idx, found := findStartIndex(remaining, "c")
	require.True(t, found)
	assert.Equal(t, 2, idx)
	assert.Equal(t, "d", remaining[idx])
}

func TestFindStartIndex_PastEnd(t *testing.T) {
	t.Parallel()

	_, found := findStartIndex([]string{"a", "b", "c"}, "z")
	assert.False(t, found)
}

func TestFindStartIndex_Empty(t *testing.T) {
	t.Parallel()

	_, found := findStartIndex(nil, "a")
	assert.False(t, found)
}

func summariesWithArns(arns ...string) []models.BackupSummary {
	out := make([]models.BackupSummary, 0, len(arns))
	for i, a := range arns {
		out = append(out, models.BackupSummary{
			BackupArn:              a,
			BackupCreationDateTime: float64(i),
		})
	}

	return out
}

func arnsOf(s []models.BackupSummary) []string {
	out := make([]string, 0, len(s))
	for _, b := range s {
		out = append(out, b.BackupArn)
	}

	return out
}

func TestPaginateBackupSummaries_BoundaryWalk(t *testing.T) {
	t.Parallel()

	arns := make([]string, 0, 21)
	for i := range 21 {
		arns = append(arns, string(rune('a'+i)))
	}

	all := summariesWithArns(arns...)

	var collected []string

	cursor := ""
	for {
		page, next := paginateBackupSummaries(all, cursor, 5)
		collected = append(collected, arnsOf(page)...)

		if next == "" {
			break
		}

		cursor = next
	}

	require.Equal(t, arns, collected)
}

func TestPaginateBackupSummaries_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := summariesWithArns("a", "b", "c", "d")

	page1, tok1 := paginateBackupSummaries(all, "", 2)
	require.Equal(t, []string{"a", "b"}, arnsOf(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateBackupSummaries(all, tok1, 2)
	require.Equal(t, []string{"c", "d"}, arnsOf(page2))
	assert.Empty(t, tok2)
}

func TestPaginateBackupSummaries_SinglePage(t *testing.T) {
	t.Parallel()

	all := summariesWithArns("a", "b")

	page, tok := paginateBackupSummaries(all, "", 10)
	require.Equal(t, []string{"a", "b"}, arnsOf(page))
	assert.Empty(t, tok)
}

func TestPaginateBackupSummaries_Empty(t *testing.T) {
	t.Parallel()

	page, tok := paginateBackupSummaries(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateBackupSummaries_StaleCursorRestartsFromZero records observed
// (not asserted-correct) behaviour: paginateBackupSummaries locates the
// cursor by exact ARN match. When the named backup has since been deleted,
// the match fails and start silently falls back to 0, restarting pagination
// from the beginning rather than resuming past the deleted entry.
//
// This diverges from this package's own findStartIndex (used by ListTables),
// which is deletion-tolerant by construction: it searches for the first
// entry strictly greater than the cursor rather than an exact match, so a
// deleted cursor still resumes in the right place. paginateBackupSummaries
// cannot adopt that pattern directly because its sort order is a composite
// (CreationDateTime, BackupArn) key and the cursor carries only the ARN half
// -- reconstructing the correct resume position for a deleted ARN would
// require encoding the creation time in the cursor too, which AWS's
// LastEvaluatedBackupArn (a bare ARN string) does not leave room for. AWS
// does not document ListBackups' behaviour for a stale ExclusiveStartBackupArn,
// so this test pins the current behaviour rather than asserting it is right.
func TestPaginateBackupSummaries_StaleCursorRestartsFromZero(t *testing.T) {
	t.Parallel()

	all := summariesWithArns("a", "b", "c", "d", "e")

	page1, tok := paginateBackupSummaries(all, "", 2)
	require.Equal(t, []string{"a", "b"}, arnsOf(page1))
	require.Equal(t, "b", tok, "token names the last item of the page just returned")

	// "b" is deleted between calls.
	remaining := summariesWithArns("a", "c", "d", "e")

	page2, _ := paginateBackupSummaries(remaining, tok, 2)
	assert.Equal(t, []string{"a", "c"}, arnsOf(page2),
		"documented current behaviour: restarts from the beginning rather than resuming after the deleted cursor")
}
