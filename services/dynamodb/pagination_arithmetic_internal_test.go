package dynamodb

import (
	"testing"
	"time"

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

// TestPaginateBackupSummaries_StaleCursorRestartsFromZero covers the
// fallback path for a cursor that doesn't even parse as a gopherstack backup
// ARN (backupArnCreatedAtSeconds finds no "/backup/{millis}-" segment to
// recover a sort position from): resumeIndexAfterBackupCursor falls back to
// 0, matching this package's pre-gopherstack-zdwf behaviour for that case.
// The letter-only ARNs here ("a", "b", ...) are deliberately not
// gopherstack-shaped, to exercise exactly this fallback.
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
		"non-ARN cursor: falls back to restarting from the beginning")
}

// TestPaginateBackupSummaries_StaleCursorResumesFromEmbeddedTimestamp covers
// gopherstack-zdwf: when ExclusiveStartBackupArn names a backup deleted since
// the cursor was issued, gopherstack recovers the (CreationDateTime,
// BackupArn) sort position from the creation timestamp its own ARN format
// embeds (backupARN, backup_ops.go:24-33), instead of silently restarting at
// page one.
func TestPaginateBackupSummaries_StaleCursorResumesFromEmbeddedTimestamp(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	buildRealARNSummaries := func(count int) []models.BackupSummary {
		out := make([]models.BackupSummary, 0, count)
		for i := range count {
			ts := base.Add(time.Duration(i) * time.Second)
			out = append(out, models.BackupSummary{
				BackupArn:              backupARN("us-east-1", "000000000000", "tbl", ts),
				BackupCreationDateTime: float64(ts.Unix()),
			})
		}

		return out
	}

	tests := []struct {
		name           string
		wantAfterPage  []int
		firstPageSize  int
		deleteIndex    int
		secondPageSize int
	}{
		{
			name:          "deleted_cursor_resumes_after_its_position",
			firstPageSize: 2, deleteIndex: 1, secondPageSize: 2,
			wantAfterPage: []int{2, 3},
		},
		{
			name:          "deleted_last_cursor_resumes_at_end",
			firstPageSize: 4, deleteIndex: 3, secondPageSize: 2,
			wantAfterPage: []int{4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			all := buildRealARNSummaries(5)

			_, tok := paginateBackupSummaries(all, "", tt.firstPageSize)
			require.NotEmpty(t, tok)

			cursorArn := all[tt.deleteIndex].BackupArn
			require.Equal(t, cursorArn, tok, "test setup: cursor must name the last item of the first page")

			remaining := make([]models.BackupSummary, 0, len(all)-1)
			for i, s := range all {
				if i != tt.deleteIndex {
					remaining = append(remaining, s)
				}
			}

			page2, _ := paginateBackupSummaries(remaining, tok, tt.secondPageSize)

			want := make([]string, 0, len(tt.wantAfterPage))
			for _, i := range tt.wantAfterPage {
				want = append(want, all[i].BackupArn)
			}

			assert.Equal(t, want, arnsOf(page2))
		})
	}
}
