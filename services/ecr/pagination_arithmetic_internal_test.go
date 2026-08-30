package ecr

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func digestImages(digests ...string) []Image {
	out := make([]Image, 0, len(digests))
	for _, d := range digests {
		out = append(out, Image{ImageDigest: d, ImageStatus: imageStatusActive})
	}

	return out
}

func imageDigests(imgs []Image) []string {
	out := make([]string, 0, len(imgs))
	for _, img := range imgs {
		out = append(out, img.ImageDigest)
	}

	return out
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// ── filterAndPaginateImages (DescribeImages) ────────────────────────────

func TestFilterAndPaginateImages_BoundaryWalk(t *testing.T) {
	t.Parallel()

	digests := []string{"d0", "d1", "d2", "d3", "d4", "d5", "d6"}
	all := digestImages(digests...)

	const maxResults = 3

	var collected []string

	token := ""
	for {
		imgs := filterAndPaginateImages(all, nil, token)

		var page []Image
		if len(imgs) > maxResults {
			page = imgs[:maxResults]
		} else {
			page = imgs
		}

		collected = append(collected, imageDigests(page)...)

		if len(imgs) <= maxResults {
			break
		}

		token = b64(imgs[maxResults].ImageDigest)
	}

	require.Equal(t, digests, collected)
}

func TestFilterAndPaginateImages_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := digestImages("d0", "d1")
	imgs := filterAndPaginateImages(all, nil, "")
	assert.Equal(t, []string{"d0", "d1"}, imageDigests(imgs))
}

func TestFilterAndPaginateImages_EmptyCollection(t *testing.T) {
	t.Parallel()

	imgs := filterAndPaginateImages(nil, nil, "")
	assert.Empty(t, imgs)
}

func TestFilterAndPaginateImages_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	all := digestImages("d0", "d1", "d2")
	token := b64("d1")
	imgs := filterAndPaginateImages(all, nil, token)
	assert.Equal(t, []string{"d1", "d2"}, imageDigests(imgs))
}

// TestFilterAndPaginateImages_StaleCursor_DeletedItem reproduces the case a
// deletion between DescribeImages calls triggers: the digest the cursor
// names is gone from the current image set. The helper must resume after
// where that digest would have sorted, not silently restart at the front of
// the (already sorted) list.
func TestFilterAndPaginateImages_StaleCursor_DeletedItem(t *testing.T) {
	t.Parallel()

	// d1 was the resume point but has since been deleted.
	remaining := digestImages("d0", "d2", "d3")
	token := b64("d1")

	imgs := filterAndPaginateImages(remaining, nil, token)
	assert.Equal(t, []string{"d2", "d3"}, imageDigests(imgs),
		"must resume after the deleted digest's sort position, not restart at page one")
}

func TestFilterAndPaginateImages_TamperedCursor_NoMatch(t *testing.T) {
	t.Parallel()

	all := digestImages("d0", "d1", "d2")
	imgs := filterAndPaginateImages(all, nil, b64("zzz-does-not-exist"))
	assert.Empty(t, imgs)
}

// ── paginateLifecyclePreviewEntries (GetLifecyclePolicyPreview) ─────────

func previewEntries(digests ...string) []LifecyclePolicyPreviewEntry {
	out := make([]LifecyclePolicyPreviewEntry, 0, len(digests))
	for _, d := range digests {
		out = append(out, LifecyclePolicyPreviewEntry{ImageDigest: d})
	}

	return out
}

func previewDigests(entries []LifecyclePolicyPreviewEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.ImageDigest)
	}

	return out
}

func TestPaginateLifecyclePreviewEntries_BoundaryWalk(t *testing.T) {
	t.Parallel()

	digests := []string{"d0", "d1", "d2", "d3", "d4"}
	all := previewEntries(digests...)

	var collected []string

	token := ""
	for {
		page, next := paginateLifecyclePreviewEntries(all, token, 2)
		collected = append(collected, previewDigests(page)...)

		if next == "" {
			break
		}

		token = next
		// The helper re-slices from the full (unfiltered) entries each
		// call, as the real handler does: it re-fetches "entries" fresh.
	}

	require.Equal(t, digests, collected)
}

func TestPaginateLifecyclePreviewEntries_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	all := previewEntries("d0", "d1", "d2", "d3")

	page1, tok1 := paginateLifecyclePreviewEntries(all, "", 2)
	require.Equal(t, []string{"d0", "d1"}, previewDigests(page1))
	require.NotEmpty(t, tok1)

	page2, tok2 := paginateLifecyclePreviewEntries(all, tok1, 2)
	assert.Equal(t, []string{"d2", "d3"}, previewDigests(page2))
	assert.Empty(t, tok2)
}

func TestPaginateLifecyclePreviewEntries_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	all := previewEntries("d0", "d1")
	page, tok := paginateLifecyclePreviewEntries(all, "", 10)
	assert.Equal(t, []string{"d0", "d1"}, previewDigests(page))
	assert.Empty(t, tok)
}

func TestPaginateLifecyclePreviewEntries_EmptyCollection(t *testing.T) {
	t.Parallel()

	page, tok := paginateLifecyclePreviewEntries(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// TestPaginateLifecyclePreviewEntries_StaleCursor_DeletedItem: entries here
// are sorted by ImagePushedAt (push time), not by ImageDigest -- the digest
// cursor has no ordering relationship to list position, so unlike the
// digest-sorted helpers above, a miss cannot be resolved by resuming
// "after" the missing digest. The only safe answer is an empty page, never
// a restart at page one.
func TestPaginateLifecyclePreviewEntries_StaleCursor_DeletedItem(t *testing.T) {
	t.Parallel()

	remaining := previewEntries("d0", "d2", "d3") // d1 deleted
	page, tok := paginateLifecyclePreviewEntries(remaining, b64("d1"), 10)
	assert.Empty(t, page, "a cursor with no valid resume point must terminate, not restart at page one")
	assert.Empty(t, tok)
}

func TestPaginateLifecyclePreviewEntries_TamperedCursor_Malformed(t *testing.T) {
	t.Parallel()

	all := previewEntries("d0", "d1", "d2")
	page, tok := paginateLifecyclePreviewEntries(all, "not-valid-base64!!!", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

// ── paginatePullTimeUpdateExclusions (ListPullTimeUpdateExclusions) ─────

func TestPaginatePullTimeUpdateExclusions_BoundaryWalk(t *testing.T) {
	t.Parallel()

	arns := []string{"arn:0", "arn:1", "arn:2", "arn:3", "arn:4"}

	var collected []string

	token := ""
	for {
		page, next := paginatePullTimeUpdateExclusions(arns, token, 2)
		collected = append(collected, page...)

		if next == "" {
			break
		}

		token = next
	}

	require.Equal(t, arns, collected)
}

func TestPaginatePullTimeUpdateExclusions_ExactDivisionNoTrailingCursor(t *testing.T) {
	t.Parallel()

	arns := []string{"arn:0", "arn:1", "arn:2", "arn:3"}

	page1, tok1 := paginatePullTimeUpdateExclusions(arns, "", 2)
	require.Equal(t, []string{"arn:0", "arn:1"}, page1)
	require.NotEmpty(t, tok1)

	page2, tok2 := paginatePullTimeUpdateExclusions(arns, tok1, 2)
	assert.Equal(t, []string{"arn:2", "arn:3"}, page2)
	assert.Empty(t, tok2)
}

func TestPaginatePullTimeUpdateExclusions_SinglePageNoCursor(t *testing.T) {
	t.Parallel()

	arns := []string{"arn:0", "arn:1"}
	page, tok := paginatePullTimeUpdateExclusions(arns, "", 10)
	assert.Equal(t, arns, page)
	assert.Empty(t, tok)
}

func TestPaginatePullTimeUpdateExclusions_EmptyCollection(t *testing.T) {
	t.Parallel()

	page, tok := paginatePullTimeUpdateExclusions(nil, "", 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}

func TestPaginatePullTimeUpdateExclusions_CursorRoundTrip(t *testing.T) {
	t.Parallel()

	arns := []string{"arn:0", "arn:1", "arn:2"}
	token := b64("arn:1")
	page, _ := paginatePullTimeUpdateExclusions(arns, token, 10)
	assert.Equal(t, []string{"arn:1", "arn:2"}, page)
}

func TestPaginatePullTimeUpdateExclusions_StaleCursor_DeletedItem(t *testing.T) {
	t.Parallel()

	// arn:1 was the resume point, since removed from the exclusion list.
	remaining := []string{"arn:0", "arn:2", "arn:3"}
	page, _ := paginatePullTimeUpdateExclusions(remaining, b64("arn:1"), 10)
	assert.Equal(t, []string{"arn:2", "arn:3"}, page,
		"must resume after the deleted ARN's sort position, not restart at page one")
}

func TestPaginatePullTimeUpdateExclusions_TamperedCursor_NoMatch(t *testing.T) {
	t.Parallel()

	arns := []string{"arn:0", "arn:1", "arn:2"}
	page, tok := paginatePullTimeUpdateExclusions(arns, b64("zzz-does-not-exist"), 10)
	assert.Empty(t, page)
	assert.Empty(t, tok)
}
