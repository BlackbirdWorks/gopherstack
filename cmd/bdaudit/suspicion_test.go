package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignificantWords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		title string
		want  []string
	}{
		{
			name:  "stopwords and short words dropped",
			title: "cloudformation UpdateStack never enforces the stored stack policy",
			want:  []string{"cloudformation", "updatestack", "enforces", "stored", "stack", "policy"},
		},
		{
			name:  "duplicate words deduped",
			title: "stack stack Stack",
			want:  []string{"stack"},
		},
		{
			name:  "all stopwords yields nothing",
			title: "the and or of",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, significantWords(tt.title))
		})
	}
}

func TestDiscoveredFromCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		issues  map[string]issue
		wantIDs []string
	}{
		{
			name: "open issue with topically overlapping closed parent is flagged",
			issues: map[string]issue{
				"gopherstack-cqy3": {
					ID:     "gopherstack-cqy3",
					Title:  "cloudformation UpdateStack never enforces the stored stack policy",
					Status: "open",
					Dependencies: []dependency{
						{IssueID: "gopherstack-cqy3", DependsOnID: "gopherstack-ygfk", Type: depTypeDiscoveredFrom},
					},
				},
				"gopherstack-ygfk": {
					ID:     "gopherstack-ygfk",
					Status: statusClosed,
					CloseReason: "FIXED: cloudformation stack policies stored, echoed and never consulted " +
						"by UpdateStack.",
				},
			},
			wantIDs: []string{"gopherstack-cqy3"},
		},
		{
			name: "parent naming the child id explicitly is excluded",
			issues: map[string]issue{
				"gopherstack-glfv": {
					ID:     "gopherstack-glfv",
					Title:  "dynamodb ReturnConsumedCapacity=INDEXES never returns per-index breakdown",
					Status: "open",
					Dependencies: []dependency{
						{IssueID: "gopherstack-glfv", DependsOnID: "gopherstack-rkmp", Type: depTypeDiscoveredFrom},
					},
				},
				"gopherstack-rkmp": {
					ID:     "gopherstack-rkmp",
					Status: statusClosed,
					CloseReason: "FLAGGED, not fixed: gopherstack-glfv (P3) ReturnConsumedCapacity=INDEXES " +
						"never returns a per-index breakdown.",
				},
			},
			wantIDs: nil,
		},
		{
			name: "closed child is never a candidate",
			issues: map[string]issue{
				"gopherstack-done": {
					ID:     "gopherstack-done",
					Title:  "cloudformation stack policy bug",
					Status: statusClosed,
					Dependencies: []dependency{
						{IssueID: "gopherstack-done", DependsOnID: "gopherstack-parent", Type: depTypeDiscoveredFrom},
					},
				},
				"gopherstack-parent": {
					ID:          "gopherstack-parent",
					Status:      statusClosed,
					CloseReason: "cloudformation stack policy bug fixed",
				},
			},
			wantIDs: nil,
		},
		{
			name: "open parent is never a candidate source",
			issues: map[string]issue{
				"gopherstack-child": {
					ID:     "gopherstack-child",
					Title:  "cloudformation stack policy bug",
					Status: "open",
					Dependencies: []dependency{
						{IssueID: "gopherstack-child", DependsOnID: "gopherstack-parent", Type: depTypeDiscoveredFrom},
					},
				},
				"gopherstack-parent": {
					ID:          "gopherstack-parent",
					Status:      "open",
					CloseReason: "",
				},
			},
			wantIDs: nil,
		},
		{
			name: "non discovered-from dependency type is ignored",
			issues: map[string]issue{
				"gopherstack-child": {
					ID:     "gopherstack-child",
					Title:  "cloudformation stack policy bug enforcement",
					Status: "open",
					Dependencies: []dependency{
						{IssueID: "gopherstack-child", DependsOnID: "gopherstack-parent", Type: "related"},
					},
				},
				"gopherstack-parent": {
					ID:          "gopherstack-parent",
					Status:      statusClosed,
					CloseReason: "cloudformation stack policy bug enforcement fixed",
				},
			},
			wantIDs: nil,
		},
		{
			name: "below overlap threshold is not flagged",
			issues: map[string]issue{
				"gopherstack-child": {
					ID:     "gopherstack-child",
					Title:  "unrelated dynamodb pagination bug",
					Status: "open",
					Dependencies: []dependency{
						{IssueID: "gopherstack-child", DependsOnID: "gopherstack-parent", Type: depTypeDiscoveredFrom},
					},
				},
				"gopherstack-parent": {
					ID:          "gopherstack-parent",
					Status:      statusClosed,
					CloseReason: "cloudformation stack policy bug fixed",
				},
			},
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := discoveredFromCandidates(tt.issues, defaultMinOverlap)
			assert.Equal(t, tt.wantIDs, suspicionIDs(rows))
		})
	}
}

func TestStaleRefCandidates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "services", "example"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "services", "example", "handler.go"),
		[]byte("line1\nline2\nline3\n"),
		0o600,
	))

	tests := []struct {
		name    string
		wantIDs []string
		issue   issue
	}{
		{
			name: "missing file is flagged",
			issue: issue{
				ID:          "gopherstack-a",
				Status:      "open",
				Description: "see services/example/gone.go:4 for the bug",
			},
			wantIDs: []string{"gopherstack-a"},
		},
		{
			name: "cited line beyond file length is flagged",
			issue: issue{
				ID:          "gopherstack-b",
				Status:      "open",
				Description: "see services/example/handler.go:99 for the bug",
			},
			wantIDs: []string{"gopherstack-b"},
		},
		{
			name: "cited line within file length is quiet",
			issue: issue{
				ID:          "gopherstack-c",
				Status:      "open",
				Description: "see services/example/handler.go:2 for the bug",
			},
			wantIDs: nil,
		},
		{
			name: "path without a known repo top dir is ignored",
			issue: issue{
				ID:          "gopherstack-d",
				Status:      "open",
				Description: "see vendor/types.go:9999 in the SDK",
			},
			wantIDs: nil,
		},
		{
			name: "closed issue is never scanned",
			issue: issue{
				ID:          "gopherstack-e",
				Status:      statusClosed,
				Description: "see services/example/gone.go:4 for the bug",
			},
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issues := map[string]issue{tt.issue.ID: tt.issue}
			rows := staleRefCandidates(issues, dir)
			assert.Equal(t, tt.wantIDs, suspicionIDs(rows))
		})
	}
}

func TestSuspicionPassRanksByScore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	issues := map[string]issue{
		"gopherstack-low": {
			ID:     "gopherstack-low",
			Title:  "dynamodb global tables autoscaling settings drop",
			Status: "open",
			Dependencies: []dependency{
				{IssueID: "gopherstack-low", DependsOnID: "gopherstack-lowparent", Type: depTypeDiscoveredFrom},
			},
		},
		"gopherstack-lowparent": {
			ID:          "gopherstack-lowparent",
			Status:      statusClosed,
			CloseReason: "dynamodb global tables autoscaling fixed",
		},
		"gopherstack-high": {
			ID:     "gopherstack-high",
			Title:  "cloudformation UpdateStack never enforces the stored stack policy document",
			Status: "open",
			Dependencies: []dependency{
				{IssueID: "gopherstack-high", DependsOnID: "gopherstack-highparent", Type: depTypeDiscoveredFrom},
			},
		},
		"gopherstack-highparent": {
			ID:     "gopherstack-highparent",
			Status: statusClosed,
			CloseReason: "FIXED: cloudformation stack policies stored, echoed and never consulted by " +
				"UpdateStack -- the policy document is now evaluated.",
		},
	}

	rows := suspicionPass(issues, dir, defaultMinOverlap)

	require.Len(t, rows, 2)
	assert.Equal(t, "gopherstack-high", rows[0].IssueID)
	assert.Equal(t, "gopherstack-low", rows[1].IssueID)
	assert.GreaterOrEqual(t, rows[0].Score, rows[1].Score)
}

func suspicionIDs(rows []suspicionRow) []string {
	if len(rows) == 0 {
		return nil
	}

	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.IssueID
	}

	return ids
}
