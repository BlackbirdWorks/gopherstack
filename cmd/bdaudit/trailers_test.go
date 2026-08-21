package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractClosedIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "single id with period",
			body: "fix(pipes): read the DLQ from source params\n\nCloses gopherstack-6ffg.\n",
			want: []string{"gopherstack-6ffg"},
		},
		{
			name: "trailer mid sentence stops at the aside",
			body: "Closes gopherstack-c7s3 -- and corrects that issue's root cause, which I got wrong.",
			want: []string{"gopherstack-c7s3"},
		},
		{
			name: "and-joined pair stops before an unrelated list",
			body: "Closes gopherstack-6flj and gopherstack-21my, and records the follow-ups filed " +
				"during the sweep: gopherstack-cnhp, gopherstack-z31a and gopherstack-mtqf.",
			want: []string{"gopherstack-21my", "gopherstack-6flj"},
		},
		{
			name: "repeated keyword comma chain",
			body: "Closes gopherstack-3bsb, closes gopherstack-u9e5, closes gopherstack-7mmd",
			want: []string{"gopherstack-3bsb", "gopherstack-7mmd", "gopherstack-u9e5"},
		},
		{
			name: "lowercase keyword",
			body: "closes gopherstack-1gfi",
			want: []string{"gopherstack-1gfi"},
		},
		{
			name: "fixes and resolves both recognized",
			body: "Fixes gopherstack-0mtk, a silent bug.\n\nResolves gopherstack-28ce as well.",
			want: []string{"gopherstack-0mtk", "gopherstack-28ce"},
		},
		{
			name: "refs trailer is not a closing trailer",
			body: "Refs: gopherstack-6ffg, gopherstack-0bpp",
			want: nil,
		},
		{
			name: "bare close without s is not a closing trailer",
			body: "chore(beads): close gopherstack-c7s3 and file the follow-up",
			want: nil,
		},
		{
			name: "duplicate id across two trailers dedupes",
			body: "Closes gopherstack-abcd.\n\nAlso fixes gopherstack-abcd for good measure.",
			want: []string{"gopherstack-abcd"},
		},
		{
			name: "no trailer at all",
			body: "refactor(bedrock): split giant files by op-family",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := extractClosedIDs(tt.body)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestScanTrailers(t *testing.T) {
	t.Parallel()

	issues := map[string]issue{
		"gopherstack-open1":   {ID: "gopherstack-open1", Status: "open"},
		"gopherstack-closed1": {ID: "gopherstack-closed1", Status: statusClosed},
		"gopherstack-inprog":  {ID: "gopherstack-inprog", Status: "in_progress"},
	}

	tests := []struct {
		name          string
		commits       []gitCommit
		wantNotClosed []string
		wantTypos     []string
	}{
		{
			name: "closed trailer for still-open issue is flagged",
			commits: []gitCommit{
				{Hash: "aaa", Body: "Closes gopherstack-open1."},
			},
			wantNotClosed: []string{"gopherstack-open1"},
		},
		{
			name: "closed trailer for already-closed issue is quiet",
			commits: []gitCommit{
				{Hash: "bbb", Body: "Closes gopherstack-closed1."},
			},
		},
		{
			name: "in-progress counts as not closed",
			commits: []gitCommit{
				{Hash: "ccc", Body: "Fixes gopherstack-inprog."},
			},
			wantNotClosed: []string{"gopherstack-inprog"},
		},
		{
			name: "unknown id is a typo, not a not-closed finding",
			commits: []gitCommit{
				{Hash: "ddd", Body: "Closes gopherstack-nope."},
			},
			wantTypos: []string{"gopherstack-nope"},
		},
		{
			name: "no commits produces no findings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			notClosed, typos := scanTrailers(tt.commits, issues)

			assert.Equal(t, tt.wantNotClosed, idsOf(notClosed))
			assert.Equal(t, tt.wantTypos, idsOf(typos))
		})
	}
}

func idsOf(findings []trailerFinding) []string {
	if len(findings) == 0 {
		return nil
	}

	ids := make([]string, len(findings))
	for i, f := range findings {
		ids[i] = f.IssueID
	}

	return ids
}
