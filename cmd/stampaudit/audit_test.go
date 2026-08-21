package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateGapDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		commitDate string
		auditDate  string
		want       int
	}{
		{name: "same day", commitDate: "2026-07-01T00:00:00Z", auditDate: "2026-07-01", want: 0},
		{name: "audit a week later", commitDate: "2026-07-01T00:00:00Z", auditDate: "2026-07-08", want: 7},
		{name: "audit a month later", commitDate: "2026-07-01T00:00:00Z", auditDate: "2026-08-01", want: 31},
		{name: "commit after audit date", commitDate: "2026-07-10T00:00:00Z", auditDate: "2026-07-01", want: -9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commit, err := time.Parse(time.RFC3339, tt.commitDate)
			require.NoError(t, err)
			audit, err := time.Parse(auditDateLayout, tt.auditDate)
			require.NoError(t, err)

			assert.Equal(t, tt.want, dateGapDays(commit, audit))
		})
	}
}

func TestStaleDatePredicate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		gapDays   int
		threshold int
		want      bool
	}{
		{name: "well within threshold", gapDays: 3, threshold: 7, want: false},
		{name: "exactly at threshold is not stale", gapDays: 7, threshold: 7, want: false},
		{name: "one day over threshold", gapDays: 8, threshold: 7, want: true},
		{name: "far over threshold", gapDays: 34, threshold: 7, want: true},
		{name: "negative gap is not stale", gapDays: -5, threshold: 7, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, staleDatePredicate(tt.gapDays, tt.threshold))
		})
	}
}

const threshold = 7

// setupAuditRepo builds a small repo with one reachable commit on main and
// one unmerged branch tip, returning the runner and both shas.
func setupAuditRepo(t *testing.T) (gitRunner, string, string) {
	t.Helper()

	g := newTestRepo(t)
	mainSHA := commitAt(t, g.dir, "a", "2026-07-01T00:00:00Z")
	runGit(t, g.dir, "checkout", "-q", "-b", "feature")
	branchSHA := commitAt(t, g.dir, "b", "2026-07-05T00:00:00Z")

	return g, mainSHA, branchSHA
}

func TestAuditOne(t *testing.T) {
	t.Parallel()

	g, mainSHA, branchSHA := setupAuditRepo(t)

	tests := []struct {
		name            string
		m               manifest
		wantOutcome     shaOutcome
		wantUnreachable bool
		wantStaleDate   bool
	}{
		{
			name: "reachable and recent is clean",
			m: manifest{
				service: "clean-svc", rawCommit: mainSHA, rawCommitFound: true,
				rawDate: "2026-07-02", rawDateFound: true,
			},
			wantOutcome: outcomeResolved,
		},
		{
			name: "unmerged branch tip is unreachable but not stale",
			m: manifest{
				service: "unreachable-svc", rawCommit: branchSHA, rawCommitFound: true,
				rawDate: "2026-07-06", rawDateFound: true,
			},
			wantOutcome:     outcomeResolved,
			wantUnreachable: true,
		},
		{
			name: "unmerged branch tip with a stale date is both",
			m: manifest{
				service: "both-svc", rawCommit: branchSHA, rawCommitFound: true,
				rawDate: "2026-08-01", rawDateFound: true,
			},
			wantOutcome:     outcomeResolved,
			wantUnreachable: true,
			wantStaleDate:   true,
		},
		{
			name: "unknown sha is missing",
			m: manifest{
				service: "missing-svc", rawCommit: "deadbeef", rawCommitFound: true,
				rawDate: "2026-07-06", rawDateFound: true,
			},
			wantOutcome: outcomeMissing,
		},
		{
			name: "placeholder value",
			m: manifest{
				service: "placeholder-svc", rawCommit: "PENDING", rawCommitFound: true,
				rawDate: "2026-07-06", rawDateFound: true,
			},
			wantOutcome: outcomePlaceholder,
		},
		{
			name:        "absent field",
			m:           manifest{service: "absent-svc"},
			wantOutcome: outcomeAbsent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := auditOne(g, "main", threshold, tt.m)

			assert.Equal(t, tt.wantOutcome, r.outcome)
			assert.Equal(t, tt.wantUnreachable, r.unreachable)
			assert.Equal(t, tt.wantStaleDate, r.staleDate)
		})
	}
}

func TestAuditOneSuggestionValidatesItsOwnGate(t *testing.T) {
	t.Parallel()

	g, _, branchSHA := setupAuditRepo(t)

	// The merge-base of the branch tip and main is the "a" commit
	// (2026-07-01). An audit date far enough past that to fail the date
	// test on the ORIGINAL sha should also fail it on the suggestion,
	// since they resolve to a nearby date here -- the suggestion must
	// say so rather than presenting itself as a fix.
	m := manifest{
		service: "svc", rawCommit: branchSHA, rawCommitFound: true,
		rawDate: "2026-08-01", rawDateFound: true,
	}

	r := auditOne(g, "main", threshold, m)

	require.True(t, r.unreachable)
	require.NotEmpty(t, r.suggestedMergeBase)
	require.True(t, r.suggestedGapKnown)
	assert.False(t, r.suggestedPassesGate,
		"suggestion built from a commit 31 days before the audit date should still fail the date test")
}

func TestComputeSummary(t *testing.T) {
	t.Parallel()

	results := []result{
		{outcome: outcomeResolved, reachabilityKnown: true, reachable: true, gapKnown: true},
		{outcome: outcomeResolved, unreachable: true, reachabilityKnown: true},
		{outcome: outcomeResolved, unreachable: true, staleDate: true, reachabilityKnown: true, gapKnown: true},
		{outcome: outcomeMissing},
		{outcome: outcomePlaceholder},
		{outcome: outcomeAbsent},
	}

	s := computeSummary(results)

	assert.Equal(t, 6, s.total)
	assert.Equal(t, 3, s.resolved)
	assert.Equal(t, 1, s.clean)
	assert.Equal(t, 1, s.unreachable)
	assert.Equal(t, 1, s.both)
	assert.Equal(t, 1, s.missing)
	assert.Equal(t, 1, s.placeholder)
	assert.Equal(t, 1, s.absent)
}
