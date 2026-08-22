package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResultNote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantSub string
		r       result
	}{
		{
			name:    "absent",
			r:       result{outcome: outcomeAbsent},
			wantSub: "no last_audit_commit field",
		},
		{
			name:    "placeholder",
			r:       result{outcome: outcomePlaceholder, citation: citation{raw: "HEAD"}},
			wantSub: "no commit-sha prefix",
		},
		{
			name:    "missing",
			r:       result{outcome: outcomeMissing, citation: citation{sha: "deadbeef"}},
			wantSub: "not found in local repository",
		},
		{
			name: "resolved with suffix",
			r: result{
				outcome:  outcomeResolved,
				citation: citation{sha: "fba3c784", suffix: "+uncommitted"},
			},
			wantSub: "trailing",
		},
		{
			name:    "resolved clean has no note",
			r:       result{outcome: outcomeResolved, hasAuditDate: true},
			wantSub: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := resultNote(tt.r)

			if tt.wantSub == "" {
				assert.Empty(t, got)

				return
			}
			assert.Contains(t, got, tt.wantSub)
		})
	}
}

func TestOutcomeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		o    shaOutcome
	}{
		{name: "resolved", o: outcomeResolved, want: "resolved"},
		{name: "missing", o: outcomeMissing, want: "missing-object"},
		{name: "placeholder", o: outcomePlaceholder, want: "placeholder-value"},
		{name: "absent", o: outcomeAbsent, want: "no-field"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, outcomeString(tt.o))
		})
	}
}

func TestAppendSuggestionWordsAFailingSuggestionAsFailing(t *testing.T) {
	t.Parallel()

	r := result{
		citation:            citation{raw: "abc1234"},
		suggestedMergeBase:  "def5678",
		suggestedGapKnown:   true,
		suggestedGapDays:    30,
		suggestedPassesGate: false,
	}

	got := appendSuggestion("", r)

	assert.Contains(t, got, "STILL FAILS")
	assert.Contains(t, got, "do not use")
}

func TestAppendSuggestionWordsAPassingSuggestionAsPassing(t *testing.T) {
	t.Parallel()

	r := result{
		citation:            citation{raw: "abc1234"},
		suggestedMergeBase:  "def5678",
		suggestedGapKnown:   true,
		suggestedGapDays:    2,
		suggestedPassesGate: true,
	}

	got := appendSuggestion("", r)

	assert.Contains(t, got, "passes date test")
	assert.NotContains(t, got, "STILL FAILS")
}
