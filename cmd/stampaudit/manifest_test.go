package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     string
		prefix      string
		wantValue   string
		wantComment string
		wantFound   bool
	}{
		{
			name:      "plain value no comment",
			content:   "service: dlm\nlast_audit_commit: abc1234\n",
			prefix:    commitFieldPrefix,
			wantValue: "abc1234",
			wantFound: true,
		},
		{
			name:        "value with trailing comment",
			content:     "last_audit_commit: fca4a71a1   # HEAD when this manifest was written\n",
			prefix:      commitFieldPrefix,
			wantValue:   "fca4a71a1",
			wantComment: "HEAD when this manifest was written",
			wantFound:   true,
		},
		{
			name:    "field absent",
			content: "service: dlm\nlast_audit_date: 2026-07-01\n",
			prefix:  commitFieldPrefix,
		},
		{
			name:      "date field",
			content:   "last_audit_date: 2026-08-10\n",
			prefix:    dateFieldPrefix,
			wantValue: "2026-08-10",
			wantFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			value, comment, found := extractField(tt.content, tt.prefix)

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantValue, value)
			assert.Equal(t, tt.wantComment, comment)
		})
	}
}

func TestParseManifest(t *testing.T) {
	t.Parallel()

	content := "service: dlm\n" +
		"last_audit_commit: fca4a71a1   # HEAD when this manifest was written\n" +
		"last_audit_date: 2026-08-10\n"

	m := parseManifest("dlm", content)

	assert.Equal(t, "dlm", m.service)
	assert.True(t, m.rawCommitFound)
	assert.Equal(t, "fca4a71a1", m.rawCommit)
	assert.True(t, m.rawDateFound)
	assert.Equal(t, "2026-08-10", m.rawDate)
}

func TestClassifyCitation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		raw        string
		wantSHA    string
		wantSuffix string
		wantKind   citationKind
		found      bool
	}{
		{
			name:     "plain short sha",
			raw:      "fca4a71a1",
			found:    true,
			wantKind: citationSHA,
			wantSHA:  "fca4a71a1",
		},
		{
			name:     "full 40-char sha",
			raw:      "1c6af314f4ed210dbc03be80042c6af2aa07448f",
			found:    true,
			wantKind: citationSHA,
			wantSHA:  "1c6af314f4ed210dbc03be80042c6af2aa07448f",
		},
		{
			name:       "sha with uncommitted suffix",
			raw:        "fba3c784+uncommitted",
			found:      true,
			wantKind:   citationSHA,
			wantSHA:    "fba3c784",
			wantSuffix: "+uncommitted",
		},
		{
			name:     "uppercase hex normalizes to lowercase match",
			raw:      "ABCDEF1",
			found:    true,
			wantKind: citationSHA,
			wantSHA:  "abcdef1",
		},
		{
			name:     "HEAD placeholder",
			raw:      "HEAD",
			found:    true,
			wantKind: citationPlaceholder,
		},
		{
			name:     "PENDING placeholder",
			raw:      "PENDING",
			found:    true,
			wantKind: citationPlaceholder,
		},
		{
			name:     "prose placeholder",
			raw:      "(uncommitted at time of writing)",
			found:    true,
			wantKind: citationPlaceholder,
		},
		{
			name:     "field not found",
			found:    false,
			wantKind: citationAbsent,
		},
		{
			name:     "field found but empty",
			raw:      "   ",
			found:    true,
			wantKind: citationAbsent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := manifest{rawCommit: tt.raw, rawCommitFound: tt.found}
			c := classifyCitation(m)

			require.Equal(t, tt.wantKind, c.kind)
			assert.Equal(t, tt.wantSHA, c.sha)
			assert.Equal(t, tt.wantSuffix, c.suffix)
		})
	}
}
