package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintParityOnly(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{
			Service: "transcribe", Class: "filter_default_semantics", Verdict: "clean",
			Date: "2026-08-30", Commit: "20ac224ab", Source: "parity+bd_comment",
		},
		{
			Service: "s3", Class: "filter_default_semantics", Verdict: "clean",
			Date: "2026-08-30", Commit: "6c73794e2", Source: "bd_comment",
		},
		{
			Service: "swf", Class: "filter_default_semantics", Verdict: "clean",
			Date: "2026-08-30", Commit: "0fdecf5cc", Source: "parity",
		},
	}

	tests := []struct {
		name       string
		rows       []Row
		wantHeader string
		wantIn     []string
		wantOut    []string
	}{
		{
			name:       "mixed sources: only the parity-only row is listed",
			rows:       rows,
			wantHeader: "1 row(s)",
			wantIn:     []string{"swf"},
			wantOut:    []string{"transcribe", "s3"},
		},
		{
			name:       "no rows at all",
			rows:       nil,
			wantHeader: "0 row(s)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			printParityOnly(&buf, tt.rows)

			out := buf.String()
			assert.Contains(t, out, tt.wantHeader)

			for _, s := range tt.wantIn {
				assert.Contains(t, out, s)
			}

			for _, s := range tt.wantOut {
				assert.NotContains(t, out, s)
			}
		})
	}
}

// TestRealLedger_CleanVerdictsCarryProvenance guards gopherstack-ri57: a
// clean verdict whose evidence rests on PARITY.md and/or a bd comment,
// with no code diff naming the service in a commit subject, must still
// appear as a row -- and must carry a Source tag saying so, rather than
// reading as equal-confidence to a commit-verified row.
func TestRealLedger_CleanVerdictsCarryProvenance(t *testing.T) {
	t.Parallel()

	repoRoot, err := repoRootDir()
	require.NoError(t, err)

	rows, err := LoadLedger(filepath.Join(repoRoot, "cmd", "covledger", "coverage.yaml"))
	require.NoError(t, err)

	tests := []struct {
		name       string
		service    string
		wantSource string
	}{
		{
			name:       "transcribe: parity plus the uox6 pass-10 comment",
			service:    "transcribe",
			wantSource: "parity+bd_comment",
		},
		{
			name:       "s3: clean verdict rode along in the securityhub fix commit",
			service:    "s3",
			wantSource: "bd_comment",
		},
		{
			name:       "appmesh: clean verdict rode along in the iotwireless/shield fix commit",
			service:    "appmesh",
			wantSource: "bd_comment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svcRows := RowsForService(rows, tt.service)
			require.NotEmpty(t, svcRows, "%s must have at least one row", tt.service)

			row := svcRows[0]
			assert.Equal(t, "filter_default_semantics", row.Class)
			assert.Equal(t, "clean", row.Verdict)
			assert.Equal(t, tt.wantSource, row.Source)
		})
	}
}
