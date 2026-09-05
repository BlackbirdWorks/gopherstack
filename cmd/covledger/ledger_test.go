package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowsForService(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{Service: "opensearch", Class: "wrong_wire_key", Verdict: "fixed", Date: "2026-08-29", Commit: "a576f56ca"},
		{
			Service: "opensearch",
			Class:   "filter_default_semantics",
			Verdict: "clean",
			Date:    "2026-08-30",
			Commit:  "ac5c674d2",
		},
		{
			Service: "opensearch",
			Class:   "pagination_ordering",
			Verdict: "fixed",
			Date:    "2026-08-30",
			Commit:  "3e2998719",
		},
		{
			Service: "medialive",
			Class:   "request_field_never_read",
			Verdict: "fixed",
			Date:    "2026-08-29",
			Commit:  "39a3c1453",
		},
	}

	tests := []struct {
		name    string
		service string
		want    []string // classes, in expected order
	}{
		{
			name:    "service with rows for several classes",
			service: "opensearch",
			want:    []string{"filter_default_semantics", "pagination_ordering", "wrong_wire_key"},
		},
		{
			name:    "service with exactly one row",
			service: "medialive",
			want:    []string{"request_field_never_read"},
		},
		{
			name:    "service with no rows at all",
			service: "rds",
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := RowsForService(rows, tt.service)

			gotClasses := make([]string, len(got))
			for i, r := range got {
				gotClasses[i] = r.Class
			}

			if tt.want == nil {
				assert.Empty(t, gotClasses)

				return
			}

			assert.Equal(t, tt.want, gotClasses)
		})
	}
}

func TestMissingForClass(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{Service: "opensearch", Class: "wrong_wire_key", Verdict: "fixed", Date: "2026-08-29", Commit: "a576f56ca"},
		{Service: "medialive", Class: "wrong_wire_key", Verdict: "clean", Date: "2026-08-29", Commit: "39a3c1453"},
		{
			Service: "opensearch",
			Class:   "pagination_ordering",
			Verdict: "fixed",
			Date:    "2026-08-30",
			Commit:  "3e2998719",
		},
	}
	allServices := []string{"opensearch", "medialive", "personalize", "rds"}

	tests := []struct {
		name  string
		class string
		want  []string
	}{
		{
			name:  "a service with no row for this class is reported missing",
			class: "wrong_wire_key",
			want:  []string{"personalize", "rds"},
		},
		{
			name:  "a class with only one covered service leaves the rest missing",
			class: "pagination_ordering",
			want:  []string{"medialive", "personalize", "rds"},
		},
		{
			name:  "a class with no rows at all reports every service missing",
			class: "fabricated_error_code",
			want:  []string{"medialive", "opensearch", "personalize", "rds"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MissingForClass(rows, tt.class, allServices)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRowsSourcedOnly(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{
			Service: "swf", Class: "filter_default_semantics", Verdict: "clean",
			Date: "2026-08-30", Commit: "0fdecf5cc", Source: "parity",
		},
		{
			Service: "codeartifact", Class: "filter_default_semantics", Verdict: "fixed", Date: "2026-08-30",
			Commit: "c75ee725b", Source: "commit+parity",
		},
		{
			Service: "docdb",
			Class:   "filter_default_semantics",
			Verdict: "clean",
			Date:    "2026-08-31",
			Commit:  "30d61130d",
			Source:  "bd_comment",
		},
		{Service: "acm", Class: "pagination_ordering", Verdict: "clean", Date: "2026-08-30", Commit: "e263119ce"},
	}

	tests := []struct {
		name   string
		source string
		want   []string // services
	}{
		{name: "parity-only rows", source: "parity", want: []string{"swf"}},
		{name: "bd_comment-only rows", source: "bd_comment", want: []string{"docdb"}},
		{name: "multi-source rows never match a single-tag query", source: "commit", want: nil},
		{name: "empty legacy source", source: "", want: []string{"acm"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := RowsSourcedOnly(rows, tt.source)

			gotServices := make([]string, len(got))
			for i, r := range got {
				gotServices[i] = r.Service
			}

			if tt.want == nil {
				assert.Empty(t, gotServices)

				return
			}

			assert.Equal(t, tt.want, gotServices)
		})
	}
}

func TestLoadConflicts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "coverage.yaml")

	content := `rows:
  - service: opensearch
    class: wrong_wire_key
    verdict: fixed
    date: "2026-08-29"
    commit: a576f56ca
conflicts:
  - service: medialive
    class: filter_default_semantics
    note: "PARITY.md says clean, bd comment on gopherstack-uox6 says a bug was fixed here"
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	conflicts, err := LoadConflicts(path)
	require.NoError(t, err)
	require.Len(t, conflicts, 1)
	assert.Equal(t, "medialive", conflicts[0].Service)
	assert.Equal(t, "filter_default_semantics", conflicts[0].Class)
	assert.NotEmpty(t, conflicts[0].Note)
}

func TestLoadLedger_SourceAndReasoningRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "coverage.yaml")

	content := `rows:
  - service: personalize
    class: filter_default_semantics
    verdict: inapplicable
    date: "2026-08-30"
    commit: ac5c674d2
    source: parity
    reasoning: "recipeProvider has exactly one legal value (SERVICE), so no legal value could change the result"
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	rows, err := LoadLedger(path)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "parity", rows[0].Source)
	assert.Contains(t, rows[0].Reasoning, "exactly one legal value")
}

func TestLoadLedger(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "coverage.yaml")

	content := `rows:
  - service: opensearch
    class: wrong_wire_key
    verdict: fixed
    date: "2026-08-29"
    commit: a576f56ca
  - service: medialive
    class: filter_default_semantics
    verdict: clean
    date: "2026-08-30"
    commit: ac5c674d2
`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	rows, err := LoadLedger(path)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "opensearch", rows[0].Service)
	assert.Equal(t, "medialive", rows[1].Service)
}

func TestLoadLedger_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadLedger(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.Error(t, err)
}

// TestRealLedgerValidates loads the actual coverage.yaml shipped with this
// tool and validates it against the real services/ tree, so a future edit
// that introduces a typo'd service or class name fails the test suite
// rather than only being caught by a human running the binary.
func TestRealLedgerValidates(t *testing.T) {
	t.Parallel()

	repoRoot, err := repoRootDir()
	require.NoError(t, err)

	rows, err := LoadLedger(filepath.Join(repoRoot, "cmd", "covledger", "coverage.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	servicesDir, err := servicesRootDir()
	require.NoError(t, err)

	knownServices, err := listServiceDirs(servicesDir)
	require.NoError(t, err)

	errs := Validate(rows, knownServices)
	assert.Empty(t, errs, "coverage.yaml must validate cleanly: %v", errs)
}
