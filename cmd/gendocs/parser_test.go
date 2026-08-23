package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		line    string
		wantKey string
		wantOK  bool
	}{
		{
			name:    "plain key",
			line:    "  CreateAgent: {wire: ok, errors: ok}",
			wantKey: "CreateAgent",
			wantOK:  true,
		},
		{
			name:    "slash joined keys",
			line:    "  AddPermission/RemovePermission: {wire: ok, errors: ok}",
			wantKey: "AddPermission/RemovePermission",
			wantOK:  true,
		},
		{
			name:    "three way slash join",
			line:    "  DatasetGroup/Dataset/Schema: {status: fixed}",
			wantKey: "DatasetGroup/Dataset/Schema",
			wantOK:  true,
		},
		{
			name:    "parenthetical",
			line:    "  Database/TableMetadata (Get/List): {wire: ok}",
			wantKey: "Database/TableMetadata (Get/List)",
			wantOK:  true,
		},
		{
			name:    "internal space",
			line:    "  Create/UpdateConfigurationTemplate response shape: {status: fixed}",
			wantKey: "Create/UpdateConfigurationTemplate response shape",
			wantOK:  true,
		},
		{
			name:   "not an entry: prose with comma before colon-brace",
			line:   "  account policies, data protection/resource/index policies: {status: ok}",
			wantOK: false,
		},
		{
			name:   "not an entry: reserved key at column 0 is a section header",
			line:   "ops: {not: a, real: entry}",
			wantOK: false,
		},
		{
			name:    "entry: indented reserved-word key is a real entry",
			line:    "  " + keyLeaks + `: {status: ok, note: "single reconciler goroutine"}`,
			wantKey: keyLeaks,
			wantOK:  true,
		},
		{
			name:    "entry: 0-space non-reserved key is the tolerated mis-indented final entry",
			line:    "persistence: {status: ok, note: \"snapshot/restore verified\"}",
			wantKey: "persistence",
			wantOK:  true,
		},
		{
			name:   "not an entry: 0-space reserved-word key reads as its own section header",
			line:   "leaks: {status: clean, note: \"no goroutines\"}",
			wantOK: false,
		},
		{
			name:   "not an entry: quoted note continuation",
			line:   `  is the flat` + " `{introspectionId, introspectionResult: {models, nextToken},",
			wantOK: false,
		},
		{
			name:   "not an entry: no brace",
			line:   "  CreateAgent: ok",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key, _, ok := matchEntry(tc.line)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				assert.Equal(t, tc.wantKey, key)
			}
		})
	}
}

func TestParseOpsBlock_UnparsedEntryWarning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		line        string
		wantWarning bool
	}{
		{
			name:        "comma-joined family name reported",
			line:        "  Delete/UpdateServerCertificate, DeleteInstanceProfile: {wire: ok, errors: ok}",
			wantWarning: true,
		},
		{
			name:        "asterisk wildcard family name reported",
			line:        "  Start*DetectionJob (9 families): {wire: ok, errors: ok}",
			wantWarning: true,
		},
		{
			name:        "ordinary wrapped note prose stays silent",
			line:        "  this continues the previous op's note across a wrapped line.",
			wantWarning: false,
		},
		{
			name:        "quoted code snippet in note stays silent",
			line:        `  returned a fabricated ` + "`{\"ResourceDashboard\": {}}`" + ` envelope`,
			wantWarning: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lines := []string{
				"  FirstOp: {wire: ok, errors: ok, state: ok, persist: ok}",
				tc.line,
				"overall: A",
			}

			doc := &ParityDoc{}
			ops, next := parseOpsBlock(lines, 0, doc, "services/example/PARITY.md", 0)

			require.Len(t, ops, 1, "the malformed line must not be counted as an op")
			assert.Equal(t, "FirstOp", ops[0].Name)
			assert.Equal(t, 2, next, "block must stop at the reserved overall: terminator")

			if tc.wantWarning {
				require.Len(t, doc.Warnings, 1)
				assert.Contains(t, doc.Warnings[0], "services/example/PARITY.md:2:")
			} else {
				assert.Empty(t, doc.Warnings)
			}
		})
	}
}

func TestParseFamiliesBlock_ReservedKeyCollision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lines       []string
		wantNames   []string
		wantNext    int
		wantWarning bool
	}{
		{
			name: "indented entry whose key is reserved parses and counts",
			lines: []string{
				"  " + keyLeaks + `: {status: ok, note: "single reconciler goroutine per backend"}`,
				"gaps:",
			},
			wantNames: []string{keyLeaks},
			wantNext:  1,
		},
		{
			name: "genuine column-0 section header ends the block, not swallowed as an entry",
			lines: []string{
				"  db_instance_lifecycle: {status: ok}",
				"leaks: {status: clean, note: \"no goroutines in this service\"}",
			},
			wantNames: []string{"db_instance_lifecycle"},
			wantNext:  1,
		},
		{
			name: "0-space mis-indented entry with a non-reserved key still tolerated",
			lines: []string{
				"  db_instance_lifecycle: {status: ok}",
				"persistence: {status: ok, note: \"snapshot/restore round-trips verified\"}",
				"gaps:",
			},
			wantNames: []string{"db_instance_lifecycle", "persistence"},
			wantNext:  2,
		},
		{
			name: "0-space entry whose key is reserved reads as that key's own section header",
			lines: []string{
				"  db_instance_lifecycle: {status: ok}",
				"leaks: {status: clean, note: \"no goroutines in this service\"}",
				"gaps:",
			},
			wantNames: []string{"db_instance_lifecycle"},
			wantNext:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc := &ParityDoc{}
			families, next := parseFamiliesBlock(tc.lines, 0, doc, "services/example/PARITY.md", 0)

			names := make([]string, len(families))
			for i, f := range families {
				names[i] = f.Name
			}

			require.Equal(t, tc.wantNames, names)
			assert.Equal(t, tc.wantNext, next)
			assert.Empty(t, doc.Warnings, "a reserved-word collision must never be reported as an unparsed entry")
		})
	}
}

func TestParseParityFile_WidenedFamilyKeys(t *testing.T) {
	t.Parallel()

	content := `---
service: example
overall: A
families:
  AddPermission/RemovePermission: {status: ok}
  Database/TableMetadata (Get/List): {status: ok}
  Create/UpdateConfigurationTemplate response shape: {status: fixed}
  account policies, data protection/resource/index policies: {status: ok, note: "comma-joined, still unparsed"}
gaps: []
---
`
	dir := t.TempDir()
	path := filepath.Join(dir, "PARITY.md")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	doc, err := ParseParityFile(path)
	require.NoError(t, err)

	require.Len(t, doc.Families, 3, "the three widened-charset keys must parse as entries")
	assert.Len(t, doc.Warnings, 1, "the comma-joined key must be reported, not silently dropped")
}

// TestParseParityFile_BlockStyleOpCounted reproduces gopherstack-7o96 exactly:
// a services/stepfunctions/PARITY.md op entry rewritten from the repo's
// inline convention into YAML block style vanished from the count with no
// warning. This must fail against the pre-fix parser (doc.Ops has length 1,
// not 2) -- confirmed by hand-reverting parser.go's block-entry support and
// rerunning.
// TestParseParityFile_UnrecognizedStatusToken proves gopherstack-cr41's fix:
// an ops:/families: entry that parses fine but carries a status token
// classifyToken doesn't recognize must fail the run exactly like an entry
// that doesn't parse at all (gopherstack-7o96), rather than silently
// bucketing it as "other".
func TestParseParityFile_UnrecognizedStatusToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "closed vocabulary only",
			content: `---
service: example
overall: A
ops:
  CreateThing: {wire: ok, errors: fixed, state: partial, persist: n/a}
families:
  core: {status: deferred}
gaps: []
---
`,
			wantErr: false,
		},
		{
			name: "inline op field",
			content: `---
service: example
overall: A
ops:
  CreateThing: {wire: ok (fixed), errors: ok, state: ok, persist: ok}
gaps: []
---
`,
			wantErr: true,
		},
		{
			name: "block op field",
			content: `---
service: example
overall: A
ops:
  CreateThing:
    wire: ok
    errors: ok
    state: partial->ok
    persist: ok
gaps: []
---
`,
			wantErr: true,
		},
		{
			name: "family status field",
			content: `---
service: example
overall: A
families:
  core: {status: honest-disclosed-limitation}
gaps: []
---
`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "PARITY.md")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))

			doc, err := ParseParityFile(path)
			require.NoError(t, err)

			runErr := checkParseWarnings([]*ParityDoc{doc})
			if tc.wantErr {
				assert.NotEmpty(t, doc.Warnings)
				require.Error(t, runErr)
			} else {
				assert.Empty(t, doc.Warnings)
				require.NoError(t, runErr)
			}
		})
	}
}

func TestParseParityFile_LastAuditCommitToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "short sha",
			content: `---
service: example
last_audit_commit: c79ebf1b5
overall: A
---
`,
			wantErr: false,
		},
		{
			name: "full sha",
			content: `---
service: example
last_audit_commit: 8ddfcca9b7157a079a75e8cda1d26d70118f4ae9
overall: A
---
`,
			wantErr: false,
		},
		{
			name: "absent value",
			content: `---
service: example
last_audit_commit:                                # unknown: pass ran without git access -- gopherstack-33in
overall: A
---
`,
			wantErr: false,
		},
		{
			name: "field omitted entirely",
			content: `---
service: example
overall: A
---
`,
			wantErr: false,
		},
		{
			name: "HEAD placeholder",
			content: `---
service: example
last_audit_commit: HEAD
overall: A
---
`,
			wantErr: true,
		},
		{
			name: "prose placeholder",
			content: `---
service: example
last_audit_commit: pending (uncommitted this pass -- see git log at merge time)
overall: A
---
`,
			wantErr: true,
		},
		{
			name: "sha with trailing suffix",
			content: `---
service: example
last_audit_commit: 749ff939+wt
overall: A
---
`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "PARITY.md")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))

			doc, err := ParseParityFile(path)
			require.NoError(t, err)

			runErr := checkParseWarnings([]*ParityDoc{doc})
			if tc.wantErr {
				assert.NotEmpty(t, doc.Warnings)
				require.Error(t, runErr)
			} else {
				assert.Empty(t, doc.Warnings)
				require.NoError(t, runErr)
			}
		})
	}
}

// TestParseParityFile_DuplicateTopLevelKey guards the union-merge defect
// class from gopherstack-z31a: two branches' PARITY.md frontmatter merged
// into one file, each carrying its own copy of a scalar key. Both prior
// occurrences in this test use valid-shaped values (real shas, a real
// grade), so checkLastAuditCommitToken/checkStatusToken never fire -- only a
// dedicated duplicate-key check catches this.
func TestParseParityFile_DuplicateTopLevelKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "duplicate last_audit_commit, both valid shas",
			content: `---
service: example
last_audit_commit: 40f05928
last_audit_date: 2026-08-10
overall: A
last_audit_commit: e4139790
last_audit_date: 2026-08-19
overall: A
---
`,
			wantErr: true,
		},
		{
			name: "duplicate overall, both valid grades",
			content: `---
service: example
last_audit_commit: 40f05928
overall: A
overall: B
---
`,
			wantErr: true,
		},
		{
			name: "no duplicate",
			content: `---
service: example
last_audit_commit: 40f05928
overall: A
---
`,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "PARITY.md")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))

			doc, err := ParseParityFile(path)
			require.NoError(t, err)

			runErr := checkParseWarnings([]*ParityDoc{doc})
			if tc.wantErr {
				assert.NotEmpty(t, doc.Warnings)
				require.Error(t, runErr)
			} else {
				assert.Empty(t, doc.Warnings)
				require.NoError(t, runErr)
			}
		})
	}
}

func TestParseParityFile_BlockStyleOpCounted(t *testing.T) {
	t.Parallel()

	content := `---
service: stepfunctions
overall: A
ops:
  CreateActivity: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExecutionHistory:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED 2026-08-21 (bd gopherstack-r80d, batch 10): a long note that
      wraps across several physical lines, the same shape that triggered
      the real services/stepfunctions/PARITY.md regression this reproduces.
gaps: []
---
`
	dir := t.TempDir()
	path := filepath.Join(dir, "PARITY.md")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	doc, err := ParseParityFile(path)
	require.NoError(t, err)

	require.Len(t, doc.Ops, 2, "the block-style GetExecutionHistory entry must be counted, not silently dropped")
	assert.Empty(t, doc.Warnings)

	names := make([]string, len(doc.Ops))
	for i, op := range doc.Ops {
		names[i] = op.Name
	}
	assert.Equal(t, []string{"CreateActivity", "GetExecutionHistory"}, names)

	geh := doc.Ops[1]
	assert.Equal(t, "fixed", geh.Wire)
	assert.Equal(t, "ok", geh.Errors)
	assert.Equal(t, "ok", geh.State)
	assert.Equal(t, "ok", geh.Persist)
}

func TestParseOpsBlock_BlockStyleEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lines     []string
		wantNames []string
		wantNext  int
	}{
		{
			name: "block entry then inline entry",
			lines: []string{
				"  GetExecutionHistory:",
				"    wire: fixed",
				"    errors: ok",
				"    state: ok",
				"    persist: ok",
				"    note: >",
				"      a wrapped note, several lines long.",
				"  CreateActivity: {wire: ok, errors: ok, state: ok, persist: ok}",
				"gaps:",
			},
			wantNames: []string{"GetExecutionHistory", "CreateActivity"},
			wantNext:  8,
		},
		{
			name: "block entry at column 0",
			lines: []string{
				"  FirstOp: {wire: ok, errors: ok}",
				"SecondOp:",
				"  wire: ok",
				"  errors: ok",
			},
			wantNames: []string{"FirstOp", "SecondOp"},
			wantNext:  4,
		},
		{
			name: "block entry immediately before terminator",
			lines: []string{
				"  OnlyOp:",
				"    wire: ok",
				"    errors: ok",
				"overall: A",
			},
			wantNames: []string{"OnlyOp"},
			wantNext:  3,
		},
		{
			name: "ad hoc list key not treated as entry",
			lines: []string{
				"  RealOp: {wire: ok, errors: ok}",
				"invented_ops_removed:",
				`  - "SomeOp: not a real action, deleted this sweep"`,
				"gaps:",
			},
			wantNames: []string{"RealOp"},
			wantNext:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc := &ParityDoc{}
			ops, next := parseOpsBlock(tc.lines, 0, doc, "services/example/PARITY.md", 0)

			names := make([]string, len(ops))
			for i, op := range ops {
				names[i] = op.Name
			}

			require.Equal(t, tc.wantNames, names)
			assert.Equal(t, tc.wantNext, next)
			assert.Empty(t, doc.Warnings)
		})
	}
}

// TestParseFamiliesBlock_MultilineInlineNote guards the one real
// bare-colon-in-a-block false-positive risk found across every
// services/*/PARITY.md: services/redshiftdata's error_codes family uses an
// inline flow map whose "note: >" folds across several physical lines,
// including one that itself ends in a bare colon. Brace-depth tracking must
// treat every line up to the closing "}" as that same entry's continuation,
// never as a new block-style entry.
func TestParseFamiliesBlock_MultilineInlineNote(t *testing.T) {
	t.Parallel()

	lines := []string{
		"  error_codes: {status: ok, note: >",
		"    ValidationException and ResourceNotFoundException are the only two",
		"    error types this backend can actually produce, and both are field-diffed this pass:",
		"    ErrorFault Client -> HTTP 400 for both, matching handler.go exactly.}",
		"  NextFamily: {status: ok}",
		"gaps:",
	}

	doc := &ParityDoc{}
	families, next := parseFamiliesBlock(lines, 0, doc, "services/example/PARITY.md", 0)

	names := make([]string, len(families))
	for i, f := range families {
		names[i] = f.Name
	}

	require.Equal(t, []string{"error_codes", "NextFamily"}, names)
	assert.Equal(t, 5, next)
	assert.Empty(t, doc.Warnings)
}

func TestWarnUnparsedEntry_BlockForm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		line        string
		wantWarning bool
	}{
		{
			name:        "comma-joined bare-colon key reported",
			line:        "  Delete/UpdateServerCertificate, DeleteInstanceProfile:",
			wantWarning: true,
		},
		{
			name:        "ordinary wrapped note prose stays silent",
			line:        "  this continues the previous op's note across a wrapped line",
			wantWarning: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lines := []string{
				"  FirstOp: {wire: ok, errors: ok, state: ok, persist: ok}",
				tc.line,
				"  irrelevant continuation, not a list item",
				"overall: A",
			}

			doc := &ParityDoc{}
			ops, next := parseOpsBlock(lines, 0, doc, "services/example/PARITY.md", 0)

			require.Len(t, ops, 1, "the malformed line must not be counted as an op")
			assert.Equal(t, "FirstOp", ops[0].Name)
			assert.Equal(t, 3, next, "block must stop at the reserved overall: terminator")

			if tc.wantWarning {
				require.Len(t, doc.Warnings, 1)
				assert.Contains(t, doc.Warnings[0], "services/example/PARITY.md:2:")
			} else {
				assert.Empty(t, doc.Warnings)
			}
		})
	}
}
