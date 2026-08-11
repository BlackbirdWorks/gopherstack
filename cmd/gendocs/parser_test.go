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
			name:   "not an entry: reserved top-level key",
			line:   "  ops: {not: a, real: entry}",
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
