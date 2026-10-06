package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMissingForClass_SubjectRowDoesNotCover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rows []Row
		want []string
	}{
		{
			name: "subject-less row covers",
			rows: []Row{{Service: "a", Class: "wrong_wire_key", Verdict: "clean"}},
			want: []string{"b"},
		},
		{
			name: "subject row leaves service uncovered",
			rows: []Row{{Service: "a", Class: "wrong_wire_key", Verdict: "inapplicable", Subject: "Op.field"}},
			want: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, MissingForClass(tt.rows, "wrong_wire_key", []string{"a", "b"}))
		})
	}
}

func TestValidate_SubjectKey(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"a": true}
	base := Row{Service: "a", Class: "filter_default_semantics", Verdict: "inapplicable", Commit: "abc", Reasoning: "r"}

	tests := []struct {
		name    string
		subject []string
		wantErr int
	}{
		{"distinct subjects allowed", []string{"X.f", "Y.g"}, 0},
		{"same subject duplicates", []string{"X.f", "X.f"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := make([]Row, 0, len(tt.subject))

			for _, s := range tt.subject {
				r := base
				r.Subject = s
				rows = append(rows, r)
			}

			assert.Len(t, Validate(rows, known), tt.wantErr)
		})
	}
}

func TestInapplicableRows_SortedAndFiltered(t *testing.T) {
	t.Parallel()

	rows := []Row{
		{Service: "b", Class: "wrong_wire_key", Verdict: "fixed"},
		{Service: "b", Class: "wrong_wire_key", Verdict: "inapplicable", Subject: "z"},
		{Service: "a", Class: "wrong_wire_key", Verdict: "inapplicable", Subject: "y"},
		{Service: "b", Class: "wrong_wire_key", Verdict: "inapplicable", Subject: "a"},
	}

	got := InapplicableRows(rows)
	require.Len(t, got, 3)
	assert.Equal(t, "a", got[0].Service)
	assert.Equal(t, "a", got[1].Subject)
	assert.Equal(t, "z", got[2].Subject)

	var buf bytes.Buffer

	printInapplicable(&buf, rows)
	assert.Contains(t, buf.String(), "3 recorded refusal(s)")
}

func TestAppendRow(t *testing.T) {
	t.Parallel()

	known := map[string]bool{"acm": true}
	good := Row{
		Service: "acm", Class: "wrong_wire_key", Verdict: "clean", Date: "2026-10-05", Commit: "abc1234",
		Subject: `Op."q"`,
	}

	tests := []struct {
		name    string
		row     Row
		wantErr bool
	}{
		{"valid row appended", good, false},
		{
			"unknown service rejected",
			Row{Service: "nope", Class: "wrong_wire_key", Verdict: "clean", Commit: "x"},
			true,
		},
		{
			"inapplicable without reasoning rejected",
			Row{Service: "acm", Class: "wrong_wire_key", Verdict: "inapplicable", Commit: "x"},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "coverage.yaml")
			require.NoError(t, os.WriteFile(path, []byte("rows:\n"), 0o600))

			err := AppendRow(path, nil, tt.row, known)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			rows, loadErr := LoadLedger(path)
			require.NoError(t, loadErr)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.row, rows[0])
		})
	}
}
