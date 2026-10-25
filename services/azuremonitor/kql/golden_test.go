package kql_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuremonitor/kql"
)

var fixedNow = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

type fixture map[string]*kql.Rel

func (f fixture) Table(_, name string) (*kql.Rel, bool) {
	r, ok := f[name]

	return r, ok
}

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}

	return t
}

func newFixture() fixture {
	logs := &kql.Rel{
		Cols: []kql.Col{
			{Name: "TimeGenerated", Type: kql.TDatetime}, {Name: "Computer", Type: kql.TString},
			{Name: "Level", Type: kql.TString}, {Name: "Value", Type: kql.TReal}, {Name: "N", Type: kql.TLong},
			{Name: "Message", Type: kql.TString}, {Name: "Props", Type: kql.TDynamic},
		},
		Rows: [][]any{
			{ts("2024-03-01T10:00:00Z"), "web1", "Info", 1.5, int64(1), "Started service alpha",
				map[string]any{"env": "prod", "tags": []any{"a", "b"}}},
			{ts("2024-03-01T10:30:00Z"), "web1", "Error", 4.0, int64(2), "Failed to connect to db",
				map[string]any{"env": "prod"}},
			{ts("2024-03-01T11:00:00Z"), "web2", "Info", 2.5, int64(3), "Started service beta",
				map[string]any{"env": "dev"}},
			{ts("2024-03-01T11:15:00Z"), "web2", "Warning", 3.0, int64(4), "Disk space low",
				map[string]any{"env": "dev", "tags": []any{}}},
			{ts("2024-03-01T11:45:00Z"), "db1", "Error", 10.0, int64(5), "Connection reset", nil},
			{ts("2024-03-01T09:00:00Z"), "db1", "Info", 0.5, int64(6), "", map[string]any{"env": "prod"}},
			{ts("2024-02-29T23:30:00Z"), "web1", "Info", 7.0, int64(7), "Nightly backup done", map[string]any{}},
		},
	}
	other := &kql.Rel{
		Cols: []kql.Col{{Name: "TimeGenerated", Type: kql.TDatetime}, {Name: "Computer", Type: kql.TString},
			{Name: "Extra", Type: kql.TLong}},
		Rows: [][]any{{ts("2024-03-01T08:00:00Z"), "app1", int64(99)}},
	}

	return fixture{"Logs": logs, "Other": other}
}

func render(rel *kql.Rel) (string, string) {
	cols := ""
	for i, c := range rel.Cols {
		if i > 0 {
			cols += ","
		}

		cols += c.Name + ":" + string(c.Type)
	}

	rows := make([][]any, len(rel.Rows))
	for i, r := range rel.Rows {
		rows[i] = make([]any, len(r))
		for j, v := range r {
			rows[i][j] = kql.ResultValue(rel.Cols[j].Type, v)
		}
	}

	raw, _ := json.Marshal(rows)

	return cols, string(raw)
}

type goldenCase struct {
	name, query, cols, rows string
}

func TestGolden(t *testing.T) {
	t.Parallel()

	for _, tt := range goldenCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rel, err := kql.Execute(tt.query, kql.Options{Now: fixedNow, Resolver: newFixture()})
			require.NoError(t, err)

			cols, rows := render(rel)
			if tt.cols != "" {
				assert.Equal(t, tt.cols, cols)
			}

			assert.JSONEq(t, tt.rows, rows)
		})
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, query, kind, code string
	}{
		{"unknown column", `Logs | where Nope == 1`, "SemanticError", "SEM0100"},
		{"unknown table", `NoSuchTable | take 1`, "SemanticError", "SEM0100"},
		{"join unsupported", `Logs | join Other on Computer`, "SemanticError", "SEM0001"},
		{"mv-expand unsupported", `Logs | mv-expand Props`, "SemanticError", "SEM0001"},
		{"search unsupported", `search "x"`, "SemanticError", "SEM0001"},
		{"function let unsupported", `let f = (x:long) { x + 1 }; Logs | take 1`, "SemanticError", "SEM0001"},
		{"syntax", `Logs | where | take 1`, "SyntaxError", "SYN0002"},
		{"unterminated string", `Logs | where Computer == "web1`, "SyntaxError", "SYN0002"},
		{"type mismatch", `Logs | where Computer == 1`, "SemanticError", "SEM0100"},
		{"unknown function", `Logs | project x = frobnicate(N)`, "SemanticError", "SEM0100"},
		{"sum of string", `Logs | summarize sum(Computer)`, "SemanticError", "SEM0100"},
		{"scalar query", `1 + 1`, "SemanticError", "SEM0100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := kql.Execute(tt.query, kql.Options{Now: fixedNow, Resolver: newFixture()})
			require.Error(t, err)

			var ke *kql.Error
			require.ErrorAs(t, err, &ke)
			assert.Equal(t, tt.kind, ke.Kind)
			assert.Equal(t, tt.code, ke.Code)
		})
	}
}
