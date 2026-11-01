package dmsengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

func employeeTable() dmsengine.Table {
	return dmsengine.Table{Schema: "test", Name: "employee", PK: []string{"empid"}, Columns: []dmsengine.Column{
		{Name: "empid", Native: "int", Kind: dmsengine.KindInt},
		{Name: "dept", Native: "varchar(20)", Kind: dmsengine.KindString, Nullable: true},
		{Name: "salary", Native: "decimal(10,2)", Kind: dmsengine.KindDecimal, Nullable: true},
		{Name: "started", Native: "date", Kind: dmsengine.KindTime, Nullable: true},
		{Name: "notes", Native: "text", Kind: dmsengine.KindString, Nullable: true},
	}}
}

func filterRule(filters string) string {
	return `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",
		"object-locator":{"schema-name":"test","table-name":"employee"},"rule-action":"include","filters":[` +
		filters + `]}]}`
}

func srcFilter(col, conds string) string {
	return `{"filter-type":"source","column-name":"` + col + `","filter-conditions":[` + conds + `]}`
}

func TestSelectionFilterMatch(t *testing.T) {
	t.Parallel()

	row := func(id, dept, salary, started string) dmsengine.Row {
		r := dmsengine.Row{id, dept, salary, started, nil}
		if dept == "<null>" {
			r[1] = nil
		}

		return r
	}

	tests := []struct {
		name    string
		filters string
		rows    []dmsengine.Row
		want    []bool
	}{
		{
			name:    "gte",
			filters: srcFilter("empid", `{"filter-operator":"gte","value":"100"}`),
			rows: []dmsengine.Row{
				row("99", "a", "1", "2020-01-01"),
				row("100", "a", "1", "2020-01-01"),
				row("1000", "a", "1", "2020-01-01"),
			},
			want: []bool{false, true, true},
		},
		{
			name: "or of conditions in one filter",
			filters: srcFilter(
				"empid",
				`{"filter-operator":"lte","value":"10"},{"filter-operator":"between","start-value":"50",`+
					`"end-value":"75"},{"filter-operator":"gte","value":"100"}`,
			),
			rows: []dmsengine.Row{
				row("5", "a", "1", "2020-01-01"),
				row("20", "a", "1", "2020-01-01"),
				row("75", "a", "1", "2020-01-01"),
				row("120", "a", "1", "2020-01-01"),
			},
			want: []bool{true, false, true, true},
		},
		{
			name: "and of filters",
			filters: srcFilter("empid", `{"filter-operator":"ste","value":"100"}`) + "," +
				srcFilter("dept", `{"filter-operator":"eq","value":"tech"}`),
			rows: []dmsengine.Row{
				row("50", "tech", "1", "2020-01-01"),
				row("50", "ops", "1", "2020-01-01"),
				row("500", "tech", "1", "2020-01-01"),
			},
			want: []bool{true, false, false},
		},
		{
			name:    "null and notnull",
			filters: srcFilter("dept", `{"filter-operator":"null"}`),
			rows:    []dmsengine.Row{row("1", "<null>", "1", "2020-01-01"), row("2", "ops", "1", "2020-01-01")},
			want:    []bool{true, false},
		},
		{
			name:    "notbetween skips null",
			filters: srcFilter("salary", `{"filter-operator":"notbetween","start-value":"50","end-value":"75"}`),
			rows: []dmsengine.Row{
				row("1", "a", "49.5", "2020-01-01"),
				row("2", "a", "60", "2020-01-01"),
				{"3", "a", nil, nil, nil},
			},
			want: []bool{true, false, false},
		},
		{
			name:    "noteq numeric compare not text",
			filters: srcFilter("empid", `{"filter-operator":"noteq","value":"9"}`),
			rows:    []dmsengine.Row{row("9", "a", "1", "2020-01-01"), row("10", "a", "1", "2020-01-01")},
			want:    []bool{false, true},
		},
		{
			name:    "date",
			filters: srcFilter("started", `{"filter-operator":"gte","value":"2002-01-01"}`),
			rows:    []dmsengine.Row{row("1", "a", "1", "2001-12-31"), row("2", "a", "1", "2002-01-01")},
			want:    []bool{false, true},
		},
		{
			name:    "negative with null matches nothing",
			filters: srcFilter("empid", `{"filter-operator":"noteq","value":"1"},{"filter-operator":"null"}`),
			rows:    []dmsengine.Row{row("2", "a", "1", "2020-01-01")},
			want:    []bool{false},
		},
		{
			name: "inclusive negative and null drop null rows",
			filters: srcFilter(
				"dept",
				`{"filter-operator":"eq","value":"a"},{"filter-operator":"noteq","value":"b"},`+
					`{"filter-operator":"null"}`,
			),
			rows: []dmsengine.Row{row("1", "c", "1", "2020-01-01"), row("2", "<null>", "1", "2020-01-01")},
			want: []bool{true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(filterRule(tt.filters))
			require.NoError(t, err)

			sel, err := m.SelectFiltered([]dmsengine.Table{employeeTable()})
			require.NoError(t, err)
			require.Len(t, sel, 1)

			for i, r := range tt.rows {
				assert.Equal(t, tt.want[i], sel[0].Filter.Match(r), "row %d", i)
			}
		})
	}
}

func TestSelectionFilterSQL(t *testing.T) {
	t.Parallel()

	filters := srcFilter(
		"empid",
		`{"filter-operator":"lte","value":"10"},{"filter-operator":"between","start-value":"50","end-value":"75"}`,
	) +
		"," + srcFilter(
		"dept",
		`{"filter-operator":"notnull"}`,
	)

	m, err := dmsengine.ParseMappings(filterRule(filters))
	require.NoError(t, err)

	sel, err := m.SelectFiltered([]dmsengine.Table{employeeTable()})
	require.NoError(t, err)

	where, args := sel[0].Filter.SQL(dmsengine.FamilyPostgres, 1)
	assert.Equal(t, `("empid" <= $1 OR "empid" BETWEEN $2 AND $3) AND ("dept" IS NOT NULL)`, where)
	assert.Equal(t, []any{"10", "50", "75"}, args)

	where, _ = sel[0].Filter.SQL(dmsengine.FamilyMySQL, 1)
	assert.Equal(t, "(`empid` <= ? OR `empid` BETWEEN ? AND ?) AND (`dept` IS NOT NULL)", where)
}

func TestParseMappingsRuleErrors(t *testing.T) {
	t.Parallel()

	sel := `{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"%",` +
		`"table-name":"%"},"rule-action":"include"}`
	tests := []struct {
		name string
		rule string
	}{
		{
			"filter on wildcard table",
			`{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"test",` +
				`"table-name":"emp%"},"rule-action":"include","filters":[` + srcFilter(
				"a",
				`{"filter-operator":"eq","value":"1"}`,
			) + `]}`,
		},
		{
			"bad operator",
			`{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"test",` +
				`"table-name":"employee"},"rule-action":"include","filters":[` + srcFilter(
				"a",
				`{"filter-operator":"like","value":"1"}`,
			) + `]}`,
		},
		{
			"add-column without data-type",
			`{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"add-column",` +
				`"rule-target":"column","object-locator":{"schema-name":"%","table-name":"%"},"value":"x"}`,
		},
		{
			"unsupported data type",
			`{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"change-data-type",` +
				`"rule-target":"column","object-locator":{"schema-name":"%","table-name":"%","column-name":"%"},` +
				`"data-type":{"type":"map"}}`,
		},
		{
			"define-primary-key wildcard",
			`{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"define-primary-key",` +
				`"rule-target":"table","object-locator":{"schema-name":"%","table-name":"t"},` +
				`"primary-key-def":{"name":"pk","columns":["a"]}}`,
		},
		{
			"before image both empty",
			`{"rule-type":"transformation","rule-id":"2","rule-name":"2",` +
				`"rule-action":"add-before-image-columns","rule-target":"column",` +
				`"object-locator":{"schema-name":"%","table-name":"%"},` +
				`"before-image-def":{"column-prefix":"","column-suffix":""}}`,
		},
		{
			"data masking",
			`{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"data-masking-hash-mask",` +
				`"rule-target":"column","object-locator":{"schema-name":"%","table-name":"%","column-name":"%"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dmsengine.ParseMappings(`{"rules":[` + sel + `,` + tt.rule + `]}`)
			require.ErrorIs(t, err, dmsengine.ErrMapping)
		})
	}
}

func TestMatchPatternWildcards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"emp_", "empx", true},
		{"emp_", "emp", false},
		{"emp_", "empxx", false},
		{"e_p%", "excess", false},
		{"e_p%", "exp_anything", true},
		{"%", "", true},
		{"a.c", "abc", false},
		{"a%c", "a\nc", true},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"/"+tt.s, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dmsengine.MatchPattern(tt.pattern, tt.s))
		})
	}
}

func TestSelectLoadOrderAndExplicit(t *testing.T) {
	t.Parallel()

	m, err := dmsengine.ParseMappings(`{"rules":[
		{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"app",
		"table-name":"items"},"rule-action":"include","load-order":"1"},
		{"rule-type":"selection","rule-id":"2","rule-name":"2","object-locator":{"schema-name":"app",
		"table-name":"orders"},"rule-action":"explicit","load-order":"2"}]}`)
	require.NoError(t, err)

	sel, err := m.SelectFiltered(twoTables().tables)
	require.NoError(t, err)
	require.Len(t, sel, 2)
	assert.Equal(t, "orders", sel[0].Table.Name)
	assert.Equal(t, "items", sel[1].Table.Name)
}
