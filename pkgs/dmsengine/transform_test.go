package dmsengine_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

func transformDoc(rules ...string) string {
	sel := `{"rule-type":"selection","rule-id":"1","rule-name":"1",
		"object-locator":{"schema-name":"test","table-name":"employee"},"rule-action":"include"}`

	return `{"rules":[` + strings.Join(append([]string{sel}, rules...), ",") + `]}`
}

func addColumn(id, name, expr, dataType string) string {
	return `{"rule-type":"transformation","rule-id":"` + id + `","rule-name":"` + id + `","rule-action":"add-column",
		"rule-target":"column","object-locator":{"schema-name":"test","table-name":"employee"},"value":"` + name + `",
		"expression":"` + expr + `","data-type":` + dataType + `}`
}

func TestAddColumnExpressions(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	row := dmsengine.Row{"7", "Tech", "20500.50", "2002-01-15", nil}

	tests := []struct {
		want any
		name string
		expr string
		typ  string
	}{
		{name: "concat", expr: `$dept||'_'||$empid`, typ: `{"type":"string","length":50}`, want: "Tech_7"},
		{
			name: "case round",
			expr: `CASE WHEN round($salary)>=20000 THEN 'SENIOR' ELSE 'JUNIOR' END`,
			typ:  `{"type":"string","length":50}`,
			want: "SENIOR",
		},
		{name: "round int8", expr: `round($salary)`, typ: `{"type":"int8"}`, want: "20501"},
		{name: "arithmetic", expr: `$empid*2+1`, typ: `{"type":"int4"}`, want: "15"},
		{name: "lower upper", expr: `lower($dept)||upper($dept)`, typ: `{"type":"string"}`, want: "techTECH"},
		{name: "substr hex", expr: `substr(hex($dept),1,4)`, typ: `{"type":"string"}`, want: "5465"},
		{name: "sha256", expr: `hash_sha256($dept)`, typ: `{"type":"string","length":64}`, want: ""},
		{name: "coalesce null", expr: `coalesce($notes,'none')`, typ: `{"type":"string"}`, want: "none"},
		{name: "datetime now", expr: `datetime()`, typ: `{"type":"datetime",` +
			`"precision":6}`, want: "2026-03-04 05:06:07"},
		{name: "strftime", expr: `strftime('%Y/%m/%d', $started)`, typ: `{"type":"string"}`, want: "2002/01/15"},
		{name: "header operation", expr: `$AR_H_OPERATION`, typ: `{"type":"string"}`, want: "INSERT"},
		{name: "operation indicator", expr: `operation_indicator('D','U','I')`, typ: `{"type":"string"}`, want: "I"},
		{
			name: "source metadata",
			expr: `$AR_M_SOURCE_SCHEMA||'.'||$AR_M_SOURCE_TABLE_NAME`,
			typ:  `{"type":"string"}`,
			want: "test.employee",
		},
		{name: "boolean", expr: `$empid > 5`, typ: `{"type":"boolean"}`, want: "true"},
		{name: "replace trim", expr: `trim(replace($dept,'e','  '))`, typ: `{"type":"string"}`, want: "T  ch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(transformDoc(addColumn("2", "added", tt.expr, tt.typ)))
			require.NoError(t, err)

			plan, err := m.Plan(employeeTable(), dmsengine.FamilyMySQL, "")
			require.NoError(t, err)

			out := plan.ProjectWith(row, dmsengine.ProjectCtx{Full: true, Now: now})
			require.Len(t, out, 6)

			if tt.name == "sha256" {
				assert.Len(t, out[5], 64)

				return
			}

			assert.Equal(t, tt.want, out[5])
		})
	}
}

func TestAddColumnRejectsUnsupported(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, expr string }{
		{"unknown function", `soundex($dept)`},
		{"aggregate", `count($dept)`},
		{"missing column", `$nope`},
		{"change seq header", `$AR_H_CHANGE_SEQ`},
		{"syntax", `$dept ||`},
		{"modifiers", `date('now','+1 day')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(transformDoc(addColumn("2", "added", tt.expr, `{"type":"string"}`)))
			require.NoError(t, err)

			_, err = m.Plan(employeeTable(), dmsengine.FamilyMySQL, "")
			require.ErrorIs(t, err, dmsengine.ErrMapping)
			assert.Contains(t, err.Error(), "added")
		})
	}
}

func TestChangeDataTypeAndKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(*testing.T, dmsengine.TargetTable)
		name     string
		rule     string
		wantCols []string
		wantPK   []string
	}{
		{
			name: "change type",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"change-data-type",` +
				`"rule-target":"column",
				"object-locator":{"schema-name":"test","table-name":"employee","column-name":"empid"},
				"data-type":{"type":"string","length":"12"}}`,
			wantCols: []string{"empid", "dept", "salary", "started", "notes"}, wantPK: []string{"empid"},
			check: func(t *testing.T, tt dmsengine.TargetTable) {
				t.Helper()
				assert.Equal(t, dmsengine.KindString, tt.Columns[0].Kind)
				assert.Equal(t, "varchar(12)", tt.Columns[0].DMS.SQLType(dmsengine.FamilyPostgres))
				assert.Equal(t, "string", tt.Columns[0].Native)
			},
		},
		{
			name: "change type by existing data type",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"change-data-type",` +
				`"rule-target":"column",
				"object-locator":{"schema-name":"test","table-name":"employee","column-name":"%",
				"data-type":"numeric"},"data-type":{"type":"real8"}}`,
			wantCols: []string{"empid", "dept", "salary", "started", "notes"}, wantPK: []string{"empid"},
			check: func(t *testing.T, tt dmsengine.TargetTable) {
				t.Helper()
				assert.Equal(t, dmsengine.KindFloat, tt.Columns[2].Kind)
				assert.Equal(t, dmsengine.KindInt, tt.Columns[0].Kind)
			},
		},
		{
			name: "include-column",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"include-column",` +
				`"rule-target":"column",
				"object-locator":{"schema-name":"test","table-name":"employee","column-name":"dept"}}`,
			wantCols: []string{"dept"}, wantPK: []string{},
		},
		{
			name: "define-primary-key",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2",` +
				`"rule-action":"define-primary-key","rule-target":"table",
				"object-locator":{"schema-name":"test","table-name":"employee"},"primary-key-def":{"name":"emp_dept",
				"origin":"unique-index","columns":["dept","started"]}}`,
			wantCols: []string{"empid", "dept", "salary", "started", "notes"}, wantPK: []string{"dept", "started"},
			check: func(t *testing.T, tt dmsengine.TargetTable) {
				t.Helper()
				assert.Equal(t, "emp_dept", tt.KeyName)
				assert.True(t, tt.KeyUnique)
			},
		},
		{
			name: "before image pk-only",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2",` +
				`"rule-action":"add-before-image-columns","rule-target":"column",
				"object-locator":{"schema-name":"test","table-name":"employee"},
				"before-image-def":{"column-prefix":"BI_","column-suffix":"","column-filter":"pk-only"}}`,
			wantCols: []string{"empid", "dept", "salary", "started", "notes", "BI_empid"}, wantPK: []string{"empid"},
			check: func(t *testing.T, tt dmsengine.TargetTable) {
				t.Helper()
				assert.True(t, tt.HasBeforeImage)
			},
		},
		{
			name: "before image non-lob defaults",
			rule: `{"rule-type":"transformation","rule-id":"2","rule-name":"2",` +
				`"rule-action":"add-before-image-columns","rule-target":"column",
				"object-locator":{"schema-name":"test","table-name":"employee"},
				"before-image-def":{"column-suffix":"_old","column-prefix":"","column-filter":"non-lob"}}`,
			wantCols: []string{
				"empid",
				"dept",
				"salary",
				"started",
				"notes",
				"empid_old",
				"dept_old",
				"salary_old",
				"started_old",
			},
			wantPK: []string{"empid"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(transformDoc(tt.rule))
			require.NoError(t, err)

			plan, err := m.Plan(employeeTable(), dmsengine.FamilyMySQL, "")
			require.NoError(t, err)

			names := []string{}
			for _, c := range plan.Columns {
				names = append(names, c.Name)
			}

			assert.Equal(t, tt.wantCols, names)
			assert.Equal(t, tt.wantPK, plan.PK)

			if tt.check != nil {
				tt.check(t, plan)
			}
		})
	}
}

func TestBeforeImageProjection(t *testing.T) {
	t.Parallel()

	m, err := dmsengine.ParseMappings(transformDoc(`{"rule-type":"transformation","rule-id":"2","rule-name":"2",
		"rule-action":"add-before-image-columns","rule-target":"column",
		"object-locator":{"schema-name":"test","table-name":"employee"}}`))
	require.NoError(t, err)

	plan, err := m.Plan(employeeTable(), dmsengine.FamilyMySQL, "")
	require.NoError(t, err)

	before := dmsengine.Row{"1", "a", "1", "2020-01-01", nil}
	after := dmsengine.Row{"3", "a", "1", "2020-01-01", nil}

	assert.Equal(t, dmsengine.Row{"3", "a", "1", "2020-01-01", nil, "1"},
		plan.ProjectWith(after, dmsengine.ProjectCtx{Op: dmsengine.OpUpdate, Before: before}))
	assert.Equal(t, dmsengine.Row{"3", "a", "1", "2020-01-01", nil, nil}, plan.Project(after))
}

func TestDMSTypeSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pg           string
		my           string
		wantKindName string
		dt           dmsengine.DMSType
	}{
		{dt: dmsengine.DMSType{Name: "int8"}, pg: "bigint", my: "bigint", wantKindName: "int8"},
		{dt: dmsengine.DMSType{Name: "uint4"}, pg: "bigint", my: "int unsigned", wantKindName: "uint4"},
		{
			dt:           dmsengine.DMSType{Name: "string", Length: 30},
			pg:           "varchar(30)",
			my:           "varchar(30)",
			wantKindName: "string",
		},
		{dt: dmsengine.DMSType{Name: "string"}, pg: "text", my: "longtext", wantKindName: "string"},
		{
			dt:           dmsengine.DMSType{Name: "numeric", Precision: 5, Scale: 2},
			pg:           "numeric(5,2)",
			my:           "numeric(5,2)",
			wantKindName: "numeric",
		},
		{
			dt:           dmsengine.DMSType{Name: "datetime", Scale: 3},
			pg:           "timestamp(3)",
			my:           "datetime(3)",
			wantKindName: "datetime",
		},
		{dt: dmsengine.DMSType{Name: "boolean"}, pg: "boolean", my: "tinyint(1)", wantKindName: "boolean"},
		{dt: dmsengine.DMSType{Name: "bytes", Length: 16}, pg: "bytea", my: "varbinary(16)", wantKindName: "bytes"},
	}

	for _, tt := range tests {
		t.Run(tt.wantKindName, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.pg, tt.dt.SQLType(dmsengine.FamilyPostgres))
			assert.Equal(t, tt.my, tt.dt.SQLType(dmsengine.FamilyMySQL))
		})
	}
}
