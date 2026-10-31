package dmsengine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

func sampleTables() []dmsengine.Table {
	return []dmsengine.Table{
		{Schema: "app", Name: "items", PK: []string{"id"}, Columns: []dmsengine.Column{
			{Name: "id", Native: "int"}, {Name: "Name", Native: "varchar(20)"}, {Name: "secret", Native: "text"},
		}},
		{Schema: "app", Name: "orders", PK: []string{"id"}, Columns: []dmsengine.Column{{Name: "id", Native: "int"}}},
		{Schema: "app", Name: "tmp_scratch", Columns: []dmsengine.Column{{Name: "x", Native: "int"}}},
		{Schema: "other", Name: "items", Columns: []dmsengine.Column{{Name: "id", Native: "int"}}},
	}
}

func TestMappingsSelect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		doc   string
		wants []string
	}{
		{
			name: "include all in schema",
			doc: `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",
				"object-locator":{"schema-name":"app","table-name":"%"},"rule-action":"include"}]}`,
			wants: []string{"app.items", "app.orders", "app.tmp_scratch"},
		},
		{
			name: "exclude by wildcard after include",
			doc: `{"rules":[
				{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"app","table-name":"%"},"rule-action":"include"},
				{"rule-type":"selection","rule-id":"2","rule-name":"2","object-locator":{"schema-name":"app","table-name":"tmp%"},"rule-action":"exclude"}]}`,
			wants: []string{"app.items", "app.orders"},
		},
		{
			name: "rules apply in rule-id order not document order",
			doc: `{"rules":[
				{"rule-type":"selection","rule-id":"2","rule-name":"2","object-locator":{"schema-name":"app","table-name":"tmp%"},"rule-action":"exclude"},
				{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"app","table-name":"%"},"rule-action":"include"}]}`,
			wants: []string{"app.items", "app.orders"},
		},
		{
			name: "explicit table across any schema",
			doc: `{"rules":[{"rule-type":"selection","rule-id":1,"rule-name":"1",
				"object-locator":{"schema-name":"%","table-name":"items"},"rule-action":"explicit"}]}`,
			wants: []string{"app.items", "other.items"},
		},
		{name: "no rules selects nothing", doc: `{"rules":[]}`, wants: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := dmsengine.ParseMappings(tt.doc)
			require.NoError(t, err)

			got := []string{}
			for _, tb := range m.Select(sampleTables()) {
				got = append(got, tb.Key())
			}

			assert.Equal(t, tt.wants, got)
		})
	}
}

func TestMappingsPlan(t *testing.T) {
	t.Parallel()

	rule := func(id, target, action, schema, table, column, value, old string) string {
		return `{"rule-type":"transformation","rule-id":"` + id + `","rule-name":"` + id + `","rule-target":"` + target +
			`","rule-action":"` + action + `","object-locator":{"schema-name":"` + schema + `","table-name":"` + table +
			`","column-name":"` + column + `"},"value":"` + value + `","old-value":"` + old + `"}`
	}

	tests := []struct {
		name       string
		rules      []string
		override   string
		wantSchema string
		wantTable  string
		wantCols   []string
		wantPK     []string
	}{
		{name: "no rules", wantSchema: "app", wantTable: "items", wantCols: []string{"id", "Name", "secret"}, wantPK: []string{"id"}},
		{
			name: "rename schema and table", rules: []string{
				rule("1", "schema", "rename", "app", "%", "", "dst", ""),
				rule("2", "table", "rename", "app", "items", "", "things", ""),
			},
			wantSchema: "dst", wantTable: "things", wantCols: []string{"id", "Name", "secret"}, wantPK: []string{"id"},
		},
		{
			name: "table prefix and suffix then lowercase", rules: []string{
				rule("1", "table", "add-prefix", "app", "items", "", "pre_", ""),
				rule("2", "table", "add-suffix", "app", "items", "", "_SUF", ""),
				rule("3", "table", "convert-lowercase", "app", "items", "", "", ""),
			},
			wantSchema: "app", wantTable: "pre_items_suf", wantCols: []string{"id", "Name", "secret"}, wantPK: []string{"id"},
		},
		{
			name: "replace and remove prefix", rules: []string{
				rule("1", "table", "replace-prefix", "app", "items", "", "xx", "it"),
			},
			wantSchema: "app", wantTable: "xxems", wantCols: []string{"id", "Name", "secret"}, wantPK: []string{"id"},
		},
		{
			name: "column rename remove and uppercase", rules: []string{
				rule("1", "column", "rename", "app", "items", "Name", "label", ""),
				rule("2", "column", "remove-column", "app", "items", "secret", "", ""),
				rule("3", "column", "convert-uppercase", "app", "items", "id", "", ""),
			},
			wantSchema: "app", wantTable: "items", wantCols: []string{"ID", "label"}, wantPK: []string{"ID"},
		},
		{
			name: "rules for other tables are ignored", rules: []string{
				rule("1", "table", "rename", "app", "orders", "", "o2", ""),
			},
			wantSchema: "app", wantTable: "items", wantCols: []string{"id", "Name", "secret"}, wantPK: []string{"id"},
		},
		{
			name: "TargetSchema override wins", rules: []string{rule("1", "schema", "rename", "app", "%", "", "dst", "")},
			override: "forced", wantSchema: "forced", wantTable: "items", wantCols: []string{"id", "Name", "secret"},
			wantPK: []string{"id"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc := `{"rules":[` + join(tt.rules) + `]}`
			m, err := dmsengine.ParseMappings(doc)
			require.NoError(t, err)

			got := m.Plan(sampleTables()[0], dmsengine.FamilyMySQL, tt.override)
			assert.Equal(t, tt.wantSchema, got.Schema)
			assert.Equal(t, tt.wantTable, got.Name)

			cols := []string{}
			for _, c := range got.Columns {
				cols = append(cols, c.Name)
			}

			assert.Equal(t, tt.wantCols, cols)
			assert.Equal(t, tt.wantPK, got.PK)
		})
	}
}

func join(parts []string) string {
	out := ""

	for i, p := range parts {
		if i > 0 {
			out += ","
		}

		out += p
	}

	return out
}

func TestParseMappingsRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
	}{
		{name: "bad json", doc: `{`},
		{name: "unknown rule type", doc: `{"rules":[{"rule-type":"nope","rule-id":"1"}]}`},
		{name: "unknown selection action", doc: `{"rules":[{"rule-type":"selection","rule-id":"1","rule-action":"drop"}]}`},
		{name: "remove-column on table", doc: `{"rules":[{"rule-type":"transformation","rule-id":"1","rule-target":"table","rule-action":"remove-column"}]}`},
		{name: "bad target", doc: `{"rules":[{"rule-type":"transformation","rule-id":"1","rule-target":"db","rule-action":"rename"}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dmsengine.ParseMappings(tt.doc)
			require.Error(t, err)
		})
	}
}

func TestParseTaskSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		doc     string
		mode    dmsengine.PrepMode
		schema  string
		wantErr bool
	}{
		{name: "empty defaults", doc: "", mode: dmsengine.PrepDropAndCreate},
		{
			name: "truncate", doc: `{"FullLoadSettings":{"TargetTablePrepMode":"TRUNCATE_BEFORE_LOAD"}}`,
			mode: dmsengine.PrepTruncateBefore,
		},
		{
			name: "do nothing with schema", doc: `{"TargetMetadata":{"TargetSchema":"x"},"FullLoadSettings":{"TargetTablePrepMode":"DO_NOTHING"}}`,
			mode: dmsengine.PrepDoNothing, schema: "x",
		},
		{name: "bad mode", doc: `{"FullLoadSettings":{"TargetTablePrepMode":"NOPE"}}`, wantErr: true},
		{name: "bad json", doc: `{`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := dmsengine.ParseTaskSettings(tt.doc)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.mode, got.PrepMode)
			assert.Equal(t, tt.schema, got.TargetSchema)
		})
	}
}

func TestTargetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		src, dst dmsengine.Family
		native   string
		want     string
	}{
		{name: "same family keeps native", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyMySQL, native: "int unsigned", want: "int unsigned"},
		{name: "mysql int to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "int", want: "integer"},
		{name: "mysql unsigned int to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "int unsigned", want: "bigint"},
		{name: "mysql varchar to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "varchar(20)", want: "varchar(20)"},
		{name: "mysql decimal to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "decimal(10,2)", want: "numeric(10,2)"},
		{name: "mysql datetime to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "datetime", want: "timestamp"},
		{name: "mysql longtext to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "longtext", want: "text"},
		{name: "mysql blob to pg", src: dmsengine.FamilyMySQL, dst: dmsengine.FamilyPostgres, native: "blob", want: "bytea"},
		{name: "pg text to mysql", src: dmsengine.FamilyPostgres, dst: dmsengine.FamilyMySQL, native: "text", want: "longtext"},
		{name: "pg boolean to mysql", src: dmsengine.FamilyPostgres, dst: dmsengine.FamilyMySQL, native: "boolean", want: "tinyint(1)"},
		{name: "pg varchar to mysql", src: dmsengine.FamilyPostgres, dst: dmsengine.FamilyMySQL, native: "character varying(40)", want: "varchar(40)"},
		{name: "pg timestamptz to mysql", src: dmsengine.FamilyPostgres, dst: dmsengine.FamilyMySQL, native: "timestamp with time zone", want: "datetime(6)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, dmsengine.TargetType(tt.src, tt.dst, tt.native))
		})
	}
}
