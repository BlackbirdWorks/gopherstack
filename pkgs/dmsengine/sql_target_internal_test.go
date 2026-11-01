package dmsengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSQLTargetCreate(t *testing.T) {
	t.Parallel()

	tab := func(src Family, pk []string, cols ...TargetColumn) TargetTable {
		return TargetTable{SourceFamily: src, Schema: "s", Name: "t", PK: pk, Columns: cols}
	}
	col := func(name, native string, nullable bool) TargetColumn {
		return TargetColumn{Name: name, Native: native, Nullable: nullable}
	}

	tests := []struct {
		name string
		want string
		tab  TargetTable
		fam  Family
	}{
		{
			name: "mysql to mysql keeps native types",
			fam:  FamilyMySQL,
			tab:  tab(FamilyMySQL, []string{"id"}, col("id", "int unsigned", false), col("n", "varchar(5)", true)),
			want: "CREATE TABLE IF NOT EXISTS `s`.`t` (`id` int unsigned NOT NULL, `n` varchar(5), PRIMARY KEY (`id`))",
		},
		{
			name: "mysql to postgres maps types and quotes",
			fam:  FamilyPostgres,
			tab: tab(
				FamilyMySQL,
				[]string{"id"},
				col("id", "int", false),
				col("When", "datetime", true),
				col("p", "decimal(8,2)", true),
			),
			want: `CREATE TABLE IF NOT EXISTS "s"."t" ("id" integer NOT NULL, "When" timestamp, "p" numeric(8,2), ` +
				`PRIMARY KEY ("id"))`,
		},
		{
			name: "postgres text pk becomes varchar on mysql",
			fam:  FamilyMySQL,
			tab:  tab(FamilyPostgres, []string{"k"}, col("k", "text", false), col("v", "text", true)),
			want: "CREATE TABLE IF NOT EXISTS `s`.`t` (`k` varchar(255) NOT NULL, `v` longtext, PRIMARY KEY (`k`))",
		},
		{
			name: "no primary key",
			fam:  FamilyPostgres,
			tab:  tab(FamilyPostgres, nil, col("a", "integer", true)),
			want: `CREATE TABLE IF NOT EXISTS "s"."t" ("a" integer)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tg := &sqlTarget{fam: tt.fam, src: tt.tab.SourceFamily}
			assert.Equal(t, tt.want, tg.createSQL(tt.tab))
		})
	}
}

func TestSQLTargetWhere(t *testing.T) {
	t.Parallel()

	tab := TargetTable{PK: []string{"id"}, Columns: []TargetColumn{
		{Name: "id"}, {Name: "n"},
	}}
	keyless := TargetTable{Columns: tab.Columns}

	pg := &sqlTarget{fam: FamilyPostgres}
	my := &sqlTarget{fam: FamilyMySQL}

	w, args := pg.where(tab, Row{"1", "x"}, 2)
	assert.Equal(t, `"id" = $3`, w)
	assert.Equal(t, []any{"1"}, args)

	w, args = my.where(keyless, Row{"1", nil}, 0)
	assert.Equal(t, "`id` = ? AND `n` IS NULL", w)
	assert.Equal(t, []any{"1"}, args)
}
