package dsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidIndexDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ddl  string
		want bool
	}{
		{name: "plain", ddl: "CREATE INDEX i ON t (v)", want: true},
		{name: "unique lowercase", ddl: "create unique index i on t (v)", want: true},
		{name: "partial with quoted semicolon", ddl: "CREATE INDEX i ON t (v) WHERE v <> ';'", want: true},
		{name: "quoted identifier semicolon", ddl: `CREATE INDEX "a;b" ON t (v)`, want: true},
		{name: "reset then alter", ddl: "RESET SESSION AUTHORIZATION; ALTER ROLE app SUPERUSER", want: false},
		{name: "index then alter", ddl: "CREATE INDEX i ON t (v); ALTER ROLE app SUPERUSER", want: false},
		{name: "trailing separator", ddl: "CREATE INDEX i ON t (v);", want: false},
		{name: "not an index", ddl: "ALTER ROLE app SUPERUSER", want: false},
		{
			name: "dollar quote hides separator",
			ddl:  "CREATE INDEX i ON t (v) WHERE $$ ' $$ = ''; ALTER ROLE a SUPERUSER",
			want: false,
		},
		{
			name: "comment hides quote",
			ddl:  "CREATE INDEX i ON t (v) /* ' */; ALTER ROLE a SUPERUSER; /* ' */",
			want: false,
		},
		{name: "line comment", ddl: "CREATE INDEX i ON t (v) -- x", want: false},
		{name: "backslash", ddl: `CREATE INDEX i ON t (v) WHERE v = E'\'';`, want: false},
		{name: "empty", ddl: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, validIndexDDL(tt.ddl))
		})
	}
}
