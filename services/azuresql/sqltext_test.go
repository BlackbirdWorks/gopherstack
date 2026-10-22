package azuresql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresql"
)

func TestSQLTextQuotesServerSide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func() (string, error)
		name    string
		want    []string
		wantErr bool
	}{
		{
			name: "create escapes quotes and quotes the identifier in the engine",
			call: func() (string, error) { return azuresql.CreateDatabaseSQL("it's", "") },
			want: []string{"N'it''s'", "QUOTENAME(@n)", "DB_ID(@n) IS NULL"},
		},
		{
			name: "create with collation",
			call: func() (string, error) { return azuresql.CreateDatabaseSQL("app", "Latin1_General_CI_AS") },
			want: []string{"COLLATE Latin1_General_CI_AS"},
		},
		{
			name:    "collation with injection",
			call:    func() (string, error) { return azuresql.CreateDatabaseSQL("app", "x; DROP DATABASE y") },
			wantErr: true,
		},
		{name: "empty name", call: func() (string, error) { return azuresql.CreateDatabaseSQL("", "") }, wantErr: true},
		{
			name:    "newline in name",
			call:    func() (string, error) { return azuresql.DropDatabaseSQL("a\nGO") },
			wantErr: true,
		},
		{
			name: "drop kicks sessions out first",
			call: func() (string, error) { return azuresql.DropDatabaseSQL("app") },
			want: []string{"SINGLE_USER WITH ROLLBACK IMMEDIATE", "DROP DATABASE ' + QUOTENAME(@n)"},
		},
		{
			name: "login quotes the password server-side",
			call: func() (string, error) { return azuresql.LoginSQL("adm", "p'w"), nil },
			want: []string{"N'p''w'", "QUOTENAME(@p, '''')", "QUOTENAME(@l)", "sysadmin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()
			if tt.wantErr {
				require.ErrorIs(t, err, azuresql.ErrInvalid)

				return
			}

			require.NoError(t, err)

			for _, w := range tt.want {
				assert.Contains(t, got, w)
			}
		})
	}
}
