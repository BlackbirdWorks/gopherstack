package gcpsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"single", "select 1", []string{"select 1"}},
		{"two", "select 1; select 2;", []string{"select 1", "select 2"}},
		{"quoted semicolon", "select 'a;b'; select \"c;d\"", []string{"select 'a;b'", "select \"c;d\""}},
		{"escaped quote", `select 'it\'s;ok'; select 2`, []string{`select 'it\'s;ok'`, "select 2"}},
		{"backtick", "select `a;b` from t; select 2", []string{"select `a;b` from t", "select 2"}},
		{"line comment", "select 1 -- x; y\n; select 2", []string{"select 1 -- x; y", "select 2"}},
		{"hash comment", "# only\nselect 1;", []string{"# only\nselect 1"}},
		{"block comment", "select /* ; */ 1; select 2", []string{"select /* ; */ 1", "select 2"}},
		{"empty", " ; ;", nil},
		{"comment only tail", "select 1; -- done", []string{"select 1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, splitStatements(tt.in))
		})
	}
}
