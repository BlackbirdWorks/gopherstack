package pgengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParameterStmtQuotesTheValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		param string
		value string
		want  string
		reset bool
	}{
		{"plain", "work_mem", "8192", `ALTER SYSTEM SET "work_mem" = '8192'`, false},
		{
			"quote doubled",
			"log_statement",
			"x'; DROP DATABASE postgres; --",
			`ALTER SYSTEM SET "log_statement" = 'x''; DROP DATABASE postgres; --'`,
			false,
		},
		{"dotted", "azure.extensions", "A,B", `ALTER SYSTEM SET "azure.extensions" = 'A,B'`, false},
		{"reset", "work_mem", "ignored", `ALTER SYSTEM RESET "work_mem"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parameterStmt(tt.param, tt.value, tt.reset)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
