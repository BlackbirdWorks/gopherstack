package mysqlengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVariableArgBindsNumbersAsNumbers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		in   string
	}{
		{int64(123), "integer", "123"},
		{int64(-1), "negative", "-1"},
		{0.5, "float", "0.5"},
		{"latin1", "text", "latin1"},
		{"+00:00", "offset", "+00:00"},
		{"NaN", "nan", "NaN"},
		{"0x10", "hex", "0x10"},
		{"1e3", "exponent", "1e3"},
		{"007", "leading zeros", "007"},
		{"+5", "plus sign", "+5"},
		{int64(0), "zero", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, variableArg(tt.in))
		})
	}
}

func TestReadOnlyStmtsUsePersistExceptOnMySQL57(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		[]string{"SET PERSIST read_only = ON", "SET PERSIST super_read_only = ON"},
		readOnlyStmts(true, false),
	)
	assert.Equal(
		t,
		[]string{"SET GLOBAL read_only = OFF", "SET GLOBAL super_read_only = OFF"},
		readOnlyStmts(false, true),
	)
	assert.True(t, (&Server{spec: Spec{Image: "mysql:5.7"}}).noPersist())
	assert.False(t, (&Server{spec: Spec{Image: "mysql:8.4"}}).noPersist())
}
