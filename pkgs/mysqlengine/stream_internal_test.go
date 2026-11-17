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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, variableArg(tt.in))
		})
	}
}
