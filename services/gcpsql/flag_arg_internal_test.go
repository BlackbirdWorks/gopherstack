package gcpsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLFlagArg(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		in   string
	}{
		{name: "int", in: "128", want: int64(128)},
		{name: "negative", in: "-1", want: int64(-1)},
		{name: "decimal", in: "1.5", want: 1.5},
		{name: "true", in: "true", want: true},
		{name: "false", in: "FALSE", want: false},
		{name: "on", in: "ON", want: "ON"},
		{name: "text", in: "utf8mb4", want: "utf8mb4"},
		{name: "exponent", in: "1e3", want: "1e3"},
		{name: "hex", in: "0x10", want: "0x10"},
		{name: "inf", in: "Inf", want: "Inf"},
		{name: "injection", in: "1; DROP TABLE x", want: "1; DROP TABLE x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, mysqlFlagArg(tt.in))
		})
	}
}
