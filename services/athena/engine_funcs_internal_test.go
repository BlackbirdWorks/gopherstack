package athena

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDFDateAddBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    driver.Value
		want driver.Value
		name string
	}{
		{name: "in range", n: int64(2), want: "2024-01-03 00:00:00.000"},
		{name: "string in range", n: "2", want: "2024-01-03 00:00:00.000"},
		{name: "above int32", n: int64(3_000_000_000), want: nil},
		{name: "below int32", n: "-3000000000", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := udfDateAdd(nil, []driver.Value{"day", tt.n, "2024-01-01 00:00:00.000"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
