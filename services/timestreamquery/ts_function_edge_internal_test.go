package timestreamquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func scalarOf(t *testing.T, expr string) (string, error) {
	t.Helper()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	pg, err := b.QueryWithOptions(t.Context(), QueryOptions{QueryString: "SELECT " + expr})
	if err != nil {
		return "", err
	}

	require.Len(t, pg.Rows, 1)

	return datumText(pg.Rows[0].Data[0]), nil
}

func datumText(d Datum) string {
	switch {
	case d.ScalarValue != nil:
		return *d.ScalarValue
	case d.NullValue != nil:
		return "NULL"
	}

	parts := make([]string, len(d.ArrayValue))
	for i, e := range d.ArrayValue {
		parts[i] = datumText(e)
	}

	return "[" + strings.Join(parts, ",") + "]"
}

func TestFunctionEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		want    string
		wantErr string
	}{
		{name: "negate year", expr: "-1year", want: "-1-0"},
		{name: "negate month sum", expr: "-1month + 3month", want: "0-2"},
		{name: "negate month operand", expr: "5month - -1year", want: "1-5"},
		{
			name: "negate month shift",
			expr: "TIMESTAMP '2024-03-31 00:00:00' + -1month",
			want: "2024-02-29 00:00:00.000000000",
		},
		{name: "negate interval keyword", expr: "-INTERVAL '2' YEAR", want: "-2-0"},
		{name: "log10 tenth", expr: "log10(0.1)", want: "-1.0"},
		{name: "log10 1e15", expr: "log10(1e15)", want: "15.0"},
		{name: "log10 1e-4", expr: "log10(0.0001)", want: "-4.0"},
		{name: "log10 non power", expr: "log10(2.0)", want: "0.3010299956639812"},
		{
			name:    "sequence full int range",
			expr:    "sequence(-9223372036854775807-1, 9223372036854775807)",
			wantErr: "invalid sequence bounds",
		},
		{
			name: "sequence ends at max",
			expr: "sequence(9223372036854775806, 9223372036854775807)",
			want: "[9223372036854775806,9223372036854775807]",
		},
		{name: "sequence step overflow", expr: "sequence(9223372036854775807, 9223372036854775800, -3)",
			want: "[9223372036854775807,9223372036854775804,9223372036854775801]"},
		{name: "sequence step away", expr: "sequence(1, 5, -1)", want: "[]"},
		{name: "sequence no args", expr: "sequence()", wantErr: "wrong number of arguments"},
		{name: "sequence one arg", expr: "sequence(1)", wantErr: "wrong number of arguments"},
		{name: "integral no args", expr: "integral_trapezoidal()", wantErr: "wrong number of arguments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := scalarOf(t, tt.expr)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)

			assert.Equal(t, tt.want, got)
		})
	}
}
