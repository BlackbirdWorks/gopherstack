package athena

import (
	"database/sql/driver"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUDFHugeIntegerArgs(t *testing.T) {
	t.Parallel()

	const arr = `[1,2,3]`

	huge := int64(math.MaxInt64)
	tiny := int64(math.MinInt64)

	tests := []struct {
		fn   func(*fnCtx, []driver.Value) (driver.Value, error)
		want driver.Value
		name string
		args []driver.Value
	}{
		{
			name: "split limit huge",
			fn:   udfSplit,
			args: []driver.Value{"a,b,c", ",", huge},
			want: containerMark + `["a","b","c"]`,
		},
		{name: "split_part huge", fn: udfSplitPart, args: []driver.Value{"a,b", ",", huge}, want: nil},
		{name: "element_at huge", fn: udfElementAt, args: []driver.Value{arr, huge}, want: nil},
		{name: "element_at tiny", fn: udfElementAt, args: []driver.Value{arr, tiny}, want: nil},
		{name: "slice start huge", fn: udfSlice, args: []driver.Value{arr, huge, int64(1)}, want: containerMark + `[]`},
		{
			name: "slice length huge",
			fn:   udfSlice,
			args: []driver.Value{arr, int64(2), huge},
			want: containerMark + `[2,3]`,
		},
		{name: "regexp_extract group huge", fn: udfRegexpExtract, args: []driver.Value{"abc", "(b)", huge}, want: nil},
		{
			name: "regexp_extract_all group huge", fn: udfRegexpExtractAll,
			args: []driver.Value{"abc", "(b)", huge}, want: containerMark + `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.fn(nil, tt.args)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPadHugeLength(t *testing.T) {
	t.Parallel()

	for _, left := range []bool{true, false} {
		_, err := pad([]driver.Value{"x", int64(math.MaxInt64), "0"}, left)
		require.ErrorIs(t, err, errPadTooLong)
	}

	got, err := pad([]driver.Value{"abc", int64(math.MaxInt64), "0"}, true)
	require.Error(t, err)
	assert.Nil(t, got)
}
