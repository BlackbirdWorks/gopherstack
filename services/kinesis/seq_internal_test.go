package kinesis

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendPaddedMatchesSprintf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		v     uint64
		width int
	}{
		{name: "zero", v: 0, width: 4},
		{name: "small", v: 7, width: 20},
		{name: "exact", v: 1234, width: 4},
		{name: "overflow", v: 123456, width: 4},
		{name: "millis", v: 1790000000000, width: 14},
		{name: "max", v: ^uint64(0), width: 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := fmt.Sprintf("%0*d", tc.width, tc.v)
			assert.Equal(t, want, string(appendPadded(nil, strconv.AppendUint(nil, tc.v, 10), tc.width)))
		})
	}
}
