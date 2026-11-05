package redisdocker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOwnedSlotsParsesMyselfLine(t *testing.T) {
	t.Parallel()

	nodes := "aaa 1.1.1.1:1@2 master - 0 0 1 connected 100-200\n" +
		"bbb :6379@16379,127.0.0.1 myself,master - 0 0 0 connected 0-9 20 [93->-ccc] 30-31\n"

	assert.Equal(t, []SlotRange{{0, 9}, {20, 20}, {30, 31}}, ownedSlots(nodes))
	assert.Empty(t, ownedSlots("aaa 1.1.1.1:1@2 master - 0 0 1 connected 1-2\n"))
}

func TestSubtractSlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    []SlotRange
		owned   []SlotRange
		missing []SlotRange
		wantErr bool
	}{
		{name: "nothing owned", want: []SlotRange{{0, 9}}, missing: []SlotRange{{0, 9}}},
		{name: "partly owned", want: []SlotRange{{0, 9}}, owned: []SlotRange{{3, 4}, {9, 9}},
			missing: []SlotRange{{0, 2}, {5, 8}}},
		{name: "all owned", want: []SlotRange{{0, 9}}, owned: []SlotRange{{0, 9}}},
		{name: "owned outside shard", want: []SlotRange{{0, 9}}, owned: []SlotRange{{10, 10}}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := subtractSlots(tc.want, tc.owned)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrSlots)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.missing, got)
		})
	}
}
