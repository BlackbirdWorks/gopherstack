package mysqlengine

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMySQLServerFlags(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"--server-id=7", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"}, replicationFlags(7))
	assert.Contains(t, replicationFlags(0)[0], "--server-id=1", "a zero id is replaced")
}

func binlogEvent(ts uint32, typ byte, size uint32) []byte {
	ev := make([]byte, size)
	binary.LittleEndian.PutUint32(ev[0:4], ts)
	ev[4] = typ
	binary.LittleEndian.PutUint32(ev[9:13], size)

	return ev
}

func TestBinlogCutoff(t *testing.T) {
	t.Parallel()

	log := make([]byte, 0, 256)
	log = append(log, 0xfe, 'b', 'i', 'n')
	log = append(log, binlogEvent(100, 15, 40)...)

	first := uint64(len(log))
	log = append(log, binlogEvent(100, eventGTID, 30)...)
	log = append(log, binlogEvent(100, 2, 50)...)

	second := uint64(len(log))
	log = append(log, binlogEvent(105, eventGTID, 30)...)
	log = append(log, binlogEvent(105, 2, 50)...)

	tests := []struct {
		name      string
		lo        uint64
		want      uint64
		stop      uint32
		wantFound bool
	}{
		{name: "stops before the later transaction", lo: 4, stop: 102, want: second, wantFound: true},
		{name: "stop before everything", lo: 4, stop: 50, want: first, wantFound: true},
		{name: "stop after everything", lo: 4, stop: 500, want: uint64(len(log))},
		{name: "start past the early one", lo: first, stop: 106, want: uint64(len(log))},
		{name: "start at a boundary", lo: second, stop: 105, want: second, wantFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, found := binlogCutoff(log, tt.lo, tt.stop)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantFound, found)
		})
	}
}
