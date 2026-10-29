package gcpsql

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
)

func TestPGLaunchSpec(t *testing.T) {
	t.Parallel()

	rec := &Recovery{At: time.Date(2026, 10, 29, 12, 0, 1, 500_000_000, time.UTC), Volume: "src-pitr"}

	tests := []struct {
		name       string
		wantMounts []string
		wantEnv    []string
		eng        dockerEngine
		wantEntry  bool
		wantCmd    bool
	}{
		{name: "plain without volumes", eng: dockerEngine{}, wantCmd: true},
		{
			name: "primary with volumes", eng: dockerEngine{volData: "d", volPITR: "p"}, wantEntry: true,
			wantMounts: []string{"d:" + pgDataDir, "p:" + pitrMount},
		},
		{
			name: "replica", wantEntry: true, wantMounts: []string{"d:" + pgDataDir},
			eng: dockerEngine{volData: "d", spec: LaunchSpec{
				Replica: &ReplicaSource{Host: "h", Port: 5432, Password: "pw"},
			}},
			wantEnv: []string{dbrepl.PasswordEnv + "=pw"},
		},
		{
			name: "recovery", wantEntry: true,
			eng:        dockerEngine{volData: "d", volPITR: "p", spec: LaunchSpec{Recovery: rec}},
			wantMounts: []string{"d:" + pgDataDir, "p:" + pitrMount, "src-pitr:" + pitrSrcMount + ":ro"},
			wantEnv:    []string{recoveryEnv + "=2026-10-29 12:00:01.500000+00"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var s container.Spec

			tt.eng.pgLaunch(&s)

			assert.Equal(t, tt.wantMounts, s.Mounts)
			assert.Equal(t, tt.wantEnv, s.Env)
			assert.Equal(t, tt.wantEntry, len(s.Entrypoint) > 0)
			assert.Equal(t, tt.wantCmd, len(s.Cmd) > 0)
			assert.Contains(t, append(append([]string{}, s.Cmd...), s.Entrypoint...), "wal_level=replica")
		})
	}
}

func TestMySQLServerFlags(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"--server-id=7", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"}, mysqlServerFlags(7))
	assert.Contains(t, mysqlServerFlags(0)[0], "--server-id=1", "a zero id is replaced")
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

	log := []byte{0xfe, 'b', 'i', 'n'}
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
