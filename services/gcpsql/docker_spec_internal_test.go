package gcpsql

import (
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
		eng        *dockerEngine
		name       string
		wantMounts []string
		wantEnv    []string
		wantEntry  bool
		wantCmd    bool
	}{
		{name: "plain without volumes", eng: &dockerEngine{}, wantCmd: true},
		{
			name: "primary with volumes", eng: &dockerEngine{volData: "d", volPITR: "p"}, wantEntry: true,
			wantMounts: []string{"d:" + pgDataDir, "p:" + pitrMount},
		},
		{
			name: "replica", wantEntry: true, wantMounts: []string{"d:" + pgDataDir},
			eng: &dockerEngine{volData: "d", spec: LaunchSpec{
				Replica: &ReplicaSource{Host: "h", Port: 5432, Password: "pw"},
			}},
			wantEnv: []string{dbrepl.PasswordEnv + "=pw"},
		},
		{
			name: "recovery", wantEntry: true,
			eng:        &dockerEngine{volData: "d", volPITR: "p", spec: LaunchSpec{Recovery: rec}},
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
