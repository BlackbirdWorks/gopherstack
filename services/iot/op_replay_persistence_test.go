package iot_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpReplaySurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		token       string
		replayToken string
		want        bool
	}{
		{name: "same_token_replays", token: "tok-1", replayToken: "tok-1", want: true},
		{name: "other_token_misses", token: "tok-1", replayToken: "tok-2", want: false},
		{name: "empty_token_misses", token: "", replayToken: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			b1.RecordOpCompleted("DeletePackage", tt.token, "pkg")
			assert.Equal(t, tt.want, b1.OpReplayed("DeletePackage", tt.replayToken, "pkg"))

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(t.Context(), b1.Snapshot(t.Context())))

			assert.Equal(t, tt.want, b2.OpReplayed("DeletePackage", tt.replayToken, "pkg"))
			assert.False(t, b2.OpReplayed("DeletePackage", tt.replayToken, "other"))
		})
	}
}
