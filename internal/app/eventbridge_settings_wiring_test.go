package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestEventBridgeSettingsReachBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		allow   bool
		wantErr bool
	}{
		{name: "default rejects", allow: false, wantErr: true},
		{name: "flag allows", allow: true, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
			cli.EventBridge.AllowScheduleOnCustomBus = tt.allow

			services, err := initializeServicesT(t,
				&service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()},
			)
			require.NoError(t, err)

			var bk *ebbackend.InMemoryBackend

			for _, s := range services {
				if h, ok := s.(*ebbackend.Handler); ok {
					bk, _ = h.Backend.(*ebbackend.InMemoryBackend)
				}
			}

			require.NotNil(t, bk)

			_, err = bk.CreateEventBus(t.Context(), ebbackend.CreateEventBusParams{Name: "custom"})
			require.NoError(t, err)

			_, err = bk.PutRule(t.Context(), ebbackend.PutRuleInput{
				Name: "r", EventBusName: "custom", ScheduleExpression: "rate(5 minutes)",
			})
			if tt.wantErr {
				require.ErrorIs(t, err, ebbackend.ErrInvalidParameter)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
