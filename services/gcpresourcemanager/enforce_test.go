package gcpresourcemanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

func TestCheckProject(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	tests := []struct {
		name    string
		project string
		wantErr bool
	}{
		{"id", seedID, false},
		{"projects prefix", "projects/" + seedID, false},
		{"unknown", "nope-nope", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := e.backend.CheckProject(tt.project, "pubsub.googleapis.com")
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, codes.PermissionDenied, status.Code(err))

			for _, d := range status.Convert(err).Details() {
				if i, ok := d.(*errdetails.ErrorInfo); ok {
					assert.Equal(t, "CONSUMER_INVALID", i.GetReason())
					assert.Equal(t, "pubsub.googleapis.com", i.GetMetadata()["service"])

					return
				}
			}

			require.Fail(t, "no ErrorInfo")
		})
	}
}

func TestEnforcementGateFollowsSettings(t *testing.T) {
	t.Parallel()

	h := gcpresourcemanager.NewHandler(gcpresourcemanager.NewInMemoryBackend())
	assert.Nil(t, h.EnforcementGate())

	h.SetEnforcement(gcpenforce.Settings{Projects: true})
	g := h.EnforcementGate()
	require.NotNil(t, g)
	assert.True(t, g.Settings.Projects)
}
