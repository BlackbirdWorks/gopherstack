package container_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errVolumeBoom = errors.New("volume api failed")

type volumeAPI struct {
	err error
	*mockAPI
	listed  map[string]time.Time
	created map[string]map[string]string
	removed []string
}

func (v *volumeAPI) VolumeCreate(_ context.Context, name string, labels map[string]string) error {
	if v.err != nil {
		return v.err
	}

	v.created[name] = labels

	return nil
}

func (v *volumeAPI) VolumeRemove(_ context.Context, name string) error {
	v.removed = append(v.removed, name)

	return v.err
}

func (v *volumeAPI) VolumeListUnused(context.Context, string) (map[string]time.Time, error) {
	return v.listed, v.err
}

func TestDockerRuntime_Volumes(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		wantErr  error
		api      func() container.APIClient
		name     string
		wantList []string
	}{
		{
			name: "old unused volumes only",
			api: func() container.APIClient {
				return &volumeAPI{
					mockAPI: &mockAPI{}, created: map[string]map[string]string{},
					listed: map[string]time.Time{
						"old-b": now.Add(-time.Hour),
						"old-a": now.Add(-time.Hour),
						"new":   now,
					},
				}
			},
			wantList: []string{"old-a", "old-b"},
		},
		{
			name:    "client without volumes",
			api:     func() container.APIClient { return &mockAPI{} },
			wantErr: container.ErrVolumeUnsupported,
		},
		{
			name: "api error",
			api: func() container.APIClient {
				return &volumeAPI{mockAPI: &mockAPI{}, created: map[string]map[string]string{}, err: errVolumeBoom}
			},
			wantErr: errVolumeBoom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := tt.api()
			rt := newRuntime(api)

			var vr container.VolumeRuntime = rt

			got, err := vr.ListUnusedVolumes(t.Context(), "l", 10*time.Minute)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.ErrorIs(t, vr.CreateVolume(t.Context(), "v", nil), tt.wantErr)
				require.ErrorIs(t, vr.RemoveVolume(t.Context(), "v"), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantList, got)

			require.NoError(t, vr.CreateVolume(t.Context(), "v", map[string]string{"k": "1"}))
			require.NoError(t, vr.RemoveVolume(t.Context(), "v"))

			va, ok := api.(*volumeAPI)
			require.True(t, ok)
			assert.Equal(t, map[string]string{"k": "1"}, va.created["v"])
			assert.Equal(t, []string{"v"}, va.removed)
		})
	}
}
