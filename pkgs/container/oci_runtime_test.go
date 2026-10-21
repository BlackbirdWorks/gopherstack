package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errListRuntimes = errors.New("info failed")

type runtimeAPI struct {
	*mockAPI
	err error
	rts []string
}

func (r *runtimeAPI) Runtimes(context.Context) ([]string, error) { return r.rts, r.err }

func TestCreateAndStart_OCIRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		oci  string
	}{
		{name: "runsc", oci: "runsc"},
		{name: "default", oci: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := &mockAPI{}
			_, err := newRuntime(api).CreateAndStart(t.Context(), container.Spec{Image: "img", OCIRuntime: tt.oci})
			require.NoError(t, err)
			assert.Equal(t, tt.oci, api.lastHost.Runtime)
		})
	}
}

func TestResolveOCIRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err        error
		api        container.APIClient
		name       string
		want       string
		candidates []string
	}{
		{name: "present", api: &runtimeAPI{mockAPI: &mockAPI{}, rts: []string{"runc", "runsc"}},
			candidates: []string{"runsc"}, want: "runsc"},
		{name: "second_candidate", api: &runtimeAPI{mockAPI: &mockAPI{}, rts: []string{"kata-runtime"}},
			candidates: []string{"kata", "kata-runtime"}, want: "kata-runtime"},
		{name: "missing", api: &runtimeAPI{mockAPI: &mockAPI{}, rts: []string{"runc"}},
			candidates: []string{"runsc"}, err: container.ErrOCIRuntimeMissing},
		{name: "list_error", api: &runtimeAPI{mockAPI: &mockAPI{}, err: errListRuntimes},
			candidates: []string{"runsc"}, err: container.ErrOCIRuntimeUnverifiable},
		{name: "unsupported_client", api: &mockAPI{},
			candidates: []string{"runsc"}, err: container.ErrOCIRuntimeUnverifiable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := container.ResolveOCIRuntime(t.Context(), newRuntime(tt.api), tt.candidates...)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
