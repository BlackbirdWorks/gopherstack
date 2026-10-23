package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var (
	errBind  = errors.New("bind for 127.0.0.1:1 failed: port is already allocated")
	errOther = errors.New("no such image")
)

type retryRuntime struct {
	container.Runtime

	errs  []error
	specs []container.Spec
}

func (r *retryRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.specs = append(r.specs, spec)

	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]

		if err != nil {
			return "", err
		}
	}

	return "cid", nil
}

func TestCreateAndStartRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		name         string
		errs         []error
		wantAttempts int
		wantReleases int
	}{
		{name: "first_ok", wantAttempts: 1},
		{name: "conflict_once", errs: []error{errBind}, wantAttempts: 2, wantReleases: 1},
		{name: "other_error", errs: []error{errOther}, wantErr: errOther, wantAttempts: 1, wantReleases: 1},
		{
			name:         "bounded",
			errs:         []error{errBind, errBind, errBind, errBind, errBind, errBind},
			wantErr:      errBind,
			wantAttempts: container.PortRetries,
			wantReleases: container.PortRetries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &retryRuntime{errs: tt.errs}
			prepares, releases := 0, 0

			id, err := container.CreateAndStartRetry(t.Context(), rt,
				func() (container.Spec, error) {
					prepares++

					host := "127.0.0.1:" + string(rune('a'+prepares))

					return container.Spec{Name: "n", Ports: []string{host + ":1"}}, nil
				},
				func() { releases++ })

			assert.Equal(t, tt.wantAttempts, prepares)
			assert.Equal(t, tt.wantReleases, releases)
			require.Len(t, rt.specs, tt.wantAttempts)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "cid", id)

			if tt.wantAttempts > 1 {
				assert.NotEqual(t, rt.specs[0].Ports, rt.specs[1].Ports)
			}
		})
	}
}

func TestIsPortConflict(t *testing.T) {
	t.Parallel()

	assert.True(t, container.IsPortConflict(errBind))
	assert.False(t, container.IsPortConflict(errOther))
	assert.False(t, container.IsPortConflict(nil))
}
