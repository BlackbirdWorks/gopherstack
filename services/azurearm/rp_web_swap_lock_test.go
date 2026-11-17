package azurearm_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type warmRecorder struct {
	azurearm.FunctionApps

	entered chan string
	release chan struct{}
}

func newWarmRecorder() *warmRecorder {
	return &warmRecorder{entered: make(chan string, 4), release: make(chan struct{})}
}

func (w *warmRecorder) PutApp(name string, _ azurearm.FunctionAppConfig) (azurearm.FunctionAppInfo, error) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local", SCMHost: name + ".scm.local"}, nil
}

func (w *warmRecorder) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	return azurearm.FunctionAppInfo{Created: time.Now(), HostName: name + ".local"}, true
}

func (w *warmRecorder) Settings(string) (map[string]string, error) { return map[string]string{}, nil }

func (w *warmRecorder) SetSettings(string, map[string]string) error { return nil }

func (w *warmRecorder) SetConnectionStrings(string, map[string]string) error { return nil }

func (w *warmRecorder) DeleteApp(string) error { return nil }

func (w *warmRecorder) SwapContent(string, string) error { return nil }

func (w *warmRecorder) WarmUp(ctx context.Context, name string) error {
	w.entered <- name

	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSwapWarmUpDoesNotHoldTheProviderLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		during  func(t *testing.T, h *azurearm.Handler, wp *azurearm.WebProvider, site, slot azurearm.ResourceID)
		name    string
	}{
		{
			name: "other work proceeds and the swap completes",
			during: func(t *testing.T, h *azurearm.Handler, wp *azurearm.WebProvider, site, slot azurearm.ResourceID) {
				t.Helper()

				got, err := h.Registry.Get(t.Context(), site)
				require.NoError(t, err)
				assert.NotEmpty(t, got)

				_, err = wp.PutConfig(t.Context(), slot, "web", map[string]any{
					"properties": map[string]any{"appCommandLine": "x"},
				}, true)
				require.ErrorIs(t, err, azurearm.ErrWebSwapPending, "the warming source rejects changes")

				err = wp.SwapSlots(t.Context(), slot, map[string]any{"targetSlot": "production"})
				require.ErrorIs(t, err, azurearm.ErrWebSwapPending, "a second swap is refused while one warms")
			},
		},
		{
			name: "reset during the warm-up cancels the swap",
			during: func(t *testing.T, _ *azurearm.Handler, wp *azurearm.WebProvider, _, slot azurearm.ResourceID) {
				t.Helper()
				require.NoError(t, wp.ResetSlotConfig(t.Context(), slot))
			},
			wantErr: azurearm.ErrWebSwapFailed,
		},
		{
			name: "deleting the site during the warm-up fails the swap",
			during: func(t *testing.T, h *azurearm.Handler, _ *azurearm.WebProvider, site, _ azurearm.ResourceID) {
				t.Helper()
				require.NoError(t, h.Registry.Delete(t.Context(), site))
			},
			wantErr: azurearm.ErrWebSiteNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newWebHandler(t)
			rec := newWarmRecorder()
			h.Registry.SetFunctionApps(rec)

			ctx := t.Context()
			site := webID([]string{"sites"}, "app1")
			slot := webID([]string{"sites", "slots"}, "app1", "stage")
			body := map[string]any{"kind": "functionapp,linux", "location": "local"}

			_, _, err := h.Registry.Put(ctx, site, body)
			require.NoError(t, err)
			_, _, err = h.Registry.Put(ctx, slot, body)
			require.NoError(t, err)

			p, _ := h.Registry.ProviderNamed("Microsoft.Web")
			wp, ok := p.(*azurearm.WebProvider)
			require.True(t, ok)

			done := make(chan error, 1)

			go func() { done <- wp.SwapSlots(ctx, slot, map[string]any{"targetSlot": "production"}) }()

			require.Equal(t, "app1/stage", <-rec.entered)
			tt.during(t, h, wp, site, slot)
			close(rec.release)

			err = <-done

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}
