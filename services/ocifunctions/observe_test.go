package ocifunctions_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

func TestInvokeObserver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		observed bool
		wantApp  bool
	}{
		{"observer registered", true, true},
		{"no observer", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ie := newInvokeEnv(t, nil, nil)

			var (
				mu     sync.Mutex
				events []oci.InvokeEvent
			)

			if tt.observed {
				ie.backend.SetInvokeObserver(func(ev oci.InvokeEvent) {
					mu.Lock()
					events = append(events, ev)
					mu.Unlock()
				})
			}

			_, _, err := ie.invoke(t, "hello", nil)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()

			if !tt.wantApp {
				assert.Empty(t, events)

				return
			}

			require.Len(t, events, 1)
			assert.Equal(t, *ie.app.Id, events[0].AppID)
			assert.Equal(t, *ie.fn.Id, events[0].FunctionID)
			assert.Equal(t, "fn", events[0].FunctionName)
			assert.Equal(t, 200, events[0].Status)
			assert.NotEmpty(t, events[0].CallID)
		})
	}
}
