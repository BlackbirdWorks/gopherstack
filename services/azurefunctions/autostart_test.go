package azurefunctions_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func TestAutoStart(t *testing.T) {
	t.Parallel()

	timer := func(disabled string) map[string]string {
		return map[string]string{
			"host.json": `{"version":"2.0"}`,
			"tick/function.json": `{"bindings":[{"type":"timerTrigger","direction":"in","name":"t",` +
				`"schedule":"0 */5 * * * *"}]` +
				disabled + `}`,
		}
	}

	tests := []struct {
		files    map[string]string
		name     string
		wantHost bool
	}{
		{timer(""), "timer trigger starts the host", true},
		{timer(`,"disabled":true`), "disabled timer stays down", false},
		{
			map[string]string{
				"host.json":       `{"version":"2.0"}`,
				"h/function.json": `{"bindings":[{"type":"httpTrigger","direction":"in","name":"r","authLevel":"anonymous"}]}`,
			},
			"http only waits for a request",
			false,
		},
		{
			map[string]string{
				"host.json":       `{"version":"2.0"}`,
				"q/function.json": `{"bindings":[{"type":"queueTrigger","direction":"in","name":"m","queueName":"q"}]}`,
			},
			"queue trigger starts the host",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})

			_, err := e.backend.Deploy("app", zipOf(t, tt.files), "t", "m")
			require.NoError(t, err)

			if !tt.wantHost {
				// A request-driven start would show up after a request; none was sent.
				assert.Never(t, func() bool { return e.rt.running() > 0 }, 200*time.Millisecond, 20*time.Millisecond)

				return
			}

			require.Eventually(t, func() bool { return e.rt.running() == 1 }, 10*time.Second, 20*time.Millisecond)

			require.NoError(t, e.backend.Restart("app"))
			require.Eventually(t, func() bool { return len(e.rt.specs()) == 2 }, 10*time.Second, 20*time.Millisecond,
				"a restart brings the background host back without a request")

			require.NoError(t, e.backend.SetStopped("app", true))
			assert.Eventually(t, func() bool { return e.rt.running() == 0 }, 10*time.Second, 20*time.Millisecond)
		})
	}
}
