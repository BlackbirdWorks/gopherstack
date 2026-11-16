package lambdaweb_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

func TestLogs_RuntimeShimLines(t *testing.T) {
	t.Parallel()

	const tagged = `{"__lambdaweb":1,"level":"WARN","message":{"k":"v"},"requestId":"req-9","endpointName":"other"}`

	tests := []struct {
		name      string
		level     types.ApplicationLogLevel
		wantCount int
	}{
		{name: "kept at info", level: types.ApplicationLogLevelInfo, wantCount: 1},
		{name: "dropped above warn", level: types.ApplicationLogLevelError, wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, logs := newLoggedEnv(t, logOpts{
				appLevel: tc.level,
				emit:     func(_ lambdaweb.LaunchSpec, line func(bool, string)) { line(false, tagged) },
			})

			require.Equal(t, http.StatusOK, d.get(t).StatusCode)

			app := appEvents(events(t, logs, "/aws/lambda/web/app"))
			require.Len(t, app, tc.wantCount)

			if tc.wantCount == 0 {
				return
			}

			assert.Equal(t, "WARN", app[0]["level"], "the console level wins over the stdout default")
			assert.Equal(t, map[string]any{"k": "v"}, app[0]["message"])
			assert.Equal(t, "req-9", app[0]["requestId"], "the shim's request attribution wins")
			assert.Equal(t, "other", app[0]["endpointName"])
		})
	}
}
