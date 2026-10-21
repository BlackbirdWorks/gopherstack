// Package resttest drives raw REST/JSON request flows against a gcprest-served emulator.
package resttest

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const requestTimeout = 30 * time.Second

// Step is one request in a flow and the response it must produce.
type Step struct {
	Name     string
	Method   string
	Path     string
	Body     string
	Contains []string
	Status   int
}

// Run issues steps in order against base (host:port) and checks status and body fragments.
func Run(t *testing.T, base string, steps []Step) {
	t.Helper()

	// No keep-alive: a pooled connection's readLoop can outlive the test and trip goleak.
	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true
	client := &http.Client{Timeout: requestTimeout, Transport: tr}
	t.Cleanup(client.CloseIdleConnections)

	for _, s := range steps {
		t.Run(s.Name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(
				t.Context(),
				s.Method,
				"http://"+base+s.Path,
				strings.NewReader(s.Body),
			)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, s.Status, resp.StatusCode, string(b))
			assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

			for _, frag := range s.Contains {
				assert.Contains(t, string(b), frag)
			}
		})
	}
}
