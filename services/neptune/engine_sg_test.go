package neptune_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// freshConnGet issues a GET on a new connection and returns the body, empty when the server hangs up.
func freshConnGet(t *testing.T, target string) string {
	t.Helper()

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	require.NoError(t, err)

	resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return ""
	}

	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(resp.Body)

	return string(raw)
}

func TestEngineSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		openCypher bool
		enforce    bool
	}{
		{"openCypher front enforced", true, true},
		{"bare relay enforced", false, true},
		{"flag off publishes directly", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sgtest.New(sgtest.OpenTo("sg-1", 8182, "203.0.113.0/24"))
			rt := &upstreamRuntime{fakeEngineRuntime: &fakeEngineRuntime{}}
			b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(neptune.EngineConfig{
				Runtime: rt, Probe: func(context.Context, string) error { return nil },
				StartTimeout: time.Minute, OpenCypher: tt.openCypher,
			})
			t.Cleanup(b.Close)

			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			_, err := b.CreateDBCluster(t.Context(), "c1", "", 0, neptune.DBClusterCreateOptions{
				EngineVersion: "1.3.0.0", VpcSecurityGroupIDs: []string{"sg-1"},
			})
			require.NoError(t, err)

			c := waitCluster(t, b, "c1", "available")
			url := "http://" + net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port)) + "/x"
			spec := rt.createdSpecs()[0]

			if !tt.enforce {
				assert.Contains(t, spec.Ports[0], ":"+strconv.Itoa(c.Port)+":")
				assert.Equal(t, "upstream:/x", freshConnGet(t, url))

				return
			}

			assert.True(t, strings.HasPrefix(spec.Ports[0], "127.0.0.1:"))
			assert.Empty(t, freshConnGet(t, url), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 8182, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 8182, "127.0.0.1/32").Ingress...)
			src.Set(g)

			assert.Equal(t, "upstream:/x", freshConnGet(t, url), "authorized loopback connects on the next connection")
		})
	}
}
