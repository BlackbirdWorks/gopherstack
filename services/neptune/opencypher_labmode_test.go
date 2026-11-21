package neptune_test

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestOpenCypherDatetimeMillisecondLabMode(t *testing.T) {
	t.Parallel()

	rt := &upstreamRuntime{fakeEngineRuntime: &fakeEngineRuntime{}}
	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{
		Runtime: rt, Probe: func(context.Context, string) error { return nil },
		StartTimeout: time.Minute, OpenCypher: true,
	})
	t.Cleanup(b.Close)

	_, err := b.CreateDBClusterParameterGroup(t.Context(), "lab", "neptune1.3", "lab")
	require.NoError(t, err)

	_, err = b.CreateDBCluster(t.Context(), "c1", "lab", 0, neptune.DBClusterCreateOptions{EngineVersion: "1.3.2.0"})
	require.NoError(t, err)

	c := waitCluster(t, b, "c1", "available")
	base := "http://" + net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))
	query := url.Values{"query": {"RETURN datetime('2024-04-01T23:59:59Z') AS d"}}.Encode()

	datetimeOf := func() any {
		code, out := cypherDo(t, http.MethodPost, base+"/openCypher", "application/x-www-form-urlencoded", query)
		require.Equal(t, http.StatusOK, code)

		rows, ok := out["results"].([]any)
		require.True(t, ok)
		require.Len(t, rows, 1)

		return rows[0].(map[string]any)["d"]
	}

	assert.Equal(t, "2024-04-01T23:59:59Z", datetimeOf())

	_, err = b.ModifyDBClusterParameterGroup(t.Context(), "lab", []neptune.ParameterInput{{
		ParameterName: "neptune_lab_mode", ParameterValue: "Streams=disabled, DatetimeMillisecond=enabled",
		ApplyMethod: "pending-reboot",
	}})
	require.NoError(t, err)

	assert.Equal(t, "2024-04-01T23:59:59.000Z", datetimeOf())

	code, status := cypherDo(t, http.MethodGet, base+"/status", "", "")
	require.Equal(t, http.StatusOK, code)

	lab, ok := status["labMode"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "enabled", lab["DatetimeMillisecond"])
}
