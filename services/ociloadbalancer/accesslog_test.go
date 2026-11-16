package ociloadbalancer_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logRec struct {
	data     map[string]any
	lbID     string
	category string
}

func TestAccessAndErrorLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantCats   []string
		backends   int
		wantStatus int
	}{
		{name: "served", backends: 1, wantStatus: http.StatusOK, wantCats: []string{"access"}},
		{
			name: "no backend", backends: 0, wantStatus: http.StatusServiceUnavailable,
			wantCats: []string{"access", "error"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			logs := make(chan logRec, 8)
			e.backend.SetLogSink(func(lbID, category string, _ time.Time, data map[string]any) {
				logs <- logRec{lbID: lbID, category: category, data: data}
			})

			d := baseDetails(tc.name)
			if tc.backends > 0 {
				d.BackendSets = map[string]loadbalancer.BackendSetDetails{
					"bs": backendSet("ROUND_ROBIN", startBackend(t, "a")),
				}
			} else {
				d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN")}
			}

			d.Listeners = map[string]loadbalancer.ListenerDetails{"l": listener()}
			id := e.mkLB(t, d)

			r := e.call(t, id, httpPort, "", "/x?y=1", nil)
			assert.Equal(t, tc.wantStatus, r.status)

			var cats []string

			for range tc.wantCats {
				var rec logRec

				select {
				case rec = <-logs:
				case <-t.Context().Done():
					require.FailNow(t, "log record not delivered")
				}
				assert.Equal(t, id, rec.lbID)

				cats = append(cats, rec.category)

				if rec.category == "access" {
					assert.EqualValues(t, tc.wantStatus, rec.data["lbStatusCode"])
					assert.Equal(t, "l", rec.data["listenerName"])
					assert.Equal(t, "GET /x?y=1 HTTP/1.1", rec.data["request"])
					assert.Contains(t, rec.data, "clientAdd")
				} else {
					assert.Contains(t, rec.data, "errorLog")
				}
			}

			assert.Equal(t, tc.wantCats, cats)
			assert.Empty(t, logs)
		})
	}
}
