package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ociqueue"
)

const (
	ociQueueComp = "ocid1.compartment.oc1..aaaa"
	ociQueueBase = "/dashboard/api/v1/oci/queue"
)

func newOCIQueueDashboard(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	b := ociqueue.NewInMemoryBackend()
	b.SetTransition(0)

	two := 2
	_, err := b.CreateQueue(ociqueue.CreateQueueIn{Name: "jobs", Compartment: ociQueueComp, DLQCount: &two})
	require.NoError(t, err)

	pg, err := b.ListQueues(ociQueueComp, "", "", "", "", ociqueue.ListOpts{})
	require.NoError(t, err)
	require.Len(t, pg.Data, 1)

	id := pg.Data[0].ID
	ops := ociqueue.NewHandler(b)
	e := echo.New()
	e.Any("/*", ops.Handler())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/20210201/queues/"+id+"/messages",
		strings.NewReader(`{"messages":[{"content":"first"},{"content":"second"}]}`))
	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return dashboard.NewHandler(dashboard.Config{OCIQueueOps: ops}), id
}

func TestOCIQueueProxy(t *testing.T) {
	t.Parallel()

	api := ociQueueBase + "/api"

	tests := []struct {
		steps func(id string) []gcpStep
		name  string
	}{
		{name: "read", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, ociQueueBase + "/compartments", "", ociQueueComp, 200},
				{http.MethodGet, api + "/queues?compartmentId=" + ociQueueComp, "", `"displayName":"jobs"`, 200},
				{http.MethodGet, api + "/queues/" + id, "", `"deadLetterQueueDeliveryCount":2`, 200},
				{http.MethodGet, api + "/queues/" + id + "/stats", "", `"visibleMessages":2`, 200},
			}
		}},
		{name: "peek", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, api + "/queues/" + id + "/peek", "", `"content":"first"`, 200},
				{http.MethodGet, api + "/queues/" + id + "/peek?limit=1", "", `"content":"first"`, 200},
				{http.MethodGet, api + "/queues/" + id + "/peek?dlq=true", "", `"messages":[]`, 200},
				{http.MethodGet, api + "/queues/" + id + "/stats", "", `"inFlightMessages":0`, 200},
				{
					http.MethodGet,
					api + "/queues/ocid1.queue.oc1..nope/peek",
					"",
					"NotAuthorizedOrNotFound",
					http.StatusNotFound,
				},
			}
		}},
		{name: "purge", steps: func(id string) []gcpStep {
			return []gcpStep{
				{
					http.MethodPost,
					api + "/queues/" + id + "/actions/purge",
					`{"purgeType":"NORMAL"}`,
					"",
					http.StatusAccepted,
				},
				{http.MethodGet, api + "/queues/" + id + "/stats", "", `"visibleMessages":0`, 200},
				{http.MethodPost, api + "/queues/" + id + "/actions/purge", `{"purgeType":"x"}`, "purgeType", 400},
			}
		}},
		{name: "rejects", steps: func(id string) []gcpStep {
			return []gcpStep{
				{http.MethodDelete, api + "/queues/" + id, "", "", http.StatusMethodNotAllowed},
				{http.MethodPost, api + "/queues", `{}`, "unsupported", 400},
				{http.MethodGet, api + "/queues/" + id + "/messages", "", "unsupported", 400},
				{http.MethodPost, api + "/queues/" + id + "/messages", `{}`, "unsupported", 400},
				{http.MethodPost, api + "/queues/" + id + "/actions/updateMessages", `{}`, "unsupported", 400},
				{http.MethodGet, api + "/workRequests", "", "unsupported", 400},
				{http.MethodGet, api + "/queues/%2e%2e", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, id := newOCIQueueDashboard(t)
			runGCPSteps(t, h, tt.steps(id))
		})
	}

	t.Run("peek keeps messages receivable", func(t *testing.T) {
		t.Parallel()

		h, id := newOCIQueueDashboard(t)
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(echo.NewContext(
			httptest.NewRequest(http.MethodGet, api+"/queues/"+id+"/peek", nil), rec)))
		assert.NotContains(t, rec.Body.String(), "receipt")
		assert.Contains(t, rec.Body.String(), `"deliveryCount":0`)
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/queues?compartmentId=x", nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
