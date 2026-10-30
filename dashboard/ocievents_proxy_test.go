package dashboard_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocievents"
)

const (
	evComp  = "ocid1.compartment.oc1..events"
	evBase  = "/dashboard/api/v1/oci/events"
	evAPI   = evBase + "/api"
	evTopic = "ocid1.onstopic.oc1.iad.topic1"
)

func newEventsDashboard(t *testing.T) (*dashboard.DashboardHandler, *ocievents.InMemoryBackend) {
	t.Helper()

	b := ocievents.NewInMemoryBackend()
	ops := ocievents.NewHandler(b)

	return dashboard.NewHandler(dashboard.Config{OCIEventsOps: ops}), b
}

func evRuleBody(extra string) string {
	return `{"compartmentId":"` + evComp + `","displayName":"r1","isEnabled":true,` +
		`"condition":"{\"eventType\":\"com.oraclecloud.objectstorage.createobject\"}",` + extra +
		`"actions":{"actions":[{"actionType":"ONS","topicId":"` + evTopic + `","isEnabled":true}]}}`
}

func TestOCIEventsProxy(t *testing.T) {
	t.Parallel()

	cq := "?compartmentId=" + evComp

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{name: "create read toggle delete", steps: []gcpStep{
			{http.MethodPost, evAPI + "/rules", evRuleBody(""), `"displayName":"r1"`, 200},
			{http.MethodGet, evBase + "/compartments", "", evComp, 200},
			{http.MethodGet, evAPI + "/rules" + cq, "", `"displayName":"r1"`, 200},
			{http.MethodGet, evBase + "/deliveries", "", "[]", 200},
		}},
		{name: "body rebuild", steps: []gcpStep{
			{
				http.MethodPost,
				evAPI + "/rules",
				evRuleBody(`"freeformTags":{"x":"y"},"definedTags":{"a":{"b":"c"}},`),
				`"displayName":"r1"`,
				200,
			},
			{http.MethodGet, evAPI + "/rules" + cq, "", `"displayName":"r1"`, 200},
		}},
		{name: "rejects", steps: []gcpStep{
			{http.MethodPut, evAPI + "/rules", `{}`, "unsupported", 400},
			{http.MethodPost, evAPI + "/rules/x/actions/changeCompartment", `{}`, "unsupported", 400},
			{http.MethodGet, evAPI + "/other", "", "unsupported", 400},
			{http.MethodGet, evAPI + "/rules/%2e%2e", "", "unsupported", 400},
			{http.MethodPost, evAPI + "/rules/x", `{}`, "unsupported", 400},
			{http.MethodPost, evAPI + "/rules", `{"compartmentId":"c"}`, "invalid", 400},
			{http.MethodPut, evAPI + "/rules/x", `{"displayName":"n"}`, "invalid", 400},
			{
				http.MethodPost,
				evAPI + "/rules",
				`{"compartmentId":"c","displayName":"n","isEnabled":true,"condition":"{}",` +
					`"actions":{"actions":[{"actionType":"EMAIL","isEnabled":true}]}}`,
				"invalid",
				400,
			},
			{
				http.MethodPost,
				evAPI + "/rules",
				`{"compartmentId":"c","displayName":"n","isEnabled":true,"condition":"{}",` +
					`"actions":{"actions":[{"actionType":"ONS","topicId":"t"}]}}`,
				"invalid",
				400,
			},
		}},
		{name: "backend errors surface", steps: []gcpStep{
			{
				http.MethodPost,
				evAPI + "/rules",
				`{"compartmentId":"c","displayName":"n","isEnabled":true,"condition":"not json",` +
					`"actions":{"actions":[{"actionType":"ONS","topicId":"t","isEnabled":true}]}}`,
				"condition is not valid JSON",
				400,
			},
			{http.MethodGet, evAPI + "/rules/ocid1.eventrule.oc1..none", "", "", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newEventsDashboard(t)
			runGCPSteps(t, h, tt.steps)
		})
	}

	t.Run("toggle and delete", func(t *testing.T) {
		t.Parallel()

		h, b := newEventsDashboard(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, evAPI + "/rules", evRuleBody(""), `"displayName":"r1"`, 200},
		})

		rules, err := b.ListRules(evComp, "", "", ocievents.ListOpts{})
		require.NoError(t, err)
		require.Len(t, rules.Data, 1)

		id := rules.Data[0].ID
		runGCPSteps(t, h, []gcpStep{
			{
				http.MethodPut,
				evAPI + "/rules/" + id,
				`{"isEnabled":false,"displayName":"hijack"}`,
				`"isEnabled":false`,
				200,
			},
			{http.MethodGet, evAPI + "/rules/" + id, "", `"displayName":"r1"`, 200},
			{http.MethodDelete, evAPI + "/rules/" + id, "", "", http.StatusNoContent},
		})
	})

	t.Run("delivery log", func(t *testing.T) {
		t.Parallel()

		h, b := newEventsDashboard(t)
		runGCPSteps(t, h, []gcpStep{
			{http.MethodPost, evAPI + "/rules", evRuleBody(""), `"displayName":"r1"`, 200},
		})

		b.Emit(ocievents.Event{
			EventType: "com.oraclecloud.objectstorage.createobject", CompartmentID: evComp,
			Source: "ObjectStorage", ResourceName: "o",
		})
		b.Wait()

		runGCPSteps(t, h, []gcpStep{
			{
				http.MethodGet,
				evBase + "/deliveries",
				"",
				`"eventType":"com.oraclecloud.objectstorage.createobject"`,
				200,
			},
		})
	})

	t.Run("unavailable without the service", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, evAPI + "/rules", "", "", http.StatusServiceUnavailable},
			{http.MethodGet, evBase + "/deliveries", "", "", http.StatusServiceUnavailable},
		})
	})
}
