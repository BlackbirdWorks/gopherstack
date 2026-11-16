package gcpeventarc_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
)

type fakeClusters map[string]bool

func (f fakeClusters) ClusterExists(name string) bool { return f[name] }

func gkeDestination(cluster, ns, svc string) *eventarcpb.Destination {
	return &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Gke{Gke: &eventarcpb.GKE{
		Cluster: cluster, Location: location, Namespace: ns, Service: svc, Path: "/hook",
	}}}
}

func TestGKEDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dest *eventarcpb.Destination
		name string
		want codes.Code
	}{
		{name: "existing cluster", dest: gkeDestination("known", "default", "svc"), want: codes.OK},
		{name: "missing cluster", dest: gkeDestination("absent", "default", "svc"), want: codes.FailedPrecondition},
		{name: "missing namespace", dest: gkeDestination("known", "", "svc"), want: codes.InvalidArgument},
		{name: "missing service", dest: gkeDestination("known", "default", ""), want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetServices(gcpeventarc.Services{
				PubSub: e.pubsub.Backend,
				Clusters: fakeClusters{
					"projects/" + project + "/locations/" + location + "/clusters/known": true,
				},
			})

			op, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "gke-trigger", Trigger: pubsubTrigger("", tt.dest),
			})
			require.Equal(t, tt.want, code(err), "%v", err)

			if tt.want != codes.OK {
				return
			}

			trig, err := op.Wait(t.Context())
			require.NoError(t, err)

			body := `{"subscription":"s","message":{"data":"e30=","messageId":"m1","publishTime":"2026-01-01T00:00:00Z"}}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost,
				"/_eventarc/push/"+trig.GetName(), strings.NewReader(body))
			e.handler.Backend.PushHandler().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}
