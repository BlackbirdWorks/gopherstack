package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type fakeRun map[string]string

func (f fakeRun) GetService(name string) (*runpb.Service, error) {
	if u, ok := f[name]; ok {
		return &runpb.Service{Name: name, Uri: u}, nil
	}

	return nil, errNoEventarcTarget
}

type fakeFns map[string]string

func (f fakeFns) GetFunction(name string) (*functionspb.Function, error) {
	if u, ok := f[name]; ok {
		return &functionspb.Function{Name: name, Url: u}, nil
	}

	return nil, errNoEventarcTarget
}

func TestEventarcTargets(t *testing.T) {
	t.Parallel()

	const base = "projects/p/locations/us-central1"

	tests := []struct {
		name    string
		targets eventarcTargets
		service string
		want    string
		wantErr bool
	}{
		{
			name:    "run service",
			targets: eventarcTargets{run: fakeRun{base + "/services/svc": "http://svc.localhost:1"}},
			service: "svc", want: "http://svc.localhost:1",
		},
		{
			name:    "function falls back to its service id",
			targets: eventarcTargets{run: fakeRun{}, fns: fakeFns{base + "/functions/fn": "http://fn.localhost:2"}},
			service: "fn", want: "http://fn.localhost:2",
		},
		{
			name: "run wins over a function of the same id",
			targets: eventarcTargets{
				run: fakeRun{base + "/services/x": "http://run"},
				fns: fakeFns{base + "/functions/x": "http://fn"},
			},
			service: "x",
			want:    "http://run",
		},
		{name: "unknown", targets: eventarcTargets{run: fakeRun{}, fns: fakeFns{}}, service: "ghost", wantErr: true},
		{name: "nothing wired", service: "ghost", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.targets.ServiceURL("p", "us-central1", tt.service)
			if tt.wantErr {
				require.ErrorIs(t, err, errNoEventarcTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWireGCPEventarcOptionalPeers(t *testing.T) {
	t.Parallel()

	ea := gcpeventarcbackend.NewHandler(gcpeventarcbackend.NewInMemoryBackend())

	assert.NotPanics(t, func() { wireGCPEventarc(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() { wireGCPEventarc(map[string]service.Registerable{"GCPEventarc": ea}) })
}

func TestWireGCPEventarcStorageToHTTPEndpoint(t *testing.T) {
	t.Parallel()

	got := make(chan *http.Request, 1)
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got <- r
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dest.Close)

	ea := gcpeventarcbackend.NewHandler(gcpeventarcbackend.NewInMemoryBackend())
	ea.Port = 0
	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	ps.Port = 0
	gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())

	wireGCPEventarc(map[string]service.Registerable{"GCPEventarc": ea, "GCPPubSub": ps, "GCPStorage": gcs})

	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })
	require.NoError(t, ea.StartWorker(t.Context()))
	t.Cleanup(func() { ea.Shutdown(context.Background()) })

	_, err := gcs.Backend.CreateBucket("p", gcpstoragebackend.Patch{"name": []byte(`"wired"`)})
	require.NoError(t, err)

	op, err := ea.Backend.CreateTrigger(&eventarcpb.CreateTriggerRequest{
		Parent: "projects/p/locations/us-central1", TriggerId: "wired-trigger",
		Trigger: &eventarcpb.Trigger{
			EventFilters: []*eventarcpb.EventFilter{
				{Attribute: "type", Value: "google.cloud.storage.object.v1.finalized"},
				{Attribute: "bucket", Value: "wired"},
			},
			Destination: &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_HttpEndpoint{
				HttpEndpoint: &eventarcpb.HttpEndpoint{Uri: dest.URL + "/hook"},
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, op.GetDone())

	_, err = gcs.Backend.PutObject(
		"wired",
		"f.txt",
		[]byte("data"),
		gcpstoragebackend.Patch{},
		gcpstoragebackend.Preconds{},
	)
	require.NoError(t, err)

	r := <-got
	assert.Equal(t, "/hook", r.URL.Path)
	assert.Equal(t, "google.cloud.storage.object.v1.finalized", r.Header.Get("Ce-Type"))
	assert.Equal(t, "objects/f.txt", r.Header.Get("Ce-Subject"))
}
