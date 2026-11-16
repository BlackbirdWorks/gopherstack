package gcpeventarc_test

import (
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
)

type fakeOrg map[string]bool

func (f fakeOrg) ProjectInOrganization(p string) (bool, bool) {
	v, ok := f[p]

	return v, ok
}

func TestGoogleAPISourceStorageScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sub      func() *eventarcpb.GoogleApiSource
		name     string
		wantName string
	}{
		{
			name: "project subscription skips other projects",
			sub: func() *eventarcpb.GoogleApiSource {
				return &eventarcpb.GoogleApiSource{
					WideScopeSubscription: &eventarcpb.GoogleApiSource_ProjectSubscriptions_{
						ProjectSubscriptions: &eventarcpb.GoogleApiSource_ProjectSubscriptions{
							List: []string{"projects/" + project},
						},
					},
				}
			},
			wantName: "mine.txt",
		},
		{
			name: "organization subscription skips projects outside it",
			sub: func() *eventarcpb.GoogleApiSource {
				return &eventarcpb.GoogleApiSource{
					WideScopeSubscription: &eventarcpb.GoogleApiSource_OrganizationSubscription_{
						OrganizationSubscription: &eventarcpb.GoogleApiSource_OrganizationSubscription{Enabled: true},
					},
				}
			},
			wantName: "mine.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetServices(gcpeventarc.Services{
				PubSub: e.pubsub.Backend,
				Org:    fakeOrg{project: true, "outsider": false},
			})

			bus := e.createBus(t, "scope-bus")
			p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL+"/api", "", ""))
			e.createEnrollment(t, "storage-only", bus,
				`message.type == "google.cloud.storage.object.v1.finalized"`, p)

			src := tt.sub()
			src.Destination = bus

			op, err := e.client.CreateGoogleApiSource(t.Context(), &eventarcpb.CreateGoogleApiSourceRequest{
				Parent: parent, GoogleApiSourceId: "scope-src", GoogleApiSource: src,
			})
			require.NoError(t, err)
			_, err = op.Wait(t.Context())
			require.NoError(t, err)

			for _, ev := range []gcpeventarc.StorageEvent{
				{
					Type: "finalized", Bucket: "b", Name: "theirs.txt", Data: []byte(`{"name":"theirs.txt"}`),
					Project: "outsider", Time: time.Unix(1, 0),
				},
				{
					Type: "finalized", Bucket: "b", Name: "mine.txt", Data: []byte(`{"name":"mine.txt"}`),
					Project: project, Time: time.Unix(2, 0),
				},
			} {
				e.handler.Backend.HandleStorageEvent(ev)
			}

			got := e.dest.next(t)
			assert.Equal(t, "objects/"+tt.wantName, got.header.Get("Ce-Subject"))
			e.dest.none(t)
		})
	}
}
