package gcpeventarc_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
)

func TestCrossBackendChecksConcurrent(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	svc := gcpeventarc.Services{
		PubSub: e.pubsub.Backend, Buckets: e.gcsBack,
		Org:      fakeOrg{project: true},
		Clusters: fakeClusters{"projects/" + project + "/locations/" + location + "/clusters/known": true},
	}
	e.handler.Backend.SetServices(svc)
	require.NoError(t, e.gcs.Bucket("race-bkt").Create(t.Context(), project,
		&storage.BucketAttrs{Location: "US-CENTRAL1"}))

	var wg sync.WaitGroup

	for i := range 4 {
		wg.Go(func() {
			_, _ = e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: fmt.Sprintf("race-%d", i),
				Trigger: storageTrigger("finalized", "race-bkt", gkeDestination("known", "default", "svc")),
			})
		})
	}

	wg.Go(func() {
		for range 20 {
			e.handler.Backend.SetServices(svc)
			e.handler.Backend.SetPushBase("http://127.0.0.1:9")
		}
	})

	wg.Go(func() {
		for range 20 {
			e.handler.Backend.HandleStorageEvent(gcpeventarc.StorageEvent{
				Type: "finalized", Bucket: "race-bkt", Name: "o", Project: project, Time: time.Unix(1, 0),
			})
		}
	})

	wg.Wait()
}
