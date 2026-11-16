package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
)

func TestStorageTriggerBucketRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		loc    string
		want   codes.Code
	}{
		{name: "same region", bucket: "same-region-bkt", loc: "US-CENTRAL1", want: codes.OK},
		{name: "multi region", bucket: "multi-region-bkt", loc: "US", want: codes.OK},
		{name: "other region", bucket: "other-region-bkt", loc: "EUROPE-WEST1", want: codes.InvalidArgument},
		{name: "other multi", bucket: "other-multi-bkt", loc: "ASIA", want: codes.InvalidArgument},
		{name: "unknown bucket", bucket: "unknown-bkt", loc: "", want: codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetServices(gcpeventarc.Services{
				PubSub: e.pubsub.Backend, Buckets: e.gcsBack,
			})

			if tt.loc != "" {
				require.NoError(t, e.gcs.Bucket(tt.bucket).Create(t.Context(), project,
					&storage.BucketAttrs{Location: tt.loc}))
			}

			_, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "regional-trigger",
				Trigger: storageTrigger("finalized", tt.bucket, runDestination("receiver", "")),
			})
			assert.Equal(t, tt.want, code(err), "%v", err)
		})
	}
}

func TestStorageTriggerUpdateBucketRegion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.handler.Backend.SetServices(gcpeventarc.Services{PubSub: e.pubsub.Backend, Buckets: e.gcsBack})
	require.NoError(t, e.gcs.Bucket("eu-update-bkt").Create(t.Context(), project,
		&storage.BucketAttrs{Location: "EUROPE-WEST1"}))

	trig := e.createTrigger(t, "moving-trigger",
		storageTrigger("finalized", "nobucket", runDestination("receiver", "")))
	trig.EventFilters = storageTrigger("finalized", "eu-update-bkt", nil).GetEventFilters()

	_, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
		Trigger: trig, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"event_filters"}},
	})
	assert.Equal(t, codes.InvalidArgument, code(err), "%v", err)
}
