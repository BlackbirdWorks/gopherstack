package gcpstorage_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

const (
	notifTopic = "projects/test-project/topics/gcs-events"
	notifSub   = "projects/test-project/subscriptions/gcs-events-sub"
)

type pubsubNotifier struct{ b *gcppubsub.InMemoryBackend }

func (n pubsubNotifier) TopicExists(topic string) bool {
	_, err := n.b.GetTopic(topic)

	return err == nil
}

func (n pubsubNotifier) Publish(topic string, data []byte, attrs map[string]string) error {
	_, err := n.b.Publish(&pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
	})

	return err
}

type notifEnv struct {
	*sdkEnv

	ps *gcppubsub.Handler
}

func newNotifEnv(t *testing.T) *notifEnv {
	t.Helper()

	e := newSDK(t)
	psb := gcppubsub.NewInMemoryBackend()
	e.backend.SetNotifier(pubsubNotifier{b: psb})

	_, err := psb.CreateTopic(&pubsubpb.Topic{Name: notifTopic})
	require.NoError(t, err)

	_, err = psb.CreateSubscription(&pubsubpb.Subscription{Name: notifSub, Topic: notifTopic})
	require.NoError(t, err)

	return &notifEnv{sdkEnv: e, ps: gcppubsub.NewHandler(psb)}
}

func (n *notifEnv) drain(t *testing.T) []*pubsubpb.PubsubMessage {
	t.Helper()

	res, err := n.ps.SubscriberAPI().Pull(context.Background(), &pubsubpb.PullRequest{
		Subscription: notifSub, MaxMessages: 100, ReturnImmediately: true, //nolint:staticcheck // immediate poll in tests
	})
	require.NoError(t, err)

	out := make([]*pubsubpb.PubsubMessage, 0, len(res.GetReceivedMessages()))
	for _, m := range res.GetReceivedMessages() {
		out = append(out, m.GetMessage())
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].GetAttributes()["eventType"] < out[j].GetAttributes()["eventType"]
	})

	return out
}

func TestBucketNotifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act       func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64
		check     func(t *testing.T, msgs []*pubsubpb.PubsubMessage, gens map[string]int64)
		name      string
		cfg       storage.Notification
		versioned bool
	}{
		{
			name: "finalize carries documented attributes and the object resource",
			cfg:  storage.Notification{PayloadFormat: storage.JSONPayload},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				a := write(t, b.Object("a.txt"), []byte("hi"), 0, nil)

				return map[string]int64{"a": a.Generation}
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, gens map[string]int64) {
				require.Len(t, msgs, 1)

				at := msgs[0].GetAttributes()
				assert.Equal(t, "OBJECT_FINALIZE", at["eventType"])
				assert.Equal(t, "JSON_API_V1", at["payloadFormat"])
				assert.Equal(t, "bkt", at["bucketId"])
				assert.Equal(t, "a.txt", at["objectId"])
				assert.Equal(t, itoa(gens["a"]), at["objectGeneration"])
				assert.Regexp(t, `^projects/_/buckets/bkt/notificationConfigs/\d+$`, at["notificationConfig"])
				assert.NotContains(t, at, "overwroteGeneration")

				_, err := time.Parse(time.RFC3339Nano, at["eventTime"])
				require.NoError(t, err)

				var res map[string]any
				require.NoError(t, json.Unmarshal(msgs[0].GetData(), &res))
				assert.Equal(t, "storage#object", res["kind"])
				assert.Equal(t, "a.txt", res["name"])
				assert.Equal(t, "bkt", res["bucket"])
			},
		},
		{
			name: "overwrite in an unversioned bucket finalizes and deletes",
			cfg:  storage.Notification{PayloadFormat: storage.JSONPayload},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				first := write(t, b.Object("a"), []byte("1"), 0, nil)
				_ = readAll(t, b.Object("a"))
				second := write(t, b.Object("a"), []byte("2"), 0, nil)

				return map[string]int64{"first": first.Generation, "second": second.Generation}
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, gens map[string]int64) {
				byType := map[string][]map[string]string{}
				for _, m := range msgs {
					byType[m.GetAttributes()["eventType"]] = append(byType[m.GetAttributes()["eventType"]], m.GetAttributes())
				}

				require.Len(t, byType["OBJECT_FINALIZE"], 2)
				require.Len(t, byType["OBJECT_DELETE"], 1)

				del := byType["OBJECT_DELETE"][0]
				assert.Equal(t, itoa(gens["first"]), del["objectGeneration"])
				assert.Equal(t, itoa(gens["second"]), del["overwrittenByGeneration"])

				var over map[string]string

				for _, f := range byType["OBJECT_FINALIZE"] {
					if f["objectGeneration"] == itoa(gens["second"]) {
						over = f
					}
				}

				assert.Equal(t, itoa(gens["first"]), over["overwroteGeneration"])
			},
		},
		{
			name:      "overwrite in a versioned bucket archives",
			cfg:       storage.Notification{PayloadFormat: storage.JSONPayload},
			versioned: true,
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				first := write(t, b.Object("a"), []byte("1"), 0, nil)
				second := write(t, b.Object("a"), []byte("2"), 0, nil)

				return map[string]int64{"first": first.Generation, "second": second.Generation}
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, gens map[string]int64) {
				var types []string

				for _, m := range msgs {
					types = append(types, m.GetAttributes()["eventType"])

					if m.GetAttributes()["eventType"] == "OBJECT_ARCHIVE" {
						assert.Equal(t, itoa(gens["first"]), m.GetAttributes()["objectGeneration"])
						assert.Equal(t, itoa(gens["second"]), m.GetAttributes()["overwrittenByGeneration"])
					}
				}

				assert.Equal(t, []string{"OBJECT_ARCHIVE", "OBJECT_FINALIZE", "OBJECT_FINALIZE"}, types)
			},
		},
		{
			name: "metadata update and delete",
			cfg:  storage.Notification{PayloadFormat: storage.JSONPayload},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				write(t, b.Object("a"), []byte("1"), 0, nil)
				_, err := b.Object("a").Update(t.Context(), storage.ObjectAttrsToUpdate{ContentType: "text/plain"})
				require.NoError(t, err)
				require.NoError(t, b.Object("a").Delete(t.Context()))

				return nil
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, _ map[string]int64) {
				var types []string
				for _, m := range msgs {
					types = append(types, m.GetAttributes()["eventType"])

					if m.GetAttributes()["eventType"] == "OBJECT_DELETE" {
						var res map[string]any
						require.NoError(t, json.Unmarshal(m.GetData(), &res))
						assert.NotEmpty(t, res["timeDeleted"])
					}
				}

				assert.Equal(t, []string{"OBJECT_DELETE", "OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE"}, types)
			},
		},
		{
			name: "event type filter",
			cfg: storage.Notification{
				PayloadFormat: storage.JSONPayload, EventTypes: []string{storage.ObjectDeleteEvent},
			},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				write(t, b.Object("a"), []byte("1"), 0, nil)
				require.NoError(t, b.Object("a").Delete(t.Context()))

				return nil
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, _ map[string]int64) {
				require.Len(t, msgs, 1)
				assert.Equal(t, "OBJECT_DELETE", msgs[0].GetAttributes()["eventType"])
			},
		},
		{
			name: "object name prefix filter",
			cfg:  storage.Notification{PayloadFormat: storage.JSONPayload, ObjectNamePrefix: "logs/"},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				write(t, b.Object("data/x"), []byte("1"), 0, nil)
				write(t, b.Object("logs/y"), []byte("1"), 0, nil)

				return nil
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, _ map[string]int64) {
				require.Len(t, msgs, 1)
				assert.Equal(t, "logs/y", msgs[0].GetAttributes()["objectId"])
			},
		},
		{
			name: "custom attributes and no payload",
			cfg: storage.Notification{
				PayloadFormat: storage.NoPayload, CustomAttributes: map[string]string{"env": "dev"},
			},
			act: func(t *testing.T, e *sdkEnv, b *storage.BucketHandle) map[string]int64 {
				write(t, b.Object("a"), []byte("1"), 0, nil)

				return nil
			},
			check: func(t *testing.T, msgs []*pubsubpb.PubsubMessage, _ map[string]int64) {
				require.Len(t, msgs, 1)
				assert.Equal(t, "dev", msgs[0].GetAttributes()["env"])
				assert.Equal(t, "NONE", msgs[0].GetAttributes()["payloadFormat"])
				assert.Empty(t, msgs[0].GetData())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newNotifEnv(t)
			b := e.bucket(t, "bkt", &storage.BucketAttrs{VersioningEnabled: tt.versioned})

			cfg := tt.cfg
			cfg.TopicProjectID, cfg.TopicID = "test-project", "gcs-events"

			got, err := b.AddNotification(t.Context(), &cfg)
			require.NoError(t, err)
			assert.NotEmpty(t, got.ID)

			gens := tt.act(t, e.sdkEnv, b)
			tt.check(t, e.drain(t), gens)
		})
	}
}

func TestNotificationConfigCRUD(t *testing.T) {
	t.Parallel()

	e := newNotifEnv(t)
	b := e.bucket(t, "bkt", nil)

	_, err := b.AddNotification(t.Context(), &storage.Notification{
		TopicProjectID: "test-project", TopicID: "missing", PayloadFormat: storage.JSONPayload,
	})

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	n, err := b.AddNotification(t.Context(), &storage.Notification{
		TopicProjectID: "test-project", TopicID: "gcs-events", PayloadFormat: storage.JSONPayload,
		EventTypes: []string{storage.ObjectFinalizeEvent}, ObjectNamePrefix: "p/",
	})
	require.NoError(t, err)

	list, err := b.Notifications(t.Context())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, []string{storage.ObjectFinalizeEvent}, list[n.ID].EventTypes)
	assert.Equal(t, "p/", list[n.ID].ObjectNamePrefix)
	assert.Equal(t, "gcs-events", list[n.ID].TopicID)

	attrs, err := b.Attrs(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 2, attrs.MetaGeneration, "adding a config bumps metageneration")

	require.NoError(t, b.DeleteNotification(t.Context(), n.ID))
	require.ErrorAs(t, b.DeleteNotification(t.Context(), n.ID), &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	list, err = b.Notifications(t.Context())
	require.NoError(t, err)
	assert.Empty(t, list)
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
