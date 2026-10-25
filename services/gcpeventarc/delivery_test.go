package gcpeventarc_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestPubsubDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topic    string
		dest     func(e *env) *eventarcpb.Destination
		sa       string
		wantPath string
	}{
		{
			name:     "http endpoint with user topic",
			topic:    "projects/" + project + "/topics/orders",
			dest:     func(e *env) *eventarcpb.Destination { return httpDestination(e.dest.srv.URL + "/hook") },
			wantPath: "/hook",
		},
		{
			name:     "cloud run service and path with auto topic",
			dest:     func(*env) *eventarcpb.Destination { return runDestination("receiver", "/events") },
			wantPath: "/events",
		},
		{
			name:  "service account adds an identity token",
			topic: "projects/" + project + "/topics/orders",
			dest:  func(e *env) *eventarcpb.Destination { return httpDestination(e.dest.srv.URL) },
			sa:    "sender@test-project.iam.gserviceaccount.com", wantPath: "/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			if tt.topic != "" {
				_, err := e.psClient.TopicAdminClient.CreateTopic(t.Context(), topicPB(tt.topic))
				require.NoError(t, err)
			}

			trig := pubsubTrigger(tt.topic, tt.dest(e))
			trig.ServiceAccount = tt.sa
			created := e.createTrigger(t, "orders-trigger", trig)

			topic := created.GetTransport().GetPubsub().GetTopic()
			require.NotEmpty(t, topic)
			require.NotEmpty(t, created.GetTransport().GetPubsub().GetSubscription())

			e.publish(t, topic, &pubsub.Message{Data: []byte("hello"), Attributes: map[string]string{"k": "v"}})

			got := e.dest.next(t)
			assert.Equal(t, tt.wantPath, got.path)
			assert.Equal(t, "1.0", got.header.Get("Ce-Specversion"))
			assert.Equal(t, "google.cloud.pubsub.topic.v1.messagePublished", got.header.Get("Ce-Type"))
			assert.Equal(t, "//pubsub.googleapis.com/"+topic, got.header.Get("Ce-Source"))
			assert.NotEmpty(t, got.header.Get("Ce-Id"))
			assert.NotEmpty(t, got.header.Get("Ce-Time"))
			assert.Equal(t, "application/json", got.header.Get("Content-Type"))

			if tt.sa == "" {
				assert.Empty(t, got.header.Get("Authorization"))
			} else {
				assert.True(t, strings.HasPrefix(got.header.Get("Authorization"), "Bearer "))
			}

			var body struct {
				Message struct {
					Data       string            `json:"data"`
					Attributes map[string]string `json:"attributes"`
					MessageID  string            `json:"messageId"`
				} `json:"message"`
				Subscription string `json:"subscription"`
			}
			require.NoError(t, json.Unmarshal(got.body, &body))

			raw, err := base64.StdEncoding.DecodeString(body.Message.Data)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(raw))
			assert.Equal(t, map[string]string{"k": "v"}, body.Message.Attributes)
			assert.Equal(t, got.header.Get("Ce-Id"), body.Message.MessageID)
			assert.Equal(t, created.GetTransport().GetPubsub().GetSubscription(), body.Subscription)
		})
	}
}

func TestPubsubRetryAndStop(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := "projects/" + project + "/topics/retry"
	_, err := e.psClient.TopicAdminClient.CreateTopic(t.Context(), topicPB(topic))
	require.NoError(t, err)

	created := e.createTrigger(t, "retry-trigger", pubsubTrigger(topic, httpDestination(e.dest.srv.URL)))

	e.dest.failNext(2)
	e.publish(t, topic, &pubsub.Message{Data: []byte("again")})

	got := e.dest.next(t)
	assert.Equal(t, "google.cloud.pubsub.topic.v1.messagePublished", got.header.Get("Ce-Type"))
	assert.GreaterOrEqual(t, e.dest.callCount(), 3, "two failures are retried before the success")

	op, err := e.client.DeleteTrigger(t.Context(), &eventarcpb.DeleteTriggerRequest{
		Name: parent + "/triggers/retry-trigger",
	})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.psClient.SubscriptionAdminClient.GetSubscription(t.Context(), &pubsubpb.GetSubscriptionRequest{
		Subscription: created.GetTransport().GetPubsub().GetSubscription(),
	})
	assert.Equal(t, codes.NotFound, code(err), "the transport subscription goes with the trigger")

	_, err = e.psClient.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	require.NoError(t, err, "a user-supplied topic is never deleted")
}

func TestStorageDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger func(dest *eventarcpb.Destination) *eventarcpb.Trigger
		act     func(t *testing.T, b *storage.BucketHandle)
		want    string
		subject string
	}{
		{
			name: "finalized on upload",
			trigger: func(d *eventarcpb.Destination) *eventarcpb.Trigger {
				return storageTrigger("finalized", "photos", d)
			},
			act: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()
				w := b.Object("a/cat.png").NewWriter(t.Context())
				w.ContentType = "image/png"
				_, err := w.Write([]byte("png"))
				require.NoError(t, err)
				require.NoError(t, w.Close())
			},
			want: "google.cloud.storage.object.v1.finalized", subject: "objects/a/cat.png",
		},
		{
			name: "deleted on remove",
			trigger: func(d *eventarcpb.Destination) *eventarcpb.Trigger {
				return storageTrigger("deleted", "photos", d)
			},
			act: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()
				w := b.Object("gone.txt").NewWriter(t.Context())
				_, err := w.Write([]byte("x"))
				require.NoError(t, err)
				require.NoError(t, w.Close())
				require.NoError(t, b.Object("gone.txt").Delete(t.Context()))
			},
			want: "google.cloud.storage.object.v1.deleted", subject: "objects/gone.txt",
		},
		{
			name: "metadataUpdated on patch",
			trigger: func(d *eventarcpb.Destination) *eventarcpb.Trigger {
				return storageTrigger("metadataUpdated", "photos", d)
			},
			act: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()
				w := b.Object("m.txt").NewWriter(t.Context())
				_, err := w.Write([]byte("x"))
				require.NoError(t, err)
				require.NoError(t, w.Close())
				_, err = b.Object("m.txt").Update(t.Context(), storage.ObjectAttrsToUpdate{Metadata: map[string]string{"a": "b"}})
				require.NoError(t, err)
			},
			want: "google.cloud.storage.object.v1.metadataUpdated", subject: "objects/m.txt",
		},
		{
			name: "path pattern selects objects",
			trigger: func(d *eventarcpb.Destination) *eventarcpb.Trigger {
				return storageTrigger("finalized", "photos", d, &eventarcpb.EventFilter{
					Attribute: "subject", Value: "objects/images/*.png", Operator: "match-path-pattern",
				})
			},
			act: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()
				for _, n := range []string{"docs/readme.txt", "images/deep/x.png", "images/logo.jpg", "images/logo.png"} {
					w := b.Object(n).NewWriter(t.Context())
					_, err := w.Write([]byte("x"))
					require.NoError(t, err)
					require.NoError(t, w.Close())
				}
			},
			want: "google.cloud.storage.object.v1.finalized", subject: "objects/images/logo.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bucket := e.gcs.Bucket("photos")
			require.NoError(t, bucket.Create(t.Context(), project, nil))

			e.createTrigger(t, "storage-trigger", tt.trigger(httpDestination(e.dest.srv.URL+"/gcs")))
			tt.act(t, bucket)

			got := e.dest.next(t)
			assert.Equal(t, "/gcs", got.path)
			assert.Equal(t, tt.want, got.header.Get("Ce-Type"))
			assert.Equal(t, tt.subject, got.header.Get("Ce-Subject"))
			assert.Equal(t, "//storage.googleapis.com/projects/_/buckets/photos", got.header.Get("Ce-Source"))
			assert.Equal(t, "1.0", got.header.Get("Ce-Specversion"))
			assert.Equal(t, "application/json", got.header.Get("Content-Type"))

			var data map[string]any
			require.NoError(t, json.Unmarshal(got.body, &data))
			assert.Equal(t, "photos", data["bucket"])
			assert.Equal(t, strings.TrimPrefix(tt.subject, "objects/"), data["name"])
		})
	}
}

func TestStorageFiltersExcludeOtherBuckets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	for _, b := range []string{"watched", "other"} {
		require.NoError(t, e.gcs.Bucket(b).Create(t.Context(), project, nil))
	}

	e.createTrigger(t, "watch", storageTrigger("finalized", "watched", httpDestination(e.dest.srv.URL)))

	w := e.gcs.Bucket("other").Object("o.txt").NewWriter(t.Context())
	_, err := w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	w = e.gcs.Bucket("watched").Object("w.txt").NewWriter(t.Context())
	_, err = w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	got := e.dest.next(t)
	assert.Equal(t, "objects/w.txt", got.header.Get("Ce-Subject"))
	e.dest.none(t)
}

func TestUpdatedDestinationReceivesEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	topic := "projects/" + project + "/topics/upd"
	_, err := e.psClient.TopicAdminClient.CreateTopic(t.Context(), topicPB(topic))
	require.NoError(t, err)

	created := e.createTrigger(t, "upd-trigger", pubsubTrigger(topic, httpDestination(e.dest.srv.URL+"/old")))
	created.Destination = httpDestination(e.dest.srv.URL + "/new")

	op, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
		Trigger:    created,
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"destination"}},
	})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	e.publish(t, topic, &pubsub.Message{Data: []byte("x")})
	assert.Equal(t, "/new", e.dest.next(t).path)
}
