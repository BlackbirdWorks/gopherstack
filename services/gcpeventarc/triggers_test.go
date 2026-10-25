package gcpeventarc_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestCreateTriggerShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	got := e.createTrigger(t, "shape-trigger", pubsubTrigger("", runDestination("receiver", "/x")))

	assert.Equal(t, parent+"/triggers/shape-trigger", got.GetName())
	assert.NotEmpty(t, got.GetUid())
	assert.NotEmpty(t, got.GetEtag())
	assert.Equal(t, "application/json", got.GetEventDataContentType())
	assert.True(t, got.GetCreateTime().IsValid())

	ps := got.GetTransport().GetPubsub()
	assert.Contains(t, ps.GetTopic(), "projects/"+project+"/topics/eventarc-"+location+"-shape-trigger-")
	assert.Contains(
		t,
		ps.GetSubscription(),
		"projects/"+project+"/subscriptions/eventarc-"+location+"-shape-trigger-sub-",
	)

	read, err := e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: got.GetName()})
	require.NoError(t, err)
	assert.Equal(t, got.GetUid(), read.GetUid())
	assert.Equal(t, "receiver", read.GetDestination().GetCloudRun().GetService())

	_, err = e.psClient.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: ps.GetTopic()})
	require.NoError(t, err, "the transport topic exists in the Pub/Sub emulator")

	sub, err := e.psClient.SubscriptionAdminClient.GetSubscription(t.Context(),
		&pubsubpb.GetSubscriptionRequest{Subscription: ps.GetSubscription()})
	require.NoError(t, err)
	assert.Equal(t, ps.GetTopic(), sub.GetTopic())
	assert.Contains(t, sub.GetPushConfig().GetPushEndpoint(), "/_eventarc/push/"+got.GetName())

	op, err := e.client.DeleteTrigger(t.Context(), &eventarcpb.DeleteTriggerRequest{Name: got.GetName()})
	require.NoError(t, err)

	deleted, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, got.GetUid(), deleted.GetUid())

	_, err = e.psClient.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: ps.GetTopic()})
	assert.Equal(t, codes.NotFound, code(err), "an Eventarc-created topic is deleted with the trigger")

	_, err = e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: got.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestCreateTriggerErrors(t *testing.T) {
	t.Parallel()

	run := runDestination("receiver", "")
	topic := "projects/" + project + "/topics/missing"

	tests := []struct {
		mutate func(*eventarcpb.Trigger)
		name   string
		parent string
		id     string
		want   codes.Code
	}{
		{mutate: nil, name: "bad parent", parent: "projects/p", id: "good-trigger", want: codes.InvalidArgument},
		{
			mutate: nil,
			name:   "unknown location",
			parent: "projects/p/locations/mars-1",
			id:     "good-trigger",
			want:   codes.InvalidArgument,
		},
		{mutate: nil, name: "bad id", parent: parent, id: "A", want: codes.InvalidArgument},
		{
			mutate: func(tr *eventarcpb.Trigger) { tr.EventFilters = nil },
			name:   "no filters",
			parent: parent,
			id:     "good-trigger",
			want:   codes.InvalidArgument,
		},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = []*eventarcpb.EventFilter{{Attribute: "bucket", Value: "b"}}
		}, name: "no type filter", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = []*eventarcpb.EventFilter{typeFilter("google.cloud.audit.log.v1.written")}
		}, name: "unknown type", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = append(tr.EventFilters, typeFilter("google.cloud.pubsub.topic.v1.messagePublished"))
		}, name: "duplicate attribute", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = []*eventarcpb.EventFilter{typeFilter("google.cloud.storage.object.v1.finalized")}
		}, name: "storage needs bucket", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = append(tr.EventFilters, &eventarcpb.EventFilter{Attribute: "colour", Value: "red"})
		}, name: "unknown attribute", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = []*eventarcpb.EventFilter{
				typeFilter("google.cloud.storage.object.v1.finalized"),
				{Attribute: "bucket", Value: "b*", Operator: "match-path-pattern"},
			}
		}, name: "path pattern unsupported on bucket", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters[0].Operator = "path_pattern"
		}, name: "bad operator", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{
			mutate: func(tr *eventarcpb.Trigger) { tr.Destination = nil },
			name:   "no destination",
			parent: parent,
			id:     "good-trigger",
			want:   codes.InvalidArgument,
		},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Destination = &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudFunction{
				CloudFunction: "projects/p/locations/l/functions/f",
			}}
		}, name: "cloud function destination is read-only", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Destination = &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Workflow{
				Workflow: "projects/p/locations/l/workflows/w",
			}}
		}, name: "workflow destination unavailable", parent: parent, id: "good-trigger", want: codes.FailedPrecondition},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Destination = &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudRun{
				CloudRun: &eventarcpb.CloudRun{Service: "svc"},
			}}
		}, name: "run region required", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Destination = runDestination("svc", "events")
		}, name: "run path needs a slash", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Destination = httpDestination("ftp://host/x")
		}, name: "http endpoint scheme", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.ServiceAccount = "not-an-email"
		}, name: "service account is an email", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventDataContentType = "application/protobuf"
		}, name: "protobuf content type", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.RetryPolicy = &eventarcpb.Trigger_RetryPolicy{MaxAttempts: -1}
		}, name: "negative retry", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Labels = map[string]string{"Bad Key": "v"}
		}, name: "bad label", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Transport = pubsubTrigger(topic, run).GetTransport()
		}, name: "missing user topic", parent: parent, id: "good-trigger", want: codes.FailedPrecondition},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.EventFilters = storageTrigger("finalized", "b", run).GetEventFilters()
			tr.Transport = pubsubTrigger(topic, run).GetTransport()
		}, name: "user topic only for pubsub events", parent: parent, id: "good-trigger", want: codes.InvalidArgument},
		{mutate: func(tr *eventarcpb.Trigger) {
			tr.Channel = parent + "/channels/ghost"
		}, name: "unknown channel", parent: parent, id: "good-trigger", want: codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			trig := pubsubTrigger("", run)

			if tt.mutate != nil {
				tt.mutate(trig)
			}

			_, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: tt.parent, TriggerId: tt.id, Trigger: trig,
			})
			assert.Equal(t, tt.want, code(err), "%v", err)
		})
	}
}

func TestDuplicateTriggerAndValidateOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	trig := pubsubTrigger("", runDestination("receiver", ""))

	op, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
		Parent: parent, TriggerId: "dry-run", Trigger: trig, ValidateOnly: true,
	})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: parent + "/triggers/dry-run"})
	assert.Equal(t, codes.NotFound, code(err), "validate_only creates nothing")

	e.createTrigger(t, "dry-run", trig)

	_, err = e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
		Parent: parent, TriggerId: "dry-run", Trigger: trig,
	})
	assert.Equal(t, codes.AlreadyExists, code(err))
}

func TestUpdateTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(tr *eventarcpb.Trigger)
		check  func(t *testing.T, got *eventarcpb.Trigger)
		name   string
		paths  []string
		want   codes.Code
	}{
		{
			name:   "labels by mask",
			mutate: func(tr *eventarcpb.Trigger) { tr.Labels = map[string]string{"team": "core"} },
			paths:  []string{"labels"},
			check: func(t *testing.T, got *eventarcpb.Trigger) {
				t.Helper()
				assert.Equal(t, map[string]string{"team": "core"}, got.GetLabels())
				assert.Equal(t, "receiver", got.GetDestination().GetCloudRun().GetService())
			},
		},
		{
			name:   "destination path by nested mask",
			mutate: func(tr *eventarcpb.Trigger) { tr.Destination = runDestination("receiver", "/v2") },
			paths:  []string{"destination.cloud_run.path"},
			check: func(t *testing.T, got *eventarcpb.Trigger) {
				t.Helper()
				assert.Equal(t, "/v2", got.GetDestination().GetCloudRun().GetPath())
			},
		},
		{
			name:   "empty mask updates provided fields",
			mutate: func(tr *eventarcpb.Trigger) { tr.ServiceAccount = "me@test-project.iam.gserviceaccount.com" },
			check: func(t *testing.T, got *eventarcpb.Trigger) {
				t.Helper()
				assert.Equal(t, "me@test-project.iam.gserviceaccount.com", got.GetServiceAccount())
			},
		},
		{
			name: "type cannot change",
			mutate: func(tr *eventarcpb.Trigger) {
				tr.EventFilters = storageTrigger("finalized", "b", nil).GetEventFilters()
			},
			paths: []string{"event_filters"},
			want:  codes.InvalidArgument,
		},
		{
			name:   "invalid destination is rejected",
			mutate: func(tr *eventarcpb.Trigger) { tr.Destination = httpDestination("nope") },
			paths:  []string{"destination"},
			want:   codes.InvalidArgument,
		},
		{
			name:   "unsupported path",
			mutate: func(*eventarcpb.Trigger) {},
			paths:  []string{"transport"},
			want:   codes.InvalidArgument,
		},
		{
			name:   "stale etag aborts",
			mutate: func(tr *eventarcpb.Trigger) { tr.Etag = "stale"; tr.Labels = map[string]string{"a": "b"} },
			paths:  []string{"labels"},
			want:   codes.Aborted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			created := e.createTrigger(t, "upd-trigger", pubsubTrigger("", runDestination("receiver", "")))

			patch := &eventarcpb.Trigger{Name: created.GetName(), Etag: created.GetEtag()}
			tt.mutate(patch)

			op, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
				Trigger: patch, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.paths},
			})
			if tt.want != codes.OK {
				assert.Equal(t, tt.want, code(err), "%v", err)

				return
			}

			require.NoError(t, err)

			got, err := op.Wait(t.Context())
			require.NoError(t, err)
			tt.check(t, got)
			assert.NotEqual(t, created.GetEtag(), got.GetEtag())
			assert.Equal(t, created.GetUid(), got.GetUid())
		})
	}
}

func TestUpdateAndDeleteMissing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	name := parent + "/triggers/ghost-trigger"

	_, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
		Trigger: &eventarcpb.Trigger{Name: name},
	})
	assert.Equal(t, codes.NotFound, code(err))

	op, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
		Trigger:      pubsubTriggerNamed(name, runDestination("receiver", "")),
		AllowMissing: true,
	})
	require.NoError(t, err)

	created, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, name, created.GetName())
	assert.NotEmpty(t, created.GetTransport().GetPubsub().GetSubscription())

	_, err = e.client.DeleteTrigger(t.Context(), &eventarcpb.DeleteTriggerRequest{Name: parent + "/triggers/never-was"})
	assert.Equal(t, codes.NotFound, code(err))

	delOp, err := e.client.DeleteTrigger(t.Context(), &eventarcpb.DeleteTriggerRequest{
		Name: parent + "/triggers/never-was", AllowMissing: true,
	})
	require.NoError(t, err)
	_, err = delOp.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.client.DeleteTrigger(t.Context(), &eventarcpb.DeleteTriggerRequest{Name: name, Etag: "stale"})
	assert.Equal(t, codes.Aborted, code(err))
}

func pubsubTriggerNamed(name string, dest *eventarcpb.Destination) *eventarcpb.Trigger {
	t := pubsubTrigger("", dest)
	t.Name = name

	return t
}

func TestListTriggers(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	run := runDestination("receiver", "")

	for _, id := range []string{"trig-b", "trig-a", "trig-c"} {
		e.createTrigger(t, id, pubsubTrigger("", run))
	}

	otherParent := "projects/" + project + "/locations/europe-west1"
	op, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
		Parent: otherParent, TriggerId: "trig-eu", Trigger: pubsubTrigger("", run),
	})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	names := func(req *eventarcpb.ListTriggersRequest) []string {
		var out []string

		it := e.client.ListTriggers(t.Context(), req)
		for {
			tr, nerr := it.Next()
			if errors.Is(nerr, iterator.Done) {
				return out
			}

			require.NoError(t, nerr)

			out = append(out, tr.GetName())
		}
	}

	assert.Equal(t, []string{parent + "/triggers/trig-a", parent + "/triggers/trig-b", parent + "/triggers/trig-c"},
		names(&eventarcpb.ListTriggersRequest{Parent: parent, PageSize: 2}), "paged through every page, by name")
	assert.Equal(t, []string{parent + "/triggers/trig-c", parent + "/triggers/trig-b", parent + "/triggers/trig-a"},
		names(&eventarcpb.ListTriggersRequest{Parent: parent, OrderBy: "name desc"}))
	assert.Len(t, names(&eventarcpb.ListTriggersRequest{Parent: "projects/" + project + "/locations/-"}), 4,
		"the - location spans every region")

	_, err = e.client.ListTriggers(t.Context(), &eventarcpb.ListTriggersRequest{Parent: parent, OrderBy: "colour"}).
		Next()
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = e.client.ListTriggers(t.Context(), &eventarcpb.ListTriggersRequest{Parent: parent, Filter: "x=y"}).Next()
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestTriggerOperations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	op, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
		Parent: parent, TriggerId: "op-trigger", Trigger: pubsubTrigger("", runDestination("receiver", "")),
	})
	require.NoError(t, err)

	done, err := e.client.GetOperation(t.Context(), opRequest(op.Name()))
	require.NoError(t, err)
	assert.True(t, done.GetDone())
	assert.Contains(t, op.Name(), parent+"/operations/")

	meta, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, parent+"/triggers/op-trigger", meta.GetTarget())
	assert.Equal(t, "create", meta.GetVerb())
	assert.Equal(t, "v1", meta.GetApiVersion())

	it := e.client.ListOperations(t.Context(), opListRequest(parent))
	first, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, op.Name(), first.GetName())
}
