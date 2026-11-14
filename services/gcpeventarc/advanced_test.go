package gcpeventarc_test

import (
	"encoding/json/v2"
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/eventarc/publishing/apiv1/publishingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	busName      = parent + "/messageBuses/main-bus"
	pipelineName = parent + "/pipelines/to-http"
)

func (e *env) createBus(t *testing.T, id string) string {
	t.Helper()

	op, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
		Parent: parent, MessageBusId: id, MessageBus: &eventarcpb.MessageBus{DisplayName: id},
	})
	require.NoError(t, err)

	bus, err := op.Wait(t.Context())
	require.NoError(t, err)

	return bus.GetName()
}

func (e *env) createPipeline(t *testing.T, id string, p *eventarcpb.Pipeline) string {
	t.Helper()

	op, err := e.client.CreatePipeline(t.Context(), &eventarcpb.CreatePipelineRequest{
		Parent: parent, PipelineId: id, Pipeline: p,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got.GetName()
}

func (e *env) createEnrollment(t *testing.T, id, bus, match, pipeline string) *eventarcpb.Enrollment {
	t.Helper()

	op, err := e.client.CreateEnrollment(t.Context(), &eventarcpb.CreateEnrollmentRequest{
		Parent: parent, EnrollmentId: id,
		Enrollment: &eventarcpb.Enrollment{MessageBus: bus, CelMatch: match, Destination: pipeline},
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func httpPipeline(uri, transform, binding string) *eventarcpb.Pipeline {
	p := &eventarcpb.Pipeline{Destinations: []*eventarcpb.Pipeline_Destination{{
		DestinationDescriptor: &eventarcpb.Pipeline_Destination_HttpEndpoint_{
			HttpEndpoint: &eventarcpb.Pipeline_Destination_HttpEndpoint{Uri: uri, MessageBindingTemplate: binding},
		},
	}}}

	if transform != "" {
		p.Mediations = []*eventarcpb.Pipeline_Mediation{{
			MediationDescriptor: &eventarcpb.Pipeline_Mediation_Transformation_{
				Transformation: &eventarcpb.Pipeline_Mediation_Transformation{TransformationTemplate: transform},
			},
		}}
	}

	return p
}

func (e *env) publishJSON(t *testing.T, bus string, event map[string]any) error {
	t.Helper()

	raw, err := json.Marshal(event, jsonx.V1())
	require.NoError(t, err)

	_, err = publishingpb.NewPublisherClient(e.conn).Publish(t.Context(), &publishingpb.PublishRequest{
		MessageBus: bus, Format: &publishingpb.PublishRequest_JsonMessage{JsonMessage: string(raw)},
	})

	return err
}

func orderEvent(id, typ string) map[string]any {
	return map[string]any{
		"specversion": "1.0", "id": id, "source": "//orders/test", "type": typ,
		"datacontenttype": "application/json", "data": map[string]any{"order": "o-1", "n": 7},
	}
}

func TestAdvancedBusToPipelineDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check     func(t *testing.T, got request)
		name      string
		transform string
		binding   string
		match     string
	}{
		{
			name: "default binding is CloudEvents binary mode", match: `message.type == "order.created"`,
			check: func(t *testing.T, got request) {
				t.Helper()
				assert.Equal(t, "order.created", got.header.Get("Ce-Type"))
				assert.Equal(t, "evt-1", got.header.Get("Ce-Id"))
				assert.JSONEq(t, `{"order":"o-1","n":7}`, string(got.body))
			},
		},
		{
			name:      "CEL transformation merges and sets fields",
			match:     `message.type == "order.created" && message.data.n > 5`,
			transform: `message.merge({"data": {"extra": "x"}}).setField("data.order", "o-2")`,
			check: func(t *testing.T, got request) {
				t.Helper()
				assert.JSONEq(t, `{"order":"o-2","n":7,"extra":"x"}`, string(got.body))
			},
		},
		{
			name:      "removeFields and a message binding template",
			match:     `message.source.startsWith("//orders") && message.type == "order.created"`,
			transform: `message.removeFields(["data.n"])`,
			binding:   `{"headers": {"X-Order-Type": message.type}, "body": message.data.toJsonString()}`,
			check: func(t *testing.T, got request) {
				t.Helper()
				assert.Equal(t, "order.created", got.header.Get("X-Order-Type"))
				assert.JSONEq(t, `{"order":"o-1"}`, string(got.body))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bus := e.createBus(t, "main-bus")
			p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL+"/pipe", tt.transform, tt.binding))
			e.createEnrollment(t, "match-orders", bus, tt.match, p)
			e.createEnrollment(t, "match-nothing", bus, `message.type == "never"`, p)

			require.NoError(t, e.publishJSON(t, bus, orderEvent("evt-0", "order.skipped")))
			require.NoError(t, e.publishJSON(t, bus, orderEvent("evt-1", "order.created")))

			got := e.dest.next(t)
			assert.Equal(t, "/pipe", got.path)
			tt.check(t, got)
			e.dest.none(t)
		})
	}
}

func TestAdvancedGoogleAPISourceRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		projects []string
		wantHit  bool
	}{
		{name: "subscribed project", projects: []string{"projects/" + project}, wantHit: true},
		{name: "other project is not routed", projects: []string{"projects/elsewhere"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bus := e.createBus(t, "api-bus")
			p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL+"/api", "", ""))
			e.createEnrollment(t, "audit-only", bus, `message.type == "google.cloud.audit.log.v1.written"`, p)

			op, err := e.client.CreateGoogleApiSource(t.Context(), &eventarcpb.CreateGoogleApiSourceRequest{
				Parent: parent, GoogleApiSourceId: "src", GoogleApiSource: &eventarcpb.GoogleApiSource{
					Destination: bus,
					WideScopeSubscription: &eventarcpb.GoogleApiSource_ProjectSubscriptions_{
						ProjectSubscriptions: &eventarcpb.GoogleApiSource_ProjectSubscriptions{List: tt.projects},
					},
				},
			})
			require.NoError(t, err)
			_, err = op.Wait(t.Context())
			require.NoError(t, err)

			require.NoError(t, e.gcs.Bucket("via-source").Create(t.Context(), project, nil))

			if !tt.wantHit {
				e.dest.none(t)

				return
			}

			got := e.dest.next(t)
			assert.Equal(t, "google.cloud.audit.log.v1.written", got.header.Get("Ce-Type"))
			assert.Equal(t, "storage.buckets.create", got.header.Get("Ce-Methodname"))
		})
	}
}

func TestAdvancedPipelineRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     *eventarcpb.Pipeline_RetryPolicy
		wantDelays []time.Duration
	}{
		{
			name:       "defaults: 5 attempts, 5s doubling",
			wantDelays: []time.Duration{0, 5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second},
		},
		{
			name: "custom policy caps the delay",
			policy: &eventarcpb.Pipeline_RetryPolicy{
				MaxAttempts:   4,
				MinRetryDelay: durationpb.New(2 * time.Second),
				MaxRetryDelay: durationpb.New(3 * time.Second),
			},
			wantDelays: []time.Duration{0, 2 * time.Second, 3 * time.Second, 3 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bus := e.createBus(t, "retry-bus")
			pl := httpPipeline(e.dest.srv.URL, "", "")
			pl.RetryPolicy = tt.policy
			p := e.createPipeline(t, "to-http", pl)
			e.createEnrollment(t, "all", bus, `true`, p)

			e.handler.Backend.SetAfterFunc(e.now.after)
			e.dest.failNext(100)

			require.NoError(t, e.publishJSON(t, bus, orderEvent("evt-1", "order.created")))

			var delays []time.Duration

			for range tt.wantDelays {
				require.Eventually(t, func() bool { return e.now.pending() == 1 }, waitFor, tick)
				delays = append(delays, e.now.fireNext(t))
				require.Eventually(t, func() bool {
					return e.dest.callCount() == len(delays)
				}, waitFor, tick)
			}

			assert.Equal(t, tt.wantDelays, delays)
			assert.Equal(t, 0, e.now.pending(), "no attempt is scheduled after max_attempts")
			e.dest.none(t)
		})
	}
}

func TestAdvancedCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	bus := e.createBus(t, "main-bus")
	assert.Equal(t, busName, bus)

	p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL, "", ""))
	assert.Equal(t, pipelineName, p)

	enr := e.createEnrollment(t, "enr", bus, `message.type == "a"`, p)
	assert.NotEmpty(t, enr.GetUid())
	assert.NotEmpty(t, enr.GetEtag())

	// Get and list.
	got, err := e.client.GetEnrollment(t.Context(), &eventarcpb.GetEnrollmentRequest{Name: enr.GetName()})
	require.NoError(t, err)
	assert.Equal(t, `message.type == "a"`, got.GetCelMatch())

	names := []string{}
	it := e.client.ListMessageBusEnrollments(t.Context(), &eventarcpb.ListMessageBusEnrollmentsRequest{Parent: bus})
	for n, ierr := it.Next(); ierr == nil; n, ierr = it.Next() {
		names = append(names, n)
	}

	assert.Equal(t, []string{enr.GetName()}, names)

	// Update with a mask changes only the masked field and the etag.
	op, err := e.client.UpdateEnrollment(t.Context(), &eventarcpb.UpdateEnrollmentRequest{
		Enrollment: &eventarcpb.Enrollment{
			Name:        enr.GetName(),
			CelMatch:    `message.type == "b"`,
			DisplayName: "ignored",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"cel_match"}},
	})
	require.NoError(t, err)

	updated, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, `message.type == "b"`, updated.GetCelMatch())
	assert.Empty(t, updated.GetDisplayName())
	assert.NotEqual(t, enr.GetEtag(), updated.GetEtag())

	// A stale etag is rejected.
	_, err = e.client.UpdateEnrollment(t.Context(), &eventarcpb.UpdateEnrollmentRequest{
		Enrollment: &eventarcpb.Enrollment{Name: enr.GetName(), Etag: enr.GetEtag(), CelMatch: "true"},
	})
	assert.Equal(t, codes.Aborted, code(err))

	// Referenced resources cannot be deleted.
	_, err = e.client.DeleteMessageBus(t.Context(), &eventarcpb.DeleteMessageBusRequest{Name: bus})
	assert.Equal(t, codes.FailedPrecondition, code(err))

	_, err = e.client.DeletePipeline(t.Context(), &eventarcpb.DeletePipelineRequest{Name: p})
	assert.Equal(t, codes.FailedPrecondition, code(err))

	del, err := e.client.DeleteEnrollment(t.Context(), &eventarcpb.DeleteEnrollmentRequest{Name: enr.GetName()})
	require.NoError(t, err)
	_, err = del.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.client.GetEnrollment(t.Context(), &eventarcpb.GetEnrollmentRequest{Name: enr.GetName()})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestAdvancedValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(t *testing.T, e *env, bus, pipeline string) error
		name string
		want codes.Code
	}{
		{
			name: "invalid CEL in cel_match", want: codes.InvalidArgument,
			act: func(t *testing.T, e *env, bus, pipeline string) error {
				t.Helper()
				_, err := e.client.CreateEnrollment(t.Context(), &eventarcpb.CreateEnrollmentRequest{
					Parent: parent, EnrollmentId: "bad-cel",
					Enrollment: &eventarcpb.Enrollment{
						MessageBus:  bus,
						CelMatch:    "message.type ==",
						Destination: pipeline,
					},
				})

				return err
			},
		},
		{
			name: "cel_match must be a bool", want: codes.InvalidArgument,
			act: func(t *testing.T, e *env, bus, pipeline string) error {
				t.Helper()
				_, err := e.client.CreateEnrollment(t.Context(), &eventarcpb.CreateEnrollmentRequest{
					Parent: parent, EnrollmentId: "not-bool",
					Enrollment: &eventarcpb.Enrollment{
						MessageBus:  bus,
						CelMatch:    `"not a bool"`,
						Destination: pipeline,
					},
				})

				return err
			},
		},
		{
			name: "enrollment on a missing bus", want: codes.FailedPrecondition,
			act: func(t *testing.T, e *env, _, pipeline string) error {
				t.Helper()
				_, err := e.client.CreateEnrollment(t.Context(), &eventarcpb.CreateEnrollmentRequest{
					Parent: parent, EnrollmentId: "no-bus", Enrollment: &eventarcpb.Enrollment{
						MessageBus: parent + "/messageBuses/missing", CelMatch: "true", Destination: pipeline,
					},
				})

				return err
			},
		},
		{
			name: "pipeline retry attempts out of range", want: codes.InvalidArgument,
			act: func(t *testing.T, e *env, _, _ string) error {
				t.Helper()
				pl := httpPipeline(e.dest.srv.URL, "", "")
				pl.RetryPolicy = &eventarcpb.Pipeline_RetryPolicy{MaxAttempts: 101}
				_, err := e.client.CreatePipeline(t.Context(), &eventarcpb.CreatePipelineRequest{
					Parent: parent, PipelineId: "bad-retry", Pipeline: pl,
				})

				return err
			},
		},
		{
			name: "pipeline needs exactly one destination", want: codes.InvalidArgument,
			act: func(t *testing.T, e *env, _, _ string) error {
				t.Helper()
				_, err := e.client.CreatePipeline(t.Context(), &eventarcpb.CreatePipelineRequest{
					Parent: parent, PipelineId: "no-dest", Pipeline: &eventarcpb.Pipeline{},
				})

				return err
			},
		},
		{
			name: "publishing to a missing bus", want: codes.NotFound,
			act: func(t *testing.T, e *env, _, _ string) error {
				t.Helper()

				return e.publishJSON(t, parent+"/messageBuses/missing", orderEvent("x", "t"))
			},
		},
		{
			name: "publishing an event without a type", want: codes.InvalidArgument,
			act: func(t *testing.T, e *env, bus, _ string) error {
				t.Helper()

				return e.publishJSON(t, bus, map[string]any{"specversion": "1.0", "id": "1", "source": "s"})
			},
		},
		{
			name: "duplicate bus id", want: codes.AlreadyExists,
			act: func(t *testing.T, e *env, _, _ string) error {
				t.Helper()
				_, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
					Parent: parent, MessageBusId: "main-bus", MessageBus: &eventarcpb.MessageBus{},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			bus := e.createBus(t, "main-bus")
			p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL, "", ""))

			assert.Equal(t, tt.want, code(tt.act(t, e, bus, p)))
		})
	}
}
