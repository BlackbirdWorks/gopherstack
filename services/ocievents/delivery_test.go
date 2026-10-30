package ocievents_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ocievents "github.com/blackbirdworks/gopherstack/services/ocievents"
	ocinotifications "github.com/blackbirdworks/gopherstack/services/ocinotifications"
	ociobjectstorage "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	ocistreaming "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

type notifier struct {
	b *ocinotifications.InMemoryBackend
}

func (n notifier) TopicActive(id string) bool { return n.b.TopicActive(id) }

func (n notifier) PublishEvent(topicID, body string) error {
	_, err := n.b.PublishMessage(topicID, "JSON", ocinotifications.PublishIn{Body: &body})

	return err
}

type streamer struct{ b *ocistreaming.InMemoryBackend }

func (s streamer) StreamActive(id string) bool { return s.b.StreamActive(id) }

func (s streamer) AppendMessage(id string, key, value []byte) error {
	return s.b.AppendMessage(id, key, value)
}

func eventAt(compartment, eventType, name string) ocievents.Event {
	return ocievents.Event{
		EventType:     eventType,
		Source:        "objectstorage",
		CompartmentID: compartment,
		ResourceName:  name,
		AdditionalDetails: map[string]any{
			"bucketName":       "my_bucket",
			"publicAccessType": "NoPublicAccess",
			"archivalState":    nil,
		},
	}
}

func TestFilterMatching(t *testing.T) {
	t.Parallel()

	const create = "com.oraclecloud.objectstorage.createbucket"

	tests := []struct {
		name      string
		condition string
		event     ocievents.Event
		want      bool
	}{
		{"empty matches all", `{}`, eventAt(testCompartment, create, "b"), true},
		{"exact type", `{"eventType":"` + create + `"}`, eventAt(testCompartment, create, "b"), true},
		{
			"other type",
			`{"eventType":"com.oraclecloud.objectstorage.deletebucket"}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{"type list", `{"eventType":["x.y","` + create + `"]}`, eventAt(testCompartment, create, "b"), true},
		{"type list miss", `{"eventType":["x.y","z.w"]}`, eventAt(testCompartment, create, "b"), false},
		{
			"mid wildcard",
			`{"eventType":"com.oraclecloud.objectstorage.*bucket"}`,
			eventAt(testCompartment, create, "b"),
			true,
		},
		{"suffix wildcard", `{"eventType":"com.oraclecloud.*"}`, eventAt(testCompartment, create, "b"), true},
		{"prefix wildcard", `{"eventType":"*.createbucket"}`, eventAt(testCompartment, create, "b"), true},
		{
			"wildcard miss",
			`{"eventType":"com.oraclecloud.objectstorage.*object"}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{
			"data resourceName",
			`{"data":{"resourceName":"my_bucket"}}`,
			eventAt(testCompartment, create, "my_bucket"),
			true,
		},
		{
			"data resourceName prefix",
			`{"data":{"resourceName":"my_bucket*"}}`,
			eventAt(testCompartment, create, "my_bucket_2"),
			true,
		},
		{
			"data resourceName miss",
			`{"data":{"resourceName":"my_bucket"}}`,
			eventAt(testCompartment, create, "other"),
			false,
		},
		{"data list", `{"data":{"resourceName":["a","b","c"]}}`, eventAt(testCompartment, create, "b"), true},
		{
			"nested detail",
			`{"data":{"additionalDetails":{"publicAccessType":"NoPublicAccess"}}}`,
			eventAt(testCompartment, create, "b"),
			true,
		},
		{
			"nested detail miss",
			`{"data":{"additionalDetails":{"publicAccessType":"PublicAccess"}}}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{
			"star matches value",
			`{"data":{"additionalDetails":{"publicAccessType":"*"}}}`,
			eventAt(testCompartment, create, "b"),
			true,
		},
		{
			"star does not match null",
			`{"data":{"additionalDetails":{"archivalState":"*"}}}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{"missing field", `{"data":{"additionalDetails":{"nope":"*"}}}`, eventAt(testCompartment, create, "b"), false},
		{
			"wrong nesting",
			`{"additionalDetails":{"publicAccessType":"NoPublicAccess"}}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{
			"and across fields",
			`{"eventType":"` + create + `","data":{"resourceName":"zzz"}}`,
			eventAt(testCompartment, create, "b"),
			false,
		},
		{"envelope source", `{"source":"objectstorage"}`, eventAt(testCompartment, create, "b"), true},
		{"other compartment", `{}`, eventAt(otherCompart, create, "b"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fn := &fakeFunctions{}
			e.backend.SetFunctionInvoker(fn)
			e.createRule(t, "r", tt.condition, faasAction())

			e.backend.Emit(tt.event)

			got := fn.invoked()
			if !tt.want {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, 1)

			var env map[string]any
			require.NoError(t, json.Unmarshal([]byte(got[0]), &env))
			assert.Equal(t, tt.event.EventType, env["eventType"])
			assert.Equal(t, "0.1", env["cloudEventsVersion"])
			assert.Equal(t, "2.0", env["eventTypeVersion"])
			assert.Equal(t, "application/json", env["contentType"])
			assert.NotEmpty(t, env["eventID"])
			assert.NotEmpty(t, env["eventTime"])
		})
	}
}

func TestCompartmentScope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fn := &fakeFunctions{}
	e.backend.SetFunctionInvoker(fn)
	e.backend.SetCompartmentTree(func(id, ancestor string) bool {
		return id == "ocid1.compartment.oc1..child" && ancestor == testCompartment
	})
	e.createRule(t, "r", matchAll, faasAction())

	e.backend.Emit(eventAt("ocid1.compartment.oc1..child", "a.b", "x"))
	e.backend.Emit(eventAt(otherCompart, "a.b", "x"))

	assert.Len(t, fn.invoked(), 1)
}

func TestDisabledNotDelivered(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fn := &fakeFunctions{}
	e.backend.SetFunctionInvoker(fn)

	off := events.CreateFaaSActionDetails{IsEnabled: new(false), FunctionId: new(testFunction)}
	rule := e.createRule(t, "actions", matchAll, off, faasAction())
	e.createRule(t, "never", matchAll, off)

	e.backend.Emit(eventAt(testCompartment, "a.b", "x"))
	assert.Len(t, fn.invoked(), 1, "only the enabled action of the first rule")

	_, err := e.client.UpdateRule(t.Context(), events.UpdateRuleRequest{
		RuleId: rule.Id, IsEnabled: new(false),
	})
	require.NoError(t, err)

	e.backend.Emit(eventAt(testCompartment, "a.b", "x"))
	assert.Len(t, fn.invoked(), 1, "disabled rule")

	_, err = e.client.UpdateRule(t.Context(), events.UpdateRuleRequest{
		RuleId: rule.Id, IsEnabled: new(true),
	})
	require.NoError(t, err)

	_, err = e.client.DeleteRule(t.Context(), events.DeleteRuleRequest{RuleId: rule.Id})
	require.NoError(t, err)

	e.backend.Emit(eventAt(testCompartment, "a.b", "x"))
	assert.Len(t, fn.invoked(), 1, "deleted rule")
}

func TestDeliveryFailureRecordedOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wire   func(e *env, fn *fakeFunctions)
		action events.ActionDetails
		want   string
	}{
		{"function error", func(e *env, fn *fakeFunctions) {
			fn.err = assert.AnError
			e.backend.SetFunctionInvoker(fn)
		}, faasAction(), assert.AnError.Error()},
		{"no notifier", func(*env, *fakeFunctions) {}, onsAction(), "not available"},
		{"no streamer", func(*env, *fakeFunctions) {}, ossAction(), "not available"},
		{"no invoker", func(*env, *fakeFunctions) {}, faasAction(), "not available"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fn := &fakeFunctions{}
			tt.wire(e, fn)
			e.createRule(t, "r", matchAll, tt.action)

			e.backend.Emit(eventAt(testCompartment, "a.b", "x"))

			ds := e.backend.Deliveries()
			require.Len(t, ds, 1)
			assert.Equal(t, "FAILED", ds[0].Status)
			assert.Contains(t, ds[0].Error, tt.want)
			assert.LessOrEqual(t, len(fn.invoked()), 1, "no retry")

			status, body := rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/events/deliveries", "")
			assert.Equal(t, http.StatusOK, status)
			assert.Contains(t, body, "FAILED")
		})
	}
}

type pipeline struct {
	*env
	objects *ociobjectstorage.InMemoryBackend
	ons     *ocinotifications.InMemoryBackend
	streams *ocistreaming.InMemoryBackend
	fn      *fakeFunctions
	sink    chan string
	topic   string
	stream  string
}

func newPipeline(t *testing.T) *pipeline {
	t.Helper()

	p := &pipeline{env: newEnv(t, envOpts{}), sink: make(chan string, 16), fn: &fakeFunctions{}}

	p.objects = ociobjectstorage.NewInMemoryBackend()
	p.objects.SetClock(p.clock.Now)
	p.objects.SetObjectEventSink(func(e ociobjectstorage.ObjectEvent) {
		p.backend.Emit(ocievents.Event{
			Time: e.Time, EventType: e.EventType, Source: "objectstorage", CompartmentID: e.CompartmentID,
			ResourceName: e.ObjectName, AdditionalDetails: map[string]any{
				"eTag": e.ETag, "namespace": e.Namespace, "archivalState": nil, "bucketName": e.BucketName,
				"bucketId": e.BucketID,
			},
		})
	})

	p.ons = ocinotifications.NewInMemoryBackend()
	p.ons.SetClock(p.clock.Now)
	p.ons.SetTransition(0)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		p.sink <- r.Header.Get("X-Oci-Ns-Messagetype") + "|" + string(buf[:n])
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	topic, err := p.ons.CreateTopic(ocinotifications.CreateTopicIn{Name: "ev", Compartment: testCompartment}, "")
	require.NoError(t, err)

	p.topic = topic.TopicID

	sub, err := p.ons.CreateSubscription(ocinotifications.CreateSubscriptionIn{
		TopicID: p.topic, Compartment: testCompartment, Protocol: "HTTPS", Endpoint: srv.URL,
	}, "http://localhost")
	require.NoError(t, err)

	conf := p.ons.Deliveries("", sub.ID)
	require.NotEmpty(t, conf)

	u, err := url.Parse(conf[0].ConfirmationURL)
	require.NoError(t, err)

	_, err = p.ons.ConfirmSubscription(sub.ID, u.Query().Get("token"), "HTTPS")
	require.NoError(t, err)

	p.streams = ocistreaming.NewInMemoryBackend()
	p.streams.SetClock(p.clock.Now)
	p.streams.SetTransition(0)

	st, _, _, err := p.streams.CreateStream(ocistreaming.CreateStreamIn{
		Name: "ev", Compartment: testCompartment, Partitions: new(1),
	}, "http://localhost")
	require.NoError(t, err)

	p.stream = st.ID
	p.streams.Sweep()

	p.backend.SetNotifier(notifier{p.ons})
	p.backend.SetStreamer(streamer{p.streams})
	p.backend.SetFunctionInvoker(p.fn)

	return p
}

func (p *pipeline) bucket(t *testing.T, name string, events *bool) {
	t.Helper()

	_, err := p.objects.CreateBucket(ociobjectstorage.DefaultNamespace, ociobjectstorage.CreateBucketInput{
		Name: name, CompartmentID: testCompartment, ObjectEventsEnabled: events,
	}, "")
	require.NoError(t, err)
}

func (p *pipeline) put(t *testing.T, bucket, name string) {
	t.Helper()

	_, err := p.objects.PutObject(ociobjectstorage.DefaultNamespace, bucket, name, []byte("data"),
		ociobjectstorage.ObjectMeta{}, ociobjectstorage.Preconds{}, "")
	require.NoError(t, err)
}

func (p *pipeline) streamed(t *testing.T) []map[string]any {
	t.Helper()

	cur, err := p.streams.CreateCursor(p.stream, ocistreaming.CreateCursorIn{Type: "TRIM_HORIZON", Partition: "0"})
	require.NoError(t, err)

	msgs, _, err := p.streams.GetMessages(p.stream, cur.Value, 100)
	require.NoError(t, err)

	out := make([]map[string]any, 0, len(msgs))

	for _, m := range msgs {
		var env map[string]any
		require.NoError(t, json.Unmarshal(m.Value, &env))

		out = append(out, env)
	}

	return out
}

func TestObjectEventsDelivered(t *testing.T) {
	t.Parallel()

	p := newPipeline(t)
	p.bucket(t, "on", new(true))
	p.bucket(t, "off", nil)

	p.createRule(
		t,
		"all",
		`{"eventType":"com.oraclecloud.objectstorage.*object","data":{"additionalDetails":{"bucketName":"on"}}}`,
		events.CreateNotificationServiceActionDetails{IsEnabled: new(true), TopicId: &p.topic},
		events.CreateStreamingServiceActionDetails{IsEnabled: new(true), StreamId: &p.stream},
		faasAction(),
	)

	p.put(t, "off", "ignored")
	assert.Empty(t, p.backend.Deliveries(), "bucket without objectEventsEnabled emits nothing")

	p.put(t, "on", "v1/log.txt")
	p.put(t, "on", "v1/log.txt")

	_, err := p.objects.DeleteObject(ociobjectstorage.DefaultNamespace, "on", "v1/log.txt", "", "")
	require.NoError(t, err)

	wantTypes := []string{
		"com.oraclecloud.objectstorage.createobject", "com.oraclecloud.objectstorage.updateobject",
		"com.oraclecloud.objectstorage.deleteobject",
	}

	streamed := p.streamed(t)
	require.Len(t, streamed, 3)

	invoked := p.fn.invoked()
	require.Len(t, invoked, 3)

	for i, want := range wantTypes {
		assert.Equal(t, want, streamed[i]["eventType"])

		data, ok := streamed[i]["data"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "v1/log.txt", data["resourceName"])
		assert.Equal(t, testCompartment, data["compartmentId"])
		assert.Equal(t, "all", data["availabilityDomain"])

		details, ok := data["additionalDetails"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "on", details["bucketName"])
		assert.Equal(t, ociobjectstorage.DefaultNamespace, details["namespace"])

		var fromFn map[string]any
		require.NoError(t, json.Unmarshal([]byte(invoked[i]), &fromFn))
		assert.Equal(t, streamed[i]["eventID"], fromFn["eventID"])
	}

	for _, d := range p.backend.Deliveries() {
		assert.Equal(t, "DELIVERED", d.Status, d.Error)
	}

	p.ons.DeliverDue(t.Context(), func(fn func()) { fn() })

	got := make([]string, 0, 3)
	for range 3 {
		got = append(got, <-p.sink)
	}

	for i, want := range wantTypes {
		assert.Contains(t, got[i], "Notification|")
		assert.Contains(t, got[i], want)
	}
}

func TestActionTargetMustExist(t *testing.T) {
	t.Parallel()

	p := newPipeline(t)

	tests := []struct {
		action events.ActionDetails
		name   string
	}{
		{
			name: "topic",
			action: events.CreateNotificationServiceActionDetails{
				IsEnabled: new(true),
				TopicId:   new("ocid1.onstopic.oc1..nope"),
			},
		},
		{
			name: "stream",
			action: events.CreateStreamingServiceActionDetails{
				IsEnabled: new(true),
				StreamId:  new("ocid1.stream.oc1..nope"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := p.client.CreateRule(t.Context(), events.CreateRuleRequest{
				DisplayName: new(
					"x",
				),
				IsEnabled:     new(true),
				Condition:     new(matchAll),
				CompartmentId: new(testCompartment),
				Actions:       &events.ActionDetailsList{Actions: []events.ActionDetails{tt.action}},
			})
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	created := e.createRule(t, "persist", `{"eventType":"a.b"}`, faasAction())

	snap := e.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	other := ocievents.NewInMemoryBackend()
	other.SetClock(e.clock.Now)
	other.SetDispatcher(func(fn func()) { fn() })

	fn := &fakeFunctions{}
	other.SetFunctionInvoker(fn)
	require.NoError(t, ocievents.NewHandler(other).Restore(t.Context(), snap))

	got, _, err := other.GetRule(*created.Id)
	require.NoError(t, err)
	assert.Equal(t, "persist", got.DisplayName)
	assert.JSONEq(t, `{"eventType":"a.b"}`, got.Condition)

	other.Emit(eventAt(testCompartment, "a.b", "x"))
	assert.Len(t, fn.invoked(), 1)

	require.Error(t, ocievents.NewHandler(other).Restore(t.Context(), []byte(`{"rules":{"x":null},"version":1}`)))
}
