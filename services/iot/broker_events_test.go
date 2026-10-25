package iot_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

const eventWait = 5 * time.Second

type eventEnv struct {
	backend *iot.InMemoryBackend
	client  *iotsdk.Client
	port    int
}

func newEventEnv(t *testing.T) *eventEnv {
	t.Helper()

	backend := newRefBackend()
	port := freeTCPPort(t)
	broker := startTestBroker(t, backend, port)
	h := iot.NewHandler(backend, broker)

	return &eventEnv{backend: backend, client: newTestIoTClient(t, h), port: port}
}

type eventWatcher struct {
	msgs chan pahomqtt.Message
}

func (e *eventEnv) watch(t *testing.T, filters ...string) *eventWatcher {
	t.Helper()

	w := &eventWatcher{msgs: make(chan pahomqtt.Message, 256)}
	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", e.port)).
		SetClientID(fmt.Sprintf("watcher-%s", t.Name())).
		SetAutoReconnect(false).
		SetConnectTimeout(3 * time.Second).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) { w.msgs <- m })

	c := pahomqtt.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(3*time.Second))
	require.NoError(t, tok.Error())
	t.Cleanup(func() { c.Disconnect(0) })

	for _, f := range filters {
		st := c.Subscribe(f, 0, nil)
		require.True(t, st.WaitTimeout(3*time.Second))
		require.NoError(t, st.Error())
	}

	return w
}

// next returns the payload of the next message on topic, skipping others.
func (w *eventWatcher) next(t *testing.T, topic string) map[string]any {
	t.Helper()

	timeout := time.After(eventWait)

	for {
		select {
		case m := <-w.msgs:
			if m.Topic() != topic {
				continue
			}

			var out map[string]any
			require.NoError(t, json.Unmarshal(m.Payload(), &out))

			return out
		case <-timeout:
			require.FailNow(t, "no event on "+topic)

			return nil
		}
	}
}

func (e *eventEnv) enable(t *testing.T, categories ...string) {
	t.Helper()

	cfgs := map[string]iottypes.Configuration{}
	for _, c := range categories {
		cfgs[c] = iottypes.Configuration{Enabled: true}
	}

	_, err := e.client.UpdateEventConfigurations(t.Context(), &iotsdk.UpdateEventConfigurationsInput{
		EventConfigurations: cfgs,
	})
	require.NoError(t, err)
}

func TestBrokerLifecycleEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act        func(t *testing.T, e *eventEnv)
		check      func(t *testing.T, w *eventWatcher)
		name       string
		wantReason string
	}{
		{
			name: "connect_then_client_disconnect",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				c := connectPaho(t, e.port, "dev-1")
				c.Disconnect(100)
			},
			check: func(t *testing.T, w *eventWatcher) {
				t.Helper()

				conn := w.next(t, "$aws/events/presence/connected/dev-1")
				assert.Equal(t, "connected", conn["eventType"])
				assert.Equal(t, "dev-1", conn["clientId"])
				assert.Equal(t, "127.0.0.1", conn["ipAddress"])
				assert.NotEmpty(t, conn["sessionIdentifier"])
				assert.Contains(t, conn, "principalIdentifier")
				assert.InDelta(t, float64(time.Now().UnixMilli()), conn["timestamp"], float64(time.Minute.Milliseconds()))

				disc := w.next(t, "$aws/events/presence/disconnected/dev-1")
				assert.Equal(t, "disconnected", disc["eventType"])
				assert.Equal(t, "CLIENT_INITIATED_DISCONNECT", disc["disconnectReason"])
				assert.Equal(t, true, disc["clientInitiatedDisconnect"])
				assert.Equal(t, conn["sessionIdentifier"], disc["sessionIdentifier"])
				assert.Equal(t, conn["versionNumber"], disc["versionNumber"])
			},
		},
		{
			name: "connection_lost",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				conn := rawConnect(t, e.port, "dev-lost")
				_ = conn.Close()
			},
			check: func(t *testing.T, w *eventWatcher) {
				t.Helper()

				w.next(t, "$aws/events/presence/connected/dev-lost")
				disc := w.next(t, "$aws/events/presence/disconnected/dev-lost")
				assert.Equal(t, "CONNECTION_LOST", disc["disconnectReason"])
				assert.Equal(t, false, disc["clientInitiatedDisconnect"])
			},
		},
		{
			name: "duplicate_client_id",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_ = rawConnect(t, e.port, "dev-dup")
				_ = connectPaho(t, e.port, "dev-dup")
			},
			check: func(t *testing.T, w *eventWatcher) {
				t.Helper()

				var connected []map[string]any

				var disc map[string]any

				for len(connected) < 2 || disc == nil {
					select {
					case m := <-w.msgs:
						var p map[string]any
						require.NoError(t, json.Unmarshal(m.Payload(), &p))

						switch {
						case p["clientId"] != "dev-dup":
						case p["eventType"] == "connected":
							connected = append(connected, p)
						default:
							disc = p
						}
					case <-time.After(eventWait):
						require.FailNow(t, "missing duplicate-client events")
					}
				}

				assert.Equal(t, "DUPLICATE_CLIENTID", disc["disconnectReason"])
				assert.Equal(t, connected[0]["sessionIdentifier"], disc["sessionIdentifier"])
				assert.NotEqual(t, connected[0]["sessionIdentifier"], connected[1]["sessionIdentifier"])
				assert.Greater(t, connected[1]["versionNumber"], connected[0]["versionNumber"])
			},
		},
		{
			name: "subscribe_and_unsubscribe",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				c := connectPaho(t, e.port, "dev-sub")
				require.NoError(t, c.Subscribe("foo/bar", 0, nil).Error())
				tok := c.Subscribe("device/data", 0, nil)
				require.True(t, tok.WaitTimeout(3*time.Second))
				ut := c.Unsubscribe("foo/bar")
				require.True(t, ut.WaitTimeout(3*time.Second))
			},
			check: func(t *testing.T, w *eventWatcher) {
				t.Helper()

				conn := w.next(t, "$aws/events/presence/connected/dev-sub")
				sub := w.next(t, "$aws/events/subscriptions/subscribed/dev-sub")
				assert.Equal(t, "subscribed", sub["eventType"])
				assert.Equal(t, []any{"foo/bar"}, sub["topics"])
				assert.Equal(t, conn["sessionIdentifier"], sub["sessionIdentifier"])

				unsub := w.next(t, "$aws/events/subscriptions/unsubscribed/dev-sub")
				assert.Equal(t, "unsubscribed", unsub["eventType"])
				assert.Equal(t, []any{"foo/bar"}, unsub["topics"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEventEnv(t)
			w := e.watch(t, "$aws/events/presence/#", "$aws/events/subscriptions/#")
			tt.act(t, e)
			tt.check(t, w)
		})
	}
}

func TestBrokerLifecycleEventsFeedRules(t *testing.T) {
	t.Parallel()

	e := newEventEnv(t)

	_, err := e.client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
		RuleName: aws.String("presenceRule"),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql: aws.String("SELECT clientId, eventType FROM '$aws/events/presence/+/+'"),
			Actions: []iottypes.Action{{Republish: &iottypes.RepublishAction{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/r"), Topic: aws.String("seen/presence"),
			}}},
		},
	})
	require.NoError(t, err)

	w := e.watch(t, "seen/presence")
	c := connectPaho(t, e.port, "rule-dev")
	c.Disconnect(100)

	got := w.next(t, "seen/presence")
	assert.Equal(t, "rule-dev", got["clientId"])
	assert.Equal(t, "connected", got["eventType"])
}

func TestRegistryEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act     func(t *testing.T, e *eventEnv)
		check   func(t *testing.T, p map[string]any)
		name    string
		topic   string
		enabled []string
	}{
		{
			name: "thing_created", enabled: []string{"THING"}, topic: "$aws/events/thing/t1/created",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("t1"),
					AttributePayload: &iottypes.AttributePayload{
						Attributes: map[string]string{"a": "1"},
					},
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_EVENT", p["eventType"])
				assert.Equal(t, "CREATED", p["operation"])
				assert.Equal(t, "123456789012", p["accountId"])
				assert.Equal(t, "t1", p["thingName"])
				assert.InDelta(t, 1, p["versionNumber"], 0)
				assert.Nil(t, p["thingTypeName"])
				assert.Equal(t, map[string]any{"a": "1"}, p["attributes"])
				assert.NotEmpty(t, p["eventId"])
				assert.NotEmpty(t, p["thingId"])
			},
		},
		{
			name: "thing_updated", enabled: []string{"THING"}, topic: "$aws/events/thing/t2/updated",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t2")})
				require.NoError(t, err)
				_, err = e.client.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
					ThingName:        aws.String("t2"),
					AttributePayload: &iottypes.AttributePayload{Attributes: map[string]string{"k": "v"}},
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "UPDATED", p["operation"])
				assert.InDelta(t, 2, p["versionNumber"], 0)
				assert.Equal(t, map[string]any{"k": "v"}, p["attributes"])
			},
		},
		{
			name: "thing_deleted", enabled: []string{"THING"}, topic: "$aws/events/thing/t3/deleted",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("t3")})
				require.NoError(t, err)
				_, err = e.client.DeleteThing(t.Context(), &iotsdk.DeleteThingInput{ThingName: aws.String("t3")})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "DELETED", p["operation"])
			},
		},
		{
			name: "thing_type_lifecycle", enabled: []string{"THING_TYPE"}, topic: "$aws/events/thingType/tt1/deleted",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{
					ThingTypeName: aws.String("tt1"),
					ThingTypeProperties: &iottypes.ThingTypeProperties{
						ThingTypeDescription: aws.String("My thing type"),
						SearchableAttributes: []string{"a1"},
					},
				})
				require.NoError(t, err)
				_, err = e.client.DeprecateThingType(t.Context(), &iotsdk.DeprecateThingTypeInput{
					ThingTypeName: aws.String("tt1"),
				})
				require.NoError(t, err)
				_, err = e.client.DeleteThingType(t.Context(), &iotsdk.DeleteThingTypeInput{ThingTypeName: aws.String("tt1")})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_TYPE_EVENT", p["eventType"])
				assert.Equal(t, "DELETED", p["operation"])
				assert.Equal(t, true, p["isDeprecated"])
				assert.NotNil(t, p["deprecationDate"])
				assert.Equal(t, []any{"a1"}, p["searchableAttributes"])
				assert.Equal(t, "My thing type", p["description"])
			},
		},
		{
			name: "thing_type_association_added", enabled: []string{"THING_TYPE_ASSOCIATION"},
			topic: "$aws/events/thingTypeAssociation/thing/tat/thingType/tt2/added",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("tt2")})
				require.NoError(t, err)
				_, err = e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("tat"), ThingTypeName: aws.String("tt2"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_TYPE_ASSOCIATION_EVENT", p["eventType"])
				assert.Equal(t, "ADDED", p["operation"])
				assert.Equal(t, "tt2", p["thingTypeName"])
				assert.Equal(t, "tat", p["thingName"])
			},
		},
		{
			name: "thing_type_association_removed", enabled: []string{"THING_TYPE_ASSOCIATION"},
			topic: "$aws/events/thingTypeAssociation/thing/tar/thingType/tt3/removed",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThingType(t.Context(), &iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("tt3")})
				require.NoError(t, err)
				_, err = e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
					ThingName: aws.String("tar"), ThingTypeName: aws.String("tt3"),
				})
				require.NoError(t, err)
				_, err = e.client.UpdateThing(t.Context(), &iotsdk.UpdateThingInput{
					ThingName: aws.String("tar"), RemoveThingType: true,
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "REMOVED", p["operation"])
			},
		},
		{
			name: "thing_group_created_with_parent", enabled: []string{"THING_GROUP"},
			topic: "$aws/events/thingGroup/child/created",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{ThingGroupName: aws.String("root")})
				require.NoError(t, err)
				_, err = e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
					ThingGroupName: aws.String("child"), ParentGroupName: aws.String("root"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_GROUP_EVENT", p["eventType"])
				assert.Equal(t, "root", p["parentGroupName"])
				assert.NotEmpty(t, p["parentGroupId"])
				assert.Len(t, p["rootToParentThingGroups"], 1)
				assert.InDelta(t, 1, p["versionNumber"], 0)
			},
		},
		{
			name: "thing_group_hierarchy_added", enabled: []string{"THING_GROUP_HIERARCHY"},
			topic: "$aws/events/thingGroupHierarchy/thingGroup/root2/childThingGroup/child2/added",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{ThingGroupName: aws.String("root2")})
				require.NoError(t, err)
				_, err = e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
					ThingGroupName: aws.String("child2"), ParentGroupName: aws.String("root2"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_GROUP_HIERARCHY_EVENT", p["eventType"])
				assert.Equal(t, "root2", p["thingGroupName"])
				assert.Equal(t, "child2", p["childGroupName"])
				assert.NotEmpty(t, p["childGroupId"])
			},
		},
		{
			name: "thing_group_membership_removed", enabled: []string{"THING_GROUP_MEMBERSHIP"},
			topic: "$aws/events/thingGroupMembership/thingGroup/g1/thing/m1/removed",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("m1")})
				require.NoError(t, err)
				_, err = e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{ThingGroupName: aws.String("g1")})
				require.NoError(t, err)
				_, err = e.client.AddThingToThingGroup(t.Context(), &iotsdk.AddThingToThingGroupInput{
					ThingName: aws.String("m1"), ThingGroupName: aws.String("g1"),
				})
				require.NoError(t, err)
				_, err = e.client.RemoveThingFromThingGroup(t.Context(), &iotsdk.RemoveThingFromThingGroupInput{
					ThingName: aws.String("m1"), ThingGroupName: aws.String("g1"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "THING_GROUP_MEMBERSHIP_EVENT", p["eventType"])
				assert.Equal(t, "REMOVED", p["operation"])
				assert.Contains(t, p["groupArn"], ":thinggroup/g1")
				assert.Contains(t, p["thingArn"], ":thing/m1")
				assert.NotEmpty(t, p["membershipId"])
			},
		},
		{
			name: "job_and_execution_canceled_then_completed", enabled: []string{"JOB", "JOB_EXECUTION"},
			topic: "$aws/events/job/j1/completed",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				out, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("jt")})
				require.NoError(t, err)
				_, err = e.client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId: aws.String("j1"), Targets: []string{*out.ThingArn},
					Document: aws.String(`{"a":1}`), Description: aws.String("My Job Description"),
				})
				require.NoError(t, err)
				_, err = e.client.CancelJobExecution(t.Context(), &iotsdk.CancelJobExecutionInput{
					JobId: aws.String("j1"), ThingName: aws.String("jt"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "JOB", p["eventType"])
				assert.Equal(t, "completed", p["operation"])
				assert.Equal(t, "COMPLETED", p["status"])
				assert.Equal(t, "j1", p["jobId"])
				assert.Equal(t, "My Job Description", p["description"])
				assert.Contains(t, p, "jobProcessDetails")
				assert.NotZero(t, p["completedAt"])
			},
		},
		{
			name: "job_canceled", enabled: []string{"JOB"}, topic: "$aws/events/job/j2/canceled",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				out, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("jt2")})
				require.NoError(t, err)
				_, err = e.client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId: aws.String("j2"), Targets: []string{*out.ThingArn}, Document: aws.String(`{"a":1}`),
				})
				require.NoError(t, err)
				_, err = e.client.CancelJob(t.Context(), &iotsdk.CancelJobInput{JobId: aws.String("j2")})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "CANCELED", p["status"])
				assert.Len(t, p["targets"], 1)
			},
		},
		{
			name: "job_execution_canceled", enabled: []string{"JOB_EXECUTION"},
			topic: "$aws/events/jobExecution/j3/canceled",
			act: func(t *testing.T, e *eventEnv) {
				t.Helper()
				out, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("jt3")})
				require.NoError(t, err)
				_, err = e.client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId: aws.String("j3"), Targets: []string{*out.ThingArn}, Document: aws.String(`{"a":1}`),
				})
				require.NoError(t, err)
				_, err = e.client.CancelJobExecution(t.Context(), &iotsdk.CancelJobExecutionInput{
					JobId: aws.String("j3"), ThingName: aws.String("jt3"),
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, p map[string]any) {
				t.Helper()
				assert.Equal(t, "JOB_EXECUTION", p["eventType"])
				assert.Equal(t, "CANCELED", p["status"])
				assert.Contains(t, p["thingArn"], ":thing/jt3")
				assert.Equal(t, map[string]any{}, p["statusDetails"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEventEnv(t)
			w := e.watch(t, "$aws/events/#")
			e.enable(t, tt.enabled...)
			tt.act(t, e)
			tt.check(t, w.next(t, tt.topic))
		})
	}
}

func TestRegistryEventsAreOptIn(t *testing.T) {
	t.Parallel()

	e := newEventEnv(t)
	w := e.watch(t, "$aws/events/thing/#")

	_, err := e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("off")})
	require.NoError(t, err)

	e.enable(t, "THING")

	_, err = e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("on")})
	require.NoError(t, err)

	select {
	case m := <-w.msgs:
		assert.Equal(t, "$aws/events/thing/on/created", m.Topic())
	case <-time.After(eventWait):
		require.FailNow(t, "no event for the enabled thing")
	}
}

func TestRegistryEventsFeedRules(t *testing.T) {
	t.Parallel()

	e := newEventEnv(t)
	e.enable(t, "THING")

	_, err := e.client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
		RuleName: aws.String("thingRule"),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql: aws.String("SELECT thingName FROM '$aws/events/thing/+/created'"),
			Actions: []iottypes.Action{{Republish: &iottypes.RepublishAction{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/r"), Topic: aws.String("seen/things"),
			}}},
		},
	})
	require.NoError(t, err)

	w := e.watch(t, "seen/things")

	_, err = e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{ThingName: aws.String("ruled")})
	require.NoError(t, err)

	assert.Equal(t, "ruled", w.next(t, "seen/things")["thingName"])
}

func TestBrokerConnectFailedEvent(t *testing.T) {
	t.Parallel()

	e := newCustomAuthEnv(t)
	e.createAuthorizer(t, "def", &iot.CreateAuthorizerInput{SigningDisabled: true})
	require.NoError(t, e.backend.SetDefaultAuthorizer("def"))
	e.lambda.setRespond(func(ev map[string]any) any {
		if mqttPassword(ev) == "observer" {
			return observerAllow()
		}

		return map[string]any{"isAuthenticated": false}
	})

	msgs := make(chan pahomqtt.Message, 16)
	obs, err := e.connect(t, "observer", "u", "observer", msgs, nil)
	require.NoError(t, err)
	require.NoError(t, subscribeErr(obs, "$aws/events/presence/connect_failed/#"))

	_, err = e.connect(t, "intruder", "u", "wrong", nil, nil)
	require.Error(t, err)

	select {
	case m := <-msgs:
		assert.Equal(t, "$aws/events/presence/connect_failed/intruder", m.Topic())

		var p map[string]any
		require.NoError(t, json.Unmarshal(m.Payload(), &p))
		assert.Equal(t, "connect_failed", p["eventType"])
		assert.Equal(t, "AUTHORIZATION_FAILED", p["connectFailureReason"])
		assert.Equal(t, "intruder", p["clientId"])
		assert.NotEmpty(t, p["sessionIdentifier"])
	case <-time.After(eventWait):
		require.FailNow(t, "no connect_failed event")
	}
}
