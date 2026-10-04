package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
)

const (
	lookupSecretPerm = `secretsmanager:GetSecretValue","secretsmanager:DescribeSecret`
	lookupTopic      = "lk/in"
)

// lookupRule creates a rule whose SQS action runs as its own role, so f.role is only the lookup role.
func (f *iotRuleFixture) lookupRule(t *testing.T, name, sql string) string {
	t.Helper()

	url, _ := authzQueue(t, f.fx, name+"-q")
	sqsRole := authzRole(t, f.fx, name+"-sqs", "iot.amazonaws.com", "sqs:SendMessage")

	_, err := iot.NewFromConfig(f.fx.cfg).CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
		RuleName: aws.String(name),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql: aws.String(sql), AwsIotSqlVersion: aws.String("2016-03-23"),
			Actions: []iottypes.Action{{Sqs: &iottypes.SqsAction{
				QueueUrl: aws.String(url), RoleArn: aws.String(sqsRole),
			}}},
		},
	})
	require.NoError(t, err)

	return url
}

func (f *iotRuleFixture) wantBody(t *testing.T, url, want string) {
	t.Helper()

	assert.JSONEq(t, want, f.bodies(t, url, 1)[0])
}

func (f *iotRuleFixture) putItem(t *testing.T, table string, item map[string]ddbtypes.AttributeValue) {
	t.Helper()

	_, err := dynamodb.NewFromConfig(f.fx.cfg).PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String(table), Item: item,
	})
	require.NoError(t, err)
}

type lookupCase struct {
	setup   func(t *testing.T, f *iotRuleFixture)
	name    string
	sql     string
	perm    string
	payload string
	want    string
	enforce bool
}

func TestIoTRuleLookupFunctions(t *testing.T) {
	t.Parallel()

	tests := []lookupCase{
		{
			name: "get_dynamodb_hash_key", perm: "dynamodb:GetItem", enforce: true,
			sql: "SELECT get_dynamodb('devices','id',id,'${role}').city AS city FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				iotTable(t, f, "devices", false)
				f.putItem(t, "devices", map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{
						Value: "d1",
					}, "city": &ddbtypes.AttributeValueMemberS{Value: "Oslo"},
				})
			},
			payload: `{"id":"d1"}`, want: `{"city":"Oslo"}`,
		},
		{
			name: "get_dynamodb_range_key", perm: "dynamodb:GetItem", enforce: true,
			sql: "SELECT get_dynamodb('readings','id',id,'seq',seq,'${role}') AS item FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				iotTable(t, f, "readings", true)
				f.putItem(t, "readings", map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{
						Value: "d1",
					}, "seq": &ddbtypes.AttributeValueMemberN{Value: "7"},
					"ok": &ddbtypes.AttributeValueMemberBOOL{Value: true},
				})
			},
			payload: `{"id":"d1","seq":7}`, want: `{"item":{"id":"d1","seq":7,"ok":true}}`,
		},
		{
			name: "get_dynamodb_missing_item_is_undefined", perm: "dynamodb:GetItem", enforce: true,
			sql:     "SELECT 1 AS one, get_dynamodb('devices2','id',id,'${role}') AS item FROM '" + lookupTopic + "'",
			setup:   func(t *testing.T, f *iotRuleFixture) { t.Helper(); iotTable(t, f, "devices2", false) },
			payload: `{"id":"nope"}`, want: `{"one":1}`,
		},
		{
			name: "get_secret_json_key", perm: lookupSecretPerm, enforce: true,
			sql: "SELECT get_secret('apikey','SecretString','k','${role}') AS key FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := secretsmanager.NewFromConfig(f.fx.cfg).
					CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("apikey"), SecretString: aws.String(`{"k":"s3cret","other":1}`),
					})
				require.NoError(t, err)
			},
			payload: `{}`, want: `{"key":"s3cret"}`,
		},
		{
			name: "get_secret_whole_object", perm: lookupSecretPerm,
			sql: "SELECT get_secret('objsecret','SecretString','${role}') AS s FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := secretsmanager.NewFromConfig(f.fx.cfg).
					CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("objsecret"), SecretString: aws.String(`{"k":"v"}`),
					})
				require.NoError(t, err)
			},
			payload: `{}`, want: `{"s":{"k":"v"}}`,
		},
		{
			name: "get_secret_binary", perm: lookupSecretPerm, enforce: true,
			sql: "SELECT get_secret('binsecret','SecretBinary','${role}') AS b FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := secretsmanager.NewFromConfig(f.fx.cfg).
					CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("binsecret"), SecretBinary: []byte("bin-value"),
					})
				require.NoError(t, err)
			},
			payload: `{}`, want: `{"b":"` + base64.StdEncoding.EncodeToString([]byte("bin-value")) + `"}`,
		},
		{
			name: "get_thing_shadow_classic", perm: "iot:GetThingShadow", enforce: true,
			sql: "SELECT get_thing_shadow('thing1','${role}').state.reported.temp AS temp FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				f.updateShadow(t, "thing1", "", `{"state":{"reported":{"temp":21}}}`)
			},
			payload: `{}`, want: `{"temp":21}`,
		},
		{
			name: "get_thing_shadow_named", perm: "iot:GetThingShadow", enforce: true,
			sql: "SELECT get_thing_shadow('thing2','cfg','${role}').state.reported.mode AS mode FROM '" +
				lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				f.updateShadow(t, "thing2", "cfg", `{"state":{"reported":{"mode":"eco"}}}`)
			},
			payload: `{}`, want: `{"mode":"eco"}`,
		},
		{
			name: "get_registry_data_describe_thing", perm: "iot:DescribeThing", enforce: true,
			sql: "SELECT get_registry_data('DescribeThing','sensor1','${role}').attributes.limit AS lim FROM '" +
				lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := iot.NewFromConfig(f.fx.cfg).CreateThing(t.Context(), &iot.CreateThingInput{
					ThingName:        aws.String("sensor1"),
					AttributePayload: &iottypes.AttributePayload{Attributes: map[string]string{"limit": "9"}},
				})
				require.NoError(t, err)
			},
			payload: `{}`, want: `{"lim":"9"}`,
		},
		{
			name: "get_registry_data_thing_groups", perm: "iot:ListThingGroupsForThing", enforce: true,
			sql: "SELECT get_registry_data('ListThingGroupsForThing','sensor2','${role}')" +
				".thingGroups[0].groupName AS g FROM '" +
				lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				c := iot.NewFromConfig(f.fx.cfg)
				_, err := c.CreateThing(t.Context(), &iot.CreateThingInput{ThingName: aws.String("sensor2")})
				require.NoError(t, err)
				_, err = c.CreateThingGroup(
					t.Context(),
					&iot.CreateThingGroupInput{ThingGroupName: aws.String("floor1")},
				)
				require.NoError(t, err)
				_, err = c.AddThingToThingGroup(t.Context(), &iot.AddThingToThingGroupInput{
					ThingName: aws.String("sensor2"), ThingGroupName: aws.String("floor1"),
				})
				require.NoError(t, err)
			},
			payload: `{}`, want: `{"g":"floor1"}`,
		},
		{
			name: "set_variable_reused_with_lookup", perm: "dynamodb:GetItem", enforce: true,
			sql: "SET @dev = get_dynamodb('devices3','id',id,'${role}') SELECT @dev.city AS city, " +
				"@dev.zone AS zone FROM '" + lookupTopic + "' WHERE @dev.city = 'Rome'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				iotTable(t, f, "devices3", false)
				f.putItem(t, "devices3", map[string]ddbtypes.AttributeValue{
					"id": &ddbtypes.AttributeValueMemberS{
						Value: "d1",
					}, "city": &ddbtypes.AttributeValueMemberS{Value: "Rome"},
					"zone": &ddbtypes.AttributeValueMemberS{Value: "z9"},
				})
			},
			payload: `{"id":"d1"}`, want: `{"city":"Rome","zone":"z9"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, tt.enforce, tt.perm)
			tt.setup(t, f)

			url := f.lookupRule(t, "lk", replaceRole(tt.sql, f.role))
			f.publish(t, lookupTopic, tt.payload)
			f.wantBody(t, url, tt.want)
		})
	}
}

func replaceRole(sql, role string) string {
	return strings.ReplaceAll(sql, "${role}", role)
}

type iotFakeLambda struct {
	out      string
	payloads []string
	mu       sync.Mutex
}

func (l *iotFakeLambda) RequestLambda(_ context.Context, _, _ string, payload []byte) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.payloads = append(l.payloads, string(payload))

	return []byte(l.out), nil
}

func (l *iotFakeLambda) calls() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.payloads...)
}

func (f *iotRuleFixture) updateShadow(t *testing.T, thing, shadow, doc string) {
	t.Helper()

	in := &iotdataplane.UpdateThingShadowInput{ThingName: aws.String(thing), Payload: []byte(doc)}
	if shadow != "" {
		in.ShadowName = aws.String(shadow)
	}

	_, err := iotdataplane.NewFromConfig(f.fx.cfg).UpdateThingShadow(t.Context(), in)
	require.NoError(t, err)
}

func TestIoTRuleLookupEnforcementDenied(t *testing.T) {
	t.Parallel()

	tests := []lookupCase{
		{
			name: "get_secret_without_describe_secret", perm: "secretsmanager:GetSecretValue",
			sql: "SELECT get_secret('apikey','SecretString','k','${role}') AS key FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := secretsmanager.NewFromConfig(f.fx.cfg).
					CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("apikey"), SecretString: aws.String(`{"k":"s3cret"}`),
					})
				require.NoError(t, err)
			},
		},
		{
			name: "get_secret_wrong_permission", perm: "dynamodb:GetItem",
			sql: "SELECT get_secret('apikey','SecretString','k','${role}') AS key FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				_, err := secretsmanager.NewFromConfig(f.fx.cfg).
					CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
						Name: aws.String("apikey"), SecretString: aws.String(`{"k":"s3cret"}`),
					})
				require.NoError(t, err)
			},
		},
		{
			name: "get_dynamodb_wrong_permission", perm: "sqs:SendMessage",
			sql: "SELECT get_dynamodb('devices','id',id,'${role}') AS d FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				iotTable(t, f, "devices", false)
				f.putItem(
					t,
					"devices",
					map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "d1"}},
				)
			},
		},
		{
			name: "get_thing_shadow_wrong_permission", perm: "sqs:SendMessage",
			sql: "SELECT get_thing_shadow('thing1','${role}') AS s FROM '" + lookupTopic + "'",
			setup: func(t *testing.T, f *iotRuleFixture) {
				t.Helper()
				f.updateShadow(t, "thing1", "", `{"state":{"reported":{"temp":21}}}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, true, tt.perm)
			tt.setup(t, f)

			url := f.lookupRule(t, "lk", replaceRole(tt.sql, f.role))
			sentinel := f.lookupRule(t, "sentinel", "SELECT 'ok' AS s FROM '"+lookupTopic+"'")

			f.publish(t, lookupTopic, `{"id":"d1"}`)

			f.wantBody(t, sentinel, `{"s":"ok"}`)
			assert.False(t, authzReceived(t, f.fx, url), "a denied lookup terminates the rule without running actions")
		})
	}
}

func TestIoTRuleAWSLambdaFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		grant   bool
		enforce bool
		want    bool
	}{
		{name: "allowed_by_resource_policy", grant: true, enforce: true, want: true},
		{name: "denied_without_resource_policy", enforce: true},
		{name: "enforcement_off", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, tt.enforce, "*")
			fnARN := authzLambda(t, f.fx, "iot-fn")
			fake := &iotFakeLambda{out: `{"out":"done"}`}

			iotH, ok := serviceByName(f.fx.services)["IoT"].(*iotbackend.Handler)
			require.True(t, ok)

			iotBk, bkOk := iotH.Backend.(*iotbackend.InMemoryBackend)
			require.True(t, bkOk)
			iotBk.SetActionTargets(&iotbackend.ActionTargets{Lambda: fake})

			if tt.grant {
				_, err := lambda.NewFromConfig(f.fx.cfg).AddPermission(t.Context(), &lambda.AddPermissionInput{
					FunctionName: aws.String("iot-fn"), StatementId: aws.String("s1"),
					Action: aws.String("lambda:InvokeFunction"), Principal: aws.String("iot.amazonaws.com"),
					SourceArn: aws.String("arn:aws:iot:us-east-1:000000000000:rule/lk"),
				})
				require.NoError(t, err)
			}

			url := f.lookupRule(t, "lk", "SELECT aws_lambda('"+fnARN+"', {\"n\": n}).out AS o FROM '"+lookupTopic+"'")
			f.publish(t, lookupTopic, `{"n":4}`)

			if !tt.want {
				assert.False(t, authzReceived(t, f.fx, url))
				assert.Empty(t, fake.calls())

				return
			}

			f.wantBody(t, url, `{"o":"done"}`)
			require.Len(t, fake.calls(), 1)
			assert.JSONEq(t, `{"n":4}`, fake.calls()[0])
		})
	}
}

func TestIoTRuleBasicIngest(t *testing.T) {
	t.Parallel()

	f := newIoTRuleFixture(t, false, "*")
	url := f.lookupRule(t, "ingest", "SELECT topic() AS t, n FROM 'bi/#'")
	other := f.lookupRule(t, "other", "SELECT 'other' AS o FROM 'bi/#'")

	ingestSub := iotSubscribe(t, f, "$aws/rules/#")
	normalSub := iotSubscribe(t, f, "bi/#")

	f.publish(t, "$aws/rules/ingest/bi/x", `{"n":1}`)
	f.publish(t, "$aws/rules/missing/bi/x", `{"n":2}`)
	f.publish(t, "$aws/rules/ingest/other/topic", `{"n":3}`)
	f.publish(t, "bi/y", `{"n":4}`)

	assert.ElementsMatch(t, []string{`{"t":"bi/x","n":1}`, `{"t":"bi/y","n":4}`}, f.bodies(t, url, 2))
	f.wantBody(t, other, `{"o":"other"}`)
	assert.False(t, authzReceived(t, f.fx, other), "only the named rule runs for a Basic Ingest topic")

	select {
	case m := <-normalSub:
		assert.Equal(t, "bi/y", m)
	case <-time.After(authzDeadline):
		t.Fatal("normal subscriber got no message")
	}

	select {
	case m := <-ingestSub:
		t.Fatalf("Basic Ingest message was delivered to a subscriber on %s", m)
	default:
	}
}

func iotSubscribe(t *testing.T, f *iotRuleFixture, filter string) <-chan string {
	t.Helper()

	got := make(chan string, 8)
	opts := pahomqtt.NewClientOptions().
		AddBroker("tcp://127.0.0.1:" + strconv.Itoa(f.port)).
		SetClientID("sub-" + strconv.Itoa(len(filter))).
		SetConnectTimeout(authzDeadline).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) { got <- m.Topic() })

	c := pahomqtt.NewClient(opts)

	require.Eventually(t, func() bool {
		tok := c.Connect()

		return tok.WaitTimeout(authzDeadline) && tok.Error() == nil
	}, authzDeadline, authzTick)
	t.Cleanup(func() { c.Disconnect(0) })

	tok := c.Subscribe(filter, 0, nil)
	require.True(t, tok.WaitTimeout(authzDeadline))
	require.NoError(t, tok.Error())

	return got
}

func TestIoTRuleConnectionFunctions(t *testing.T) {
	t.Parallel()

	f := newIoTRuleFixture(t, false, "*")
	url := f.lookupRule(t, "conn", "SELECT sourceip() AS ip, traceid() AS tid, clientid() AS cid FROM 'mq/#'")

	opts := pahomqtt.NewClientOptions().
		AddBroker("tcp://127.0.0.1:" + strconv.Itoa(f.port)).SetClientID("device-7").SetConnectTimeout(authzDeadline)
	c := pahomqtt.NewClient(opts)

	require.Eventually(t, func() bool {
		tok := c.Connect()

		return tok.WaitTimeout(authzDeadline) && tok.Error() == nil
	}, authzDeadline, authzTick)
	t.Cleanup(func() { c.Disconnect(0) })

	tok := c.Publish("mq/a", 0, false, `{}`)
	require.True(t, tok.WaitTimeout(authzDeadline))

	var msg map[string]string
	require.NoError(t, json.Unmarshal([]byte(f.bodies(t, url, 1)[0]), &msg))

	assert.Equal(t, "device-7", msg["cid"])
	assert.NotNil(t, net.ParseIP(msg["ip"]))
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`, msg["tid"])

	f.publish(t, "mq/b", `{}`)
	assert.JSONEq(t, `{"cid":"n/a"}`, f.bodies(t, url, 1)[0])
}
