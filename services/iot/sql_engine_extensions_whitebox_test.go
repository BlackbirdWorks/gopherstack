package iot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

func TestSQLSetVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		payload string
		want    string
	}{
		{
			name:    "celsius",
			sql:     "SET @c = (temp_fahrenheit - 32) * 5 / 9 SELECT @c AS celsius, humidity FROM 'd/x'",
			payload: `{"temp_fahrenheit":50,"humidity":60}`,
			want:    `{"celsius":10,"humidity":60}`,
		},
		{
			name:    "member_access",
			sql:     "SET @s = d.sensors SELECT @s.t AS t, @s.h AS h FROM 'd/x'",
			payload: `{"d":{"sensors":{"t":75,"h":60}}}`,
			want:    `{"t":75,"h":60}`,
		},
		{
			name:    "multiple_and_where",
			sql:     "SET @a = 50, @b = @a + 1 SELECT @b AS b FROM 'd/x' WHERE t > @a",
			payload: `{"t":60}`,
			want:    `{"b":51}`,
		},
		{
			name:    "where_blocks",
			sql:     "SET @a = 50 SELECT * FROM 'd/x' WHERE t > @a",
			payload: `{"t":10}`,
			want:    ``,
		},
		{
			name:    "unaliased_member_key",
			sql:     "SET @o = {\"name\": 'x'} SELECT @o.name FROM 'd/x'",
			payload: `{}`,
			want:    `{"name":"x"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := runSQL(tt.sql, sqlVersionLatest, "d/x", tt.payload)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSQLSetErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		{name: "undeclared", sql: "SELECT @x FROM 't'"},
		{name: "duplicate", sql: "SET @a = 1, @a = 2 SELECT @a FROM 't'"},
		{name: "no_at", sql: "SET a = 1 SELECT a FROM 't'"},
		{name: "self_reference", sql: "SET @a = @a SELECT @a FROM 't'"},
		{
			name: "too_many",
			sql:  "SET @a1=1,@a2=1,@a3=1,@a4=1,@a5=1,@a6=1,@a7=1,@a8=1,@a9=1,@a10=1,@a11=1 SELECT * FROM 't'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseRuleSQLVersion(tt.sql, sqlVersionLatest)
			require.ErrorIs(t, err, ErrSQLParse)
		})
	}
}

func TestSQLTimeFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "parse_time_zone",
			sql:  `SELECT parse_time("yyyy.MM.dd G 'at' HH:mm:ss z", 100000000, 'America/Belize') AS ts FROM 'a'`,
			want: `{"ts":"1970.01.01 AD at 21:46:40 CST"}`,
		},
		{
			name: "parse_time_utc_default",
			sql:  `SELECT parse_time('yyyy-MM-dd HH:mm:ss.SSS', 1700000000123) AS ts FROM 'a'`,
			want: `{"ts":"2023-11-14 22:13:20.123"}`,
		},
		{
			name: "parse_time_names",
			sql:  `SELECT parse_time('EEEE, MMMM d, yyyy h:mm a', 1700000000123) AS ts FROM 'a'`,
			want: `{"ts":"Tuesday, November 14, 2023 10:13 PM"}`,
		},
		{name: "parse_time_bad_zone", sql: `SELECT parse_time('yyyy', 0, 'Nope/Zone') AS ts FROM 'a'`, want: `{}`},
		{
			name: "epoch_offset_zone",
			sql:  `SELECT time_to_epoch("2020-04-03 09:45:18 UTC+01:00", "yyyy-MM-dd HH:mm:ss VV") AS e FROM 'a'`,
			want: `{"e":1585903518000}`,
		},
		{
			name: "epoch_month_name",
			sql:  `SELECT time_to_epoch("18 December 2015", "dd MMMM yyyy") AS e FROM 'a'`,
			want: `{"e":1450396800000}`,
		},
		{
			name: "epoch_zone_id_millis",
			sql: `SELECT time_to_epoch("2007-12-03 10:15:30.592 America/Los_Angeles", ` +
				`"yyyy-MM-dd HH:mm:ss.SSS z") AS e FROM 'a'`,
			want: `{"e":1196705730592}`,
		},
		{name: "epoch_mismatch", sql: `SELECT time_to_epoch("nope", "yyyy-MM-dd") AS e FROM 'a'`, want: `{}`},
		{name: "epoch_bad_day", sql: `SELECT time_to_epoch("2021-02-31", "yyyy-MM-dd") AS e FROM 'a'`, want: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := runSQL(tt.sql, sqlVersionLatest, "a", `{}`)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSQLMessagePropertyFunctions(t *testing.T) {
	t.Parallel()

	props := &mqttProps{
		contentType: "text/plain", responseTopic: "r/t", correlation: []byte("cid"), utf8: true,
		user: []userProp{{"k", "v1"}, {"o", "ov"}, {"k", "v2"}},
	}

	tests := []struct {
		name  string
		sql   string
		props *mqttProps
		want  string
	}{
		{
			name: "mqtt_properties", sql: "SELECT get_mqtt_property('content_type') AS c, " +
				"get_mqtt_property('format_indicator') AS f, get_mqtt_property('response_topic') AS r, " +
				"get_mqtt_property('correlation_data') AS d, get_mqtt_property('bogus') AS b FROM 'a'",
			props: props, want: `{"c":"text/plain","f":"UTF8_DATA","r":"r/t","d":"Y2lk"}`,
		},
		{
			name:  "format_default",
			sql:   "SELECT get_mqtt_property('format_indicator') AS f, get_mqtt_property('content_type') AS c FROM 'a'",
			props: &mqttProps{}, want: `{"f":"UNSPECIFIED_BYTES"}`,
		},
		{
			name:  "user_properties",
			sql:   "SELECT get_user_properties('k') AS k, get_user_properties('none') AS n FROM 'a'",
			props: props, want: `{"k":["v1","v2"]}`,
		},
		{
			name: "user_properties_all", sql: "SELECT get_user_properties() AS all FROM 'a'",
			props: props, want: `{"all":[{"k":"v1"},{"o":"ov"},{"k":"v2"}]}`,
		},
		{name: "no_mqtt", sql: "SELECT get_mqtt_property('content_type') AS c FROM 'a'", want: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := ParseRuleSQLVersion(tt.sql, sqlVersionLatest)
			require.NoError(t, err)

			msg := &ruleMessage{topic: "a", payload: []byte(`{}`), original: []byte(`{}`), props: tt.props}
			out, _ := parsed.apply(msg)
			assert.JSONEq(t, tt.want, string(out))
		})
	}
}

func TestSQLConnectionFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		msg  ruleMessage
	}{
		{
			name: "mqtt_client",
			msg:  ruleMessage{sourceIP: "10.1.2.3", traceID: "tid-1", principal: "thumb"},
			want: `{"ip":"10.1.2.3","tid":"tid-1","p":"thumb"}`,
		},
		{name: "not_mqtt", msg: ruleMessage{}, want: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := ParseRuleSQLVersion(
				"SELECT sourceip() AS ip, traceid() AS tid, principal() AS p FROM 'a'",
				sqlVersionLatest,
			)
			require.NoError(t, err)

			msg := tt.msg
			msg.payload, msg.original, msg.topic = []byte(`{}`), []byte(`{}`), "a"
			out, _ := parsed.apply(&msg)
			assert.JSONEq(t, tt.want, string(out))
		})
	}
}

var errFakeLookup = errors.New("lookup failed")

type fakeLookups struct {
	err       error
	item      map[string]any
	gotKey    map[string]any
	shadow    []byte
	lambdaOut []byte
	gotLambda []byte
	secret    SecretValue
	reads     int
}

func (f *fakeLookups) GetItem(_ context.Context, _, _ string, key map[string]any) (map[string]any, error) {
	f.reads++
	f.gotKey = key

	return f.item, f.err
}

func (f *fakeLookups) GetSecretValue(context.Context, string, string) (SecretValue, error) {
	return f.secret, f.err
}

func (f *fakeLookups) GetThingShadow(context.Context, string, string, string) ([]byte, error) {
	return f.shadow, f.err
}

func (f *fakeLookups) RequestLambda(_ context.Context, _, _ string, payload []byte) ([]byte, error) {
	f.gotLambda = payload

	return f.lambdaOut, f.err
}

type denyRole struct{ deny string }

func (d denyRole) AuthorizeRole(_, role, _, _ string) error {
	if role == d.deny {
		return roleauth.ErrAccessDenied
	}

	return nil
}

func lookupMessage(t *testing.T, f *fakeLookups, auth roleauth.Authorizer) (*ruleMessage, *InMemoryBackend) {
	t.Helper()

	b := NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	b.SetActionTargets(&ActionTargets{DynamoReader: f, Secrets: f, Shadows: f, Lambda: f})

	if auth != nil {
		b.SetRoleAuthorizer(auth)
	}

	msg := &ruleMessage{
		received: time.UnixMilli(1), topic: "a", region: "us-east-1", account: "123456789012",
		ruleARN: "arn:aws:iot:us-east-1:123456789012:rule/r", payload: []byte(`{"id":"d1","n":7}`),
		original: []byte(`{"id":"d1","n":7}`), hook: &ruleHook{backend: b, ctx: t.Context()},
	}

	return msg, b
}

func TestSQLLookupFunctions(t *testing.T) {
	t.Parallel()

	ddbItem := map[string]any{
		"id": map[string]any{"S": "d1"}, "loc": map[string]any{"M": map[string]any{
			"city": map[string]any{"S": "Oslo"}, "floor": map[string]any{"N": "3"},
		}},
		"tags": map[string]any{"SS": []any{"x", "y"}}, "ok": map[string]any{"BOOL": true},
	}

	tests := []struct {
		name  string
		sql   string
		want  string
		fake  fakeLookups
		fatal bool
	}{
		{
			name: "dynamodb", sql: "SELECT get_dynamodb('T','id',id,'arn:aws:iam::1:role/r').loc.city AS city FROM 'a'",
			fake: fakeLookups{item: ddbItem}, want: `{"city":"Oslo"}`,
		},
		{
			name: "dynamodb_second_call_aborts", sql: "SELECT get_dynamodb('T','id',id,'role').loc.floor AS f, " +
				"get_dynamodb('T','id',id,'role').tags AS t FROM 'a'",
			fake: fakeLookups{item: ddbItem}, fatal: true,
		},
		{
			name: "dynamodb_value_types", sql: "SELECT get_dynamodb('T','id',id,'role') AS i FROM 'a'",
			fake: fakeLookups{item: ddbItem},
			want: `{"i":{"id":"d1","loc":{"city":"Oslo","floor":3},"ok":true,"tags":["x","y"]}}`,
		},
		{
			name: "dynamodb_missing_item", sql: "SELECT get_dynamodb('T','id',id,'role') AS i FROM 'a'",
			fake: fakeLookups{}, want: `{}`,
		},
		{
			name: "dynamodb_range", sql: "SELECT get_dynamodb('T','id',id,'seq',n,'role').ok AS ok FROM 'a'",
			fake: fakeLookups{item: ddbItem}, want: `{"ok":true}`,
		},
		{
			name: "dynamodb_error_aborts", sql: "SELECT get_dynamodb('T','id',id,'role') AS i FROM 'a'",
			fake: fakeLookups{err: errFakeLookup}, fatal: true,
		},
		{
			name: "secret_key", sql: "SELECT get_secret('s','SecretString','api','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", String: `{"api":"v","other":1}`}}, want: `{"k":"v"}`,
		},
		{
			name: "secret_object", sql: "SELECT get_secret('s','SecretString','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", String: `{"api":"v"}`}}, want: `{"k":{"api":"v"}}`,
		},
		{
			name: "secret_plain_string", sql: "SELECT get_secret('s','SecretString','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", String: `plain`}}, want: `{"k":"plain"}`,
		},
		{
			name: "secret_key_on_plain_fails", sql: "SELECT get_secret('s','SecretString','api','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", String: `plain`}}, fatal: true,
		},
		{
			name: "secret_binary", sql: "SELECT get_secret('s','SecretBinary','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", Binary: []byte("hi")}}, want: `{"k":"aGk="}`,
		},
		{
			name: "secret_binary_key_fails", sql: "SELECT get_secret('s','SecretBinary','k','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", Binary: []byte("hi")}}, fatal: true,
		},
		{
			name: "secret_bad_type", sql: "SELECT get_secret('s','Other','role') AS k FROM 'a'",
			fake: fakeLookups{secret: SecretValue{ARN: "arn", String: `x`}}, fatal: true,
		},
		{
			name: "shadow_classic", sql: "SELECT get_thing_shadow('t','role').state.reported.on AS on FROM 'a'",
			fake: fakeLookups{shadow: []byte(`{"state":{"reported":{"on":true}}}`)}, want: `{"on":true}`,
		},
		{
			name: "shadow_named", sql: "SELECT get_thing_shadow('t','s','role').state.reported.v AS v FROM 'a'",
			fake: fakeLookups{shadow: []byte(`{"state":{"reported":{"v":2}}}`)}, want: `{"v":2}`,
		},
		{
			name: "shadow_error_aborts", sql: "SELECT get_thing_shadow('t','role') AS s FROM 'a'",
			fake: fakeLookups{err: errFakeLookup}, fatal: true,
		},
		{
			name: "lambda",
			sql: "SELECT aws_lambda('arn:aws:lambda:us-east-1:123456789012:function:f', " +
				"{\"n\": n}).out AS o FROM 'a'",
			fake: fakeLookups{lambdaOut: []byte(`{"out":"done"}`)}, want: `{"o":"done"}`,
		},
		{
			name: "lambda_non_json",
			sql:  "SELECT aws_lambda('arn:aws:lambda:us-east-1:123456789012:function:f', n) AS o FROM 'a'",
			fake: fakeLookups{lambdaOut: []byte(`not json`)}, fatal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fake
			msg, _ := lookupMessage(t, &f, nil)

			parsed, err := ParseRuleSQLVersion(tt.sql, sqlVersionLatest)
			require.NoError(t, err)

			out, _ := parsed.apply(msg)
			if tt.fatal {
				require.Error(t, msg.fatal)

				return
			}

			require.NoError(t, msg.fatal)
			assert.JSONEq(t, tt.want, string(out))
		})
	}
}

func TestSQLLookupKeysAndPayloads(t *testing.T) {
	t.Parallel()

	f := &fakeLookups{item: map[string]any{}, lambdaOut: []byte(`{}`)}
	msg, _ := lookupMessage(t, f, nil)

	parsed, err := ParseRuleSQLVersion(
		"SELECT get_dynamodb('T','id',id,'seq',n,'role') AS i, "+
			"aws_lambda('arn:aws:lambda:us-east-1:1:function:f', {\"x\": n}) AS l FROM 'a'",
		sqlVersionLatest)
	require.NoError(t, err)

	parsed.apply(msg)
	require.NoError(t, msg.fatal)
	assert.Equal(t, map[string]any{
		"id": map[string]any{"S": "d1"}, "seq": map[string]any{"N": "7"},
	}, f.gotKey)
	assert.JSONEq(t, `{"x":7}`, string(f.gotLambda))
}

func TestSQLLookupAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		sql   string
		fatal bool
	}{
		{name: "dynamodb_allowed", sql: "SELECT get_dynamodb('T','id',id,'ok') AS i FROM 'a'"},
		{name: "dynamodb_denied", sql: "SELECT get_dynamodb('T','id',id,'deny') AS i FROM 'a'", fatal: true},
		{name: "secret_denied", sql: "SELECT get_secret('s','SecretString','deny') AS i FROM 'a'", fatal: true},
		{name: "shadow_denied", sql: "SELECT get_thing_shadow('t','deny') AS i FROM 'a'", fatal: true},
		{
			name:  "registry_denied",
			sql:   "SELECT get_registry_data('DescribeThing','t','deny') AS i FROM 'a'",
			fatal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeLookups{
				item: map[string]any{}, secret: SecretValue{ARN: "arn", String: "s"}, shadow: []byte(`{}`),
			}
			msg, _ := lookupMessage(t, f, denyRole{deny: "deny"})

			parsed, err := ParseRuleSQLVersion(tt.sql, sqlVersionLatest)
			require.NoError(t, err)

			parsed.apply(msg)

			if tt.fatal {
				require.ErrorIs(t, msg.fatal, errSQLFunction)

				return
			}

			require.NoError(t, msg.fatal)
		})
	}
}

func TestSQLRegistryData(t *testing.T) {
	t.Parallel()

	f := &fakeLookups{}
	msg, b := lookupMessage(t, f, nil)

	_, err := b.CreateThing(&CreateThingInput{
		ThingName: "sensor1", ThingTypeName: "",
		AttributePayload: &AttributePayload{Attributes: map[string]string{"limit": "9"}},
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "describe_thing",
			sql:  "SELECT get_registry_data('DescribeThing','sensor1','role').attributes.limit AS l FROM 'a'",
			want: `{"l":"9"}`,
		},
		{
			name: "list_groups",
			sql:  "SELECT get_registry_data('ListThingGroupsForThing','sensor1','role').thingGroups AS g FROM 'a'",
			want: `{"g":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := *msg
			m.calls = nil

			parsed, perr := ParseRuleSQLVersion(tt.sql, sqlVersionLatest)
			require.NoError(t, perr)

			out, _ := parsed.apply(&m)
			require.NoError(t, m.fatal)
			assert.JSONEq(t, tt.want, string(out))
		})
	}

	m := *msg
	parsed, perr := ParseRuleSQLVersion(
		"SELECT get_registry_data('DescribeThing','nothing','role') AS g FROM 'a'",
		sqlVersionLatest,
	)
	require.NoError(t, perr)
	parsed.apply(&m)
	require.ErrorIs(t, m.fatal, errSQLFunction)
}

func TestSQLLookupNeedsVersion2016(t *testing.T) {
	t.Parallel()

	_, err := ParseRuleSQLVersion("SELECT get_dynamodb('T','id',id,'r') FROM 'a'", sqlVersion2015)
	require.ErrorIs(t, err, ErrSQLParse)

	_, err = ParseRuleSQLVersion("SELECT get_secret('s','SecretString','r') FROM 'a'", sqlVersion2015)
	require.NoError(t, err)
}

func TestBasicIngestTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, topic, rule, rest string
		ok                      bool
	}{
		{name: "plain", topic: "a/b"},
		{name: "ingest", topic: "$aws/rules/r1/a/b", rule: "r1", rest: "a/b", ok: true},
		{name: "ingest_no_topic", topic: "$aws/rules/r1", rule: "r1", ok: true},
		{name: "empty_rule", topic: "$aws/rules//a"},
		{name: "other_reserved", topic: "$aws/things/t/shadow"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rule, rest, ok := basicIngestTopic(tt.topic)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.rule, rule)
			assert.Equal(t, tt.rest, rest)
		})
	}
}

func TestRuleWithoutFromRunsOnlyViaBasicIngest(t *testing.T) {
	t.Parallel()

	rule := &TopicRule{SQL: "SELECT * WHERE n > 1", Enabled: true, AWSIoTSQLVersion: sqlVersion2016}

	assert.False(t, rule.fire(&ruleMessage{topic: "a", payload: []byte(`{"n":5}`), original: []byte(`{"n":5}`)}))
	assert.True(t, rule.fire(&ruleMessage{
		topic: "a", payload: []byte(`{"n":5}`), original: []byte(`{"n":5}`), ingest: true,
	}))
	assert.False(t, rule.fire(&ruleMessage{
		topic: "a", payload: []byte(`{"n":0}`), original: []byte(`{"n":0}`), ingest: true,
	}))
}
