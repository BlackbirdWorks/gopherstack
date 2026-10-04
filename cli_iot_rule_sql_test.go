package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *iotRuleFixture) sqsRule(t *testing.T, sql, version string) string {
	t.Helper()

	url, _ := authzQueue(t, f.fx, "iot-sql-q")

	input := &iot.CreateTopicRuleInput{
		RuleName: aws.String("sqlrule"),
		TopicRulePayload: &iottypes.TopicRulePayload{
			Sql: aws.String(sql), AwsIotSqlVersion: aws.String(version),
			Actions: []iottypes.Action{{Sqs: &iottypes.SqsAction{
				QueueUrl: aws.String(url), RoleArn: aws.String(f.role),
			}}},
		},
	}

	_, err := iot.NewFromConfig(f.fx.cfg).CreateTopicRule(t.Context(), input)
	require.NoError(t, err)

	return url
}

func TestIoTRuleSQLProjection(t *testing.T) {
	t.Parallel()

	const payload = `{"color":"red","temperature":50,"loc":{"lat":1.5,"lon":2.5}}`

	tests := []struct {
		name    string
		sql     string
		version string
		topic   string
		payload string
		want    string
		raw     bool
	}{
		{
			name: "fields_and_alias", sql: "SELECT color AS my_color, temperature FROM 'dev/+'",
			version: "2016-03-23", topic: "dev/a", payload: payload, want: `{"my_color":"red","temperature":50}`,
		},
		{
			name: "functions_and_arithmetic", version: "2016-03-23", topic: "dev/a", payload: payload,
			sql:  "SELECT topic(2) AS device, (temperature - 32) * 5 / 9 AS celsius, upper(color) AS c FROM 'dev/#'",
			want: `{"device":"a","celsius":10,"c":"RED"}`,
		},
		{
			name: "nested_and_star", sql: "SELECT *, loc.lat AS lat FROM 'dev/+'", version: "2016-03-23",
			topic: "dev/a", payload: payload,
			want: `{"color":"red","temperature":50,"loc":{"lat":1.5,"lon":2.5},"lat":1.5}`,
		},
		{
			name: "undefined_omitted", sql: "SELECT color, missing, loc.nope AS n FROM 'dev/+'", version: "2016-03-23",
			topic: "dev/a", payload: payload, want: `{"color":"red"}`,
		},
		{
			name: "where_and_case", version: "2015-10-08", topic: "dev/a", payload: payload,
			sql:  "SELECT CASE color WHEN 'red' THEN 'stop' ELSE 'go' END AS s FROM 'dev/+' WHERE temperature > 40",
			want: `{"s":"stop"}`,
		},
		{
			name: "select_value_array", sql: "SELECT VALUE (SELECT VALUE n FROM e) FROM 'dev/+'", version: "2016-03-23",
			topic: "dev/a", payload: `{"e":[{"n":1},{"n":2}]}`, want: `[1,2]`,
		},
		{
			name: "non_json_passthrough", sql: "SELECT * FROM 'dev/+'", version: "2016-03-23",
			topic: "dev/a", payload: "plain text", want: "plain text", raw: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")
			url := f.sqsRule(t, tt.sql, tt.version)

			f.publish(t, tt.topic, tt.payload)

			got := f.bodies(t, url, 1)
			require.Len(t, got, 1)

			if tt.raw {
				assert.Equal(t, tt.want, got[0])

				return
			}

			assert.JSONEq(t, tt.want, got[0])
		})
	}
}

func TestIoTRuleSQLWhereFilters(t *testing.T) {
	t.Parallel()

	f := newIoTRuleFixture(t, false, "*")
	url := f.sqsRule(t, "SELECT temperature FROM 'dev/+' WHERE temperature > 40", "2016-03-23")

	f.publish(t, "dev/a", `{"temperature":10}`)
	f.publish(t, "dev/a", `{"temperature":99}`)

	got := f.bodies(t, url, 1)
	require.Len(t, got, 1)
	assert.JSONEq(t, `{"temperature":99}`, got[0])
}

func TestIoTRuleSQLParseException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
	}{
		{name: "missing_from", sql: "SELECT *"},
		{name: "unknown_function", sql: "SELECT nosuchfn(a) FROM 'a/b'"},
		{name: "bad_topic_filter", sql: "SELECT * FROM 'a/#/b'"},
		{name: "unbalanced", sql: "SELECT (a FROM 'a/b'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newIoTRuleFixture(t, false, "*")
			c := iot.NewFromConfig(f.fx.cfg)
			payload := func(sql string) *iottypes.TopicRulePayload {
				return &iottypes.TopicRulePayload{
					Sql: aws.String(sql), AwsIotSqlVersion: aws.String("2016-03-23"), Actions: []iottypes.Action{},
				}
			}

			_, err := c.CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
				RuleName: aws.String("bad"), TopicRulePayload: payload(tt.sql),
			})

			var parseErr *iottypes.SqlParseException
			require.ErrorAs(t, err, &parseErr, "create: %v", err)

			_, err = c.CreateTopicRule(t.Context(), &iot.CreateTopicRuleInput{
				RuleName: aws.String("good"), TopicRulePayload: payload("SELECT * FROM 'a/b'"),
			})
			require.NoError(t, err)

			_, err = c.ReplaceTopicRule(t.Context(), &iot.ReplaceTopicRuleInput{
				RuleName: aws.String("good"), TopicRulePayload: payload(tt.sql),
			})
			require.ErrorAs(t, err, &parseErr, "replace: %v", err)
		})
	}
}
