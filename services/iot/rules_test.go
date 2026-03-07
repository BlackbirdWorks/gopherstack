package iot_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// IoT SQL rule test constants.
// These are MQTT topic rule selectors, not database queries.
//
//nolint:unqueryvet // IoT SQL rules use SELECT * legitimately; not database queries
const (
	sqlAllSensorTemp      = "SELECT * FROM 'sensor/temperature'"
	sqlAllSensorHash      = "SELECT * FROM 'sensor/#'"
	sqlSensorTempGT50     = "SELECT * FROM 'sensor/#' WHERE temperature > 50"
	sqlSensorTempLT20     = "SELECT * FROM 'sensor/#' WHERE temperature < 20"
	sqlDeviceStatusEqAct  = "SELECT * FROM 'device/status' WHERE status = 'active'"
	sqlSensorStatusNeqOff = "SELECT * FROM 'sensor/#' WHERE status != 'off'"
	sqlSensorValueGTE100  = "SELECT * FROM 'sensor/#' WHERE value >= 100"
	sqlAllHash            = "SELECT * FROM '#'"
	sqlSelectStar         = "SELECT *"
)

func TestParseRuleSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sql           string
		wantTopic     string
		wantCondition string
	}{
		{
			name:      "simple_topic",
			sql:       sqlAllSensorTemp,
			wantTopic: "sensor/temperature",
		},
		{
			name:      "wildcard_topic",
			sql:       sqlAllSensorHash,
			wantTopic: "sensor/#",
		},
		{
			name:          "with_where_clause",
			sql:           sqlSensorTempGT50,
			wantTopic:     "sensor/#",
			wantCondition: "temperature > 50",
		},
		{
			name:          "with_equals_condition",
			sql:           sqlDeviceStatusEqAct,
			wantTopic:     "device/status",
			wantCondition: "status = 'active'",
		},
		{
			name:      "no_from_clause",
			sql:       sqlSelectStar,
			wantTopic: "",
		},
		{
			name:      "empty",
			sql:       "",
			wantTopic: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := iot.ParseRuleSQL(tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTopic, got.TopicPattern)
			assert.Equal(t, tt.wantCondition, got.Condition)
		})
	}
}

func TestMatchesTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		topic   string
		want    bool
	}{
		{
			name:    "exact_match",
			pattern: "sensor/temperature",
			topic:   "sensor/temperature",
			want:    true,
		},
		{
			name:    "no_match",
			pattern: "sensor/temperature",
			topic:   "sensor/humidity",
			want:    false,
		},
		{
			name:    "hash_wildcard_matches_subtopic",
			pattern: "sensor/#",
			topic:   "sensor/temperature",
			want:    true,
		},
		{
			name:    "hash_wildcard_matches_deep",
			pattern: "sensor/#",
			topic:   "sensor/room1/temperature",
			want:    true,
		},
		{
			name:    "hash_only_matches_all",
			pattern: "#",
			topic:   "anything/goes/here",
			want:    true,
		},
		{
			name:    "plus_single_level",
			pattern: "sensor/+/temperature",
			topic:   "sensor/room1/temperature",
			want:    true,
		},
		{
			name:    "plus_does_not_match_multiple_levels",
			pattern: "sensor/+/temperature",
			topic:   "sensor/room1/sub/temperature",
			want:    false,
		},
		{
			name:    "exact_match_three_levels",
			pattern: "a/b/c",
			topic:   "a/b/c",
			want:    true,
		},
		{
			name:    "fewer_topic_levels",
			pattern: "a/b/c",
			topic:   "a/b",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iot.MatchesTopic(tt.pattern, tt.topic)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluateRule(t *testing.T) {
	t.Parallel()

	makeRule := func(sql string) *iot.TopicRule {
		return &iot.TopicRule{
			RuleName:  "TestRule",
			SQL:       sql,
			Enabled:   true,
			CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		rule    *iot.TopicRule
		name    string
		topic   string
		payload []byte
		want    bool
	}{
		{
			name:    "exact_topic_match_no_condition",
			rule:    makeRule(sqlAllSensorTemp),
			topic:   "sensor/temperature",
			payload: []byte(`{"temperature": 42}`),
			want:    true,
		},
		{
			name:    "topic_no_match",
			rule:    makeRule(sqlAllSensorTemp),
			topic:   "sensor/humidity",
			payload: []byte(`{"humidity": 80}`),
			want:    false,
		},
		{
			name:    "condition_greater_than_true",
			rule:    makeRule(sqlSensorTempGT50),
			topic:   "sensor/temp",
			payload: []byte(`{"temperature": 75}`),
			want:    true,
		},
		{
			name:    "condition_greater_than_false",
			rule:    makeRule(sqlSensorTempGT50),
			topic:   "sensor/temp",
			payload: []byte(`{"temperature": 30}`),
			want:    false,
		},
		{
			name:    "condition_less_than_true",
			rule:    makeRule(sqlSensorTempLT20),
			topic:   "sensor/temp",
			payload: []byte(`{"temperature": 10}`),
			want:    true,
		},
		{
			name:    "condition_equals_string_true",
			rule:    makeRule(sqlDeviceStatusEqAct),
			topic:   "device/status",
			payload: []byte(`{"status": "active"}`),
			want:    true,
		},
		{
			name:    "condition_equals_string_false",
			rule:    makeRule(sqlDeviceStatusEqAct),
			topic:   "device/status",
			payload: []byte(`{"status": "inactive"}`),
			want:    false,
		},
		{
			name:    "disabled_rule",
			rule:    &iot.TopicRule{RuleName: "Disabled", SQL: sqlAllHash, Enabled: false},
			topic:   "any/topic",
			payload: []byte(`{}`),
			want:    false,
		},
		{
			name:    "nil_rule",
			rule:    nil,
			topic:   "any/topic",
			payload: []byte(`{}`),
			want:    false,
		},
		{
			name:    "invalid_json_payload",
			rule:    makeRule(sqlSensorTempGT50),
			topic:   "sensor/temp",
			payload: []byte(`not-json`),
			want:    false,
		},
		{
			name:    "gte_condition_true",
			rule:    makeRule(sqlSensorValueGTE100),
			topic:   "sensor/x",
			payload: []byte(`{"value": 100}`),
			want:    true,
		},
		{
			name:    "not_equals_condition",
			rule:    makeRule(sqlSensorStatusNeqOff),
			topic:   "sensor/x",
			payload: []byte(`{"status": "on"}`),
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := iot.EvaluateRule(tt.rule, tt.topic, tt.payload)
			assert.Equal(t, tt.want, got)
		})
	}
}
