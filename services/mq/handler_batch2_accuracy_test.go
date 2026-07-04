package mq_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// ── DescribeBroker: tags {} not null ─────────────────────────────────────────

func TestMQ_Batch2_DescribeBroker_TagsEmptyNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "no creation-time tags returns tags:{}",
			tags: nil,
		},
		{
			name: "empty creation-time tags map returns tags:{}",
			tags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyMQHandler(t)

			body := map[string]any{
				"brokerName": "notag-broker",
				"engineType": mq.EngineTypeActiveMQ,
			}
			if tt.tags != nil {
				body["tags"] = tt.tags
			}

			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", body)
			require.Equal(t, http.StatusAccepted, rec.Code)
			bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

			rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseAccuracyMQ(t, rec)

			tags, hasTagsKey := resp["tags"]
			assert.True(t, hasTagsKey, "DescribeBroker must include 'tags' key even when empty")
			assert.IsType(t, map[string]any{}, tags, "'tags' must be an object, not null")
			assert.Empty(t, tags, "'tags' must be empty {} not populated")
		})
	}
}

// ── DescribeBroker: users [] not absent ──────────────────────────────────────

func TestMQ_Batch2_DescribeBroker_UsersEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "nouser-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	users, hasUsersKey := resp["users"]
	assert.True(t, hasUsersKey, "DescribeBroker must include 'users' key even when empty")
	assert.IsType(t, []any{}, users, "'users' must be an array, not null")
	assert.Empty(t, users, "'users' must be [] not populated")
}

// ── DescribeConfiguration: tags {} not null ───────────────────────────────────

func TestMQ_Batch2_DescribeConfiguration_TagsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "notag-cfg",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	cfgID := parseAccuracyMQ(t, rec)["id"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations/"+cfgID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	tags, hasTagsKey := resp["tags"]
	assert.True(t, hasTagsKey, "DescribeConfiguration must include 'tags' key even when empty")
	assert.IsType(t, map[string]any{}, tags, "'tags' must be an object, not null")
	assert.Empty(t, tags, "'tags' must be empty {} not populated")
}

// ── DescribeUser: groups [] not null ─────────────────────────────────────────

func TestMQ_Batch2_DescribeUser_GroupsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	bid := createAccuracyBroker(t, h, "groups-check-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		fmt.Sprintf("/v1/brokers/%s/users/alice", bid),
		map[string]any{"password": "ValidPass12!!"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracyMQ(t, h, http.MethodGet,
		fmt.Sprintf("/v1/brokers/%s/users/alice", bid), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	groups, hasGroupsKey := resp["groups"]
	assert.True(t, hasGroupsKey, "DescribeUser must include 'groups' key even when empty")
	assert.IsType(t, []any{}, groups, "'groups' must be an array, not null")
	assert.Empty(t, groups, "'groups' must be [] when user has no groups")
}

// ── DescribeUser: groups populated when set ───────────────────────────────────

func TestMQ_Batch2_DescribeUser_GroupsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	bid := createAccuracyBroker(t, h, "roundtrip-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodPost,
		fmt.Sprintf("/v1/brokers/%s/users/bob", bid),
		map[string]any{
			"password": "ValidPass12!!",
			"groups":   []string{"admin", "ops"},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracyMQ(t, h, http.MethodGet,
		fmt.Sprintf("/v1/brokers/%s/users/bob", bid), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	groups := resp["groups"].([]any)
	require.Len(t, groups, 2)
	assert.Contains(t, groups, "admin")
	assert.Contains(t, groups, "ops")
}

// ── Broker ID shape ───────────────────────────────────────────────────────────

func TestMQ_Batch2_BrokerID_Shape(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "shape-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	// UUID format: 8-4-4-4-12 lowercase hex with dashes = 36 chars total
	assert.Len(t, bid, 36, "broker ID must be UUID format (36 chars)")
}

// ── Broker ARN shape ─────────────────────────────────────────────────────────

func TestMQ_Batch2_BrokerARN_Shape(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("123456789012", "us-east-1")
	h := mq.NewHandler(b)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "arn-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	arn := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	assert.Equal(t, "arn:aws:mq:us-east-1:123456789012:broker:arn-broker", arn)
}

// ── Broker timestamp shape ────────────────────────────────────────────────────

func TestMQ_Batch2_DescribeBroker_CreatedTimestamp(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "ts-broker",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	created, ok := resp["created"].(string)
	require.True(t, ok, "'created' must be a string")
	_, err := time.Parse(time.RFC3339, created)
	assert.NoError(t, err, "'created' must be RFC3339: %q", created)
}

// ── ListBrokers empty state ───────────────────────────────────────────────────

func TestMQ_Batch2_ListBrokers_Empty(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	summaries, hasSummaries := resp["brokerSummaries"]
	assert.True(t, hasSummaries, "ListBrokers must include 'brokerSummaries' key")
	assert.IsType(t, []any{}, summaries, "'brokerSummaries' must be an array")
	assert.Empty(t, summaries, "'brokerSummaries' must be [] when no brokers exist")

	_, hasNext := resp["nextToken"]
	assert.False(t, hasNext, "nextToken must be absent when no results")
}

// ── ListConfigurations empty state ────────────────────────────────────────────

func TestMQ_Batch2_ListConfigurations_Empty(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/configurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	cfgs, hasCfgs := resp["configurations"]
	assert.True(t, hasCfgs, "ListConfigurations must include 'configurations' key")
	assert.IsType(t, []any{}, cfgs, "'configurations' must be an array")
	assert.Empty(t, cfgs, "'configurations' must be [] when none exist")
}

// ── ListUsers empty state ─────────────────────────────────────────────────────

func TestMQ_Batch2_ListUsers_Empty(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	bid := createAccuracyBroker(t, h, "listusers-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid+"/users", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	users, hasUsers := resp["users"]
	assert.True(t, hasUsers, "ListUsers must include 'users' key")
	assert.IsType(t, []any{}, users, "'users' must be an array")
	assert.Empty(t, users, "'users' must be [] when no users exist")
}

// ── ListTags empty state ──────────────────────────────────────────────────────

func TestMQ_Batch2_ListTags_EmptyObject(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	bid := createAccuracyBroker(t, h, "listtags-broker", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	tags, hasTags := resp["tags"]
	assert.True(t, hasTags, "ListTags must include 'tags' key")
	assert.IsType(t, map[string]any{}, tags, "'tags' must be an object, not null")
	assert.Empty(t, tags, "'tags' must be {} when no tags")
}

// ── TagResource merges with creation-time tags ────────────────────────────────

func TestMQ_Batch2_TagResource_MergesWithCreationTimeTags(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("123456789012", "us-east-1")
	h := mq.NewHandler(b)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "merge-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	rec = doAccuracyMQ(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
		map[string]any{"tags": map[string]string{"team": "infra"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseAccuracyMQ(t, rec)["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"], "original tag must persist")
	assert.Equal(t, "infra", tags["team"], "new tag must be added")
}

// ── UntagResource removes only specified key ──────────────────────────────────

func TestMQ_Batch2_UntagResource_RemovesOnlySpecifiedKey(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("123456789012", "us-east-1")
	h := mq.NewHandler(b)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "untag-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"key1": "v1", "key2": "v2"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	rec = doAccuracyMQ(t, h, http.MethodDelete, "/v1/tags/"+brokerARN+"?tagKeys=key1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseAccuracyMQ(t, rec)["tags"].(map[string]any)
	assert.NotContains(t, tags, "key1", "key1 must be removed")
	assert.Equal(t, "v2", tags["key2"], "key2 must remain")
}

// ── Creation-time tags visible via ListTags ───────────────────────────────────

func TestMQ_Batch2_CreationTimeTags_VisibleViaListTags(t *testing.T) {
	t.Parallel()

	b := mq.NewInMemoryBackend("123456789012", "us-east-1")
	h := mq.NewHandler(b)

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": "ctag-broker",
		"engineType": mq.EngineTypeActiveMQ,
		"tags":       map[string]string{"created-with": "yes"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code)
	bid := parseAccuracyMQ(t, rec)["brokerId"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	brokerARN := parseAccuracyMQ(t, rec)["brokerArn"].(string)

	rec = doAccuracyMQ(t, h, http.MethodGet, "/v1/tags/"+brokerARN, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	tags := parseAccuracyMQ(t, rec)["tags"].(map[string]any)
	assert.Equal(t, "yes", tags["created-with"], "creation-time tags must be visible via ListTags")
}

// ── nextToken absent when results fit on one page ─────────────────────────────

func TestMQ_Batch2_ListBrokers_NextTokenAbsentWhenNotTruncated(t *testing.T) {
	t.Parallel()

	h := newAccuracyMQHandler(t)
	createAccuracyBroker(t, h, "broker-a", mq.EngineTypeActiveMQ)
	createAccuracyBroker(t, h, "broker-b", mq.EngineTypeActiveMQ)

	rec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseAccuracyMQ(t, rec)

	_, hasNext := resp["nextToken"]
	assert.False(t, hasNext, "nextToken must be absent when all results fit on one page")
}
