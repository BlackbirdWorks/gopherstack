package kinesis_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

type inspectEnv struct {
	client *kinesissdk.Client
	clock  *fakeClock
	url    string
}

func newInspectEnv(t *testing.T) *inspectEnv {
	t.Helper()

	clock := newFakeClock(time.Now())
	h := kinesis.NewHandler(kinesis.NewInMemoryBackend().WithClock(clock.Now))

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	h.RegisterInspectionRoutes(e)

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &inspectEnv{
		url:   srv.URL,
		clock: clock,
		client: kinesissdk.NewFromConfig(cfg, func(o *kinesissdk.Options) {
			o.BaseEndpoint = aws.String(srv.URL)
			o.HTTPClient = sdktest.PlainBodyHTTPClient()
		}),
	}
}

func (e *inspectEnv) get(t *testing.T, path string, out any) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.url+path, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if out != nil && resp.StatusCode == http.StatusOK {
		require.NoError(t, json.Unmarshal(body, out))
	}

	return resp.StatusCode
}

func (e *inspectEnv) createStream(t *testing.T, name string, shards int32) {
	t.Helper()

	_, err := e.client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(name), ShardCount: aws.Int32(shards),
	})
	require.NoError(t, err)
	e.clock.Advance(streamSettleWait)
}

func (e *inspectEnv) putRecords(t *testing.T, name string, n int) {
	t.Helper()

	for i := range n {
		_, err := e.client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
			StreamName:   aws.String(name),
			PartitionKey: aws.String("pk"),
			Data:         []byte("rec-" + strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}
}

type inspectShardsResp struct {
	StreamName string `json:"StreamName"`
	Status     string `json:"StreamStatus"`
	Shards     []struct {
		HashKeyRange struct {
			Start string `json:"StartingHashKey"`
			End   string `json:"EndingHashKey"`
		} `json:"HashKeyRange"`
		SequenceNumberRange struct {
			Start string `json:"StartingSequenceNumber"`
			End   string `json:"EndingSequenceNumber"`
		} `json:"SequenceNumberRange"`
		ShardID               string   `json:"ShardId"`
		ParentShardID         string   `json:"ParentShardId"`
		AdjacentParentShardID string   `json:"AdjacentParentShardId"`
		ChildShardIDs         []string `json:"ChildShardIds"`
		RecordCount           int      `json:"RecordCount"`
		Closed                bool     `json:"Closed"`
	} `json:"Shards"`
}

func TestInspectShards_PutSplitMerge(t *testing.T) {
	t.Parallel()

	env := newInspectEnv(t)
	env.createStream(t, "s", 1)
	env.putRecords(t, "s", 3)

	var resp inspectShardsResp
	require.Equal(t, http.StatusOK, env.get(t, "/_gopherstack/kinesis/streams/s/shards", &resp))
	require.Len(t, resp.Shards, 1)
	assert.Equal(t, "s", resp.StreamName)
	assert.Equal(t, "ACTIVE", resp.Status)
	assert.Equal(t, 3, resp.Shards[0].RecordCount)
	assert.False(t, resp.Shards[0].Closed)
	assert.Equal(t, "0", resp.Shards[0].HashKeyRange.Start)
	assert.NotEmpty(t, resp.Shards[0].SequenceNumberRange.Start)
	assert.Empty(t, resp.Shards[0].SequenceNumberRange.End)

	_, err := env.client.SplitShard(t.Context(), &kinesissdk.SplitShardInput{
		StreamName:         aws.String("s"),
		ShardToSplit:       aws.String("shardId-000000000000"),
		NewStartingHashKey: aws.String("170141183460469231731687303715884105728"),
	})
	require.NoError(t, err)
	env.clock.Advance(streamSettleWait)

	resp = inspectShardsResp{}
	require.Equal(t, http.StatusOK, env.get(t, "/_gopherstack/kinesis/streams/s/shards", &resp))
	require.Len(t, resp.Shards, 3)
	assert.True(t, resp.Shards[0].Closed)
	assert.NotEmpty(t, resp.Shards[0].SequenceNumberRange.End)
	assert.Equal(t, []string{"shardId-000000000001", "shardId-000000000002"}, resp.Shards[0].ChildShardIDs)
	assert.Equal(t, "shardId-000000000000", resp.Shards[1].ParentShardID)
	assert.Equal(t, "170141183460469231731687303715884105728", resp.Shards[2].HashKeyRange.Start)
	assert.False(t, resp.Shards[2].Closed)

	_, err = env.client.MergeShards(t.Context(), &kinesissdk.MergeShardsInput{
		StreamName:           aws.String("s"),
		ShardToMerge:         aws.String("shardId-000000000001"),
		AdjacentShardToMerge: aws.String("shardId-000000000002"),
	})
	require.NoError(t, err)
	env.clock.Advance(streamSettleWait)

	resp = inspectShardsResp{}
	require.Equal(t, http.StatusOK, env.get(t, "/_gopherstack/kinesis/streams/s/shards", &resp))
	require.Len(t, resp.Shards, 4)
	assert.Equal(t, []string{"shardId-000000000003"}, resp.Shards[1].ChildShardIDs)
	assert.Equal(t, "shardId-000000000001", resp.Shards[3].ParentShardID)
	assert.Equal(t, "shardId-000000000002", resp.Shards[3].AdjacentParentShardID)
	assert.False(t, resp.Shards[3].Closed)
	assert.True(t, resp.Shards[1].Closed)
	assert.True(t, resp.Shards[2].Closed)
}

func TestInspectShardRecords(t *testing.T) {
	t.Parallel()

	env := newInspectEnv(t)
	env.createStream(t, "s", 1)
	env.putRecords(t, "s", 5)

	type recordsResp struct {
		ShardID string `json:"ShardId"`
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
			PartitionKey   string `json:"PartitionKey"`
			Data           []byte `json:"Data"`
		} `json:"Records"`
		Count int `json:"Count"`
	}

	var all recordsResp
	require.Equal(
		t,
		http.StatusOK,
		env.get(t, "/_gopherstack/kinesis/streams/s/shards/shardId-000000000000/records", &all),
	)
	require.Equal(t, 5, all.Count)

	shardID := "shardId-000000000000"
	base := "/_gopherstack/kinesis/streams/s/shards/" + shardID + "/records"

	tests := []struct {
		name       string
		path       string
		wantFirst  string
		wantStatus int
		wantCount  int
	}{
		{name: "all", path: base, wantStatus: http.StatusOK, wantCount: 5, wantFirst: "rec-0"},
		{name: "limit", path: base + "?limit=2", wantStatus: http.StatusOK, wantCount: 2, wantFirst: "rec-0"},
		{
			name: "from", path: base + "?from=" + url.QueryEscape(all.Records[3].SequenceNumber),
			wantStatus: http.StatusOK, wantCount: 2, wantFirst: "rec-3",
		},
		{name: "limit zero", path: base + "?limit=0", wantStatus: http.StatusBadRequest},
		{name: "limit junk", path: base + "?limit=abc", wantStatus: http.StatusBadRequest},
		{
			name: "unknown shard", path: "/_gopherstack/kinesis/streams/s/shards/shardId-000000000009/records",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "unknown stream", path: "/_gopherstack/kinesis/streams/nope/shards/" + shardID + "/records",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got recordsResp

			require.Equal(t, tc.wantStatus, env.get(t, tc.path, &got))

			if tc.wantStatus != http.StatusOK {
				return
			}

			require.Len(t, got.Records, tc.wantCount)
			assert.Equal(t, tc.wantCount, got.Count)
			assert.Equal(t, tc.wantFirst, string(got.Records[0].Data))
		})
	}

	t.Run("peek does not consume", func(t *testing.T) {
		t.Parallel()

		var again recordsResp

		require.Equal(t, http.StatusOK, env.get(t, base, &again))
		assert.Equal(t, 5, again.Count)
	})
}

func TestInspectShards_UnknownStream(t *testing.T) {
	t.Parallel()

	env := newInspectEnv(t)

	assert.Equal(t, http.StatusNotFound, env.get(t, "/_gopherstack/kinesis/streams/nope/shards", nil))
}

func TestInspectConsumer(t *testing.T) {
	t.Parallel()

	env := newInspectEnv(t)
	env.createStream(t, "s", 1)

	desc, err := env.client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
		StreamName: aws.String("s"),
	})
	require.NoError(t, err)

	reg, err := env.client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
		StreamARN: desc.StreamDescriptionSummary.StreamARN, ConsumerName: aws.String("c"),
	})
	require.NoError(t, err)

	consumerARN := aws.ToString(reg.Consumer.ConsumerARN)

	type consumerResp struct {
		ConsumerName   string `json:"ConsumerName"`
		ConsumerARN    string `json:"ConsumerARN"`
		ConsumerStatus string `json:"ConsumerStatus"`
		StreamARN      string `json:"StreamARN"`
		Subscriptions  []struct {
			ShardID      string `json:"ShardId"`
			StartingType string `json:"StartingPositionType"`
		} `json:"Subscriptions"`
	}

	var got consumerResp

	require.Equal(t, http.StatusOK, env.get(t, "/_gopherstack/kinesis/consumers/"+consumerARN, &got))
	assert.Equal(t, "c", got.ConsumerName)
	assert.Equal(t, consumerARN, got.ConsumerARN)
	assert.Equal(t, "ACTIVE", got.ConsumerStatus)
	assert.Equal(t, aws.ToString(desc.StreamDescriptionSummary.StreamARN), got.StreamARN)
	assert.Empty(t, got.Subscriptions)

	assert.Equal(t, http.StatusNotFound, env.get(t, "/_gopherstack/kinesis/consumers/"+consumerARN+"x", nil))
	assert.Equal(t, http.StatusNotFound, env.get(t, "/_gopherstack/kinesis/consumers/garbage", nil))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	out, err := env.client.SubscribeToShard(ctx, &kinesissdk.SubscribeToShardInput{
		ConsumerARN:      aws.String(consumerARN),
		ShardId:          aws.String("shardId-000000000000"),
		StartingPosition: &kinesissdktypes.StartingPosition{Type: kinesissdktypes.ShardIteratorTypeTrimHorizon},
	})
	require.NoError(t, err)

	defer out.GetStream().Close()

	require.Eventually(t, func() bool {
		var live consumerResp

		return env.get(t, "/_gopherstack/kinesis/consumers/"+consumerARN, &live) == http.StatusOK &&
			len(live.Subscriptions) == 1 &&
			live.Subscriptions[0].ShardID == "shardId-000000000000" &&
			live.Subscriptions[0].StartingType == "TRIM_HORIZON"
	}, 5*time.Second, 20*time.Millisecond)

	cancel()

	require.Eventually(t, func() bool {
		var live consumerResp

		return env.get(t, "/_gopherstack/kinesis/consumers/"+consumerARN, &live) == http.StatusOK &&
			len(live.Subscriptions) == 0
	}, 5*time.Second, 20*time.Millisecond)
}
