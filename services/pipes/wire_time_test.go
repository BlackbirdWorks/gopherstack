package pipes_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestKinesisStreamSourceParameters_UnmarshalJSON_TimeIsEpochSeconds proves
// StartingPositionTimestamp parses the AWS restjson1 wire format an epoch-
// seconds JSON number, not an RFC3339 string, and rejects the latter rather
// than silently misparsing it.
func TestKinesisStreamSourceParameters_UnmarshalJSON_TimeIsEpochSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTime time.Time
		name     string
		body     string
		wantNil  bool
		wantErr  bool
	}{
		{
			name:     "whole-second epoch",
			body:     `{"StartingPositionTimestamp":1700000000,"StartingPosition":"AT_TIMESTAMP"}`,
			wantTime: time.Unix(1_700_000_000, 0).UTC(),
		},
		{
			name:     "fractional-second epoch",
			body:     `{"StartingPositionTimestamp":1700000000.5,"StartingPosition":"AT_TIMESTAMP"}`,
			wantTime: time.Unix(1_700_000_000, 500_000_000).UTC(),
		},
		{
			name:    "absent timestamp leaves nil",
			body:    `{"StartingPosition":"LATEST"}`,
			wantNil: true,
		},
		{
			name:    "RFC3339 string is rejected, not silently misparsed",
			body:    `{"StartingPositionTimestamp":"2024-01-01T00:00:00Z","StartingPosition":"AT_TIMESTAMP"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var params pipes.KinesisStreamSourceParameters
			err := json.Unmarshal([]byte(tt.body), &params)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, params.StartingPositionTimestamp)

				return
			}

			require.NotNil(t, params.StartingPositionTimestamp)
			assert.True(t, params.StartingPositionTimestamp.Equal(tt.wantTime),
				"got %s, want %s", params.StartingPositionTimestamp, tt.wantTime)
		})
	}
}

func newTestPipesClient(t *testing.T, h *pipes.Handler) *pipessdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return pipessdk.NewFromConfig(cfg, func(o *pipessdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreatePipe_KinesisStartingPositionTimestamp_RealClient proves
// CreatePipe accepts a Kinesis source's StartingPositionTimestamp through a
// real AWS SDK client. aws-sdk-go-v2's restjson1 serializer
// (serializers.go:1903-1905) encodes this member as
// ok.Double(smithytime.FormatEpochSeconds(*v.StartingPositionTimestamp)) -- a
// JSON number -- and encoding/json's default time.Time.UnmarshalJSON only
// accepts a quoted RFC3339 string, rejecting the whole CreatePipe body
// ("Time.UnmarshalJSON: input is not a JSON string") for any real client
// that sets this field. DescribePipe round-trips the same value back,
// proving the fix also covers response encoding (the real client's
// deserializer expects the same epoch-seconds number on the way out;
// deserializers.go:4988-4996). UpdatePipe is not exercised here:
// types.UpdatePipeSourceKinesisStreamParameters has no
// StartingPositionTimestamp member at all, so no real client can send it on
// UpdatePipe regardless of this fix.
func TestCreatePipe_KinesisStartingPositionTimestamp_RealClient(t *testing.T) {
	t.Parallel()

	h := pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestPipesClient(t, h)

	wantTime := time.Unix(1_700_000_000, 0).UTC()

	created, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
		Name:    aws.String("kinesis-at-timestamp-pipe"),
		RoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
		Source:  aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/s"),
		Target:  aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
		SourceParameters: &pipestypes.PipeSourceParameters{
			KinesisStreamParameters: &pipestypes.PipeSourceKinesisStreamParameters{
				StartingPosition:          pipestypes.KinesisStreamStartPositionAtTimestamp,
				StartingPositionTimestamp: aws.Time(wantTime),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Name)

	described, err := client.DescribePipe(t.Context(), &pipessdk.DescribePipeInput{
		Name: aws.String("kinesis-at-timestamp-pipe"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.SourceParameters.KinesisStreamParameters.StartingPositionTimestamp)
	assert.WithinDuration(t, wantTime, *described.SourceParameters.KinesisStreamParameters.StartingPositionTimestamp, 0)
}
