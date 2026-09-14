package mediatailor_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestRealClient_PutPlaybackConfigurationInsertionModeDefault covers
// gopherstack-xhu2t: PutPlaybackConfigurationInput.InsertionMode
// (mediatailor@v1.63.4 serializers.go:3471-3473) was undeclared, so a
// config created without it never got the documented default
// ("The default for players that do not specify an insertion mode is
// stitched") applied -- InsertionMode was simply absent from the response.
func TestRealClient_PutPlaybackConfigurationInsertionModeDefault(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(b))
	ctx := t.Context()

	byDefault, err := client.PutPlaybackConfiguration(ctx, &mediatailorsdk.PutPlaybackConfigurationInput{
		Name: aws.String("slice7-default"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.InsertionModeStitchedOnly, byDefault.InsertionMode)

	got, err := client.GetPlaybackConfiguration(ctx, &mediatailorsdk.GetPlaybackConfigurationInput{
		Name: aws.String("slice7-default"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.InsertionModeStitchedOnly, got.InsertionMode)

	explicit, err := client.PutPlaybackConfiguration(ctx, &mediatailorsdk.PutPlaybackConfigurationInput{
		Name:          aws.String("slice7-explicit"),
		InsertionMode: types.InsertionModePlayerSelect,
	})
	require.NoError(t, err)
	assert.Equal(t, types.InsertionModePlayerSelect, explicit.InsertionMode)
}

// TestRealClient_CreatePrefetchScheduleStreamID proves CreatePrefetchScheduleInput.StreamId
// (a body field) is already correctly read and stored -- confirming the
// gopherstack-99nj hand-decoded-map[string]any blind spot for this field
// (reqfielddiff flagged it, but handler_prefetch_schedules.go already reads
// body["StreamId"] and threads it through).
func TestRealClient_CreatePrefetchScheduleStreamID(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(b))
	ctx := t.Context()

	_, err := client.PutPlaybackConfiguration(ctx, &mediatailorsdk.PutPlaybackConfigurationInput{
		Name: aws.String("slice7-pc"),
	})
	require.NoError(t, err)

	created, err := client.CreatePrefetchSchedule(ctx, &mediatailorsdk.CreatePrefetchScheduleInput{
		PlaybackConfigurationName: aws.String("slice7-pc"),
		Name:                      aws.String("slice7-schedule"),
		StreamId:                  aws.String("slice7-stream-id"),
		Consumption: &types.PrefetchConsumption{
			EndTime: aws.Time(time.Now().Add(time.Hour)),
		},
		Retrieval: &types.PrefetchRetrieval{
			EndTime: aws.Time(time.Now().Add(time.Hour)),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "slice7-stream-id", aws.ToString(created.StreamId))

	got, err := client.GetPrefetchSchedule(ctx, &mediatailorsdk.GetPrefetchScheduleInput{
		PlaybackConfigurationName: aws.String("slice7-pc"),
		Name:                      aws.String("slice7-schedule"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice7-stream-id", aws.ToString(got.StreamId))
}
