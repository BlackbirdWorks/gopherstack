package mediatailor_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestCreateProgram_RejectsUnknownReferences reproduces gopherstack-vdrs item
// 2's validation gap: CreateProgram's required SourceLocationName (and the
// VodSourceName/LiveSourceName it names) was never checked against the
// backend's sourceLocations/vodSources/liveSources tables, unlike every
// other Create* op in this service (CreateVodSource/CreateLiveSource both
// reject an unknown SourceLocationName; see vod_sources.go/live_sources.go).
// A CreateProgram naming a source location, VOD source, or live source that
// does not exist reported success instead of NotFoundException.
func TestCreateProgram_RejectsUnknownReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *mediatailorsdk.Client)
		name        string
		vodSource   string
		liveSource  string
		sourceLoc   string
		programName string
	}{
		{
			name:        "unknown_source_location",
			setup:       func(_ *testing.T, _ *mediatailorsdk.Client) {},
			sourceLoc:   "no-such-source-location",
			vodSource:   "irrelevant-vod",
			programName: "prog-bad-sl",
		},
		{
			name: "unknown_vod_source_under_real_source_location",
			setup: func(t *testing.T, client *mediatailorsdk.Client) {
				t.Helper()

				_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String("sl-validation"),
					HttpConfiguration: &types.HttpConfiguration{
						BaseUrl: aws.String("https://example.com"),
					},
				})
				require.NoError(t, err)
			},
			sourceLoc:   "sl-validation",
			vodSource:   "no-such-vod-source",
			programName: "prog-bad-vod",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
			client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

			_, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
				ChannelName:  aws.String("channel-validation"),
				PlaybackMode: types.PlaybackModeLoop,
				Outputs: []types.RequestOutputItem{
					{ManifestName: aws.String("index"), SourceGroup: aws.String("default")},
				},
			})
			require.NoError(t, err)

			tc.setup(t, client)

			_, err = client.CreateProgram(t.Context(), &mediatailorsdk.CreateProgramInput{
				ChannelName:        aws.String("channel-validation"),
				ProgramName:        aws.String(tc.programName),
				SourceLocationName: aws.String(tc.sourceLoc),
				VodSourceName:      aws.String(tc.vodSource),
				ScheduleConfiguration: &types.ScheduleConfiguration{
					Transition: &types.Transition{
						Type:                     aws.String("ABSOLUTE"),
						RelativePosition:         types.RelativePositionAfterProgram,
						ScheduledStartTimeMillis: aws.Int64(time.Now().UnixMilli()),
						DurationMillis:           aws.Int64(30000),
					},
				},
			})
			require.Error(t, err, "CreateProgram must reject a reference to a resource that does not exist")

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "NotFoundException", apiErr.ErrorCode())
		})
	}
}
