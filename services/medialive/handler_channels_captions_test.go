package medialive_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestChannel_CaptionDestinationSettings drives real CreateChannel/
// DescribeChannel round trips through the real medialive SDK client,
// verifying the CaptionDescription.DestinationSettings union gopherstack-1szb
// added -- see interfaces.go for the modeled field lists.
func TestChannel_CaptionDestinationSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *medialivesdk.Client)
		name string
	}{
		{
			name: "burn-in destination settings round-trips font/color/position fields",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.CaptionDescriptions[0].CaptionDashRoles = []types.DashRoleCaption{types.DashRoleCaptionCaption}
				es.CaptionDescriptions[0].DestinationSettings = &types.CaptionDestinationSettings{
					BurnInDestinationSettings: &types.BurnInDestinationSettings{
						Alignment:         types.BurnInAlignmentCentered,
						BackgroundColor:   types.BurnInBackgroundColorBlack,
						BackgroundOpacity: aws.Int32(128),
						Font:              &types.InputLocation{Uri: aws.String("s3://bucket/font.ttf")},
						FontColor:         types.BurnInFontColorWhite,
						XPosition:         aws.Int32(10),
						YPosition:         aws.Int32(20),
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-burnin-caption"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)
				require.Len(t, desc.EncoderSettings.CaptionDescriptions, 1)

				cd := desc.EncoderSettings.CaptionDescriptions[0]
				require.Equal(t, []types.DashRoleCaption{types.DashRoleCaptionCaption}, cd.CaptionDashRoles)
				require.NotNil(t, cd.DestinationSettings)
				require.NotNil(t, cd.DestinationSettings.BurnInDestinationSettings)
				b := cd.DestinationSettings.BurnInDestinationSettings
				assert.Equal(t, types.BurnInAlignmentCentered, b.Alignment)
				assert.Equal(t, types.BurnInBackgroundColorBlack, b.BackgroundColor)
				assert.Equal(t, int32(128), aws.ToInt32(b.BackgroundOpacity))
				require.NotNil(t, b.Font)
				assert.Equal(t, "s3://bucket/font.ttf", aws.ToString(b.Font.Uri))
				assert.Equal(t, types.BurnInFontColorWhite, b.FontColor)
				assert.Equal(t, int32(10), aws.ToInt32(b.XPosition))
				assert.Equal(t, int32(20), aws.ToInt32(b.YPosition))
			},
		},
		{
			name: "dvb-sub destination settings round-trips font/color/position fields",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.CaptionDescriptions[0].DestinationSettings = &types.CaptionDestinationSettings{
					DvbSubDestinationSettings: &types.DvbSubDestinationSettings{
						Alignment:     types.DvbSubDestinationAlignmentLeft,
						ShadowColor:   types.DvbSubDestinationShadowColorWhite,
						ShadowXOffset: aws.Int32(-2),
						ShadowYOffset: aws.Int32(-2),
						SubtitleRows:  types.DvbSubDestinationSubtitleRowsRows16,
						OutlineSize:   aws.Int32(3),
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-dvbsub-caption"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				cd := desc.EncoderSettings.CaptionDescriptions[0]
				require.NotNil(t, cd.DestinationSettings)
				require.NotNil(t, cd.DestinationSettings.DvbSubDestinationSettings)
				d := cd.DestinationSettings.DvbSubDestinationSettings
				assert.Equal(t, types.DvbSubDestinationAlignmentLeft, d.Alignment)
				assert.Equal(t, types.DvbSubDestinationShadowColorWhite, d.ShadowColor)
				assert.Equal(t, int32(-2), aws.ToInt32(d.ShadowXOffset))
				assert.Equal(t, int32(-2), aws.ToInt32(d.ShadowYOffset))
				assert.Equal(t, types.DvbSubDestinationSubtitleRowsRows16, d.SubtitleRows)
				assert.Equal(t, int32(3), aws.ToInt32(d.OutlineSize))
			},
		},
		{
			name: "ebu-tt-d destination settings round-trips style and copyright fields",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.CaptionDescriptions[0].DestinationSettings = &types.CaptionDestinationSettings{
					EbuTtDDestinationSettings: &types.EbuTtDDestinationSettings{
						CopyrightHolder:   aws.String("Example Corp"),
						DefaultFontSize:   aws.Int32(80),
						DefaultLineHeight: aws.Int32(100),
						FillLineGap:       types.EbuTtDFillLineGapControlEnabled,
						FontFamily:        aws.String("monospace"),
						StyleControl:      types.EbuTtDDestinationStyleControlInclude,
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-ebuttd-caption"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				cd := desc.EncoderSettings.CaptionDescriptions[0]
				require.NotNil(t, cd.DestinationSettings)
				require.NotNil(t, cd.DestinationSettings.EbuTtDDestinationSettings)
				e := cd.DestinationSettings.EbuTtDDestinationSettings
				assert.Equal(t, "Example Corp", aws.ToString(e.CopyrightHolder))
				assert.Equal(t, int32(80), aws.ToInt32(e.DefaultFontSize))
				assert.Equal(t, int32(100), aws.ToInt32(e.DefaultLineHeight))
				assert.Equal(t, types.EbuTtDFillLineGapControlEnabled, e.FillLineGap)
				assert.Equal(t, "monospace", aws.ToString(e.FontFamily))
				assert.Equal(t, types.EbuTtDDestinationStyleControlInclude, e.StyleControl)
			},
		},
		{
			name: "ttml and webvtt destination settings round-trip styleControl",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				esTtml := minimalValidEncoderSettings()
				esTtml.CaptionDescriptions[0].DestinationSettings = &types.CaptionDestinationSettings{
					TtmlDestinationSettings: &types.TtmlDestinationSettings{
						StyleControl: types.TtmlDestinationStyleControlPassthrough,
					},
				}

				ttmlOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-ttml-caption"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esTtml,
				})
				require.NoError(t, err)

				ttmlDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: ttmlOut.Channel.Id},
				)
				require.NoError(t, err)
				ttmlCD := ttmlDesc.EncoderSettings.CaptionDescriptions[0]
				require.NotNil(t, ttmlCD.DestinationSettings.TtmlDestinationSettings)
				assert.Equal(
					t,
					types.TtmlDestinationStyleControlPassthrough,
					ttmlCD.DestinationSettings.TtmlDestinationSettings.StyleControl,
				)

				esVtt := minimalValidEncoderSettings()
				esVtt.CaptionDescriptions[0].DestinationSettings = &types.CaptionDestinationSettings{
					WebvttDestinationSettings: &types.WebvttDestinationSettings{
						StyleControl: types.WebvttDestinationStyleControlNoStyleData,
					},
				}

				vttOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-webvtt-caption"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esVtt,
				})
				require.NoError(t, err)

				vttDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: vttOut.Channel.Id},
				)
				require.NoError(t, err)
				vttCD := vttDesc.EncoderSettings.CaptionDescriptions[0]
				require.NotNil(t, vttCD.DestinationSettings.WebvttDestinationSettings)
				assert.Equal(
					t,
					types.WebvttDestinationStyleControlNoStyleData,
					vttCD.DestinationSettings.WebvttDestinationSettings.StyleControl,
				)
			},
		},
		{
			name: "field-less variants survive the round trip as a set-but-empty marker",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				markers := []struct {
					settings *types.CaptionDestinationSettings
					check    func(t *testing.T, s *types.CaptionDestinationSettings)
				}{
					{
						settings: &types.CaptionDestinationSettings{
							AribDestinationSettings: &types.AribDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.AribDestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							EmbeddedDestinationSettings: &types.EmbeddedDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.EmbeddedDestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							EmbeddedPlusScte20DestinationSettings: &types.EmbeddedPlusScte20DestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.EmbeddedPlusScte20DestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							RtmpCaptionInfoDestinationSettings: &types.RtmpCaptionInfoDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.RtmpCaptionInfoDestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							Scte20PlusEmbeddedDestinationSettings: &types.Scte20PlusEmbeddedDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.Scte20PlusEmbeddedDestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							Scte27DestinationSettings: &types.Scte27DestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.Scte27DestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							SmpteTtDestinationSettings: &types.SmpteTtDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.SmpteTtDestinationSettings)
						},
					},
					{
						settings: &types.CaptionDestinationSettings{
							TeletextDestinationSettings: &types.TeletextDestinationSettings{},
						},
						check: func(t *testing.T, s *types.CaptionDestinationSettings) {
							t.Helper()
							assert.NotNil(t, s.TeletextDestinationSettings)
						},
					},
				}

				for i, m := range markers {
					es := minimalValidEncoderSettings()
					es.CaptionDescriptions[0].DestinationSettings = m.settings

					out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
						Name:            aws.String("rt-marker-caption-" + strconv.Itoa(i)),
						ChannelClass:    types.ChannelClassStandard,
						EncoderSettings: es,
					})
					require.NoError(t, err)

					desc, err := client.DescribeChannel(
						t.Context(),
						&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
					)
					require.NoError(t, err)
					require.NotNil(t, desc.EncoderSettings.CaptionDescriptions[0].DestinationSettings)
					m.check(t, desc.EncoderSettings.CaptionDescriptions[0].DestinationSettings)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")
			h := medialive.NewHandler(backend)
			client := newTestChannelClient(t, h)
			tc.run(t, client)
		})
	}
}
