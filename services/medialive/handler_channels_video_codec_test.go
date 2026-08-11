package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// TestChannel_VideoCodecSettings drives real CreateChannel/DescribeChannel
// round trips through the real medialive SDK client, verifying
// VideoDescription.CodecSettings gopherstack-1szb added -- see interfaces.go
// for the modeled field lists. This is the last EncoderSettings union.
func TestChannel_VideoCodecSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *medialivesdk.Client)
		name string
	}{
		{
			name: "h264 settings round-trips codec, color space marker, and filter fields",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.VideoDescriptions[0].CodecSettings = &types.VideoCodecSettings{
					H264Settings: &types.H264Settings{
						AdaptiveQuantization: types.H264AdaptiveQuantizationHigh,
						ColorMetadata:        types.H264ColorMetadataInsert,
						Bitrate:              aws.Int32(5000000),
						FramerateDenominator: aws.Int32(1001),
						FramerateNumerator:   aws.Int32(24000),
						GopSize:              aws.Float64(2.5),
						ColorSpaceSettings: &types.H264ColorSpaceSettings{
							Rec709Settings: &types.Rec709Settings{},
						},
						FilterSettings: &types.H264FilterSettings{
							TemporalFilterSettings: &types.TemporalFilterSettings{
								PostFilterSharpening: types.TemporalFilterPostFilterSharpeningEnabled,
								Strength:             types.TemporalFilterStrengthStrength3,
							},
						},
						TimecodeBurninSettings: &types.TimecodeBurninSettings{
							FontSize: types.TimecodeBurninFontSizeMedium32,
							Position: types.TimecodeBurninPositionBottomRight,
							Prefix:   aws.String("EX-"),
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-h264-codec"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)
				require.Len(t, desc.EncoderSettings.VideoDescriptions, 1)

				vd := desc.EncoderSettings.VideoDescriptions[0]
				require.NotNil(t, vd.CodecSettings)
				require.NotNil(t, vd.CodecSettings.H264Settings)
				h := vd.CodecSettings.H264Settings
				assert.Equal(t, types.H264AdaptiveQuantizationHigh, h.AdaptiveQuantization)
				assert.Equal(t, types.H264ColorMetadataInsert, h.ColorMetadata)
				assert.Equal(t, int32(5000000), aws.ToInt32(h.Bitrate))
				assert.Equal(t, int32(1001), aws.ToInt32(h.FramerateDenominator))
				assert.Equal(t, int32(24000), aws.ToInt32(h.FramerateNumerator))
				assert.InDelta(t, 2.5, aws.ToFloat64(h.GopSize), 0.0001)
				require.NotNil(t, h.ColorSpaceSettings)
				assert.NotNil(t, h.ColorSpaceSettings.Rec709Settings)
				require.NotNil(t, h.FilterSettings)
				require.NotNil(t, h.FilterSettings.TemporalFilterSettings)
				assert.Equal(
					t,
					types.TemporalFilterPostFilterSharpeningEnabled,
					h.FilterSettings.TemporalFilterSettings.PostFilterSharpening,
				)
				assert.Equal(t, types.TemporalFilterStrengthStrength3, h.FilterSettings.TemporalFilterSettings.Strength)
				require.NotNil(t, h.TimecodeBurninSettings)
				assert.Equal(t, types.TimecodeBurninFontSizeMedium32, h.TimecodeBurninSettings.FontSize)
				assert.Equal(t, types.TimecodeBurninPositionBottomRight, h.TimecodeBurninSettings.Position)
				assert.Equal(t, "EX-", aws.ToString(h.TimecodeBurninSettings.Prefix))
			},
		},
		{
			name: "h265 settings round-trips hdr10 color space and bandwidth reduction filter",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.VideoDescriptions[0].CodecSettings = &types.VideoCodecSettings{
					H265Settings: &types.H265Settings{
						FramerateDenominator: aws.Int32(1),
						FramerateNumerator:   aws.Int32(30),
						Tier:                 types.H265TierHigh,
						TileHeight:           aws.Int32(20),
						TileWidth:            aws.Int32(20),
						ColorSpaceSettings: &types.H265ColorSpaceSettings{
							Hdr10Settings: &types.Hdr10Settings{
								MaxCll:  aws.Int32(1000),
								MaxFall: aws.Int32(400),
							},
						},
						FilterSettings: &types.H265FilterSettings{
							BandwidthReductionFilterSettings: &types.BandwidthReductionFilterSettings{
								PostFilterSharpening: types.BandwidthReductionPostFilterSharpeningSharpening2,
								Strength:             types.BandwidthReductionFilterStrengthStrength4,
							},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-h265-codec"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				vd := desc.EncoderSettings.VideoDescriptions[0]
				require.NotNil(t, vd.CodecSettings)
				require.NotNil(t, vd.CodecSettings.H265Settings)
				h := vd.CodecSettings.H265Settings
				assert.Equal(t, int32(1), aws.ToInt32(h.FramerateDenominator))
				assert.Equal(t, int32(30), aws.ToInt32(h.FramerateNumerator))
				assert.Equal(t, types.H265TierHigh, h.Tier)
				assert.Equal(t, int32(20), aws.ToInt32(h.TileHeight))
				assert.Equal(t, int32(20), aws.ToInt32(h.TileWidth))
				require.NotNil(t, h.ColorSpaceSettings)
				require.NotNil(t, h.ColorSpaceSettings.Hdr10Settings)
				assert.Equal(t, int32(1000), aws.ToInt32(h.ColorSpaceSettings.Hdr10Settings.MaxCll))
				assert.Equal(t, int32(400), aws.ToInt32(h.ColorSpaceSettings.Hdr10Settings.MaxFall))
				require.NotNil(t, h.FilterSettings)
				require.NotNil(t, h.FilterSettings.BandwidthReductionFilterSettings)
				assert.Equal(
					t,
					types.BandwidthReductionPostFilterSharpeningSharpening2,
					h.FilterSettings.BandwidthReductionFilterSettings.PostFilterSharpening,
				)
				assert.Equal(
					t,
					types.BandwidthReductionFilterStrengthStrength4,
					h.FilterSettings.BandwidthReductionFilterSettings.Strength,
				)
			},
		},
		{
			name: "av1 settings round-trips codec fields and an empty-marker color space variant",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.VideoDescriptions[0].CodecSettings = &types.VideoCodecSettings{
					Av1Settings: &types.Av1Settings{
						FramerateDenominator: aws.Int32(1),
						FramerateNumerator:   aws.Int32(25),
						BitDepth:             types.Av1BitDepthDepth10,
						RateControlMode:      types.Av1RateControlModeQvbr,
						QvbrQualityLevel:     aws.Int32(8),
						ColorSpaceSettings: &types.Av1ColorSpaceSettings{
							Hlg2020Settings: &types.Hlg2020Settings{},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-av1-codec"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				vd := desc.EncoderSettings.VideoDescriptions[0]
				require.NotNil(t, vd.CodecSettings)
				require.NotNil(t, vd.CodecSettings.Av1Settings)
				a := vd.CodecSettings.Av1Settings
				assert.Equal(t, int32(1), aws.ToInt32(a.FramerateDenominator))
				assert.Equal(t, int32(25), aws.ToInt32(a.FramerateNumerator))
				assert.Equal(t, types.Av1BitDepthDepth10, a.BitDepth)
				assert.Equal(t, types.Av1RateControlModeQvbr, a.RateControlMode)
				assert.Equal(t, int32(8), aws.ToInt32(a.QvbrQualityLevel))
				require.NotNil(t, a.ColorSpaceSettings)
				assert.NotNil(t, a.ColorSpaceSettings.Hlg2020Settings)
			},
		},
		{
			name: "mpeg2 settings round-trips codec fields and the shared temporal filter",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.VideoDescriptions[0].CodecSettings = &types.VideoCodecSettings{
					Mpeg2Settings: &types.Mpeg2Settings{
						FramerateDenominator: aws.Int32(1001),
						FramerateNumerator:   aws.Int32(30000),
						ColorSpace:           types.Mpeg2ColorSpacePassthrough,
						ScanType:             types.Mpeg2ScanTypeInterlaced,
						FilterSettings: &types.Mpeg2FilterSettings{
							TemporalFilterSettings: &types.TemporalFilterSettings{
								Strength: types.TemporalFilterStrengthStrength8,
							},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-mpeg2-codec"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				vd := desc.EncoderSettings.VideoDescriptions[0]
				require.NotNil(t, vd.CodecSettings)
				require.NotNil(t, vd.CodecSettings.Mpeg2Settings)
				m := vd.CodecSettings.Mpeg2Settings
				assert.Equal(t, int32(1001), aws.ToInt32(m.FramerateDenominator))
				assert.Equal(t, int32(30000), aws.ToInt32(m.FramerateNumerator))
				assert.Equal(t, types.Mpeg2ColorSpacePassthrough, m.ColorSpace)
				assert.Equal(t, types.Mpeg2ScanTypeInterlaced, m.ScanType)
				require.NotNil(t, m.FilterSettings)
				require.NotNil(t, m.FilterSettings.TemporalFilterSettings)
				assert.Equal(t, types.TemporalFilterStrengthStrength8, m.FilterSettings.TemporalFilterSettings.Strength)
			},
		},
		{
			name: "frame capture settings round-trips capture interval and timecode burn-in",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.VideoDescriptions[0].CodecSettings = &types.VideoCodecSettings{
					FrameCaptureSettings: &types.FrameCaptureSettings{
						CaptureInterval:      aws.Int32(10),
						CaptureIntervalUnits: types.FrameCaptureIntervalUnitSeconds,
						TimecodeBurninSettings: &types.TimecodeBurninSettings{
							FontSize: types.TimecodeBurninFontSizeSmall16,
							Position: types.TimecodeBurninPositionTopLeft,
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-framecapture-codec"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				vd := desc.EncoderSettings.VideoDescriptions[0]
				require.NotNil(t, vd.CodecSettings)
				require.NotNil(t, vd.CodecSettings.FrameCaptureSettings)
				f := vd.CodecSettings.FrameCaptureSettings
				assert.Equal(t, int32(10), aws.ToInt32(f.CaptureInterval))
				assert.Equal(t, types.FrameCaptureIntervalUnitSeconds, f.CaptureIntervalUnits)
				require.NotNil(t, f.TimecodeBurninSettings)
				assert.Equal(t, types.TimecodeBurninFontSizeSmall16, f.TimecodeBurninSettings.FontSize)
				assert.Equal(t, types.TimecodeBurninPositionTopLeft, f.TimecodeBurninSettings.Position)
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
