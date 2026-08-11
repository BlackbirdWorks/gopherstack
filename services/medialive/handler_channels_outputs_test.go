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

// TestChannel_OutputSettings drives real CreateChannel/DescribeChannel round
// trips through the real medialive SDK client, verifying the
// EncoderOutput.OutputSettings / OutputGroup.OutputGroupSettings unions
// gopherstack-hj9n added -- see interfaces.go for the modeled field lists.
func TestChannel_OutputSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *medialivesdk.Client)
		name string
	}{
		{
			name: "hls group+output settings round-trip destination/cdn/keyProvider/captions and m3u8Settings",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					HlsGroupSettings: &types.HlsGroupSettings{
						Destination:   &types.OutputLocationRef{DestinationRefId: aws.String("dest1")},
						Mode:          types.HlsModeLive,
						SegmentLength: aws.Int32(6),
						HlsCdnSettings: &types.HlsCdnSettings{
							HlsS3Settings: &types.HlsS3Settings{CannedAcl: types.S3CannedAclBucketOwnerFullControl},
						},
						KeyProviderSettings: &types.KeyProviderSettings{
							StaticKeySettings: &types.StaticKeySettings{
								StaticKeyValue:    aws.String("0123456789abcdef0123456789abcdef"),
								KeyProviderServer: &types.InputLocation{Uri: aws.String("https://keys.example.com")},
							},
						},
						CaptionLanguageMappings: []types.CaptionLanguageMapping{
							{
								CaptionChannel:      aws.Int32(1),
								LanguageCode:        aws.String("eng"),
								LanguageDescription: aws.String("English"),
							},
						},
					},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					HlsOutputSettings: &types.HlsOutputSettings{
						NameModifier:      aws.String("-hls"),
						H265PackagingType: types.HlsH265PackagingTypeHvc1,
						HlsSettings: &types.HlsSettings{
							StandardHlsSettings: &types.StandardHlsSettings{
								AudioRenditionSets: aws.String("audio-group-1"),
								M3u8Settings: &types.M3u8Settings{
									PcrPid:     aws.String("0x1e0"),
									ProgramNum: aws.Int32(1),
									Scte35Pid:  aws.String("0x1f0"),
								},
							},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-hls-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)
				require.Len(t, desc.EncoderSettings.OutputGroups, 1)

				g := desc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, g.OutputGroupSettings)
				require.NotNil(t, g.OutputGroupSettings.HlsGroupSettings)
				hgs := g.OutputGroupSettings.HlsGroupSettings
				assert.Equal(t, "dest1", aws.ToString(hgs.Destination.DestinationRefId))
				assert.Equal(t, types.HlsModeLive, hgs.Mode)
				assert.Equal(t, int32(6), aws.ToInt32(hgs.SegmentLength))
				require.NotNil(t, hgs.HlsCdnSettings)
				require.NotNil(t, hgs.HlsCdnSettings.HlsS3Settings)
				assert.Equal(t, types.S3CannedAclBucketOwnerFullControl, hgs.HlsCdnSettings.HlsS3Settings.CannedAcl)
				require.NotNil(t, hgs.KeyProviderSettings)
				require.NotNil(t, hgs.KeyProviderSettings.StaticKeySettings)
				sk := hgs.KeyProviderSettings.StaticKeySettings
				assert.Equal(t, "0123456789abcdef0123456789abcdef", aws.ToString(sk.StaticKeyValue))
				require.NotNil(t, sk.KeyProviderServer)
				assert.Equal(t, "https://keys.example.com", aws.ToString(sk.KeyProviderServer.Uri))
				require.Len(t, hgs.CaptionLanguageMappings, 1)
				assert.Equal(t, "eng", aws.ToString(hgs.CaptionLanguageMappings[0].LanguageCode))

				require.Len(t, g.Outputs, 1)
				require.NotNil(t, g.Outputs[0].OutputSettings)
				require.NotNil(t, g.Outputs[0].OutputSettings.HlsOutputSettings)
				hos := g.Outputs[0].OutputSettings.HlsOutputSettings
				assert.Equal(t, "-hls", aws.ToString(hos.NameModifier))
				assert.Equal(t, types.HlsH265PackagingTypeHvc1, hos.H265PackagingType)
				require.NotNil(t, hos.HlsSettings)
				require.NotNil(t, hos.HlsSettings.StandardHlsSettings)
				std := hos.HlsSettings.StandardHlsSettings
				assert.Equal(t, "audio-group-1", aws.ToString(std.AudioRenditionSets))
				require.NotNil(t, std.M3u8Settings)
				assert.Equal(t, "0x1e0", aws.ToString(std.M3u8Settings.PcrPid))
				assert.Equal(t, int32(1), aws.ToInt32(std.M3u8Settings.ProgramNum))
			},
		},
		{
			name: "archive group+output settings round-trip m2tsSettings/dvbNitSettings and the rawSettings empty marker",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					ArchiveGroupSettings: &types.ArchiveGroupSettings{
						Destination:      &types.OutputLocationRef{DestinationRefId: aws.String("archive-dest")},
						RolloverInterval: aws.Int32(300),
						ArchiveCdnSettings: &types.ArchiveCdnSettings{
							ArchiveS3Settings: &types.ArchiveS3Settings{
								CannedAcl: types.S3CannedAclBucketOwnerFullControl,
							},
						},
					},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					ArchiveOutputSettings: &types.ArchiveOutputSettings{
						NameModifier: aws.String("-archive"),
						ContainerSettings: &types.ArchiveContainerSettings{
							M2tsSettings: &types.M2tsSettings{
								TransportStreamId: aws.Int32(7),
								VideoPid:          aws.String("0x100"),
								DvbNitSettings: &types.DvbNitSettings{
									NetworkId:   aws.Int32(42),
									NetworkName: aws.String("gopherstack-net"),
								},
							},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-archive-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				g := desc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, g.OutputGroupSettings.ArchiveGroupSettings)
				ags := g.OutputGroupSettings.ArchiveGroupSettings
				assert.Equal(t, "archive-dest", aws.ToString(ags.Destination.DestinationRefId))
				assert.Equal(t, int32(300), aws.ToInt32(ags.RolloverInterval))
				require.NotNil(t, ags.ArchiveCdnSettings)
				require.NotNil(t, ags.ArchiveCdnSettings.ArchiveS3Settings)
				assert.Equal(
					t,
					types.S3CannedAclBucketOwnerFullControl,
					ags.ArchiveCdnSettings.ArchiveS3Settings.CannedAcl,
				)

				aos := g.Outputs[0].OutputSettings.ArchiveOutputSettings
				require.NotNil(t, aos)
				assert.Equal(t, "-archive", aws.ToString(aos.NameModifier))
				require.NotNil(t, aos.ContainerSettings)
				require.NotNil(t, aos.ContainerSettings.M2tsSettings)
				m2ts := aos.ContainerSettings.M2tsSettings
				assert.Equal(t, int32(7), aws.ToInt32(m2ts.TransportStreamId))
				assert.Equal(t, "0x100", aws.ToString(m2ts.VideoPid))
				require.NotNil(t, m2ts.DvbNitSettings)
				assert.Equal(t, int32(42), aws.ToInt32(m2ts.DvbNitSettings.NetworkId))
				assert.Equal(t, "gopherstack-net", aws.ToString(m2ts.DvbNitSettings.NetworkName))

				// rawSettings is an empty-marker variant -- must survive as a
				// non-nil pointer, not vanish under omitempty (gopherstack-sthr's
				// PassThroughSettings bug class).
				esRaw := minimalValidEncoderSettings()
				esRaw.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					ArchiveOutputSettings: &types.ArchiveOutputSettings{
						ContainerSettings: &types.ArchiveContainerSettings{RawSettings: &types.RawSettings{}},
					},
				}

				rawOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-archive-raw"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esRaw,
				})
				require.NoError(t, err)

				rawDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: rawOut.Channel.Id},
				)
				require.NoError(t, err)
				rawOutput := rawDesc.EncoderSettings.OutputGroups[0].Outputs[0].OutputSettings.ArchiveOutputSettings
				assert.NotNil(
					t,
					rawOutput.ContainerSettings.RawSettings,
					"rawSettings empty marker must not vanish under omitempty",
				)
			},
		},
		{
			name: "multiplexGroupSettings empty marker survives, and multiplex output settings round-trip",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					MultiplexGroupSettings: &types.MultiplexGroupSettings{},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					MultiplexOutputSettings: &types.MultiplexOutputSettings{
						Destination: &types.OutputLocationRef{DestinationRefId: aws.String("mux-dest")},
						ContainerSettings: &types.MultiplexContainerSettings{
							MultiplexM2tsSettings: &types.MultiplexM2tsSettings{
								Klv:       types.M2tsKlvPassthrough,
								PcrPeriod: aws.Int32(40),
							},
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-multiplex-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				g := desc.EncoderSettings.OutputGroups[0]
				assert.NotNil(
					t,
					g.OutputGroupSettings.MultiplexGroupSettings,
					"multiplexGroupSettings empty marker must not vanish",
				)

				mos := g.Outputs[0].OutputSettings.MultiplexOutputSettings
				require.NotNil(t, mos)
				assert.Equal(t, "mux-dest", aws.ToString(mos.Destination.DestinationRefId))
				require.NotNil(t, mos.ContainerSettings)
				require.NotNil(t, mos.ContainerSettings.MultiplexM2tsSettings)
				assert.Equal(t, types.M2tsKlvPassthrough, mos.ContainerSettings.MultiplexM2tsSettings.Klv)
				assert.Equal(t, int32(40), aws.ToInt32(mos.ContainerSettings.MultiplexM2tsSettings.PcrPeriod))
			},
		},
		{
			name: "mediaPackage group+output settings round-trip mediapackageV2GroupSettings and V2 destination settings",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					MediaPackageGroupSettings: &types.MediaPackageGroupSettings{
						Destination: &types.OutputLocationRef{DestinationRefId: aws.String("mp-dest")},
						MediapackageV2GroupSettings: &types.MediaPackageV2GroupSettings{
							Scte35Type:    types.Scte35TypeScte35WithoutSegmentation,
							SegmentLength: aws.Int32(4),
						},
					},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					MediaPackageOutputSettings: &types.MediaPackageOutputSettings{
						MediaPackageV2DestinationSettings: &types.MediaPackageV2DestinationSettings{
							AudioGroupId:  aws.String("audio-1"),
							HlsAutoSelect: types.HlsAutoSelectYes,
						},
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-mediapackage-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				g := desc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, g.OutputGroupSettings.MediaPackageGroupSettings)
				mpg := g.OutputGroupSettings.MediaPackageGroupSettings
				assert.Equal(t, "mp-dest", aws.ToString(mpg.Destination.DestinationRefId))
				require.NotNil(t, mpg.MediapackageV2GroupSettings)
				assert.Equal(t, types.Scte35TypeScte35WithoutSegmentation, mpg.MediapackageV2GroupSettings.Scte35Type)
				assert.Equal(t, int32(4), aws.ToInt32(mpg.MediapackageV2GroupSettings.SegmentLength))

				mpo := g.Outputs[0].OutputSettings.MediaPackageOutputSettings
				require.NotNil(t, mpo)
				require.NotNil(t, mpo.MediaPackageV2DestinationSettings)
				assert.Equal(t, "audio-1", aws.ToString(mpo.MediaPackageV2DestinationSettings.AudioGroupId))
				assert.Equal(t, types.HlsAutoSelectYes, mpo.MediaPackageV2DestinationSettings.HlsAutoSelect)
			},
		},
		{
			name: "rtmp/srt/udp group+output settings round-trip",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					RtmpGroupSettings: &types.RtmpGroupSettings{
						AuthenticationScheme: types.AuthenticationSchemeAkamai,
						CacheFullBehavior:    types.RtmpCacheFullBehaviorDisconnectImmediately,
						CacheLength:          aws.Int32(30),
					},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					RtmpOutputSettings: &types.RtmpOutputSettings{
						Destination:     &types.OutputLocationRef{DestinationRefId: aws.String("rtmp-dest")},
						CertificateMode: types.RtmpOutputCertificateModeVerifyAuthenticity,
						NumRetries:      aws.Int32(3),
					},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-rtmp-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				g := desc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, g.OutputGroupSettings.RtmpGroupSettings)
				assert.Equal(
					t,
					types.AuthenticationSchemeAkamai,
					g.OutputGroupSettings.RtmpGroupSettings.AuthenticationScheme,
				)
				assert.Equal(t, int32(30), aws.ToInt32(g.OutputGroupSettings.RtmpGroupSettings.CacheLength))

				ros := g.Outputs[0].OutputSettings.RtmpOutputSettings
				require.NotNil(t, ros)
				assert.Equal(t, "rtmp-dest", aws.ToString(ros.Destination.DestinationRefId))
				assert.Equal(t, types.RtmpOutputCertificateModeVerifyAuthenticity, ros.CertificateMode)
				assert.Equal(t, int32(3), aws.ToInt32(ros.NumRetries))

				esSrtUDP := minimalValidEncoderSettings()
				esSrtUDP.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					SrtGroupSettings: &types.SrtGroupSettings{
						InputLossAction: types.InputLossActionForUdpOutDropProgram,
					},
				}
				esSrtUDP.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					SrtOutputSettings: &types.SrtOutputSettings{
						Destination: &types.OutputLocationRef{DestinationRefId: aws.String("srt-dest")},
						ContainerSettings: &types.UdpContainerSettings{
							M2tsSettings: &types.M2tsSettings{VideoPid: aws.String("0x200")},
						},
						EncryptionType: types.SrtEncryptionTypeAes256,
						Latency:        aws.Int32(120),
					},
				}

				srtOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-srt-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esSrtUDP,
				})
				require.NoError(t, err)

				srtDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: srtOut.Channel.Id},
				)
				require.NoError(t, err)

				sg := srtDesc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, sg.OutputGroupSettings.SrtGroupSettings)
				assert.Equal(
					t,
					types.InputLossActionForUdpOutDropProgram,
					sg.OutputGroupSettings.SrtGroupSettings.InputLossAction,
				)

				sos := sg.Outputs[0].OutputSettings.SrtOutputSettings
				require.NotNil(t, sos)
				assert.Equal(t, "srt-dest", aws.ToString(sos.Destination.DestinationRefId))
				assert.Equal(t, types.SrtEncryptionTypeAes256, sos.EncryptionType)
				assert.Equal(t, int32(120), aws.ToInt32(sos.Latency))
				require.NotNil(t, sos.ContainerSettings)
				require.NotNil(t, sos.ContainerSettings.M2tsSettings)
				assert.Equal(t, "0x200", aws.ToString(sos.ContainerSettings.M2tsSettings.VideoPid))
			},
		},
		{
			name: "cmafIngest/frameCapture/mediaConnectRouter/msSmooth group+output settings round-trip",
			run: func(t *testing.T, client *medialivesdk.Client) {
				t.Helper()

				es := minimalValidEncoderSettings()
				es.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					CmafIngestGroupSettings: &types.CmafIngestGroupSettings{
						Destination:        &types.OutputLocationRef{DestinationRefId: aws.String("cmaf-dest")},
						Id3Behavior:        types.CmafId3BehaviorEnabled,
						SegmentLengthUnits: types.CmafIngestSegmentLengthUnitsSeconds,
						SegmentLength:      aws.Int32(2),
						CaptionLanguageMappings: []types.CmafIngestCaptionLanguageMapping{
							{CaptionChannel: aws.Int32(1), LanguageCode: aws.String("fra")},
						},
					},
				}
				es.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					CmafIngestOutputSettings: &types.CmafIngestOutputSettings{NameModifier: aws.String("-cmaf")},
				}

				out, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-cmaf-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: es,
				})
				require.NoError(t, err)

				desc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: out.Channel.Id},
				)
				require.NoError(t, err)

				g := desc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, g.OutputGroupSettings.CmafIngestGroupSettings)
				cgs := g.OutputGroupSettings.CmafIngestGroupSettings
				assert.Equal(t, "cmaf-dest", aws.ToString(cgs.Destination.DestinationRefId))
				assert.Equal(t, types.CmafId3BehaviorEnabled, cgs.Id3Behavior)
				assert.Equal(t, int32(2), aws.ToInt32(cgs.SegmentLength))
				require.Len(t, cgs.CaptionLanguageMappings, 1)
				assert.Equal(t, "fra", aws.ToString(cgs.CaptionLanguageMappings[0].LanguageCode))

				cos := g.Outputs[0].OutputSettings.CmafIngestOutputSettings
				require.NotNil(t, cos)
				assert.Equal(t, "-cmaf", aws.ToString(cos.NameModifier))

				esFcMc := minimalValidEncoderSettings()
				esFcMc.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					FrameCaptureGroupSettings: &types.FrameCaptureGroupSettings{
						Destination: &types.OutputLocationRef{DestinationRefId: aws.String("fc-dest")},
						FrameCaptureCdnSettings: &types.FrameCaptureCdnSettings{
							FrameCaptureS3Settings: &types.FrameCaptureS3Settings{
								CannedAcl: types.S3CannedAclBucketOwnerFullControl,
							},
						},
					},
				}
				esFcMc.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					FrameCaptureOutputSettings: &types.FrameCaptureOutputSettings{NameModifier: aws.String("-fc")},
				}

				fcOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-fc-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esFcMc,
				})
				require.NoError(t, err)

				fcDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: fcOut.Channel.Id},
				)
				require.NoError(t, err)

				fg := fcDesc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, fg.OutputGroupSettings.FrameCaptureGroupSettings)
				assert.Equal(
					t,
					"fc-dest",
					aws.ToString(fg.OutputGroupSettings.FrameCaptureGroupSettings.Destination.DestinationRefId),
				)
				require.NotNil(
					t,
					fg.OutputGroupSettings.FrameCaptureGroupSettings.FrameCaptureCdnSettings.FrameCaptureS3Settings,
				)
				assert.Equal(
					t,
					types.S3CannedAclBucketOwnerFullControl,
					fg.OutputGroupSettings.FrameCaptureGroupSettings.FrameCaptureCdnSettings.FrameCaptureS3Settings.CannedAcl,
				)

				fco := fg.Outputs[0].OutputSettings.FrameCaptureOutputSettings
				require.NotNil(t, fco)
				assert.Equal(t, "-fc", aws.ToString(fco.NameModifier))

				esMc := minimalValidEncoderSettings()
				esMc.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					MediaConnectRouterGroupSettings: &types.MediaConnectRouterGroupSettings{
						AvailabilityZones: []string{"us-east-1a", "us-east-1b"},
					},
				}
				esMc.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					MediaConnectRouterOutputSettings: &types.MediaConnectRouterOutputSettings{
						Destination: &types.OutputLocationRef{DestinationRefId: aws.String("mcr-dest")},
						ContainerSettings: &types.MediaConnectRouterContainerSettings{
							M2tsSettings: &types.M2tsSettings{VideoPid: aws.String("0x300")},
						},
					},
				}

				mcOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-mcr-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esMc,
				})
				require.NoError(t, err)

				mcDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: mcOut.Channel.Id},
				)
				require.NoError(t, err)

				mg := mcDesc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, mg.OutputGroupSettings.MediaConnectRouterGroupSettings)
				assert.ElementsMatch(
					t,
					[]string{"us-east-1a", "us-east-1b"},
					mg.OutputGroupSettings.MediaConnectRouterGroupSettings.AvailabilityZones,
				)

				mco := mg.Outputs[0].OutputSettings.MediaConnectRouterOutputSettings
				require.NotNil(t, mco)
				assert.Equal(t, "mcr-dest", aws.ToString(mco.Destination.DestinationRefId))
				require.NotNil(t, mco.ContainerSettings)
				require.NotNil(t, mco.ContainerSettings.M2tsSettings)
				assert.Equal(t, "0x300", aws.ToString(mco.ContainerSettings.M2tsSettings.VideoPid))

				esMs := minimalValidEncoderSettings()
				esMs.OutputGroups[0].OutputGroupSettings = &types.OutputGroupSettings{
					MsSmoothGroupSettings: &types.MsSmoothGroupSettings{
						Destination:        &types.OutputLocationRef{DestinationRefId: aws.String("mss-dest")},
						CertificateMode:    types.SmoothGroupCertificateModeVerifyAuthenticity,
						EventIdMode:        types.SmoothGroupEventIdModeUseConfigured,
						AcquisitionPointId: aws.String("acq-mss"),
					},
				}
				esMs.OutputGroups[0].Outputs[0].OutputSettings = &types.OutputSettings{
					MsSmoothOutputSettings: &types.MsSmoothOutputSettings{
						NameModifier:      aws.String("-mss"),
						H265PackagingType: types.MsSmoothH265PackagingTypeHvc1,
					},
				}

				msOut, err := client.CreateChannel(t.Context(), &medialivesdk.CreateChannelInput{
					Name: aws.String("rt-mss-outputs"), ChannelClass: types.ChannelClassStandard,
					EncoderSettings: esMs,
				})
				require.NoError(t, err)

				msDesc, err := client.DescribeChannel(
					t.Context(),
					&medialivesdk.DescribeChannelInput{ChannelId: msOut.Channel.Id},
				)
				require.NoError(t, err)

				mssg := msDesc.EncoderSettings.OutputGroups[0]
				require.NotNil(t, mssg.OutputGroupSettings.MsSmoothGroupSettings)
				assert.Equal(
					t,
					"mss-dest",
					aws.ToString(mssg.OutputGroupSettings.MsSmoothGroupSettings.Destination.DestinationRefId),
				)
				assert.Equal(
					t,
					"acq-mss",
					aws.ToString(mssg.OutputGroupSettings.MsSmoothGroupSettings.AcquisitionPointId),
				)
				assert.Equal(
					t,
					types.SmoothGroupEventIdModeUseConfigured,
					mssg.OutputGroupSettings.MsSmoothGroupSettings.EventIdMode,
				)

				mso := mssg.Outputs[0].OutputSettings.MsSmoothOutputSettings
				require.NotNil(t, mso)
				assert.Equal(t, "-mss", aws.ToString(mso.NameModifier))
				assert.Equal(t, types.MsSmoothH265PackagingTypeHvc1, mso.H265PackagingType)
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
