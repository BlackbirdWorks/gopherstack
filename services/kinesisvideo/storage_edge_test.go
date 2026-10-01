package kinesisvideo_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisvideo"
)

func testEdgeConfig(hub string) *types.EdgeConfig {
	return &types.EdgeConfig{
		HubDeviceArn: aws.String(hub),
		RecorderConfig: &types.RecorderConfig{
			MediaSourceConfig: &types.MediaSourceConfig{
				MediaUriSecretArn: aws.String("arn:aws:secretsmanager:us-east-1:123456789012:secret:cam"),
				MediaUriType:      types.MediaUriTypeRtspUri,
			},
			ScheduleConfig: &types.ScheduleConfig{
				ScheduleExpression: aws.String("0 * * * *"),
				DurationInSeconds:  aws.Int32(3600),
			},
		},
		DeletionConfig: &types.DeletionConfig{
			DeleteAfterUpload:    aws.Bool(true),
			EdgeRetentionInHours: aws.Int32(24),
			LocalSizeConfig: &types.LocalSizeConfig{
				MaxLocalMediaSizeInMB: aws.Int32(2048),
				StrategyOnFullSize:    types.StrategyOnFullSizeDeleteOldestMedia,
			},
		},
	}
}

func errCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func TestStreamStorageConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tier        types.DefaultStorageTier
		version     string
		wantTier    types.DefaultStorageTier
		wantErrCode string
	}{
		{name: "warm", tier: types.DefaultStorageTierWarm, wantTier: types.DefaultStorageTierWarm},
		{name: "hot", tier: types.DefaultStorageTierHot, wantTier: types.DefaultStorageTierHot},
		{name: "bad tier", tier: "COLD", wantErrCode: "InvalidArgumentException"},
		{
			name:        "stale version",
			tier:        types.DefaultStorageTierWarm,
			version:     "stale",
			wantErrCode: "VersionMismatchException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("tier")})
			require.NoError(t, err)

			before, err := client.DescribeStreamStorageConfiguration(
				ctx, &kinesisvideosdk.DescribeStreamStorageConfigurationInput{StreamARN: created.StreamARN})
			require.NoError(t, err)
			assert.Equal(t, types.DefaultStorageTierHot, before.StreamStorageConfiguration.DefaultStorageTier)
			assert.Equal(t, "tier", aws.ToString(before.StreamName))

			desc, err := client.DescribeStream(
				ctx,
				&kinesisvideosdk.DescribeStreamInput{StreamName: aws.String("tier")},
			)
			require.NoError(t, err)

			version := aws.ToString(desc.StreamInfo.Version)
			if tt.version != "" {
				version = tt.version
			}

			_, err = client.UpdateStreamStorageConfiguration(
				ctx,
				&kinesisvideosdk.UpdateStreamStorageConfigurationInput{
					StreamName:                 aws.String("tier"),
					CurrentVersion:             aws.String(version),
					StreamStorageConfiguration: &types.StreamStorageConfiguration{DefaultStorageTier: tt.tier},
				},
			)
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, errCode(t, err))

				return
			}

			require.NoError(t, err)

			after, err := client.DescribeStreamStorageConfiguration(
				ctx, &kinesisvideosdk.DescribeStreamStorageConfigurationInput{StreamName: aws.String("tier")})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTier, after.StreamStorageConfiguration.DefaultStorageTier)

			bumped, err := client.DescribeStream(
				ctx,
				&kinesisvideosdk.DescribeStreamInput{StreamName: aws.String("tier")},
			)
			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(desc.StreamInfo.Version), aws.ToString(bumped.StreamInfo.Version))
		})
	}
}

func TestStreamStorageConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeStreamStorageConfiguration(
		t.Context(), &kinesisvideosdk.DescribeStreamStorageConfigurationInput{StreamName: aws.String("nope")})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
}

func TestMediaStorageConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      types.MediaStorageConfigurationStatus
		wantErrCode string
		retention   int32
		useStream   bool
	}{
		{name: "enable", retention: 24, status: types.MediaStorageConfigurationStatusEnabled, useStream: true},
		{name: "disable", status: types.MediaStorageConfigurationStatusDisabled},
		{
			name: "no retention", status: types.MediaStorageConfigurationStatusEnabled, useStream: true,
			wantErrCode: "NoDataRetentionException",
		},
		{
			name: "missing stream", status: types.MediaStorageConfigurationStatusEnabled,
			wantErrCode: "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			ch, err := client.CreateSignalingChannel(
				ctx, &kinesisvideosdk.CreateSignalingChannelInput{ChannelName: aws.String("media-ch")})
			require.NoError(t, err)

			empty, err := client.DescribeMediaStorageConfiguration(
				ctx, &kinesisvideosdk.DescribeMediaStorageConfigurationInput{ChannelName: aws.String("media-ch")})
			require.NoError(t, err)
			assert.Nil(t, empty.MediaStorageConfiguration)

			cfg := &types.MediaStorageConfiguration{Status: tt.status}

			if tt.useStream {
				st, serr := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
					StreamName: aws.String("media-stream"), DataRetentionInHours: aws.Int32(tt.retention),
				})
				require.NoError(t, serr)

				cfg.StreamARN = st.StreamARN
			}

			_, err = client.UpdateMediaStorageConfiguration(ctx, &kinesisvideosdk.UpdateMediaStorageConfigurationInput{
				ChannelARN: ch.ChannelARN, MediaStorageConfiguration: cfg,
			})
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, errCode(t, err))

				return
			}

			require.NoError(t, err)

			got, err := client.DescribeMediaStorageConfiguration(
				ctx, &kinesisvideosdk.DescribeMediaStorageConfigurationInput{ChannelARN: ch.ChannelARN})
			require.NoError(t, err)
			require.NotNil(t, got.MediaStorageConfiguration)
			assert.Equal(t, tt.status, got.MediaStorageConfiguration.Status)
			assert.Equal(t, aws.ToString(cfg.StreamARN), aws.ToString(got.MediaStorageConfiguration.StreamARN))
		})
	}
}

func TestGetSignalingChannelEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantPrefix  map[string]string
		name        string
		role        types.ChannelRole
		wantErrCode string
		protocols   []types.ChannelProtocol
		missing     bool
	}{
		{
			name:      "wss and https",
			protocols: []types.ChannelProtocol{types.ChannelProtocolWss, types.ChannelProtocolHttps},
			role:      types.ChannelRoleMaster,
			wantPrefix: map[string]string{
				"WSS": "wss://", "HTTPS": "https://",
			},
		},
		{
			name:       "webrtc",
			protocols:  []types.ChannelProtocol{types.ChannelProtocolWebrtc},
			role:       types.ChannelRoleViewer,
			wantPrefix: map[string]string{"WEBRTC": ""},
		},
		{name: "unknown channel", missing: true, wantErrCode: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			ch, err := client.CreateSignalingChannel(
				ctx, &kinesisvideosdk.CreateSignalingChannelInput{ChannelName: aws.String("ep-ch")})
			require.NoError(t, err)

			arn := ch.ChannelARN
			if tt.missing {
				arn = aws.String("arn:aws:kinesisvideo:us-east-1:123456789012:channel/gone/1")
			}

			out, err := client.GetSignalingChannelEndpoint(ctx, &kinesisvideosdk.GetSignalingChannelEndpointInput{
				ChannelARN: arn,
				SingleMasterChannelEndpointConfiguration: &types.SingleMasterChannelEndpointConfiguration{
					Protocols: tt.protocols, Role: tt.role,
				},
			})
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, errCode(t, err))

				return
			}

			require.NoError(t, err)
			require.Len(t, out.ResourceEndpointList, len(tt.protocols))

			for _, ep := range out.ResourceEndpointList {
				assert.Contains(t, aws.ToString(ep.ResourceEndpoint), "kinesisvideo.us-east-1.amazonaws.com")
				assert.Greater(t, len(aws.ToString(ep.ResourceEndpoint)), len(tt.wantPrefix[string(ep.Protocol)]))
				assert.Equal(
					t,
					tt.wantPrefix[string(ep.Protocol)],
					aws.ToString(ep.ResourceEndpoint)[:len(tt.wantPrefix[string(ep.Protocol)])],
				)
			}
		})
	}
}

func TestEdgeConfiguration(t *testing.T) {
	t.Parallel()

	const hub = "arn:aws:iot:us-east-1:123456789012:thing/hub"

	tests := []struct {
		cfg         func() *types.EdgeConfig
		name        string
		wantErrCode string
		retention   int32
	}{
		{name: "ok", retention: 24, cfg: func() *types.EdgeConfig { return testEdgeConfig(hub) }},
		{
			name: "no retention", retention: 0, cfg: func() *types.EdgeConfig { return testEdgeConfig(hub) },
			wantErrCode: "NoDataRetentionException",
		},
		{
			name: "bad media uri type", retention: 24,
			cfg: func() *types.EdgeConfig {
				c := testEdgeConfig(hub)
				c.RecorderConfig.MediaSourceConfig.MediaUriType = "BAD"

				return c
			},
			wantErrCode: "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			_, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
				StreamName: aws.String("edge"), DataRetentionInHours: aws.Int32(tt.retention),
			})
			require.NoError(t, err)

			_, err = client.DescribeEdgeConfiguration(
				ctx, &kinesisvideosdk.DescribeEdgeConfigurationInput{StreamName: aws.String("edge")})
			require.Error(t, err)
			assert.Equal(t, "StreamEdgeConfigurationNotFoundException", errCode(t, err))

			started, err := client.StartEdgeConfigurationUpdate(ctx, &kinesisvideosdk.StartEdgeConfigurationUpdateInput{
				StreamName: aws.String("edge"), EdgeConfig: tt.cfg(),
			})
			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErrCode, errCode(t, err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, types.SyncStatusSyncing, started.SyncStatus)
			assert.Equal(t, "edge", aws.ToString(started.StreamName))
			assert.NotNil(t, started.CreationTime)

			got, err := client.DescribeEdgeConfiguration(
				ctx, &kinesisvideosdk.DescribeEdgeConfigurationInput{StreamARN: started.StreamARN})
			require.NoError(t, err)
			assert.Equal(t, hub, aws.ToString(got.EdgeConfig.HubDeviceArn))
			assert.Equal(t, types.MediaUriTypeRtspUri, got.EdgeConfig.RecorderConfig.MediaSourceConfig.MediaUriType)
			assert.EqualValues(t, 3600, aws.ToInt32(got.EdgeConfig.RecorderConfig.ScheduleConfig.DurationInSeconds))
			assert.EqualValues(
				t,
				2048,
				aws.ToInt32(got.EdgeConfig.DeletionConfig.LocalSizeConfig.MaxLocalMediaSizeInMB),
			)
			assert.True(t, aws.ToBool(got.EdgeConfig.DeletionConfig.DeleteAfterUpload))

			_, err = client.DeleteEdgeConfiguration(
				ctx, &kinesisvideosdk.DeleteEdgeConfigurationInput{StreamName: aws.String("edge")})
			require.NoError(t, err)

			_, err = client.DescribeEdgeConfiguration(
				ctx, &kinesisvideosdk.DescribeEdgeConfigurationInput{StreamName: aws.String("edge")})
			require.Error(t, err)
			assert.Equal(t, "StreamEdgeConfigurationNotFoundException", errCode(t, err))

			_, err = client.DeleteEdgeConfiguration(
				ctx, &kinesisvideosdk.DeleteEdgeConfigurationInput{StreamName: aws.String("edge")})
			require.Error(t, err)
			assert.Equal(t, "StreamEdgeConfigurationNotFoundException", errCode(t, err))
		})
	}
}

func TestListEdgeAgentConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hub      string
		want     []string
		maxItems int32
	}{
		{name: "hub a", hub: "hub-a", want: []string{"s1", "s2", "s3"}},
		{name: "hub b", hub: "hub-b", want: []string{"s4"}},
		{name: "no match", hub: "hub-z", want: nil},
		{name: "paged", hub: "hub-a", maxItems: 2, want: []string{"s1", "s2", "s3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			for name, hub := range map[string]string{"s1": "hub-a", "s2": "hub-a", "s3": "hub-a", "s4": "hub-b", "s5": ""} {
				_, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
					StreamName: aws.String(name), DataRetentionInHours: aws.Int32(1),
				})
				require.NoError(t, err)

				if hub == "" {
					continue
				}

				_, err = client.StartEdgeConfigurationUpdate(ctx, &kinesisvideosdk.StartEdgeConfigurationUpdateInput{
					StreamName: aws.String(name), EdgeConfig: testEdgeConfig(hub),
				})
				require.NoError(t, err)
			}

			in := &kinesisvideosdk.ListEdgeAgentConfigurationsInput{HubDeviceArn: aws.String(tt.hub)}
			if tt.maxItems > 0 {
				in.MaxResults = aws.Int32(tt.maxItems)
			}

			var got []string

			for {
				out, err := client.ListEdgeAgentConfigurations(ctx, in)
				require.NoError(t, err)

				for _, e := range out.EdgeConfigs {
					got = append(got, aws.ToString(e.StreamName))
					assert.Equal(t, tt.hub, aws.ToString(e.EdgeConfig.HubDeviceArn))
				}

				if out.NextToken == nil {
					break
				}

				in.NextToken = out.NextToken
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEdgeConfigurationSyncStatus(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := kinesisvideo.NewInMemoryBackend()

		_, err := b.CreateStream("123456789012", testRegion, "sync", "", "", "", "", 1, nil)
		require.NoError(t, err)

		cfg := kinesisvideo.EdgeConfig{
			HubDeviceARN: "hub",
			Recorder: &kinesisvideo.EdgeRecorder{
				MediaSource: &kinesisvideo.EdgeMediaSource{MediaURIType: "RTSP_URI"},
			},
		}

		st, err := b.StartEdgeConfigurationUpdate("sync", "", cfg)
		require.NoError(t, err)
		assert.Equal(t, "SYNCING", st.SyncStatus)

		got, err := b.DescribeEdgeConfiguration("sync", "")
		require.NoError(t, err)
		assert.Equal(t, "SYNCING", got.SyncStatus)

		time.Sleep(3 * time.Second)

		got, err = b.DescribeEdgeConfiguration("sync", "")
		require.NoError(t, err)
		assert.Equal(t, "IN_SYNC", got.SyncStatus)
	})
}

func TestEdgeAndMediaStorageSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{{name: "round trip"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := kinesisvideo.NewInMemoryBackend()

			st, err := b.CreateStream("123456789012", testRegion, "snap", "", "", "", "", 1, nil)
			require.NoError(t, err)

			ch, err := b.CreateSignalingChannel("123456789012", testRegion, "snap-ch", "", 0, nil)
			require.NoError(t, err)

			_, err = b.StartEdgeConfigurationUpdate("snap", "", kinesisvideo.EdgeConfig{
				HubDeviceARN: "hub",
				Recorder: &kinesisvideo.EdgeRecorder{
					MediaSource: &kinesisvideo.EdgeMediaSource{MediaURIType: "FILE_URI"},
				},
			})
			require.NoError(t, err)
			require.NoError(t, b.UpdateMediaStorageConfiguration(
				ch.ARN, kinesisvideo.MediaStorage{Status: "ENABLED", StreamARN: st.ARN}))

			restored := kinesisvideo.NewInMemoryBackend()
			require.NoError(t, restored.Restore(ctx, b.Snapshot(ctx)))

			edge, err := restored.DescribeEdgeConfiguration("snap", "")
			require.NoError(t, err)
			assert.Equal(t, "hub", edge.Config.HubDeviceARN)

			ms, err := restored.DescribeMediaStorageConfiguration("snap-ch", "")
			require.NoError(t, err)
			require.NotNil(t, ms)
			assert.Equal(t, st.ARN, ms.StreamARN)
		})
	}
}
