package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestPutFunction_ConfigFields_RoundTrip verifies CustomOutputConfiguration,
// HttpRequestConfiguration and SequentialExecutorConfiguration survive a
// real PutFunction -> GetFunction round trip through the aws-sdk-go-v2
// client. Before this fix, GetFunctionOutput/PutFunctionOutput never
// emitted any of these three members despite all being real, sometimes
// required-by-FunctionType, fields on both real *Output types
// (mediatailor@v1.63.4's deserializers.go: awsRestjson1_deserializeOpDocument
// GetFunctionOutput/PutFunctionOutput both list "CustomOutputConfiguration",
// "HttpRequestConfiguration", "SequentialExecutorConfiguration" as cases) --
// a real client always got nil for all three, on every function, regardless
// of FunctionType.
func TestPutFunction_ConfigFields_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("custom output", func(t *testing.T) {
		t.Parallel()

		backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
		client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

		_, err := client.PutFunction(t.Context(), &mediatailorsdk.PutFunctionInput{
			FunctionId:   aws.String("fn-custom-output"),
			FunctionType: types.FunctionTypeCustomOutput,
			CustomOutputConfiguration: &types.CustomOutputConfiguration{
				Runtime: types.RuntimeTypeJsonata,
				Output:  map[string]string{"player_params.device_type": "$.device.type"},
			},
		})
		require.NoError(t, err)

		out, err := client.GetFunction(t.Context(), &mediatailorsdk.GetFunctionInput{
			FunctionId: aws.String("fn-custom-output"),
		})
		require.NoError(t, err)

		require.NotNil(t, out.CustomOutputConfiguration)
		assert.Equal(t, types.RuntimeTypeJsonata, out.CustomOutputConfiguration.Runtime)
		assert.Equal(t, "$.device.type", out.CustomOutputConfiguration.Output["player_params.device_type"])
		assert.Nil(t, out.HttpRequestConfiguration)
		assert.Nil(t, out.SequentialExecutorConfiguration)
	})

	t.Run("http request", func(t *testing.T) {
		t.Parallel()

		backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
		client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

		_, err := client.PutFunction(t.Context(), &mediatailorsdk.PutFunctionInput{
			FunctionId:   aws.String("fn-http-request"),
			FunctionType: types.FunctionTypeHttpRequest,
			HttpRequestConfiguration: &types.HttpRequestConfiguration{
				MethodType:                 types.MethodTypePost,
				RequestTimeoutMilliseconds: aws.Int32(1500),
				Runtime:                    types.RuntimeTypeJsonata,
				Url:                        aws.String("https://example.com/decision"),
				Body:                       aws.String("{%device.type%}"),
				Headers:                    map[string]string{"X-Client": "gopherstack"},
			},
		})
		require.NoError(t, err)

		out, err := client.GetFunction(t.Context(), &mediatailorsdk.GetFunctionInput{
			FunctionId: aws.String("fn-http-request"),
		})
		require.NoError(t, err)

		require.NotNil(t, out.HttpRequestConfiguration)
		hrc := out.HttpRequestConfiguration
		assert.Equal(t, types.MethodTypePost, hrc.MethodType)
		assert.Equal(t, int32(1500), aws.ToInt32(hrc.RequestTimeoutMilliseconds))
		assert.Equal(t, "https://example.com/decision", aws.ToString(hrc.Url))
		assert.Equal(t, "gopherstack", hrc.Headers["X-Client"])
		assert.Nil(t, out.CustomOutputConfiguration)
		assert.Nil(t, out.SequentialExecutorConfiguration)
	})

	t.Run("sequential executor", func(t *testing.T) {
		t.Parallel()

		backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
		client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

		_, err := client.PutFunction(t.Context(), &mediatailorsdk.PutFunctionInput{
			FunctionId:   aws.String("fn-sequential"),
			FunctionType: types.FunctionTypeSequentialExecutor,
			SequentialExecutorConfiguration: &types.SequentialExecutorConfiguration{
				Runtime:             types.RuntimeTypeJsonata,
				TimeoutMilliseconds: aws.Int32(2000),
				FunctionList: []types.FunctionRef{
					{FunctionId: aws.String("fn-step-1")},
					{FunctionId: aws.String("fn-step-2"), RunCondition: aws.String("$.step1.ok")},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.GetFunction(t.Context(), &mediatailorsdk.GetFunctionInput{
			FunctionId: aws.String("fn-sequential"),
		})
		require.NoError(t, err)

		require.NotNil(t, out.SequentialExecutorConfiguration)
		sec := out.SequentialExecutorConfiguration
		assert.Equal(t, int32(2000), aws.ToInt32(sec.TimeoutMilliseconds))
		require.Len(t, sec.FunctionList, 2)
		assert.Equal(t, "fn-step-1", aws.ToString(sec.FunctionList[0].FunctionId))
		assert.Equal(t, "$.step1.ok", aws.ToString(sec.FunctionList[1].RunCondition))
		assert.Nil(t, out.CustomOutputConfiguration)
		assert.Nil(t, out.HttpRequestConfiguration)
	})
}

// TestGetPrefetchSchedule_NoCreationTimeOnWire is a raw-body test: a real
// aws-sdk-go-v2 client can't observe an extra unknown JSON key (its
// deserializeOpDocumentGetPrefetchScheduleOutput switch silently ignores
// anything outside its own case list, matching every other JSON-protocol
// unknown-field default case), so this bug can only be caught below the
// SDK. gopherstack's toPrefetchScheduleOutput emitted a "CreationTime" key
// that has no member on the real GetPrefetchScheduleOutput/
// CreatePrefetchScheduleOutput struct at all (mediatailor@v1.63.4's
// api_op_GetPrefetchSchedule.go declares Arn/Consumption/Name/
// PlaybackConfigurationName/RecurringPrefetchConfiguration/Retrieval/
// ScheduleType/StreamId/Tags only -- no CreationTime member exists on this
// op family, unlike Channel/SourceLocation/VodSource/LiveSource, which
// legitimately have one).
func TestGetPrefetchSchedule_NoCreationTimeOnWire(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestPlaybackConfig(t, h, "pc1")

	createRec := doRequest(t, h, http.MethodPost, "/prefetchSchedule/pc1/ps1", nil)
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	getRec := doRequest(t, h, http.MethodGet, "/prefetchSchedule/pc1/ps1", nil)
	require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.NotContains(
		t, resp, "CreationTime",
		"GetPrefetchScheduleOutput has no CreationTime member on the real API; emitting one is a fabricated field",
	)
}

// TestDescribeVodSource_AdBreakOpportunities_RealSDKClient verifies
// DescribeVodSourceOutput.AdBreakOpportunities -- a real, non-deprecated
// member on the real DescribeVodSourceOutput only (confirmed absent from
// Create/UpdateVodSourceOutput, both diffed separately) -- decodes to a
// non-nil empty slice through the real aws-sdk-go-v2 client instead of
// silently staying nil forever (the field had zero grep hits anywhere in
// this service before this fix).
func TestDescribeVodSource_AdBreakOpportunities_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
		SourceLocationName: aws.String("sl-adbreak"),
		HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
	})
	require.NoError(t, err)

	_, err = client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
		SourceLocationName: aws.String("sl-adbreak"),
		VodSourceName:      aws.String("vs-adbreak"),
		HttpPackageConfigurations: []types.HttpPackageConfiguration{
			{Path: aws.String("/a"), SourceGroup: aws.String("g"), Type: types.TypeHls},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeVodSource(t.Context(), &mediatailorsdk.DescribeVodSourceInput{
		SourceLocationName: aws.String("sl-adbreak"),
		VodSourceName:      aws.String("vs-adbreak"),
	})
	require.NoError(t, err)

	assert.NotNil(
		t, out.AdBreakOpportunities,
		"AdBreakOpportunities must decode to a non-nil (empty) slice, not stay absent",
	)
	assert.Empty(
		t, out.AdBreakOpportunities,
		"this backend never analyzes VOD manifests, so the honest value is empty, never fabricated",
	)
}

// TestCreateChannel_NoLogConfigurationOnWire is a raw-body test, for the
// same reason as TestGetPrefetchSchedule_NoCreationTimeOnWire: a real
// client can't observe an extra unknown JSON key. toChannelOutput is
// shared by CreateChannel, DescribeChannel and UpdateChannel, but
// LogConfiguration is a real, required member on DescribeChannelOutput
// ONLY -- CreateChannelOutput and UpdateChannelOutput (both diffed
// separately against mediatailor@v1.63.4's own deserializers) have no such
// member at all. Before this fix, CreateChannel/UpdateChannel always wired
// a fabricated LogConfiguration onto the response.
func TestCreateChannel_NoLogConfigurationOnWire(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, "/channel/ch-logconfig", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	assert.NotContains(
		t, createResp, "LogConfiguration",
		"CreateChannelOutput has no LogConfiguration member on the real API",
	)

	updateRec := doRequest(t, h, http.MethodPut, "/channel/ch-logconfig", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.NotContains(
		t, updateResp, "LogConfiguration",
		"UpdateChannelOutput has no LogConfiguration member on the real API",
	)

	describeRec := doRequest(t, h, http.MethodGet, "/channel/ch-logconfig", nil)
	require.Equal(t, http.StatusOK, describeRec.Code, describeRec.Body.String())

	var describeResp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &describeResp))
	assert.Contains(
		t, describeResp, "LogConfiguration",
		"DescribeChannelOutput.LogConfiguration IS required on the real API and must stay present",
	)
}

// TestListChannels_ItemFields_RealSDKClient verifies ListChannelsOutput's
// per-item shape. The real op's Items field is []types.Channel -- the SAME
// full type DescribeChannel returns, not a slimmer summary (confirmed:
// mediatailor@v1.63.4's api_op_ListChannels.go declares `Items
// []types.Channel`). Before this fix, ListChannels emitted only
// ChannelName/Arn/PlaybackMode/ChannelState/Tier/tags per item -- Audiences,
// CreationTime, FillerSlate, LastModifiedTime, LogConfiguration and Outputs
// were all silently dropped despite the backend's ChannelSummary already
// tracking every one of them.
func TestListChannels_ItemFields_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.CreateChannel(t.Context(), &mediatailorsdk.CreateChannelInput{
		ChannelName:  aws.String("ch-list-fields"),
		PlaybackMode: types.PlaybackModeLoop,
		Outputs: []types.RequestOutputItem{
			{ManifestName: aws.String("index"), SourceGroup: aws.String("hd")},
		},
		FillerSlate: &types.SlateSource{
			SourceLocationName: aws.String("sl1"),
			VodSourceName:      aws.String("slate-vod"),
		},
	})
	require.NoError(t, err)

	out, err := client.ListChannels(t.Context(), &mediatailorsdk.ListChannelsInput{})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	item := out.Items[0]
	require.Len(t, item.Outputs, 1, "ListChannels item must include Outputs, matching real types.Channel")
	assert.Equal(t, "index", aws.ToString(item.Outputs[0].ManifestName))
	require.NotNil(t, item.FillerSlate, "ListChannels item must include FillerSlate")
	assert.Equal(t, "slate-vod", aws.ToString(item.FillerSlate.VodSourceName))
	assert.NotNil(t, item.CreationTime, "ListChannels item must include CreationTime")
	assert.NotNil(t, item.LastModifiedTime, "ListChannels item must include LastModifiedTime")
}

// TestListVodSourcesAndListLiveSources_HttpPackageConfigurations verifies
// ListVodSourcesOutput/ListLiveSourcesOutput per-item shape.
// HttpPackageConfigurations is a real member of both types.VodSource and
// types.LiveSource (the same full types Describe returns, confirmed
// against mediatailor@v1.63.4's own deserializers), but was entirely
// absent from both list items despite the backend already tracking it
// (used correctly by DescribeVodSource/DescribeLiveSource on the same
// resource).
func TestListVodSourcesAndListLiveSources_HttpPackageConfigurations(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
		SourceLocationName: aws.String("sl-list-pkg"),
		HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
	})
	require.NoError(t, err)

	pkgCfgs := []types.HttpPackageConfiguration{
		{Path: aws.String("/a"), SourceGroup: aws.String("g"), Type: types.TypeHls},
	}

	_, err = client.CreateVodSource(t.Context(), &mediatailorsdk.CreateVodSourceInput{
		SourceLocationName:        aws.String("sl-list-pkg"),
		VodSourceName:             aws.String("vs-list-pkg"),
		HttpPackageConfigurations: pkgCfgs,
	})
	require.NoError(t, err)

	_, err = client.CreateLiveSource(t.Context(), &mediatailorsdk.CreateLiveSourceInput{
		SourceLocationName:        aws.String("sl-list-pkg"),
		LiveSourceName:            aws.String("ls-list-pkg"),
		HttpPackageConfigurations: pkgCfgs,
	})
	require.NoError(t, err)

	vodOut, err := client.ListVodSources(t.Context(), &mediatailorsdk.ListVodSourcesInput{
		SourceLocationName: aws.String("sl-list-pkg"),
	})
	require.NoError(t, err)
	require.Len(t, vodOut.Items, 1)
	require.Len(
		t, vodOut.Items[0].HttpPackageConfigurations, 1,
		"ListVodSources item must include HttpPackageConfigurations",
	)
	assert.Equal(t, "/a", aws.ToString(vodOut.Items[0].HttpPackageConfigurations[0].Path))

	liveOut, err := client.ListLiveSources(t.Context(), &mediatailorsdk.ListLiveSourcesInput{
		SourceLocationName: aws.String("sl-list-pkg"),
	})
	require.NoError(t, err)
	require.Len(t, liveOut.Items, 1)
	require.Len(
		t, liveOut.Items[0].HttpPackageConfigurations, 1,
		"ListLiveSources item must include HttpPackageConfigurations",
	)
	assert.Equal(t, "/a", aws.ToString(liveOut.Items[0].HttpPackageConfigurations[0].Path))
}

// TestListFunctions_ItemFields_RealSDKClient verifies ListFunctionsOutput's
// per-item shape. The real op's Items field is []types.Function -- the
// SAME full type GetFunction returns (confirmed against
// mediatailor@v1.63.4's api_op_ListFunctions.go), so Description and all
// three FunctionType-specific config blocks belong on every list item too.
// Before this fix, ListFunctions emitted only FunctionId/FunctionType/Arn/
// tags per item.
func TestListFunctions_ItemFields_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.PutFunction(t.Context(), &mediatailorsdk.PutFunctionInput{
		FunctionId:   aws.String("fn-list-fields"),
		FunctionType: types.FunctionTypeCustomOutput,
		Description:  aws.String("list fields test"),
		CustomOutputConfiguration: &types.CustomOutputConfiguration{
			Runtime: types.RuntimeTypeJsonata,
			Output:  map[string]string{"a": "b"},
		},
	})
	require.NoError(t, err)

	out, err := client.ListFunctions(t.Context(), &mediatailorsdk.ListFunctionsInput{})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	item := out.Items[0]
	assert.Equal(t, "list fields test", aws.ToString(item.Description), "ListFunctions item must include Description")
	require.NotNil(
		t, item.CustomOutputConfiguration,
		"ListFunctions item must include CustomOutputConfiguration",
	)
	assert.Equal(t, "b", item.CustomOutputConfiguration.Output["a"])
}

// TestListPlaybackConfigurations_ItemFields_RealSDKClient verifies
// ListPlaybackConfigurationsOutput's per-item shape. The real op's Items
// field is []types.PlaybackConfiguration -- the SAME full type
// GetPlaybackConfiguration returns (confirmed against
// mediatailor@v1.63.4's api_op_ListPlaybackConfigurations.go). Before this
// fix, ListPlaybackConfigurations dropped PlaybackEndpointPrefix,
// SessionInitializationEndpointPrefix and LogConfiguration on every item,
// despite storedPlaybackConfiguration already tracking all three (used
// correctly by GetPlaybackConfiguration on the same resource).
func TestListPlaybackConfigurations_ItemFields_RealSDKClient(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.PutPlaybackConfiguration(t.Context(), &mediatailorsdk.PutPlaybackConfigurationInput{
		Name:                  aws.String("pc-list-fields"),
		AdDecisionServerUrl:   aws.String("https://ads.example.com"),
		VideoContentSourceUrl: aws.String("https://video.example.com"),
	})
	require.NoError(t, err)

	_, err = client.ConfigureLogsForPlaybackConfiguration(
		t.Context(), &mediatailorsdk.ConfigureLogsForPlaybackConfigurationInput{
			PlaybackConfigurationName: aws.String("pc-list-fields"),
			PercentEnabled:            50,
		},
	)
	require.NoError(t, err)

	out, err := client.ListPlaybackConfigurations(t.Context(), &mediatailorsdk.ListPlaybackConfigurationsInput{})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	item := out.Items[0]
	assert.NotEmpty(
		t, aws.ToString(item.PlaybackEndpointPrefix),
		"ListPlaybackConfigurations item must include PlaybackEndpointPrefix",
	)
	assert.NotEmpty(
		t, aws.ToString(item.SessionInitializationEndpointPrefix),
		"ListPlaybackConfigurations item must include SessionInitializationEndpointPrefix",
	)
	require.NotNil(t, item.LogConfiguration, "ListPlaybackConfigurations item must include LogConfiguration")
	assert.Equal(t, int32(50), item.LogConfiguration.PercentEnabled)
}
