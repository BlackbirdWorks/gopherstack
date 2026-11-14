package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListAnalyticsDataLakeDataSets(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "datalake")

	all, err := f.client.ListAnalyticsDataLakeDataSets(t.Context(),
		&sdk.ListAnalyticsDataLakeDataSetsInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)

	ids := make([]string, 0, len(all.Results))
	for _, r := range all.Results {
		ids = append(ids, aws.ToString(r.DataSetId))
		assert.Equal(t, aws.ToString(r.DataSetId), aws.ToString(r.DataSetName))
	}

	assert.Len(t, ids, 22)
	assert.Contains(t, ids, "contact_record")
	assert.Contains(t, ids, "contact_lens_conversational_analytics")

	first, err := f.client.ListAnalyticsDataLakeDataSets(t.Context(),
		&sdk.ListAnalyticsDataLakeDataSetsInput{InstanceId: aws.String(inst), MaxResults: aws.Int32(5)})
	require.NoError(t, err)
	assert.Len(t, first.Results, 5)
	require.NotNil(t, first.NextToken)

	rest, err := f.client.ListAnalyticsDataLakeDataSets(t.Context(), &sdk.ListAnalyticsDataLakeDataSetsInput{
		InstanceId: aws.String(inst), MaxResults: aws.Int32(100), NextToken: first.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, rest.Results, 17)

	_, err = f.client.ListAnalyticsDataLakeDataSets(t.Context(),
		&sdk.ListAnalyticsDataLakeDataSetsInput{InstanceId: aws.String("ghost")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.ListAnalyticsDataLakeDataSets(t.Context(),
		&sdk.ListAnalyticsDataLakeDataSetsInput{InstanceId: aws.String(inst), MaxResults: aws.Int32(1001)})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func fullAnalyticsConfig(rules types.Behavior) *types.AnalyticsConfiguration {
	return &types.AnalyticsConfiguration{
		LanguageConfiguration:  &types.LanguageConfiguration{LanguageLocale: aws.String("en-US")},
		SentimentConfiguration: &types.SentimentConfiguration{Behavior: types.BehaviorEnable},
		RulesConfiguration:     &types.RulesConfiguration{Behavior: rules},
		RedactionConfiguration: &types.RedactionConfiguration{
			Behavior: types.BehaviorDisable, Policy: types.PolicyNone, MaskMode: types.MaskModePii,
		},
		SummaryConfiguration: &types.SummaryConfiguration{
			SummaryModes: []types.SummaryMode{types.SummaryModePostContact},
		},
	}
}

func TestStartContactConversationalAnalyticsJob(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	voice := e.outbound(t, "+442071230001", true)
	arn := e.contactARN(t, voice)
	e.stop(t, voice)

	e.f.backend.SetResolvers(fakeResolvers{
		objects: map[string]bool{"rec/call.wav": true},
		data:    map[string][]byte{"rec/call.wav": []byte("RIFF")},
	})

	noRecording := e.outbound(t, "+442071230002", true)
	e.stop(t, noRecording)

	task := e.task(t)

	start := func(contact string, mut func(*sdk.StartContactConversationalAnalyticsJobInput)) error {
		in := &sdk.StartContactConversationalAnalyticsJobInput{
			InstanceId: aws.String(e.inst), ContactId: aws.String(contact),
			AnalyticsModes:         []types.AnalyticsMode{types.AnalyticsModePostContact},
			AnalyticsConfiguration: fullAnalyticsConfig(types.BehaviorEnable),
		}
		if mut != nil {
			mut(in)
		}

		_, err := e.f.client.StartContactConversationalAnalyticsJob(t.Context(), in)

		return err
	}

	assert.Equal(t, "InvalidRequestException", apiCode(start(voice, nil)), "no recording yet")

	_, err := e.f.client.CreateAttachedFile(t.Context(), &sdk.CreateAttachedFileInput{
		InstanceId: aws.String(e.inst), AssociatedResourceArn: aws.String(arn),
		FileSourceUri: aws.String("s3://rec/call.wav"), FileUseCaseType: types.FileUseCaseTypeVoiceRecording,
	})
	require.NoError(t, err)

	tests := []struct {
		mut     func(*sdk.StartContactConversationalAnalyticsJobInput)
		name    string
		contact string
		code    string
	}{
		{name: "ok", contact: voice},
		{name: "again", contact: voice},
		{name: "unknown", contact: "ghost", code: "ResourceNotFoundException"},
		{name: "norecording", contact: noRecording, code: "InvalidRequestException"},
		{name: "notvoice", contact: task, code: "InvalidRequestException"},
		{
			name: "mode", contact: voice, code: "InvalidRequestException",
			mut: func(in *sdk.StartContactConversationalAnalyticsJobInput) {
				in.AnalyticsModes = []types.AnalyticsMode{types.AnalyticsModeRealTime}
			},
		},
		{
			name: "badbehavior", contact: voice, code: "InvalidRequestException",
			mut: func(in *sdk.StartContactConversationalAnalyticsJobInput) {
				in.AnalyticsConfiguration = fullAnalyticsConfig("Maybe")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.code, apiCode(start(tt.contact, tt.mut)))
		})
	}

	t.Run("persisted", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, start(voice, nil))
		assert.Contains(t, string(e.f.handler.Snapshot(t.Context())), "analyticsJobs")
	})
}
