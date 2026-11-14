package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnalyticsDataAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "analytics"))

	one, err := f.client.AssociateAnalyticsDataSet(
		t.Context(), &sdk.AssociateAnalyticsDataSetInput{InstanceId: inst, DataSetId: aws.String("contact_record")})
	require.NoError(t, err)
	assert.Equal(t, testAccount, aws.ToString(one.TargetAccountId))
	assert.Contains(t, aws.ToString(one.ResourceShareArn), ":ram:")
	assert.Contains(t, aws.ToString(one.ResourceShareArn), aws.ToString(one.ResourceShareId))

	again, err := f.client.AssociateAnalyticsDataSet(
		t.Context(), &sdk.AssociateAnalyticsDataSetInput{InstanceId: inst, DataSetId: aws.String("contact_record")})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(one.ResourceShareId), aws.ToString(again.ResourceShareId))

	_, err = f.client.AssociateAnalyticsDataSet(t.Context(), &sdk.AssociateAnalyticsDataSetInput{
		InstanceId: inst, DataSetId: aws.String("x"), TargetAccountId: aws.String("short"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	batch, err := f.client.BatchAssociateAnalyticsDataSet(t.Context(), &sdk.BatchAssociateAnalyticsDataSetInput{
		InstanceId: inst, DataSetIds: []string{"a", "b", ""}, TargetAccountId: aws.String("111122223333"),
	})
	require.NoError(t, err)
	assert.Len(t, batch.Created, 2)
	assert.Len(t, batch.Errors, 1)

	all, err := f.client.ListAnalyticsDataAssociations(
		t.Context(),
		&sdk.ListAnalyticsDataAssociationsInput{InstanceId: inst},
	)
	require.NoError(t, err)
	assert.Len(t, all.Results, 3)

	only, err := f.client.ListAnalyticsDataAssociations(
		t.Context(), &sdk.ListAnalyticsDataAssociationsInput{InstanceId: inst, DataSetId: aws.String("a")})
	require.NoError(t, err)
	require.Len(t, only.Results, 1)
	assert.Equal(t, "111122223333", aws.ToString(only.Results[0].TargetAccountId))

	_, err = f.client.DisassociateAnalyticsDataSet(
		t.Context(), &sdk.DisassociateAnalyticsDataSetInput{InstanceId: inst, DataSetId: aws.String("contact_record")})
	require.NoError(t, err)

	_, err = f.client.DisassociateAnalyticsDataSet(
		t.Context(), &sdk.DisassociateAnalyticsDataSetInput{InstanceId: inst, DataSetId: aws.String("contact_record")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	del, err := f.client.BatchDisassociateAnalyticsDataSet(t.Context(), &sdk.BatchDisassociateAnalyticsDataSetInput{
		InstanceId: inst, DataSetIds: []string{"a", "ghost"}, TargetAccountId: aws.String("111122223333"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, del.Deleted)
	require.Len(t, del.Errors, 1)
	assert.Equal(t, "ResourceNotFoundException", aws.ToString(del.Errors[0].ErrorCode))

	_, err = f.client.AssociateAnalyticsDataSet(
		t.Context(), &sdk.AssociateAnalyticsDataSetInput{InstanceId: aws.String("ghost"), DataSetId: aws.String("a")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestGetPromptFile(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetResolvers(fakeResolvers{
		objects: map[string]bool{"audio/hi.wav": true}, data: map[string][]byte{"audio/hi.wav": []byte("RIFFwav")},
	})
	inst := aws.String(f.newInstance(t, "prompts"))

	p, err := f.client.CreatePrompt(t.Context(),
		&sdk.CreatePromptInput{InstanceId: inst, Name: aws.String("hi"), S3Uri: aws.String("s3://audio/hi.wav")})
	require.NoError(t, err)

	got, err := f.client.GetPromptFile(t.Context(), &sdk.GetPromptFileInput{InstanceId: inst, PromptId: p.PromptId})
	require.NoError(t, err)
	assert.NotNil(t, got.LastModifiedTime)

	status, body, _ := httpDo(t, "GET", aws.ToString(got.PromptPresignedUrl), nil)
	assert.Equal(t, 200, status)
	assert.Equal(t, "RIFFwav", string(body))

	status, _, _ = httpDo(t, "GET", aws.ToString(got.PromptPresignedUrl)+"x", nil)
	assert.Equal(t, 403, status)

	_, err = f.client.GetPromptFile(
		t.Context(),
		&sdk.GetPromptFileInput{InstanceId: inst, PromptId: aws.String("nope")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
