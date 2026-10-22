package connect_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	goodVocabulary = "Phrase\tIPA\tSoundsLike\tDisplayAs\nAmazon\t\t\tAmazon\nConnect\t\t\tConnect\n"
	badVocabulary  = "Phrase\tIPA\nAmazon\t\n"
)

func TestVocabularyLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "vocab"))

	created, err := f.client.CreateVocabulary(t.Context(), &sdk.CreateVocabularyInput{
		InstanceId: inst, VocabularyName: aws.String("brands"), LanguageCode: types.VocabularyLanguageCodeEnUs,
		Content: aws.String(goodVocabulary), Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, types.VocabularyStateCreationInProgress, created.State)

	bad, err := f.client.CreateVocabulary(t.Context(), &sdk.CreateVocabularyInput{
		InstanceId: inst, VocabularyName: aws.String("broken"), LanguageCode: types.VocabularyLanguageCodeEnUs,
		Content: aws.String(badVocabulary),
	})
	require.NoError(t, err)

	_, err = f.client.CreateVocabulary(t.Context(), &sdk.CreateVocabularyInput{
		InstanceId: inst, VocabularyName: aws.String("brands"), LanguageCode: types.VocabularyLanguageCodeDeDe,
		Content: aws.String(goodVocabulary),
	})
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.AssociateDefaultVocabulary(t.Context(), &sdk.AssociateDefaultVocabularyInput{
		InstanceId: inst, LanguageCode: types.VocabularyLanguageCodeEnUs, VocabularyId: created.VocabularyId,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "still creating")

	f.clock.advance(2 * time.Second)

	got, err := f.client.DescribeVocabulary(
		t.Context(),
		&sdk.DescribeVocabularyInput{InstanceId: inst, VocabularyId: created.VocabularyId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.VocabularyStateActive, got.Vocabulary.State)
	assert.Equal(t, goodVocabulary, aws.ToString(got.Vocabulary.Content))
	assert.Equal(t, "v", got.Vocabulary.Tags["k"])

	failed, err := f.client.DescribeVocabulary(
		t.Context(),
		&sdk.DescribeVocabularyInput{InstanceId: inst, VocabularyId: bad.VocabularyId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.VocabularyStateCreationFailed, failed.Vocabulary.State)
	assert.NotEmpty(t, aws.ToString(failed.Vocabulary.FailureReason))

	_, err = f.client.AssociateDefaultVocabulary(t.Context(), &sdk.AssociateDefaultVocabularyInput{
		InstanceId: inst, LanguageCode: types.VocabularyLanguageCodeDeDe, VocabularyId: created.VocabularyId,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "language mismatch")

	_, err = f.client.AssociateDefaultVocabulary(t.Context(), &sdk.AssociateDefaultVocabularyInput{
		InstanceId: inst, LanguageCode: types.VocabularyLanguageCodeEnUs, VocabularyId: created.VocabularyId,
	})
	require.NoError(t, err)

	defaults, err := f.client.ListDefaultVocabularies(t.Context(), &sdk.ListDefaultVocabulariesInput{InstanceId: inst})
	require.NoError(t, err)
	require.Len(t, defaults.DefaultVocabularyList, 1)
	assert.Equal(t, "brands", aws.ToString(defaults.DefaultVocabularyList[0].VocabularyName))

	_, err = f.client.DeleteVocabulary(
		t.Context(),
		&sdk.DeleteVocabularyInput{InstanceId: inst, VocabularyId: created.VocabularyId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err), "default vocabulary")

	searchTests := []struct {
		name string
		in   sdk.SearchVocabulariesInput
		want int
	}{
		{name: "all", in: sdk.SearchVocabulariesInput{}, want: 2},
		{name: "prefix", in: sdk.SearchVocabulariesInput{NameStartsWith: aws.String("BRA")}, want: 1},
		{name: "active", in: sdk.SearchVocabulariesInput{State: types.VocabularyStateActive}, want: 1},
		{name: "german", in: sdk.SearchVocabulariesInput{LanguageCode: types.VocabularyLanguageCodeDeDe}, want: 0},
	}
	for _, tt := range searchTests {
		in := tt.in
		in.InstanceId = inst
		out, serr := f.client.SearchVocabularies(t.Context(), &in)
		require.NoError(t, serr, tt.name)
		assert.Len(t, out.VocabularySummaryList, tt.want, tt.name)
	}

	dst := f.restored(t)
	got, err = dst.client.DescribeVocabulary(
		t.Context(),
		&sdk.DescribeVocabularyInput{InstanceId: inst, VocabularyId: created.VocabularyId},
	)
	require.NoError(t, err)
	assert.Equal(t, "brands", aws.ToString(got.Vocabulary.Name))

	_, err = f.client.AssociateDefaultVocabulary(t.Context(), &sdk.AssociateDefaultVocabularyInput{
		InstanceId: inst, LanguageCode: types.VocabularyLanguageCodeEnUs,
	})
	require.NoError(t, err)

	del, err := f.client.DeleteVocabulary(
		t.Context(),
		&sdk.DeleteVocabularyInput{InstanceId: inst, VocabularyId: created.VocabularyId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.VocabularyStateDeleteInProgress, del.State)

	_, err = f.client.DescribeVocabulary(
		t.Context(),
		&sdk.DescribeVocabularyInput{InstanceId: inst, VocabularyId: created.VocabularyId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestVocabularyCreateValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "vocabvalid"))

	tests := []struct {
		name string
		vn   string
		lang types.VocabularyLanguageCode
		body string
	}{
		{name: "bad name", vn: "has space", lang: types.VocabularyLanguageCodeEnUs, body: goodVocabulary},
		{name: "bad language", vn: "ok", lang: "xx-XX", body: goodVocabulary},
		{name: "empty content", vn: "ok", lang: types.VocabularyLanguageCodeEnUs, body: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateVocabulary(t.Context(), &sdk.CreateVocabularyInput{
				InstanceId:     inst,
				VocabularyName: aws.String(tt.vn),
				LanguageCode:   tt.lang,
				Content:        aws.String(tt.body),
			})
			assert.Equal(t, "InvalidParameterException", apiCode(err))
		})
	}
}
