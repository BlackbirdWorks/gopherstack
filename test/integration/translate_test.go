package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	translatesdk "github.com/aws/aws-sdk-go-v2/service/translate"
	translatetypes "github.com/aws/aws-sdk-go-v2/service/translate/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTranslateClient returns a Translate client pointed at the shared test container.
func createTranslateClient(t *testing.T) *translatesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return translatesdk.NewFromConfig(cfg, func(o *translatesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Translate_TranslateText asserts the response shape and that the
// source/target language fields round-trip through the AWS SDK deserialiser.
func TestIntegration_Translate_TranslateText(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		text       string
		sourceLang string
		targetLang string
	}{
		{name: "explicit_source", text: "Hello world", sourceLang: "en", targetLang: "es"},
		{name: "auto_source", text: "Bonjour", sourceLang: "auto", targetLang: "en"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createTranslateClient(t)

			out, err := client.TranslateText(t.Context(), &translatesdk.TranslateTextInput{
				Text:               aws.String(tt.text),
				SourceLanguageCode: aws.String(tt.sourceLang),
				TargetLanguageCode: aws.String(tt.targetLang),
			})
			require.NoError(t, err, "TranslateText should succeed")
			assert.NotEmpty(t, aws.ToString(out.TranslatedText), "translated text must be populated")
			assert.Equal(t, tt.targetLang, aws.ToString(out.TargetLanguageCode))
		})
	}
}

// TestIntegration_Translate_TerminologyLifecycle drives import→get→list→delete of a
// custom terminology resource.
func TestIntegration_Translate_TerminologyLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	const csv = "en,fr\nhello,bonjour\n"

	tests := []struct {
		name     string
		termName string
	}{
		{name: "full_lifecycle", termName: "integ-term"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createTranslateClient(t)

			_, err := client.ImportTerminology(ctx, &translatesdk.ImportTerminologyInput{
				Name:            aws.String(tt.termName),
				MergeStrategy:   translatetypes.MergeStrategyOverwrite,
				TerminologyData: &translatetypes.TerminologyData{File: []byte(csv), Format: translatetypes.TerminologyDataFormatCsv},
			})
			require.NoError(t, err, "ImportTerminology should succeed")

			t.Cleanup(func() {
				_, _ = client.DeleteTerminology(ctx, &translatesdk.DeleteTerminologyInput{Name: aws.String(tt.termName)})
			})

			getOut, err := client.GetTerminology(ctx, &translatesdk.GetTerminologyInput{
				Name:                   aws.String(tt.termName),
				TerminologyDataFormat:  translatetypes.TerminologyDataFormatCsv,
			})
			require.NoError(t, err, "GetTerminology should succeed")
			require.NotNil(t, getOut.TerminologyProperties)
			assert.Equal(t, tt.termName, aws.ToString(getOut.TerminologyProperties.Name))

			listOut, err := client.ListTerminologies(ctx, &translatesdk.ListTerminologiesInput{})
			require.NoError(t, err, "ListTerminologies should succeed")

			found := false
			for _, p := range listOut.TerminologyPropertiesList {
				if aws.ToString(p.Name) == tt.termName {
					found = true

					break
				}
			}

			assert.True(t, found, "imported terminology should appear in list")

			_, err = client.DeleteTerminology(ctx, &translatesdk.DeleteTerminologyInput{Name: aws.String(tt.termName)})
			require.NoError(t, err, "DeleteTerminology should succeed")
		})
	}
}
