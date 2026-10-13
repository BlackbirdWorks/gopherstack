package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestGetModelCustomizationJob_EndTimeOnCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance bool
	}{
		{name: "in progress", advance: false},
		{name: "completed", advance: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestBedrockClient(t, bedrock.NewHandler(b))

			var jobARN string
			if tt.advance {
				jobARN, _ = newCompletedCustomizationJob(t, b, client, "job", "model")
			} else {
				job, err := createCustomizationJob(b, "job", "model")
				require.NoError(t, err)

				jobARN = job.JobArn
			}

			out, err := client.GetModelCustomizationJob(t.Context(), &bedrocksdk.GetModelCustomizationJobInput{
				JobIdentifier: aws.String(jobARN),
			})
			require.NoError(t, err)

			if tt.advance {
				require.NotNil(t, out.EndTime)
				assert.False(t, out.EndTime.IsZero())
			} else {
				assert.Nil(t, out.EndTime)
			}
		})
	}
}

func TestListGuardrails_IdentifierListsAllVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		identifier   string
		wantVersions []string
	}{
		{name: "no identifier lists drafts", identifier: "", wantVersions: []string{"DRAFT", "DRAFT"}},
		{name: "by id", identifier: "id", wantVersions: []string{"DRAFT", "1", "2"}},
		{name: "by arn", identifier: "arn", wantVersions: []string{"DRAFT", "1", "2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestBedrockClient(t, bedrock.NewHandler(b))

			created, err := client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
				Name:                    aws.String("g"),
				BlockedInputMessaging:   aws.String("no"),
				BlockedOutputsMessaging: aws.String("no"),
			})
			require.NoError(t, err)

			_, err = client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
				Name:                    aws.String("other"),
				BlockedInputMessaging:   aws.String("no"),
				BlockedOutputsMessaging: aws.String("no"),
			})
			require.NoError(t, err)

			for range 2 {
				_, err = client.CreateGuardrailVersion(t.Context(), &bedrocksdk.CreateGuardrailVersionInput{
					GuardrailIdentifier: created.GuardrailId,
				})
				require.NoError(t, err)
			}

			in := &bedrocksdk.ListGuardrailsInput{}

			switch tt.identifier {
			case "id":
				in.GuardrailIdentifier = created.GuardrailId
			case "arn":
				in.GuardrailIdentifier = created.GuardrailArn
			}

			out, err := client.ListGuardrails(t.Context(), in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Guardrails))
			for _, g := range out.Guardrails {
				got = append(got, aws.ToString(g.Version))
			}

			assert.Equal(t, tt.wantVersions, got)
		})
	}
}
