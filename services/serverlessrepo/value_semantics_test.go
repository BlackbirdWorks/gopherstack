package serverlessrepo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sarsdk "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestApplication_PartialUpdateKeepsOmittedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     *sarsdk.UpdateApplicationInput
		name       string
		wantDesc   string
		wantHome   string
		wantLabels []string
	}{
		{
			name:       "description only",
			update:     &sarsdk.UpdateApplicationInput{Description: aws.String("new")},
			wantDesc:   "new",
			wantHome:   "https://example.com/home",
			wantLabels: []string{"a", "b"},
		},
		{
			name:       "empty labels clears",
			update:     &sarsdk.UpdateApplicationInput{Labels: []string{}},
			wantDesc:   "old",
			wantHome:   "https://example.com/home",
			wantLabels: []string{},
		},
		{
			name: "labels replace",
			update: &sarsdk.UpdateApplicationInput{
				Labels: []string{"c"}, HomePageUrl: aws.String("https://x.test"),
			},
			wantDesc:   "old",
			wantHome:   "https://x.test",
			wantLabels: []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			h := serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1"))
			client, _ := newTestSARSDKClient(t, h)

			created, err := client.CreateApplication(ctx, &sarsdk.CreateApplicationInput{
				Name: aws.String("vs-app"), Author: aws.String("me"), Description: aws.String("old"),
				HomePageUrl: aws.String("https://example.com/home"), Labels: []string{"a", "b"},
				SemanticVersion: aws.String("1.0.0"), TemplateBody: aws.String(`{"Resources":{}}`),
			})
			require.NoError(t, err)

			tt.update.ApplicationId = created.ApplicationId
			_, err = client.UpdateApplication(ctx, tt.update)
			require.NoError(t, err)

			got, err := client.GetApplication(ctx, &sarsdk.GetApplicationInput{ApplicationId: created.ApplicationId})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, aws.ToString(got.Description))
			assert.Equal(t, tt.wantHome, aws.ToString(got.HomePageUrl))
			assert.Equal(t, "me", aws.ToString(got.Author))
			assert.ElementsMatch(t, tt.wantLabels, got.Labels)
			assert.Equal(t, created.CreationTime, got.CreationTime)
			require.NotNil(t, got.Version)
			assert.Equal(t, "1.0.0", aws.ToString(got.Version.SemanticVersion))
		})
	}
}
