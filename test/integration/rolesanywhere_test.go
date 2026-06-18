package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	rolesanywheresdk "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"
	rolesanywheretypes "github.com/aws/aws-sdk-go-v2/service/rolesanywhere/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createRolesAnywhereClient returns an IAM Roles Anywhere client pointed at the shared test container.
func createRolesAnywhereClient(t *testing.T) *rolesanywheresdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return rolesanywheresdk.NewFromConfig(cfg, func(o *rolesanywheresdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_RolesAnywhere_TrustAnchorLifecycle drives create→get→list→delete of a
// trust anchor.
func TestIntegration_RolesAnywhere_TrustAnchorLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name   string
		taName string
	}{
		{name: "full_lifecycle", taName: "integ-anchor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRolesAnywhereClient(t)

			createOut, err := client.CreateTrustAnchor(ctx, &rolesanywheresdk.CreateTrustAnchorInput{
				Name: aws.String(tt.taName),
				Source: &rolesanywheretypes.Source{
					SourceType: rolesanywheretypes.TrustAnchorTypeAwsAcmPca,
					SourceData: &rolesanywheretypes.SourceDataMemberAcmPcaArn{
						Value: "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/integ",
					},
				},
			})
			require.NoError(t, err, "CreateTrustAnchor should succeed")
			require.NotNil(t, createOut.TrustAnchor)
			taID := aws.ToString(createOut.TrustAnchor.TrustAnchorId)
			require.NotEmpty(t, taID, "trust anchor id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteTrustAnchor(ctx, &rolesanywheresdk.DeleteTrustAnchorInput{
					TrustAnchorId: aws.String(taID),
				})
			})

			getOut, err := client.GetTrustAnchor(ctx, &rolesanywheresdk.GetTrustAnchorInput{
				TrustAnchorId: aws.String(taID),
			})
			require.NoError(t, err, "GetTrustAnchor should succeed")
			require.NotNil(t, getOut.TrustAnchor)
			assert.Equal(t, tt.taName, aws.ToString(getOut.TrustAnchor.Name))

			listOut, err := client.ListTrustAnchors(ctx, &rolesanywheresdk.ListTrustAnchorsInput{})
			require.NoError(t, err, "ListTrustAnchors should succeed")

			found := false
			for _, ta := range listOut.TrustAnchors {
				if aws.ToString(ta.TrustAnchorId) == taID {
					found = true

					break
				}
			}

			assert.True(t, found, "created trust anchor should appear in list")

			_, err = client.DeleteTrustAnchor(ctx, &rolesanywheresdk.DeleteTrustAnchorInput{
				TrustAnchorId: aws.String(taID),
			})
			require.NoError(t, err, "DeleteTrustAnchor should succeed")
		})
	}
}
