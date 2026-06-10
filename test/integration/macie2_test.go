package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMacie2Client returns a Macie2 client pointed at the shared test container.
func createMacie2Client(t *testing.T) *macie2sdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return macie2sdk.NewFromConfig(cfg, func(o *macie2sdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_Macie2_CustomDataIdentifierLifecycle drives create→get→list→delete of a
// custom data identifier, asserting the configured regex round-trips.
func TestIntegration_Macie2_CustomDataIdentifierLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name  string
		cdiID string
		regex string
	}{
		{name: "ssn_pattern", cdiID: "integ-cdi", regex: `\d{3}-\d{2}-\d{4}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMacie2Client(t)

			createOut, err := client.CreateCustomDataIdentifier(ctx, &macie2sdk.CreateCustomDataIdentifierInput{
				Name:  aws.String(tt.cdiID),
				Regex: aws.String(tt.regex),
			})
			require.NoError(t, err, "CreateCustomDataIdentifier should succeed")
			id := aws.ToString(createOut.CustomDataIdentifierId)
			require.NotEmpty(t, id, "custom data identifier id must be returned")

			t.Cleanup(func() {
				_, _ = client.DeleteCustomDataIdentifier(
					ctx,
					&macie2sdk.DeleteCustomDataIdentifierInput{Id: aws.String(id)},
				)
			})

			getOut, err := client.GetCustomDataIdentifier(
				ctx,
				&macie2sdk.GetCustomDataIdentifierInput{Id: aws.String(id)},
			)
			require.NoError(t, err, "GetCustomDataIdentifier should succeed")
			assert.Equal(t, tt.cdiID, aws.ToString(getOut.Name))
			assert.Equal(t, tt.regex, aws.ToString(getOut.Regex))

			listOut, err := client.ListCustomDataIdentifiers(ctx, &macie2sdk.ListCustomDataIdentifiersInput{})
			require.NoError(t, err, "ListCustomDataIdentifiers should succeed")

			found := false
			for _, item := range listOut.Items {
				if aws.ToString(item.Id) == id {
					found = true

					break
				}
			}

			assert.True(t, found, "created identifier should appear in list")

			_, err = client.DeleteCustomDataIdentifier(
				ctx,
				&macie2sdk.DeleteCustomDataIdentifierInput{Id: aws.String(id)},
			)
			require.NoError(t, err, "DeleteCustomDataIdentifier should succeed")
		})
	}
}
