package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestCreateGlueIdentityCenterConfiguration_ApplicationArn proves
// CreateGlueIdentityCenterConfiguration returns a non-empty ApplicationArn
// rather than an empty envelope. Real
// CreateGlueIdentityCenterConfigurationOutput carries ApplicationArn
// (glue@v1.152.0 deserializers.go
// awsAwsjson11_deserializeOpDocumentCreateGlueIdentityCenterConfigurationOutput)
// -- the same empty-envelope bug class as ec2's DeleteLaunchTemplate.
func TestCreateGlueIdentityCenterConfiguration_ApplicationArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)
	client := newTestGlueClient(t, h)

	out, err := client.CreateGlueIdentityCenterConfiguration(
		t.Context(),
		&gluesdk.CreateGlueIdentityCenterConfigurationInput{
			InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, aws.ToString(out.ApplicationArn),
		"CreateGlueIdentityCenterConfigurationOutput.ApplicationArn must decode non-empty",
	)
}

// TestGetGlueIdentityCenterConfiguration_ApplicationArn proves
// GetGlueIdentityCenterConfiguration echoes the same ApplicationArn that
// Create returned. Real GetGlueIdentityCenterConfigurationOutput also
// carries ApplicationArn (glue@v1.152.0 deserializers.go
// awsAwsjson11_deserializeOpDocumentGetGlueIdentityCenterConfigurationOutput).
func TestGetGlueIdentityCenterConfiguration_ApplicationArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)
	client := newTestGlueClient(t, h)

	created, err := client.CreateGlueIdentityCenterConfiguration(
		t.Context(),
		&gluesdk.CreateGlueIdentityCenterConfigurationInput{
			InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
		},
	)
	require.NoError(t, err)

	out, err := client.GetGlueIdentityCenterConfiguration(
		t.Context(),
		&gluesdk.GetGlueIdentityCenterConfigurationInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, aws.ToString(out.ApplicationArn),
		"GetGlueIdentityCenterConfigurationOutput.ApplicationArn must decode non-empty",
	)
	require.Equal(t, aws.ToString(created.ApplicationArn), aws.ToString(out.ApplicationArn))
}
