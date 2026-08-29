package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestUpdateAlias_UnknownVersionSurfacesResourceNotFoundException guards
// gopherstack-huyl: UpdateAlias set FunctionVersion unconditionally, unlike
// CreateAlias, which validates the target version against the function's
// known versions. lambda@v1.101.2 deserializers.go's
// deserializeOpErrorUpdateAlias models ResourceNotFoundException (the same
// code this package's ErrVersionNotFound already maps to for CreateAlias),
// so an alias could be pointed at a version that never existed.
func TestUpdateAlias_UnknownVersionSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "updalias-wire-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	v1, err := bk.PublishVersion("updalias-wire-fn", "v1")
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
		FunctionName:    aws.String("updalias-wire-fn"),
		Name:            aws.String("prod"),
		FunctionVersion: aws.String(v1.Version),
	})
	require.NoError(t, err)

	_, err = client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName:    aws.String("updalias-wire-fn"),
		Name:            aws.String("prod"),
		FunctionVersion: aws.String("99"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

	got, getErr := client.GetAlias(t.Context(), &lambdasdk.GetAliasInput{
		FunctionName: aws.String("updalias-wire-fn"),
		Name:         aws.String("prod"),
	})
	require.NoError(t, getErr)
	assert.Equal(t, v1.Version, aws.ToString(got.FunctionVersion),
		"a rejected UpdateAlias must not leave the alias pointed at the invalid version")
}

// TestUpdateAlias_LatestVersionSucceeds confirms the new version check
// doesn't regress the $LATEST special case, which CreateAlias also exempts
// from the known-versions lookup.
func TestUpdateAlias_LatestVersionSucceeds(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "updalias-latest-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	v1, err := bk.PublishVersion("updalias-latest-fn", "v1")
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
		FunctionName:    aws.String("updalias-latest-fn"),
		Name:            aws.String("dev"),
		FunctionVersion: aws.String(v1.Version),
	})
	require.NoError(t, err)

	out, err := client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName:    aws.String("updalias-latest-fn"),
		Name:            aws.String("dev"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)
	assert.Equal(t, "$LATEST", aws.ToString(out.FunctionVersion))
}
