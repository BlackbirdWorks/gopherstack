package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestExportApi_ExportVersion_RejectsUnsupportedVersion proves
// ExportApiInput's ExportVersion query param (reqfielddiff slice 5,
// gopherstack-xhu2t) is honoured: it was previously read nowhere, so any
// value -- valid or not -- was silently accepted. AWS docs
// (api_op_ExportApi.go): "Currently, the only supported version is 1.0".
func TestExportApi_ExportVersion_RejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
	client := newTestAPIGatewayV2Client(t, h)

	created, createErr := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("export-version-api"),
		ProtocolType: "HTTP",
	})
	require.NoError(t, createErr)

	t.Run("1.0 is accepted", func(t *testing.T) {
		t.Parallel()

		_, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
			ApiId:         created.ApiId,
			Specification: aws.String("OAS30"),
			OutputType:    aws.String("JSON"),
			ExportVersion: aws.String("1.0"),
		})
		require.NoError(t, err)
	})

	t.Run("omitted is accepted", func(t *testing.T) {
		t.Parallel()

		_, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
			ApiId:         created.ApiId,
			Specification: aws.String("OAS30"),
			OutputType:    aws.String("JSON"),
		})
		require.NoError(t, err)
	})

	t.Run("2.0 is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := client.ExportApi(t.Context(), &apigatewayv2sdk.ExportApiInput{
			ApiId:         created.ApiId,
			Specification: aws.String("OAS30"),
			OutputType:    aws.String("JSON"),
			ExportVersion: aws.String("2.0"),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exportVersion")
	})
}
