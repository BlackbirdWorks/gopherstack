package integration_test

import (
	"archive/zip"
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdaclientsdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildMinimalZip returns a byte slice containing a minimal valid ZIP archive with one file.
// Errors are intentionally ignored because this is a test helper that produces fixtures;
// [zip.NewWriter] and buf.Write do not fail for simple in-memory writes.
func buildMinimalZip(filename, content string) []byte {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)

	f, _ := w.Create(filename)
	_, _ = f.Write([]byte(content))
	_ = w.Close()

	return buf.Bytes()
}

// TestLambdaLayers_FullLifecycle exercises the complete lifecycle of Lambda Layer operations
// via the AWS SDK against a running Gopherstack container.
func TestLambdaLayers_FullLifecycle(t *testing.T) {
	t.Parallel()

	client := createLambdaClient(t)
	ctx := t.Context()

	layerName := "integration-test-layer"
	zipData := buildMinimalZip("lib.py", "def handler(): pass")

	// --- PublishLayerVersion ---
	publishOut, err := client.PublishLayerVersion(ctx, &lambdaclientsdk.PublishLayerVersionInput{
		LayerName:   aws.String(layerName),
		Description: aws.String("first version"),
		Content: &lambdatypes.LayerVersionContentInput{
			ZipFile: zipData,
		},
		CompatibleRuntimes: []lambdatypes.Runtime{lambdatypes.RuntimePython39},
	})
	require.NoError(t, err, "PublishLayerVersion should succeed")
	assert.Equal(t, int64(1), publishOut.Version)
	assert.NotNil(t, publishOut.LayerVersionArn)
	assert.NotNil(t, publishOut.LayerArn)
	assert.Contains(t, *publishOut.LayerVersionArn, layerName)

	// --- PublishLayerVersion (second version) ---
	publishOut2, err := client.PublishLayerVersion(ctx, &lambdaclientsdk.PublishLayerVersionInput{
		LayerName:   aws.String(layerName),
		Description: aws.String("second version"),
		Content: &lambdatypes.LayerVersionContentInput{
			ZipFile: zipData,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), publishOut2.Version)

	// --- GetLayerVersion ---
	getOut, err := client.GetLayerVersion(ctx, &lambdaclientsdk.GetLayerVersionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
	})
	require.NoError(t, err, "GetLayerVersion should succeed")
	assert.Equal(t, int64(1), getOut.Version)
	assert.Equal(t, "first version", aws.ToString(getOut.Description))

	// --- GetLayerVersion not found ---
	_, err = client.GetLayerVersion(ctx, &lambdaclientsdk.GetLayerVersionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(99),
	})
	require.Error(t, err, "GetLayerVersion with unknown version should fail")

	// --- ListLayers ---
	listOut, err := client.ListLayers(ctx, &lambdaclientsdk.ListLayersInput{})
	require.NoError(t, err, "ListLayers should succeed")

	found := false

	for _, l := range listOut.Layers {
		if aws.ToString(l.LayerName) == layerName {
			found = true

			break
		}
	}

	assert.True(t, found, "ListLayers should include the published layer")

	// --- ListLayerVersions ---
	listVOut, err := client.ListLayerVersions(ctx, &lambdaclientsdk.ListLayerVersionsInput{
		LayerName: aws.String(layerName),
	})
	require.NoError(t, err, "ListLayerVersions should succeed")
	assert.GreaterOrEqual(t, len(listVOut.LayerVersions), 2, "should have at least 2 versions")

	// --- AddLayerVersionPermission ---
	addPermOut, err := client.AddLayerVersionPermission(ctx, &lambdaclientsdk.AddLayerVersionPermissionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
		StatementId:   aws.String("allow-all-accounts"),
		Action:        aws.String("lambda:GetLayerVersion"),
		Principal:     aws.String("*"),
	})
	require.NoError(t, err, "AddLayerVersionPermission should succeed")
	assert.NotEmpty(t, aws.ToString(addPermOut.Statement))

	// --- GetLayerVersionPolicy ---
	policyOut, err := client.GetLayerVersionPolicy(ctx, &lambdaclientsdk.GetLayerVersionPolicyInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
	})
	require.NoError(t, err, "GetLayerVersionPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "allow-all-accounts")

	// --- RemoveLayerVersionPermission ---
	_, err = client.RemoveLayerVersionPermission(ctx, &lambdaclientsdk.RemoveLayerVersionPermissionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
		StatementId:   aws.String("allow-all-accounts"),
	})
	require.NoError(t, err, "RemoveLayerVersionPermission should succeed")

	// Verify permission was removed.
	policyOut2, err := client.GetLayerVersionPolicy(ctx, &lambdaclientsdk.GetLayerVersionPolicyInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
	})
	require.NoError(t, err)
	assert.NotContains(t, aws.ToString(policyOut2.Policy), "allow-all-accounts")

	// --- DeleteLayerVersion ---
	_, err = client.DeleteLayerVersion(ctx, &lambdaclientsdk.DeleteLayerVersionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
	})
	require.NoError(t, err, "DeleteLayerVersion should succeed")

	// Verify the version is gone.
	_, err = client.GetLayerVersion(ctx, &lambdaclientsdk.GetLayerVersionInput{
		LayerName:     aws.String(layerName),
		VersionNumber: aws.Int64(1),
	})
	require.Error(t, err, "GetLayerVersion after delete should fail")
}

// TestLambdaLayers_CreateFunctionWithLayers verifies that Lambda functions can be created
// with layer ARNs and that the ARNs are preserved in the function configuration.
func TestLambdaLayers_CreateFunctionWithLayers(t *testing.T) {
	t.Parallel()

	client := createLambdaClient(t)
	ctx := t.Context()

	layerName := "fn-layer-test"
	zipData := buildMinimalZip("handler.py", "def handler(e, c): return {}")

	// Publish a layer first.
	layerOut, err := client.PublishLayerVersion(ctx, &lambdaclientsdk.PublishLayerVersionInput{
		LayerName: aws.String(layerName),
		Content: &lambdatypes.LayerVersionContentInput{
			ZipFile: zipData,
		},
	})
	require.NoError(t, err)

	layerARN := aws.ToString(layerOut.LayerVersionArn)

	// Create a function that references the layer.
	funcName := "fn-with-layer-integration"

	createOut, err := client.CreateFunction(ctx, &lambdaclientsdk.CreateFunctionInput{
		FunctionName: aws.String(funcName),
		PackageType:  lambdatypes.PackageTypeImage,
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("test:latest")},
		Role:         aws.String("arn:aws:iam::000000000000:role/test"),
		Layers:       []string{layerARN},
	})
	require.NoError(t, err)

	// SDK returns Layers as []types.Layer (objects with Arn field).
	foundLayer := false

	for _, l := range createOut.Layers {
		if aws.ToString(l.Arn) == layerARN {
			foundLayer = true

			break
		}
	}

	assert.True(t, foundLayer, "function configuration should include the layer ARN")

	// GetFunction should also return the layers.
	getOut, err := client.GetFunction(ctx, &lambdaclientsdk.GetFunctionInput{
		FunctionName: aws.String(funcName),
	})
	require.NoError(t, err)

	foundLayerInGet := false

	for _, l := range getOut.Configuration.Layers {
		if aws.ToString(l.Arn) == layerARN {
			foundLayerInGet = true

			break
		}
	}

	assert.True(t, foundLayerInGet, "GetFunction should return the layer ARN")

	// Cleanup.
	_, _ = client.DeleteFunction(ctx, &lambdaclientsdk.DeleteFunctionInput{
		FunctionName: aws.String(funcName),
	})
}
