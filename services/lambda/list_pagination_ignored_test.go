package lambda_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestListLayerVersions_SDKRoundTrip_Pagination drives the real SDK client across two
// pages of ListLayerVersions and asserts the pages are disjoint. Before the fix,
// handleListLayerVersions ignored Marker/MaxItems (both real ListLayerVersionsInput
// members, lambda@v1.101.2 api_op_ListLayerVersions.go) and always returned every
// version in one unbounded page with no NextMarker.
func TestListLayerVersions_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	const total = 25

	for i := range total {
		_, err := client.PublishLayerVersion(t.Context(), &lambdasdk.PublishLayerVersionInput{
			LayerName:   aws.String("paginated-layer"),
			Description: aws.String(fmt.Sprintf("version %d", i)),
			Content: &lambdatypes.LayerVersionContentInput{
				ZipFile: []byte(
					"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
				),
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListLayerVersions(t.Context(), &lambdasdk.ListLayerVersionsInput{
		LayerName: aws.String("paginated-layer"),
		MaxItems:  aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.LayerVersions, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListLayerVersions(t.Context(), &lambdasdk.ListLayerVersionsInput{
		LayerName: aws.String("paginated-layer"),
		MaxItems:  aws.Int32(10),
		Marker:    page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.LayerVersions, 10)

	seen := make(map[int64]bool, 20)
	for _, v := range page1.LayerVersions {
		seen[v.Version] = true
	}

	for _, v := range page2.LayerVersions {
		assert.False(t, seen[v.Version], "page 2 repeated layer version %d from page 1", v.Version)
		seen[v.Version] = true
	}

	assert.Len(t, seen, 20)
}

// TestListProvisionedConcurrencyConfigs_SDKRoundTrip_Pagination drives the real SDK
// client across two pages of ListProvisionedConcurrencyConfigs and asserts the pages
// are disjoint. Before the fix, handleListProvisionedConcurrencyConfigs ignored
// Marker/MaxItems and always returned every configured qualifier in one unbounded page.
func TestListProvisionedConcurrencyConfigs_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	const fnName = "pc-paginated-fn"
	const total = 25

	createFunctionForTest(t, h, fnName)

	for range total {
		v, err := bk.PublishVersion(fnName, "")
		require.NoError(t, err)
		_, err = bk.PutProvisionedConcurrencyConfig(fnName, v.Version, 1)
		require.NoError(t, err)
	}

	page1, err := client.ListProvisionedConcurrencyConfigs(
		t.Context(),
		&lambdasdk.ListProvisionedConcurrencyConfigsInput{
			FunctionName: aws.String(fnName),
			MaxItems:     aws.Int32(10),
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.ProvisionedConcurrencyConfigs, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListProvisionedConcurrencyConfigs(
		t.Context(),
		&lambdasdk.ListProvisionedConcurrencyConfigsInput{
			FunctionName: aws.String(fnName),
			MaxItems:     aws.Int32(10),
			Marker:       page1.NextMarker,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.ProvisionedConcurrencyConfigs, 10)

	seen := make(map[string]bool, 20)
	for _, cfg := range page1.ProvisionedConcurrencyConfigs {
		seen[aws.ToString(cfg.FunctionArn)] = true
	}

	for _, cfg := range page2.ProvisionedConcurrencyConfigs {
		assert.False(t, seen[aws.ToString(cfg.FunctionArn)],
			"page 2 repeated a provisioned concurrency config from page 1")
		seen[aws.ToString(cfg.FunctionArn)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListCodeSigningConfigs_SDKRoundTrip_Pagination drives the real SDK client across
// two pages of ListCodeSigningConfigs and asserts the pages are disjoint. Before the
// fix, handleListCodeSigningConfigs ignored Marker/MaxItems and always returned every
// code signing config in one unbounded page.
func TestListCodeSigningConfigs_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	const total = 25

	for i := range total {
		_, err := bk.CreateCodeSigningConfig(&lambda.CreateCodeSigningConfigInput{
			AllowedPublishers: &lambda.AllowedPublishers{
				SigningProfileVersionArns: []string{
					fmt.Sprintf("arn:aws:signer:us-east-1:000000000000:/signing-profiles/profile%d", i),
				},
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListCodeSigningConfigs(t.Context(), &lambdasdk.ListCodeSigningConfigsInput{
		MaxItems: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.CodeSigningConfigs, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListCodeSigningConfigs(t.Context(), &lambdasdk.ListCodeSigningConfigsInput{
		MaxItems: aws.Int32(10),
		Marker:   page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.CodeSigningConfigs, 10)

	seen := make(map[string]bool, 20)
	for _, cfg := range page1.CodeSigningConfigs {
		seen[aws.ToString(cfg.CodeSigningConfigId)] = true
	}

	for _, cfg := range page2.CodeSigningConfigs {
		assert.False(t, seen[aws.ToString(cfg.CodeSigningConfigId)],
			"page 2 repeated a code signing config from page 1")
		seen[aws.ToString(cfg.CodeSigningConfigId)] = true
	}

	assert.Len(t, seen, 20)
}

// TestListFunctionsByCodeSigningConfig_SDKRoundTrip_Pagination drives the real SDK
// client across two pages of ListFunctionsByCodeSigningConfig and asserts the pages
// are disjoint. Before the fix, handleListFunctionsByCodeSigningConfig ignored
// Marker/MaxItems and always returned every associated function in one unbounded page.
func TestListFunctionsByCodeSigningConfig_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	const total = 25

	csc, err := bk.CreateCodeSigningConfig(&lambda.CreateCodeSigningConfigInput{
		AllowedPublishers: &lambda.AllowedPublishers{
			SigningProfileVersionArns: []string{
				"arn:aws:signer:us-east-1:000000000000:/signing-profiles/shared",
			},
		},
	})
	require.NoError(t, err)

	for i := range total {
		fnName := fmt.Sprintf("csc-fn-%02d", i)
		createFunctionForTest(t, h, fnName)
		require.NoError(t, bk.PutFunctionCodeSigningConfig(fnName, csc.CodeSigningConfigArn))
	}

	page1, err := client.ListFunctionsByCodeSigningConfig(
		t.Context(),
		&lambdasdk.ListFunctionsByCodeSigningConfigInput{
			CodeSigningConfigArn: aws.String(csc.CodeSigningConfigArn),
			MaxItems:             aws.Int32(10),
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.FunctionArns, 10)
	require.NotNil(t, page1.NextMarker)

	page2, err := client.ListFunctionsByCodeSigningConfig(
		t.Context(),
		&lambdasdk.ListFunctionsByCodeSigningConfigInput{
			CodeSigningConfigArn: aws.String(csc.CodeSigningConfigArn),
			MaxItems:             aws.Int32(10),
			Marker:               page1.NextMarker,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.FunctionArns, 10)

	seen := make(map[string]bool, 20)
	for _, arn := range page1.FunctionArns {
		seen[arn] = true
	}

	for _, arn := range page2.FunctionArns {
		assert.False(t, seen[arn], "page 2 repeated function ARN %s from page 1", arn)
		seen[arn] = true
	}

	assert.Len(t, seen, 20)
}
