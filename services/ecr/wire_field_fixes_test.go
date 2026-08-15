package ecr_test

// wire_field_fixes_test.go — ratifies the gopherstack-6flj wrapper-key sweep
// fixes for this service: PutRegistryScanningConfiguration reused
// GetRegistryScanningConfigurationOutput's wrapper key/shape instead of its
// own genuinely different one; RegistryID left unpopulated on
// Get/PutRegistryScanningConfiguration and PutImageScanningConfiguration;
// BatchGetRepositoryScanningConfiguration missing appliedScanFilters;
// DescribeRepositoryCreationTemplates missing maxResults/nextToken
// pagination; and DescribeImageScanFindings' nested imageScanFindings object
// leaking imageId/repositoryName/registryId/status/description that the real
// nested ImageScanFindings shape does not have.

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/require"
)

func TestGetRegistryScanningConfiguration_RegistryIDPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	out, err := client.GetRegistryScanningConfiguration(
		t.Context(), &ecrsdk.GetRegistryScanningConfigurationInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, out.RegistryId)
	require.Equal(t, testAccountID, *out.RegistryId)
}

// TestPutRegistryScanningConfiguration_WrapperKey ratifies the real
// PutRegistryScanningConfigurationOutput shape: the settings come back under
// "registryScanningConfiguration" with no top-level registryId, a genuinely
// different shape from GetRegistryScanningConfigurationOutput's
// "scanningConfiguration"+"registryId" pair. A real SDK client can only
// decode this correctly if gopherstack emits the real wrapper key — this
// test drives the real ecrsdk client end to end, so a wrong key surfaces as
// a nil RegistryScanningConfiguration, not a JSON error.
func TestPutRegistryScanningConfiguration_WrapperKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	out, err := client.PutRegistryScanningConfiguration(
		t.Context(),
		&ecrsdk.PutRegistryScanningConfigurationInput{ScanType: types.ScanTypeEnhanced},
	)
	require.NoError(t, err)
	require.NotNil(t, out.RegistryScanningConfiguration)
	require.Equal(t, types.ScanTypeEnhanced, out.RegistryScanningConfiguration.ScanType)
}

func TestPutImageScanningConfiguration_RegistryIDPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "scan-cfg-repo")

	out, err := client.PutImageScanningConfiguration(
		t.Context(),
		&ecrsdk.PutImageScanningConfigurationInput{
			RepositoryName:             aws.String("scan-cfg-repo"),
			ImageScanningConfiguration: &types.ImageScanningConfiguration{ScanOnPush: true},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.RegistryId)
	require.Equal(t, testAccountID, *out.RegistryId)
}

func TestBatchGetRepositoryScanningConfiguration_AppliedScanFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "continuous-repo")

	_, err := client.PutRegistryScanningConfiguration(
		t.Context(),
		&ecrsdk.PutRegistryScanningConfigurationInput{
			ScanType: types.ScanTypeEnhanced,
			Rules: []types.RegistryScanningRule{
				{
					ScanFrequency: types.ScanFrequencyContinuousScan,
					RepositoryFilters: []types.ScanningRepositoryFilter{
						{Filter: aws.String("continuous-*"), FilterType: types.ScanningRepositoryFilterTypeWildcard},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	out, err := client.BatchGetRepositoryScanningConfiguration(
		t.Context(),
		&ecrsdk.BatchGetRepositoryScanningConfigurationInput{
			RepositoryNames: []string{"continuous-repo"},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.ScanningConfigurations, 1)

	cfg := out.ScanningConfigurations[0]
	require.Equal(t, types.ScanFrequencyContinuousScan, cfg.ScanFrequency)
	require.Len(t, cfg.AppliedScanFilters, 1)
	require.Equal(t, "continuous-*", *cfg.AppliedScanFilters[0].Filter)
	require.Equal(t, types.ScanningRepositoryFilterTypeWildcard, cfg.AppliedScanFilters[0].FilterType)
}

func TestBatchGetRepositoryScanningConfiguration_NoRuleMatch_NoAppliedFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)
	mustCreateRepo(t, h, "basic-repo")

	out, err := client.BatchGetRepositoryScanningConfiguration(
		t.Context(),
		&ecrsdk.BatchGetRepositoryScanningConfigurationInput{
			RepositoryNames: []string{"basic-repo"},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.ScanningConfigurations, 1)
	require.Empty(t, out.ScanningConfigurations[0].AppliedScanFilters)
}

func TestDescribeRepositoryCreationTemplates_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	for _, prefix := range []string{"aaa", "bbb", "ccc"} {
		_, err := client.CreateRepositoryCreationTemplate(
			t.Context(),
			&ecrsdk.CreateRepositoryCreationTemplateInput{
				Prefix:     aws.String(prefix),
				AppliedFor: []types.RCTAppliedFor{types.RCTAppliedForPullThroughCache},
			},
		)
		require.NoError(t, err)
	}

	page1, err := client.DescribeRepositoryCreationTemplates(
		t.Context(),
		&ecrsdk.DescribeRepositoryCreationTemplatesInput{MaxResults: aws.Int32(2)},
	)
	require.NoError(t, err)
	require.Len(t, page1.RepositoryCreationTemplates, 2)
	require.NotNil(t, page1.NextToken)
	require.NotEmpty(t, *page1.NextToken)

	page2, err := client.DescribeRepositoryCreationTemplates(
		t.Context(),
		&ecrsdk.DescribeRepositoryCreationTemplatesInput{NextToken: page1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, page2.RepositoryCreationTemplates, 1)
	require.Equal(t, "ccc", *page2.RepositoryCreationTemplates[0].Prefix)
}

// TestGetSigningConfiguration_RegistryIDPopulated and its Delete counterpart
// ratify that Get/DeleteSigningConfiguration carry registryId, while Put
// genuinely does not (PutSigningConfigurationOutput has no such field) —
// three siblings, two shapes, confirmed against each op's own real
// deserializer rather than assuming symmetry across the trio.
func TestGetSigningConfiguration_RegistryIDPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	out, err := client.GetSigningConfiguration(t.Context(), &ecrsdk.GetSigningConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, out.RegistryId)
	require.Equal(t, testAccountID, *out.RegistryId)
}

func TestDeleteSigningConfiguration_RegistryIDPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECRClient(t, h)

	out, err := client.DeleteSigningConfiguration(t.Context(), &ecrsdk.DeleteSigningConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, out.RegistryId)
	require.Equal(t, testAccountID, *out.RegistryId)
}

func TestDescribeImageScanFindings_NestedObjectDoesNotLeakTopLevelFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	mustCreateRepo(t, h, "leak-check-repo")
	digest := mustPutImage(t, h, "leak-check-repo", "v1", `{"schemaVersion":2}`)

	rec := doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "leak-check-repo",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "leak-check-repo",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := parseAccuracy(t, rec)
	findings, ok := body["imageScanFindings"].(map[string]any)
	require.True(t, ok, "imageScanFindings must be a JSON object")

	for _, leaked := range []string{"imageId", "repositoryName", "registryId", "status", "description"} {
		_, present := findings[leaked]
		require.Falsef(t, present,
			"nested imageScanFindings must not carry %q — the real ImageScanFindings"+
				" shape has no such field; it belongs only at the output's top level", leaked)
	}
}
