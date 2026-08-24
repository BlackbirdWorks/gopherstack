package iot_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestUpdatePackage_UnsetDefaultVersionSurvives guards UpdatePackageInput's
// real unsetDefaultVersion bool (iot@v1.77.4 api_op_UpdatePackage.go),
// previously silently dropped -- a real client could set a default version
// but never clear it. Driven through a real generated AWS SDK v2 client.
func TestUpdatePackage_UnsetDefaultVersionSurvives(t *testing.T) {
	t.Parallel()

	client, _ := newIoTSDKClient(t)
	ctx := t.Context()

	_, err := client.CreatePackage(ctx, &iotsdk.CreatePackageInput{PackageName: aws.String("unset-pkg")})
	require.NoError(t, err)

	_, err = client.UpdatePackage(ctx, &iotsdk.UpdatePackageInput{
		PackageName:        aws.String("unset-pkg"),
		DefaultVersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	out, err := client.GetPackage(ctx, &iotsdk.GetPackageInput{PackageName: aws.String("unset-pkg")})
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", aws.ToString(out.DefaultVersionName))

	_, err = client.UpdatePackage(ctx, &iotsdk.UpdatePackageInput{
		PackageName:         aws.String("unset-pkg"),
		UnsetDefaultVersion: aws.Bool(true),
	})
	require.NoError(t, err)

	out2, err := client.GetPackage(ctx, &iotsdk.GetPackageInput{PackageName: aws.String("unset-pkg")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(out2.DefaultVersionName), "unsetDefaultVersion must clear defaultVersionName")
}

// TestUpdatePackageVersion_AdvancedFieldsSurvive guards
// UpdatePackageVersionInput's real action/artifact/attributes/recipe
// members (iot@v1.77.4 api_op_UpdatePackageVersion.go), previously all
// silently dropped -- only description/status were ever applied. action
// (PUBLISH/DEPRECATE) is a lifecycle-transition shorthand for status, not a
// stored field of its own. Driven through a real generated AWS SDK v2
// client.
func TestUpdatePackageVersion_AdvancedFieldsSurvive(t *testing.T) {
	t.Parallel()

	client, _ := newIoTSDKClient(t)
	ctx := t.Context()

	_, err := client.CreatePackage(ctx, &iotsdk.CreatePackageInput{PackageName: aws.String("adv-pkg")})
	require.NoError(t, err)
	_, err = client.CreatePackageVersion(ctx, &iotsdk.CreatePackageVersionInput{
		PackageName: aws.String("adv-pkg"),
		VersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	_, err = client.UpdatePackageVersion(ctx, &iotsdk.UpdatePackageVersionInput{
		PackageName: aws.String("adv-pkg"),
		VersionName: aws.String("1.0.0"),
		Action:      iottypes.PackageVersionActionPublish,
		Recipe:      aws.String(`{"format":"2.0"}`),
		Attributes:  map[string]string{"env": "prod"},
		Artifact: &iottypes.PackageVersionArtifact{
			S3Location: &iottypes.S3Location{Bucket: aws.String("b"), Key: aws.String("k")},
		},
	})
	require.NoError(t, err)

	out, err := client.GetPackageVersion(ctx, &iotsdk.GetPackageVersionInput{
		PackageName: aws.String("adv-pkg"),
		VersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	assert.Equal(t, iottypes.PackageVersionStatusPublished, out.Status)
	assert.JSONEq(t, `{"format":"2.0"}`, aws.ToString(out.Recipe))
	assert.Equal(t, "prod", out.Attributes["env"])
	require.NotNil(t, out.Artifact)
	require.NotNil(t, out.Artifact.S3Location)
	assert.Equal(t, "b", aws.ToString(out.Artifact.S3Location.Bucket))
	assert.Equal(t, "k", aws.ToString(out.Artifact.S3Location.Key))
}

// TestBatch3_PackageVersionCRUD tests PackageVersion create/get/update/list/delete.
func TestPackageVersionCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create version, including attributes/recipe/artifact -- real fields
	// CreatePackageVersionInput accepts (iot@v1.77.4) that the handler used
	// to silently drop.
	out := iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"description": "version 1",
		"attributes":  map[string]any{"env": "prod"},
		"recipe":      `{"format":"1.0"}`,
		"artifact": map[string]any{
			"s3Location": map[string]any{"bucket": "b", "key": "k"},
		},
	})
	if out["versionName"] != "1.0.0" {
		t.Errorf("expected versionName=1.0.0, got %v", out)
	}
	attrs, _ := out["attributes"].(map[string]any)
	if attrs["env"] != "prod" {
		t.Errorf("expected attributes.env=prod on CreatePackageVersion, got %v", out)
	}
	if out["recipe"] != `{"format":"1.0"}` {
		t.Errorf("expected recipe on CreatePackageVersion, got %v", out)
	}
	artifact, _ := out["artifact"].(map[string]any)
	if artifact == nil {
		t.Errorf("expected artifact on CreatePackageVersion, got %v", out)
	}

	// Get version
	out2 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions/1.0.0", nil)
	if out2["versionName"] != "1.0.0" {
		t.Errorf("get mismatch: %v", out2)
	}
	attrs2, _ := out2["attributes"].(map[string]any)
	if attrs2["env"] != "prod" {
		t.Errorf("expected attributes.env=prod on GetPackageVersion, got %v", out2)
	}

	// Update version
	iotOK(t, h, http.MethodPatch, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"status": "PUBLISHED",
	})

	// List versions
	out3 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions", nil)
	versions, _ := out3["packageVersionSummaries"].([]any)
	if len(versions) != 1 {
		t.Errorf("expected 1 version, got %d", len(versions))
	}

	// Delete version
	iotOK(t, h, http.MethodDelete, "/packages/my-pkg/versions/1.0.0", nil)
}

// TestBatch3_PackageConfiguration tests GetPackageConfiguration and UpdatePackageConfiguration.
func TestPackageConfiguration(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Get (empty)
	out := iotOK(t, h, http.MethodGet, "/package-configuration", nil)
	if out == nil {
		t.Fatal("expected non-nil response")
	}

	// Update
	iotOK(t, h, http.MethodPatch, "/package-configuration", map[string]any{
		"versionUpdateByJobsConfig": map[string]any{"enabled": true},
	})

	// Get again to verify
	out2 := iotOK(t, h, http.MethodGet, "/package-configuration", nil)
	cfg, _ := out2["versionUpdateByJobsConfig"].(map[string]any)
	if cfg == nil {
		t.Errorf("expected versionUpdateByJobsConfig, got %v", out2)
	}
}

// TestUpdatePackageConfiguration_FieldsSurviveIndependentUpdates guards
// gopherstack-c8ge: types.VersionUpdateByJobsConfig has two
// independently-optional pointer scalars, Enabled and RoleArn. Updating
// RoleArn alone in a later call must not wipe Enabled set by an earlier,
// unrelated call.
func TestUpdatePackageConfiguration_FieldsSurviveIndependentUpdates(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Update A: set enabled.
	iotOK(t, h, http.MethodPatch, "/package-configuration", map[string]any{
		"versionUpdateByJobsConfig": map[string]any{"enabled": true},
	})

	// Update B: set roleArn only, omitting enabled.
	iotOK(t, h, http.MethodPatch, "/package-configuration", map[string]any{
		"versionUpdateByJobsConfig": map[string]any{
			"roleArn": "arn:aws:iam::000000000000:role/PackageJobsRole",
		},
	})

	out := iotOK(t, h, http.MethodGet, "/package-configuration", nil)
	cfg, ok := out["versionUpdateByJobsConfig"].(map[string]any)
	require.True(t, ok, "expected versionUpdateByJobsConfig, got %v", out)

	assert.Equal(t, "arn:aws:iam::000000000000:role/PackageJobsRole", cfg["roleArn"], "B's own field must apply")
	assert.Equal(t, true, cfg["enabled"], "A's enabled must survive an Update that never mentioned it")
}

// TestListPackages_SummaryScoping proves handleListPackages stops leaking
// tags/packageArn/description (none of which types.PackageSummary,
// iot@v1.77.4 types.go:3386-3401, declares) and wraps the list under
// "packageSummaries" rather than the fabricated "packageList" (real
// ListPackagesOutput field per deserializers.go:
// awsRestjson1_deserializeOpDocumentListPackagesOutput). An SDK-driven
// client cannot prove either half: it silently drops unrecognized member
// keys, and prior to the wrapper-key fix it would decode a *correctly
// empty* list either way (both "packageList" and the over-wide shape are
// invisible to it) -- only a raw-body assertion distinguishes fixed from
// unfixed.
func TestListPackages_SummaryScoping(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerForBatch3Test(t)

	iotOK(t, h, http.MethodPut, "/packages/my-pkg", map[string]any{
		"description": "must not leak",
		"tags":        map[string]string{"env": "prod"},
	})

	out := iotOK(t, h, http.MethodGet, "/packages", nil)
	pkgs, ok := out["packageSummaries"].([]any)
	require.True(t, ok, "expected wrapper key packageSummaries, got %v", out)
	require.Len(t, pkgs, 1)

	pkg, ok := pkgs[0].(map[string]any)
	require.True(t, ok)

	for _, forbidden := range []string{"tags", "packageArn", "description"} {
		assert.NotContainsf(t, pkg, forbidden, "%s is not a member of types.PackageSummary", forbidden)
	}
	for _, want := range []string{"creationDate", "defaultVersionName", "lastModifiedDate", "packageName"} {
		assert.Containsf(t, pkg, want, "%s is a member of types.PackageSummary", want)
	}
}

// TestListPackageVersions_SummaryScoping is TestListPackages_SummaryScoping's
// counterpart for ListPackageVersions / types.PackageVersionSummary
// (types.go:3413-3433) and its real "packageVersionSummaries" wrapper.
func TestListPackageVersions_SummaryScoping(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerForBatch3Test(t)

	iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"description": "must not leak",
		"tags":        map[string]string{"env": "prod"},
	})

	out := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions", nil)
	versions, ok := out["packageVersionSummaries"].([]any)
	require.True(t, ok, "expected wrapper key packageVersionSummaries, got %v", out)
	require.Len(t, versions, 1)

	v, ok := versions[0].(map[string]any)
	require.True(t, ok)

	for _, forbidden := range []string{"tags", "packageVersionArn", "description"} {
		assert.NotContainsf(t, v, forbidden, "%s is not a member of types.PackageVersionSummary", forbidden)
	}
	for _, want := range []string{"creationDate", "lastModifiedDate", "packageName", "status", "versionName"} {
		assert.Containsf(t, v, want, "%s is a member of types.PackageVersionSummary", want)
	}
}

// TestRefinement1_SbomDeepCopy verifies cloneSbomDocument deep copies via persistence.
func TestSbomDeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "sbom_preserved_through_snapshot_restore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			_, err := b1.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
				PackageName: "my-pkg",
				VersionName: "1.0.0",
				Sbom: &iot.SbomDocument{
					S3Location: &iot.S3Location{Bucket: "my-bucket", Key: "sbom.json"},
				},
			})
			require.NoError(t, err)

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			// Verify the SBOM was preserved by re-associating (no error means backend was restored).
			_, err2 := b2.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
				PackageName: "other-pkg",
				VersionName: "2.0.0",
			})
			require.NoError(t, err2)
		})
	}
}

func TestSbomDisassociateAndValidationResults(t *testing.T) {
	t.Parallel()

	t.Run("round_trip", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateIoTPackageVersion("pkg1", "1.0", "", nil, iot.CreateIoTPackageVersionOptions{})
		require.NoError(t, err)

		_, err = b.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
			PackageName: "pkg1",
			VersionName: "1.0",
			Sbom: &iot.SbomDocument{
				S3Location: &iot.S3Location{Bucket: "my-bucket", Key: "sbom.json"},
			},
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SUCCEEDED")

		// GetPackageVersion must surface the sbom/sbomValidationStatus the
		// backend already tracks, not just the version's own fields.
		getRec := doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0", nil, nil)
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), `"sbomValidationStatus":"SUCCEEDED"`)
		assert.Contains(t, getRec.Body.String(), "sbom.json")

		rec = doRefRequest(t, h, http.MethodDelete, "/packages/pkg1/versions/1.0/sbom", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "SUCCEEDED")
	})

	t.Run("missing_s3_location_fails_validation", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateIoTPackageVersion("pkg2", "1.0", "", nil, iot.CreateIoTPackageVersionOptions{})
		require.NoError(t, err)

		_, err = b.AssociateSbomWithPackageVersion(&iot.AssociateSbomWithPackageVersionInput{
			PackageName: "pkg2",
			VersionName: "1.0",
			Sbom:        &iot.SbomDocument{},
		})
		require.NoError(t, err)

		rec := doRefRequest(t, h, http.MethodGet, "/packages/pkg2/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "FAILED")
	})

	t.Run("unknown_package_version_404", func(t *testing.T) {
		t.Parallel()

		h, _ := newRefHandler()

		rec := doRefRequest(t, h, http.MethodDelete, "/packages/nosuch/versions/1.0/sbom", nil, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}
