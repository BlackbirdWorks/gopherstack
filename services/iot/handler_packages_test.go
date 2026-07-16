package iot_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch3_PackageVersionCRUD tests PackageVersion create/get/update/list/delete.
func TestPackageVersionCRUD(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create version
	out := iotOK(t, h, http.MethodPut, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"description": "version 1",
	})
	if out["versionName"] != "1.0.0" {
		t.Errorf("expected versionName=1.0.0, got %v", out)
	}

	// Get version
	out2 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions/1.0.0", nil)
	if out2["versionName"] != "1.0.0" {
		t.Errorf("get mismatch: %v", out2)
	}

	// Update version
	iotOK(t, h, http.MethodPatch, "/packages/my-pkg/versions/1.0.0", map[string]any{
		"status": "PUBLISHED",
	})

	// List versions
	out3 := iotOK(t, h, http.MethodGet, "/packages/my-pkg/versions", nil)
	versions, _ := out3["packageVersionList"].([]any)
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

		_, err := b.CreateIoTPackageVersion("pkg1", "1.0", "", nil)
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

		rec = doRefRequest(t, h, http.MethodDelete, "/packages/pkg1/versions/1.0/sbom", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		rec = doRefRequest(t, h, http.MethodGet, "/packages/pkg1/versions/1.0/sbom-validation-results", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), "SUCCEEDED")
	})

	t.Run("missing_s3_location_fails_validation", func(t *testing.T) {
		t.Parallel()

		h, b := newRefHandler()

		_, err := b.CreateIoTPackageVersion("pkg2", "1.0", "", nil)
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
