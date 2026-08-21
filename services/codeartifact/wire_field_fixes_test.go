package codeartifact_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_PermissionsPolicy_RequiresPolicyDocument proves
// PutDomainPermissionsPolicy/PutRepositoryPermissionsPolicy now reject a
// request with no policyDocument instead of silently defaulting to an
// empty-statement policy. PolicyDocument is "This member is required." on
// both real Input types (api_op_Put{Domain,Repository}PermissionsPolicy.go)
// -- confirmed via a real aws-sdk-go-v2 client's own generated validator
// (validators.go's validateOpPutDomainPermissionsPolicyInput), which refuses
// to even send a request missing it. That means a real SDK client can never
// demonstrate this bug: only a raw caller bypassing client-side validation
// can reach gopherstack's old silent-default behavior, so this is a
// raw-body test, not a real-client one.
func TestHandler_PermissionsPolicy_RequiresPolicyDocument(t *testing.T) {
	t.Parallel()

	t.Run("domain", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "ppd-domain")

		rec := doRequest(t, h, http.MethodPut, "/v1/domain/permissions/policy?domain=ppd-domain", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("repository", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "ppr-domain")
		setupRepo(t, h, "ppr-domain", "ppr-repo")

		rec := doRequest(
			t, h, http.MethodPut,
			"/v1/repository/permissions/policy?domain=ppr-domain&repository=ppr-repo", nil,
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_UpdatePackageGroup_RequiresPackageGroup proves
// UpdatePackageGroup now rejects a missing pattern with 400 ValidationException
// instead of falling through to the backend and surfacing as a misleading 404
// "package group not found". PackageGroup is "This member is required." on
// the real UpdatePackageGroupInput (api_op_UpdatePackageGroup.go), same as
// its Create/Describe/Delete siblings (already validated).
func TestHandler_UpdatePackageGroup_RequiresPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "upg-domain")

	rec := doRequest(t, h, http.MethodPut, "/v1/package-group?domain=upg-domain", map[string]any{
		"description": "no pattern given",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCodeArtifactSDK_RepositoryDescription_CreatedTime proves repoToMap now
// emits the real, always-present RepositoryDescription.CreatedTime member
// (deserializers.go's awsRestjson1_deserializeDocumentRepositoryDescription)
// on the six ops that share it (Create/Describe/Delete/Associate/
// Disassociate/UpdateRepository) -- the backend already tracked
// Repository.CreatedTime, it was simply never serialized. A raw-body
// assertion couldn't distinguish this from being read into the wrong Go
// zero value, so this drives the real SDK client.
func TestCodeArtifactSDK_RepositoryDescription_CreatedTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "ct-domain")

	client := newTestCodeArtifactClient(t, h)

	createOut, err := client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
		Domain:     aws.String("ct-domain"),
		Repository: aws.String("ct-repo"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Repository.CreatedTime)
	assert.WithinDuration(t, time.Now(), *createOut.Repository.CreatedTime, time.Minute)

	descOut, err := client.DescribeRepository(t.Context(), &casdk.DescribeRepositoryInput{
		Domain:     aws.String("ct-domain"),
		Repository: aws.String("ct-repo"),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Repository.CreatedTime)
	assert.Equal(t, *createOut.Repository.CreatedTime, *descOut.Repository.CreatedTime)
}

// TestCodeArtifactSDK_DeletePackage_SummaryShape proves DeletePackage now
// emits the real DeletePackageOutput.DeletedPackage shape --
// types.PackageSummary (format/namespace/originConfiguration/package), NOT
// types.PackageDescription (format/name/domainName/domainOwner/repository/
// namespace/originConfiguration), which handleDeletePackage was reusing
// unscoped from DescribePackage. Confirmed against aws-sdk-go-v2
// api_op_DeletePackage.go / deserializers.go's
// awsRestjson1_deserializeDocumentPackageSummary (recognises "package", not
// "name"). Before the fix, a real client's DeletedPackage.Package stayed
// empty regardless of what was deleted.
func TestCodeArtifactSDK_DeletePackage_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "dps-domain")
	setupRepo(t, h, "dps-domain", "dps-repo")

	client := newTestCodeArtifactClient(t, h)

	_, err := client.DescribePackage(t.Context(), &casdk.DescribePackageInput{
		Domain:     aws.String("dps-domain"),
		Repository: aws.String("dps-repo"),
		Format:     "npm",
		Package:    aws.String("lodash"),
	})
	require.NoError(t, err)

	out, err := client.DeletePackage(t.Context(), &casdk.DeletePackageInput{
		Domain:     aws.String("dps-domain"),
		Repository: aws.String("dps-repo"),
		Format:     "npm",
		Package:    aws.String("lodash"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DeletedPackage)
	assert.Equal(
		t, "lodash", aws.ToString(out.DeletedPackage.Package),
		"real SDK client must see the package identifier under DeletedPackage.Package; "+
			"it is lost if the wire emits \"name\" instead of \"package\"",
	)
	assert.Equal(t, "npm", string(out.DeletedPackage.Format))
}

// TestCodeArtifactSDK_PackageVersionOutcomes proves DeletePackageVersions/
// CopyPackageVersions/DisposePackageVersions/UpdatePackageVersionsStatus now
// emit failedVersions/successfulVersions as the real JSON *objects* keyed by
// version string (map[string]types.PackageVersionError / map[string]
// types.SuccessfulPackageVersionInfo, confirmed against aws-sdk-go-v2
// deserializers.go's ...PackageVersionErrorMap/
// ...SuccessfulPackageVersionInfoMap), not the array this backend used to
// build. Before the fix, a real SDK client's call to any of these four ops
// failed outright with a deserialization error (a JSON array cannot decode
// into a Go map) -- this is the total-outage class, not silent-empty.
func TestCodeArtifactSDK_PackageVersionOutcomes(t *testing.T) {
	t.Parallel()

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "pvo-del-domain")
		setupRepo(t, h, "pvo-del-domain", "pvo-del-repo")

		client := newTestCodeArtifactClient(t, h)

		_, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("pvo-del-domain"),
			Repository:     aws.String("pvo-del-repo"),
			Format:         "npm",
			Package:        aws.String("react"),
			PackageVersion: aws.String("18.0.0"),
		})
		require.NoError(t, err)

		out, err := client.DeletePackageVersions(t.Context(), &casdk.DeletePackageVersionsInput{
			Domain:     aws.String("pvo-del-domain"),
			Repository: aws.String("pvo-del-repo"),
			Format:     "npm",
			Package:    aws.String("react"),
			Versions:   []string{"18.0.0", "99.0.0"},
		})
		require.NoError(t, err, "a real client must be able to decode the response at all")
		require.Contains(t, out.SuccessfulVersions, "18.0.0")
		assert.Equal(t, "Deleted", string(out.SuccessfulVersions["18.0.0"].Status))
		require.Contains(t, out.FailedVersions, "99.0.0")
		assert.Equal(t, "NOT_FOUND", string(out.FailedVersions["99.0.0"].ErrorCode))
	})

	t.Run("copy", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "pvo-copy-domain")
		setupRepo(t, h, "pvo-copy-domain", "src")
		setupRepo(t, h, "pvo-copy-domain", "dst")

		client := newTestCodeArtifactClient(t, h)

		_, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("pvo-copy-domain"),
			Repository:     aws.String("src"),
			Format:         "npm",
			Package:        aws.String("react"),
			PackageVersion: aws.String("18.0.0"),
		})
		require.NoError(t, err)

		out, err := client.CopyPackageVersions(t.Context(), &casdk.CopyPackageVersionsInput{
			Domain:                aws.String("pvo-copy-domain"),
			SourceRepository:      aws.String("src"),
			DestinationRepository: aws.String("dst"),
			Format:                "npm",
			Package:               aws.String("react"),
			Versions:              []string{"18.0.0", "9.9.9"},
		})
		require.NoError(t, err, "a real client must be able to decode the response at all")
		require.Contains(t, out.SuccessfulVersions, "18.0.0")
		assert.Equal(t, "Published", string(out.SuccessfulVersions["18.0.0"].Status))
		require.Contains(t, out.FailedVersions, "9.9.9")
		assert.Equal(t, "NOT_FOUND", string(out.FailedVersions["9.9.9"].ErrorCode))
	})

	t.Run("dispose", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "pvo-disp-domain")
		setupRepo(t, h, "pvo-disp-domain", "pvo-disp-repo")

		client := newTestCodeArtifactClient(t, h)

		_, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("pvo-disp-domain"),
			Repository:     aws.String("pvo-disp-repo"),
			Format:         "npm",
			Package:        aws.String("react"),
			PackageVersion: aws.String("18.0.0"),
		})
		require.NoError(t, err)

		out, err := client.DisposePackageVersions(t.Context(), &casdk.DisposePackageVersionsInput{
			Domain:     aws.String("pvo-disp-domain"),
			Repository: aws.String("pvo-disp-repo"),
			Format:     "npm",
			Package:    aws.String("react"),
			Versions:   []string{"18.0.0", "9.9.9"},
		})
		require.NoError(t, err, "a real client must be able to decode the response at all")
		require.Contains(t, out.SuccessfulVersions, "18.0.0")
		assert.Equal(t, "Disposed", string(out.SuccessfulVersions["18.0.0"].Status))
		require.Contains(t, out.FailedVersions, "9.9.9")
		assert.Equal(t, "NOT_FOUND", string(out.FailedVersions["9.9.9"].ErrorCode))
	})

	t.Run("update_status", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		setupDomain(t, h, "pvo-upd-domain")
		setupRepo(t, h, "pvo-upd-domain", "pvo-upd-repo")

		client := newTestCodeArtifactClient(t, h)

		_, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("pvo-upd-domain"),
			Repository:     aws.String("pvo-upd-repo"),
			Format:         "npm",
			Package:        aws.String("react"),
			PackageVersion: aws.String("18.0.0"),
		})
		require.NoError(t, err)

		out, err := client.UpdatePackageVersionsStatus(t.Context(), &casdk.UpdatePackageVersionsStatusInput{
			Domain:       aws.String("pvo-upd-domain"),
			Repository:   aws.String("pvo-upd-repo"),
			Format:       "npm",
			Package:      aws.String("react"),
			TargetStatus: "Archived",
			Versions:     []string{"18.0.0", "9.9.9"},
		})
		require.NoError(t, err, "a real client must be able to decode the response at all")
		require.Contains(t, out.SuccessfulVersions, "18.0.0")
		assert.Equal(t, "Archived", string(out.SuccessfulVersions["18.0.0"].Status))
		require.Contains(t, out.FailedVersions, "9.9.9")
		assert.Equal(t, "NOT_FOUND", string(out.FailedVersions["9.9.9"].ErrorCode))
	})
}

// TestCodeArtifactSDK_RepositorySummary_Fields proves ListRepositories/
// ListRepositoriesInDomain now emit the full real types.RepositorySummary
// shape (arn/name/domainName/domainOwner/administratorAccount/createdTime/
// description, confirmed against aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeDocumentRepositorySummary) instead of only 4 of
// its 7 real members.
func TestCodeArtifactSDK_RepositorySummary_Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "rsf-domain")

	client := newTestCodeArtifactClient(t, h)

	{
		_, err := client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
			Domain:      aws.String("rsf-domain"),
			Repository:  aws.String("rsf-repo"),
			Description: aws.String("a test repository"),
		})
		require.NoError(t, err)
		// A second, non-matching repository -- without it, a RepositoryPrefix
		// filter test can't distinguish "filter applied" from "filter ignored"
		// (both return the same single-element list).
		_, err = client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
			Domain:     aws.String("rsf-domain"),
			Repository: aws.String("other-repo"),
		})
		require.NoError(t, err)
	}

	t.Run("list_repositories", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListRepositories(t.Context(), &casdk.ListRepositoriesInput{
			RepositoryPrefix: aws.String("rsf-"),
		})
		require.NoError(t, err)
		require.Len(t, out.Repositories, 1, "RepositoryPrefix must filter out other-repo")
		r := out.Repositories[0]
		assert.Equal(t, "rsf-repo", aws.ToString(r.Name))
		assert.NotEmpty(t, aws.ToString(r.AdministratorAccount))
		require.NotNil(t, r.CreatedTime)
		assert.WithinDuration(t, time.Now(), *r.CreatedTime, time.Minute)
		assert.Equal(t, "a test repository", aws.ToString(r.Description))
	})

	t.Run("list_repositories_in_domain", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListRepositoriesInDomain(t.Context(), &casdk.ListRepositoriesInDomainInput{
			Domain:           aws.String("rsf-domain"),
			RepositoryPrefix: aws.String("rsf-"),
		})
		require.NoError(t, err)
		require.Len(t, out.Repositories, 1, "RepositoryPrefix must filter out other-repo")
		r := out.Repositories[0]
		assert.Equal(t, "rsf-repo", aws.ToString(r.Name))
		assert.NotEmpty(t, aws.ToString(r.AdministratorAccount))
		require.NotNil(t, r.CreatedTime)
		assert.Equal(t, "a test repository", aws.ToString(r.Description))
	})
}

// TestCodeArtifactSDK_ListPackageVersions_StatusSortByDefaultDisplay proves
// ListPackageVersions now honors its real Status filter and SortBy=PUBLISHED_TIME
// ordering (both previously discarded -- serializers.go's
// SetQuery("status")/SetQuery("sortBy")), and now emits the real, previously-
// absent Namespace echo and DefaultDisplayVersion members (confirmed against
// aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeOpDocumentListPackageVersionsOutput).
func TestCodeArtifactSDK_ListPackageVersions_StatusSortByDefaultDisplay(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpv-domain")
	setupRepo(t, h, "lpv-domain", "lpv-repo")

	client := newTestCodeArtifactClient(t, h)

	{
		// "9.0.0" is created (published) first, "1.0.0" second -- lexicographic
		// version order and publish-time order disagree, so this distinguishes
		// SortBy=PUBLISHED_TIME from the default Version-ascending order.
		_, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("lpv-domain"),
			Repository:     aws.String("lpv-repo"),
			Format:         "npm",
			Namespace:      aws.String("scope"),
			Package:        aws.String("pkg"),
			PackageVersion: aws.String("9.0.0"),
		})
		require.NoError(t, err)

		_, err = client.UpdatePackageVersionsStatus(t.Context(), &casdk.UpdatePackageVersionsStatusInput{
			Domain:       aws.String("lpv-domain"),
			Repository:   aws.String("lpv-repo"),
			Format:       "npm",
			Namespace:    aws.String("scope"),
			Package:      aws.String("pkg"),
			TargetStatus: "Archived",
			Versions:     []string{"9.0.0"},
		})
		require.NoError(t, err)

		_, err = client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
			Domain:         aws.String("lpv-domain"),
			Repository:     aws.String("lpv-repo"),
			Format:         "npm",
			Namespace:      aws.String("scope"),
			Package:        aws.String("pkg"),
			PackageVersion: aws.String("1.0.0"),
		})
		require.NoError(t, err)
	}

	t.Run("status_filter", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListPackageVersions(t.Context(), &casdk.ListPackageVersionsInput{
			Domain:     aws.String("lpv-domain"),
			Repository: aws.String("lpv-repo"),
			Format:     "npm",
			Namespace:  aws.String("scope"),
			Package:    aws.String("pkg"),
			Status:     "Archived",
		})
		require.NoError(t, err)
		require.Len(t, out.Versions, 1, "Status filter must exclude the Published 1.0.0")
		assert.Equal(t, "9.0.0", aws.ToString(out.Versions[0].Version))
		assert.Equal(t, "scope", aws.ToString(out.Namespace), "real Namespace echo")
	})

	t.Run("sort_by_published_time", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListPackageVersions(t.Context(), &casdk.ListPackageVersionsInput{
			Domain:     aws.String("lpv-domain"),
			Repository: aws.String("lpv-repo"),
			Format:     "npm",
			Namespace:  aws.String("scope"),
			Package:    aws.String("pkg"),
			SortBy:     "PUBLISHED_TIME",
		})
		require.NoError(t, err)
		require.Len(t, out.Versions, 2)
		assert.Equal(t, "9.0.0", aws.ToString(out.Versions[0].Version), "published first")
		assert.Equal(t, "1.0.0", aws.ToString(out.Versions[1].Version), "published second")
		assert.Equal(
			t, "1.0.0", aws.ToString(out.DefaultDisplayVersion),
			"most recently published version, a real member this backend never emitted",
		)
	})

	t.Run("default_version_order_is_lexicographic", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListPackageVersions(t.Context(), &casdk.ListPackageVersionsInput{
			Domain:     aws.String("lpv-domain"),
			Repository: aws.String("lpv-repo"),
			Format:     "npm",
			Namespace:  aws.String("scope"),
			Package:    aws.String("pkg"),
		})
		require.NoError(t, err)
		require.Len(t, out.Versions, 2)
		assert.Equal(t, "1.0.0", aws.ToString(out.Versions[0].Version))
		assert.Equal(t, "9.0.0", aws.ToString(out.Versions[1].Version))
	})
}

// TestDescribePackage_NoInventedFields_RealClient covers gopherstack-g479:
// packageToMap emitted domainName, domainOwner and repository, none of
// which are members of types.PackageDescription -- confirmed against
// aws-sdk-go-v2/service/codeartifact@v1.41.4's deserializers.go
// (awsRestjson1_deserializeDocumentPackageDescription) and types/types.go,
// which together declare only format/name/namespace/originConfiguration.
// A typed real client silently ignores unknown JSON keys, so the only
// direct way to prove the fields are gone is the raw response body.
func TestDescribePackage_NoInventedFields_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "dpi-domain")
	setupRepo(t, h, "dpi-domain", "dpi-repo")

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/package?domain=dpi-domain&repository=dpi-repo&format=npm&package=lodash",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"domainName"`,
		"types.PackageDescription has no domainName member")
	assert.NotContains(t, body, `"domainOwner"`,
		"types.PackageDescription has no domainOwner member")
	assert.NotContains(t, body, `"repository"`,
		"types.PackageDescription has no repository member")
}
