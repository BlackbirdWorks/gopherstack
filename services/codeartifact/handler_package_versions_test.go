package codeartifact_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestHandler_DescribePackageVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codeartifact.Handler) {
				doRequest(t, h, http.MethodPost, "/v1/domain?domain=pv-domain", nil)
				doRequest(t, h, http.MethodPost, "/v1/repository?domain=pv-domain&repository=pv-repo", nil)
			},
			path:       "/v1/package/version?domain=pv-domain&repository=pv-repo&format=npm&package=my-pkg&version=1.0.0",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_version",
			path:       "/v1/package/version?domain=pv-domain&repository=pv-repo&format=npm&package=my-pkg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/version?domain=pv-domain&repository=pv-repo&format=npm&version=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "repo_not_found",
			path:       "/v1/package/version?domain=pv-domain&repository=nope&format=npm&package=my-pkg&version=1.0.0",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				pv, _ := resp["packageVersion"].(map[string]any)
				assert.Equal(t, "1.0.0", pv["version"])
				assert.NotEmpty(t, pv["status"])
			}
		})
	}
}

func TestHandler_DeletePackageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=dpv-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=dpv-domain&repository=dpv-repo", nil)

	// Seed two versions via DescribePackageVersion.
	doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version?domain=dpv-domain&repository=dpv-repo&format=npm&package=react&version=17.0.0",
		nil,
	)
	doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version?domain=dpv-domain&repository=dpv-repo&format=npm&package=react&version=18.0.0",
		nil,
	)

	// Delete one existing and one nonexistent version.
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/delete?domain=dpv-domain&repository=dpv-repo&format=npm&package=react",
		map[string]any{
			"versions": []string{"17.0.0", "99.0.0"},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// failedVersions/successfulVersions are real JSON *objects* keyed by
	// version string (map[string]types.PackageVersionError / map[string]
	// types.SuccessfulPackageVersionInfo), not arrays -- verified against
	// aws-sdk-go-v2 deserializers.go's ...PackageVersionErrorMap/
	// ...SuccessfulPackageVersionInfoMap.
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failed, _ := resp["failedVersions"].(map[string]any)
	require.Len(t, failed, 1)
	failedEntry, _ := failed["99.0.0"].(map[string]any)
	assert.Equal(t, "NOT_FOUND", failedEntry["errorCode"])

	// Repo not found.
	recNotFound := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/delete?domain=dpv-domain&repository=nope&format=npm&package=react",
		map[string]any{
			"versions": []string{"17.0.0"},
		},
	)
	assert.Equal(t, http.StatusNotFound, recNotFound.Code)

	// Missing required params.
	recBad := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/delete?domain=dpv-domain&repository=dpv-repo&package=react",
		map[string]any{
			"versions": []string{"17.0.0"},
		},
	)
	assert.Equal(t, http.StatusBadRequest, recBad.Code)
}

func TestHandler_CopyPackageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=copy-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=copy-domain&repository=src-repo", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=copy-domain&repository=dst-repo", nil)

	// Seed a version in src-repo.
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=copy-domain&repository=src-repo&format=pypi&package=boto3&version=1.26.0",
		nil,
	)

	copyURL := "/v1/package/versions/copy" +
		"?domain=copy-domain&source-repository=src-repo&destination-repository=dst-repo&format=pypi&package=boto3"

	// Copy existing + nonexistent version.
	rec := doRequest(t, h, http.MethodPost, copyURL, map[string]any{
		"versions": []string{"1.26.0", "9.9.9"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failed, _ := resp["failedVersions"].(map[string]any)
	require.Len(t, failed, 1)
	failedEntry, _ := failed["9.9.9"].(map[string]any)
	assert.Equal(t, "NOT_FOUND", failedEntry["errorCode"])

	// Verify the copied version is now accessible in dst-repo.
	descRec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=copy-domain&repository=dst-repo&format=pypi&package=boto3&version=1.26.0",
		nil,
	)
	assert.Equal(t, http.StatusOK, descRec.Code)

	noSrcURL := "/v1/package/versions/copy" +
		"?domain=copy-domain&source-repository=nope&destination-repository=dst-repo&format=pypi&package=boto3"

	// Missing source repo.
	recNoSrc := doRequest(t, h, http.MethodPost, noSrcURL, map[string]any{
		"versions": []string{"1.26.0"},
	})
	assert.Equal(t, http.StatusNotFound, recNoSrc.Code)

	// Missing required params.
	recBad := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/copy?domain=copy-domain&source-repository=src-repo"+
			"&destination-repository=dst-repo&package=boto3",
		map[string]any{
			"versions": []string{"1.26.0"},
		},
	)
	assert.Equal(t, http.StatusBadRequest, recBad.Code)
}

func TestHandler_PackageVersionRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=rev-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=rev-domain&repository=rev-repo", nil)

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=rev-domain&repository=rev-repo&format=npm&package=mypkg&version=1.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pv, _ := resp["packageVersion"].(map[string]any)
	assert.NotEmpty(t, pv["revision"])
}

func TestHandler_SuccessfulVersions(t *testing.T) {
	t.Parallel()

	t.Run("delete_versions", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=sv-domain", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=sv-domain&repository=sv-repo", nil)
		// Create version 1.0.0 via describe.
		doRequest(
			t, h, http.MethodGet,
			"/v1/package/version?domain=sv-domain&repository=sv-repo&format=npm&package=mypkg&version=1.0.0",
			nil,
		)

		rec := doRequest(
			t, h, http.MethodPost,
			"/v1/package/versions/delete?domain=sv-domain&repository=sv-repo&format=npm&package=mypkg",
			map[string]any{"versions": []string{"1.0.0", "9.9.9"}},
		)
		require.Equal(t, http.StatusOK, rec.Code)

		// successfulVersions/failedVersions are real JSON *objects* keyed by
		// version string, not arrays -- verified against aws-sdk-go-v2
		// deserializers.go's ...SuccessfulPackageVersionInfoMap/
		// ...PackageVersionErrorMap.
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		successList, _ := resp["successfulVersions"].(map[string]any)
		require.Len(t, successList, 1)
		sv, _ := successList["1.0.0"].(map[string]any)
		assert.Equal(t, "Deleted", sv["status"])
		assert.NotEmpty(t, sv["revision"])

		failedList, _ := resp["failedVersions"].(map[string]any)
		require.Len(t, failedList, 1)
		assert.Contains(t, failedList, "9.9.9")
	})

	t.Run("copy_versions", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, http.MethodPost, "/v1/domain?domain=cv-domain", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=cv-domain&repository=src-repo", nil)
		doRequest(t, h, http.MethodPost, "/v1/repository?domain=cv-domain&repository=dst-repo", nil)
		doRequest(
			t, h, http.MethodGet,
			"/v1/package/version?domain=cv-domain&repository=src-repo&format=npm&package=mypkg&version=1.0.0",
			nil,
		)

		copyPath := "/v1/package/versions/copy" +
			"?domain=cv-domain&source-repository=src-repo&destination-repository=dst-repo" +
			"&format=npm&package=mypkg"
		rec := doRequest(
			t,
			h,
			http.MethodPost,
			copyPath,
			map[string]any{"versions": []string{"1.0.0", "9.9.9"}},
		)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		// "Copied" is not a real PackageVersionStatus enum value (real values:
		// Published/Unfinished/Unlisted/Archived/Disposed/Deleted) -- the copied
		// version keeps its source status, which the stub-creating GET above set
		// to "Published".
		successList, _ := resp["successfulVersions"].(map[string]any)
		require.Len(t, successList, 1)
		sv, _ := successList["1.0.0"].(map[string]any)
		assert.Equal(t, "Published", sv["status"])
		assert.NotEmpty(t, sv["revision"])
	})
}

func TestHandler_PackageVersionMap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pvmap-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pvmap-domain&repository=pvmap-repo", nil)

	rec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=pvmap-domain&repository=pvmap-repo&format=npm&package=mypkg&version=2.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pv, _ := resp["packageVersion"].(map[string]any)

	publishedTime, _ := pv["publishedTime"].(float64)
	assert.Greater(t, publishedTime, float64(0))
	assert.NotEmpty(t, pv["revision"])
	assert.Equal(t, "npm", pv["format"])
}

func TestHandler_DisposePackageVersions_StatusChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=disp-st-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=disp-st-domain&repository=disp-st-repo", nil)
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=disp-st-domain&repository=disp-st-repo&format=npm&package=pkg&version=1.0.0",
		nil,
	)

	rec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions/dispose?domain=disp-st-domain&repository=disp-st-repo&format=npm&package=pkg",
		map[string]any{"versions": []string{"1.0.0", "9.9.9"}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// successfulVersions/failedVersions entries are real
	// types.SuccessfulPackageVersionInfo{revision,status}/
	// types.PackageVersionError{errorCode} objects, not the bare "SUCCESS"/
	// "NOT_FOUND" strings this backend used to emit -- verified against
	// aws-sdk-go-v2 deserializers.go's
	// ...SuccessfulPackageVersionInfoMap/...PackageVersionErrorMap.
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	success, _ := resp["successfulVersions"].(map[string]any)
	entry, _ := success["1.0.0"].(map[string]any)
	assert.Equal(t, "Disposed", entry["status"])
	assert.NotEmpty(t, entry["revision"])

	failed, _ := resp["failedVersions"].(map[string]any)
	failedEntry, _ := failed["9.9.9"].(map[string]any)
	assert.Equal(t, "NOT_FOUND", failedEntry["errorCode"])

	// Verify status changed to Disposed.
	descRec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=disp-st-domain&repository=disp-st-repo&format=npm&package=pkg&version=1.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	pv, _ := descResp["packageVersion"].(map[string]any)
	assert.Equal(t, "Disposed", pv["status"])
}

func TestHandler_UpdatePackageVersionsStatus_StatusChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=uvs-st-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=uvs-st-domain&repository=uvs-st-repo", nil)
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=uvs-st-domain&repository=uvs-st-repo&format=npm&package=react&version=18.0.0",
		nil,
	)

	rec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions/update_status?domain=uvs-st-domain&repository=uvs-st-repo&format=npm&package=react",
		map[string]any{"targetStatus": "Archived", "versions": []string{"18.0.0"}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify status changed.
	descRec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=uvs-st-domain&repository=uvs-st-repo&format=npm&package=react&version=18.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	pv, _ := descResp["packageVersion"].(map[string]any)
	assert.Equal(t, "Archived", pv["status"])
}

func TestHandler_PackageVersionLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lifecycle-domain")
	setupRepo(t, h, "lifecycle-domain", "lifecycle-repo")

	// Publish version.
	pubRec := doRawRequest(
		t,
		h,
		"/v1/package/version/publish?domain=lifecycle-domain"+
			"&repository=lifecycle-repo&format=npm&package=mylib&version=1.0.0&asset=mylib-1.0.0.tgz",
		[]byte("mylib-asset-content"),
	)
	require.Equal(t, http.StatusOK, pubRec.Code)
	var pubResp map[string]any
	require.NoError(t, json.Unmarshal(pubRec.Body.Bytes(), &pubResp))
	assert.Equal(t, "1.0.0", pubResp["version"])
	assert.Equal(t, "Published", pubResp["status"])
	asset, _ := pubResp["asset"].(map[string]any)
	assert.Equal(t, "mylib-1.0.0.tgz", asset["name"])

	// Describe version.
	descRec := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib&version=1.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	pv, _ := descResp["packageVersion"].(map[string]any)
	assert.Equal(t, "Published", pv["status"])

	// Archive version.
	archRec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions/update_status?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib",
		map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
	)
	require.Equal(t, http.StatusOK, archRec.Code)

	// Verify archived.
	descRec2 := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib&version=1.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, descRec2.Code)
	var descResp2 map[string]any
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	pv2, _ := descResp2["packageVersion"].(map[string]any)
	assert.Equal(t, "Archived", pv2["status"])

	// Dispose version.
	dispRec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions/dispose?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib",
		map[string]any{"versions": []string{"1.0.0"}},
	)
	require.Equal(t, http.StatusOK, dispRec.Code)

	// Verify disposed.
	descRec3 := doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib&version=1.0.0",
		nil,
	)
	require.Equal(t, http.StatusOK, descRec3.Code)
	var descResp3 map[string]any
	require.NoError(t, json.Unmarshal(descRec3.Body.Bytes(), &descResp3))
	pv3, _ := descResp3["packageVersion"].(map[string]any)
	assert.Equal(t, "Disposed", pv3["status"])

	// Delete version.
	delRec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions/delete?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib",
		map[string]any{"versions": []string{"1.0.0"}},
	)
	require.Equal(t, http.StatusOK, delRec.Code)

	// Verify gone from list.
	listRec := doRequest(
		t, h, http.MethodPost,
		"/v1/package/versions?domain=lifecycle-domain&repository=lifecycle-repo&format=npm&package=mylib",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	versions, _ := listResp["versions"].([]any)
	assert.Empty(t, versions)
}

func TestHandler_CopyPackageVersions_ToSelf(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "self-copy-domain")
	setupRepo(t, h, "self-copy-domain", "src")
	setupRepo(t, h, "self-copy-domain", "dst")

	// Seed version in src.
	doRequest(
		t, h, http.MethodGet,
		"/v1/package/version?domain=self-copy-domain&repository=src&format=npm&package=react&version=18.0.0",
		nil,
	)

	// Copy to dst.
	copyRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/copy?domain=self-copy-domain"+
			"&source-repository=src&destination-repository=dst&format=npm&package=react",
		map[string]any{"versions": []string{"18.0.0"}},
	)
	require.Equal(t, http.StatusOK, copyRec.Code)

	// Copy again (duplicate) to dst — should fail.
	copyRec2 := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/copy?domain=self-copy-domain"+
			"&source-repository=src&destination-repository=dst&format=npm&package=react",
		map[string]any{"versions": []string{"18.0.0"}},
	)
	require.Equal(t, http.StatusOK, copyRec2.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(copyRec2.Body.Bytes(), &resp))
	failed, _ := resp["failedVersions"].(map[string]any)
	require.Len(t, failed, 1)
	entry, _ := failed["18.0.0"].(map[string]any)
	assert.Equal(t, "ALREADY_EXISTS", entry["errorCode"])
}

func TestHandler_DisposePackageVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_existing_version",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dp-domain")
				setupRepo(t, h, "dp-domain", "dp-repo")
				doRequest(
					t, h, http.MethodGet,
					"/v1/package/version"+
						"?domain=dp-domain&repository=dp-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_nonexistent_version",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "dp2-domain")
				setupRepo(t, h, "dp2-domain", "dp2-repo")
			},
			path:       "/v1/package/versions/dispose?domain=dp2-domain&repository=dp2-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"9.9.9"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions/dispose?repository=dp-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions/dispose?domain=dp-domain&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&format=npm",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["successfulVersions"])
				assert.NotNil(t, resp["failedVersions"])
			}
		})
	}
}

func TestHandler_UpdatePackageVersionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *codeartifact.Handler)
		name       string
		path       string
		wantStatus int
	}{
		{
			name: "success_archive",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upvs-domain")
				setupRepo(t, h, "upvs-domain", "upvs-repo")
				doRequest(
					t,
					h,
					http.MethodGet,
					"/v1/package/version?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash&version=1.0.0",
					nil,
				)
			},
			path: "/v1/package/versions/update_status" +
				"?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "success_missing_version_returns_not_found_in_result",
			setup: func(h *codeartifact.Handler) {
				setupDomain(t, h, "upvs2-domain")
				setupRepo(t, h, "upvs2-domain", "upvs2-repo")
			},
			path: "/v1/package/versions/update_status" +
				"?domain=upvs2-domain&repository=upvs2-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"9.9.9"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_domain",
			path:       "/v1/package/versions/update_status?repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_repo",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&format=npm&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_format",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&repository=upvs-repo&package=lodash",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_package",
			path:       "/v1/package/versions/update_status?domain=upvs-domain&repository=upvs-repo&format=npm",
			body:       map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_target_status",
			path: "/v1/package/versions/update_status" +
				"?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash",
			body:       map[string]any{"versions": []string{"1.0.0"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["successfulVersions"])
				assert.NotNil(t, resp["failedVersions"])
			}
		})
	}
}

func TestListPackageVersions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/v1/domain?domain=pvpag-domain", nil)
	doRequest(t, h, http.MethodPost, "/v1/repository?domain=pvpag-domain&repository=pvpag-repo", nil)

	for i := range 5 {
		path := fmt.Sprintf(
			"/v1/package/version?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg&version=1.%d.0",
			i,
		)
		doRequest(t, h, http.MethodGet, path, nil)
	}

	rec1 := doRequest(t, h, http.MethodGet,
		"/v1/package/versions?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg&max-results=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1, _ := out1["versions"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["nextToken"].(string)
	assert.True(t, ok && nextToken != "", "nextToken must be present after partial page")

	rec2 := doRequest(t, h, http.MethodGet,
		"/v1/package/versions?domain=pvpag-domain&repository=pvpag-repo&format=npm&package=mypkg"+
			"&max-results=2&next-token="+nextToken,
		nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2, _ := out2["versions"].([]any)
	assert.Len(t, page2, 2)
}

// TestPackageVersion_HasPackageName verifies DescribePackageVersion returns packageName.

func TestPackageVersion_HasPackageName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pkgName string
	}{
		{name: "npm_package", pkgName: "my-npm-pkg"},
		{name: "maven_package", pkgName: "my-maven-pkg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/v1/domain?domain=pn-domain", nil)
			doRequest(t, h, http.MethodPost, "/v1/repository?domain=pn-domain&repository=pn-repo", nil)

			path := fmt.Sprintf(
				"/v1/package/version?domain=pn-domain&repository=pn-repo&format=npm&package=%s&version=1.0.0",
				tt.pkgName,
			)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PackageVersion struct {
					PackageName string `json:"packageName"`
					Version     string `json:"version"`
				} `json:"packageVersion"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.pkgName, out.PackageVersion.PackageName)
			assert.Equal(t, "1.0.0", out.PackageVersion.Version)
		})
	}
}

// TestCreateRepository_UpstreamRepositories verifies upstream repos are persisted
// and returned in DescribeRepository.
