package gcpartifactregistry_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func genericRepo() *artifactregistrypb.Repository {
	return &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_GENERIC}
}

func restService(t *testing.T, e *env) *arapi.Service {
	t.Helper()

	svc, err := arapi.NewService(t.Context(),
		option.WithEndpoint("http://"+e.handler.Addr()+"/"), option.WithoutAuthentication())
	require.NoError(t, err)

	return svc
}

func upload(
	t *testing.T, svc *arapi.Service, repo string, req *arapi.UploadGenericArtifactRequest, data []byte,
) (*arapi.UploadGenericArtifactMediaResponse, error) {
	t.Helper()

	return svc.Projects.Locations.Repositories.GenericArtifacts.Upload(repoName(repo), req).
		Media(bytes.NewReader(data)).Context(t.Context()).Do()
}

func download(t *testing.T, svc *arapi.Service, name string) ([]byte, error) {
	t.Helper()

	resp, err := svc.Projects.Locations.Repositories.Files.Download(name).Context(t.Context()).Download()
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	return io.ReadAll(resp.Body)
}

func httpCode(err error) int {
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		return ge.Code
	}

	return 0
}

func TestGenericUploadAndDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "gen", genericRepo())

	svc, ctx := restService(t, e), t.Context()
	content := []byte("hello generic artifact")
	fileID := "tool:1.0.0:tool.zip"

	resp, err := upload(t, svc, "gen", &arapi.UploadGenericArtifactRequest{
		PackageId: "tool", VersionId: "1.0.0", Filename: "tool.zip",
	}, content)
	require.NoError(t, err)
	assert.True(t, resp.Operation.Done)

	t.Run("files", func(t *testing.T) {
		t.Parallel()

		files := listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: repoName("gen")})
		require.Len(t, files, 1)
		assert.Equal(t, fileName("gen", fileID), files[0].GetName())
		assert.EqualValues(t, len(content), files[0].GetSizeBytes())
		assert.Equal(t, pkgName("gen", "tool")+"/versions/1.0.0", files[0].GetOwner())
		assert.Equal(t, "sha256:"+hexOf(files[0].GetHashes()[0].GetValue()), sha256Of(content))

		got, getErr := e.client.GetFile(ctx, &artifactregistrypb.GetFileRequest{Name: fileName("gen", fileID)})
		require.NoError(t, getErr)
		assert.Equal(t, files[0].GetName(), got.GetName())
	})

	t.Run("download", func(t *testing.T) {
		t.Parallel()

		data, dlErr := download(t, svc, fileName("gen", fileID))
		require.NoError(t, dlErr)
		assert.Equal(t, content, data)

		_, dlErr = download(t, svc, fileName("gen", "tool:1.0.0:other.zip"))
		assert.Equal(t, http.StatusNotFound, httpCode(dlErr))
	})

	t.Run("packages and versions", func(t *testing.T) {
		t.Parallel()

		pkg, pkgErr := e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkgName("gen", "tool")})
		require.NoError(t, pkgErr)
		assert.Equal(t, "tool", pkg.GetDisplayName())

		ver, verErr := e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{
			Name: pkgName("gen", "tool") + "/versions/1.0.0",
		})
		require.NoError(t, verErr)
		assert.NotNil(t, ver.GetCreateTime())

		repo, repoErr := e.client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: repoName("gen")})
		require.NoError(t, repoErr)
		assert.EqualValues(t, len(content), repo.GetSizeBytes())
	})

	t.Run("rejected", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			req  *arapi.UploadGenericArtifactRequest
			name string
			code int
		}{
			{&arapi.UploadGenericArtifactRequest{PackageId: "tool", VersionId: "1.0.0", Filename: "tool.zip"},
				"duplicate", http.StatusConflict},
			{&arapi.UploadGenericArtifactRequest{PackageId: "tool", VersionId: "latest", Filename: "a"},
				"latest", http.StatusBadRequest},
			{&arapi.UploadGenericArtifactRequest{PackageId: "tool", VersionId: "V1", Filename: "a"},
				"uppercase version", http.StatusBadRequest},
			{&arapi.UploadGenericArtifactRequest{PackageId: "-tool", VersionId: "1", Filename: "a"},
				"bad package", http.StatusBadRequest},
			{&arapi.UploadGenericArtifactRequest{PackageId: "tool", VersionId: "1", Filename: "a/b"},
				"bad filename", http.StatusBadRequest},
		}

		for _, tt := range tests {
			_, upErr := upload(t, svc, "gen", tt.req, []byte("x"))
			assert.Equal(t, tt.code, httpCode(upErr), tt.name)
		}

		e.createRepo(t, "dock", dockerRepo())

		_, upErr := upload(t, svc, "dock", &arapi.UploadGenericArtifactRequest{
			PackageId: "tool", VersionId: "1", Filename: "a",
		}, []byte("x"))
		assert.Equal(t, http.StatusBadRequest, httpCode(upErr))

		_, upErr = upload(t, svc, "ghost", &arapi.UploadGenericArtifactRequest{
			PackageId: "tool", VersionId: "1", Filename: "a",
		}, []byte("x"))
		assert.Equal(t, http.StatusNotFound, httpCode(upErr))
	})
}

func TestGenericFileLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "life", genericRepo())

	svc, ctx := restService(t, e), t.Context()

	for _, f := range []struct{ pkg, ver, file, body string }{
		{"alpha", "1.0", "a.txt", "aaa"}, {"alpha", "1.0", "b.txt", "bb"}, {"alpha", "2.0", "a.txt", "a2"},
		{"beta", "1.0", "c.txt", "c"},
	} {
		_, err := upload(t, svc, "life", &arapi.UploadGenericArtifactRequest{
			PackageId: f.pkg, VersionId: f.ver, Filename: f.file,
		}, []byte(f.body))
		require.NoError(t, err)
	}

	parent := repoName("life")
	owner1 := pkgName("life", "alpha") + "/versions/1.0"

	assert.Len(t, listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent}), 4)
	assert.Len(
		t,
		listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, Filter: `owner="` + owner1 + `"`}),
		2,
	)
	assert.Len(t, listFiles(t, e, &artifactregistrypb.ListFilesRequest{
		Parent: parent, Filter: `name="` + parent + `/files/alpha:*"`,
	}), 3)

	t.Run("update annotations", func(t *testing.T) {
		t.Parallel()

		name := fileName("life", "beta:1.0:c.txt")

		got, err := e.client.UpdateFile(ctx, &artifactregistrypb.UpdateFileRequest{
			File:       &artifactregistrypb.File{Name: name, Annotations: map[string]string{"k": "v"}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"annotations"}},
		})
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"k": "v"}, got.GetAnnotations())
		assert.Len(
			t,
			listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: parent, Filter: "annotations.k:v"}),
			1,
		)
	})

	t.Run("delete file", func(t *testing.T) {
		t.Parallel()

		name := fileName("life", "alpha:2.0:a.txt")

		op, err := e.client.DeleteFile(ctx, &artifactregistrypb.DeleteFileRequest{Name: name})
		require.NoError(t, err)
		require.NoError(t, op.Wait(ctx))

		_, err = e.client.GetFile(ctx, &artifactregistrypb.GetFileRequest{Name: name})
		assert.Equal(t, codes.NotFound, status.Code(err))

		_, err = e.client.DeleteFile(ctx, &artifactregistrypb.DeleteFileRequest{Name: name})
		assert.Equal(t, codes.NotFound, status.Code(err))

		_, err = download(t, svc, name)
		assert.Equal(t, http.StatusNotFound, httpCode(err))
	})

	t.Run("update version and package", func(t *testing.T) {
		t.Parallel()

		ver, err := e.client.UpdateVersion(ctx, &artifactregistrypb.UpdateVersionRequest{
			Version: &artifactregistrypb.Version{
				Name:        owner1,
				Description: "stable",
				Annotations: map[string]string{"a": "b"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description", "annotations"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "stable", ver.GetDescription())
		assert.Equal(t, map[string]string{"a": "b"}, ver.GetAnnotations())

		pkg, err := e.client.UpdatePackage(ctx, &artifactregistrypb.UpdatePackageRequest{
			Package:    &artifactregistrypb.Package{Name: pkgName("life", "beta"), DisplayName: "Beta"},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "Beta", pkg.GetDisplayName())

		_, err = e.client.UpdateVersion(ctx, &artifactregistrypb.UpdateVersionRequest{
			Version: &artifactregistrypb.Version{Name: pkgName("life", "beta") + "/versions/9"},
		})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestGenericDeleteCascades(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "cas", genericRepo())

	svc, ctx := restService(t, e), t.Context()

	up := func(pkg, ver, file string) {
		_, err := upload(t, svc, "cas", &arapi.UploadGenericArtifactRequest{
			PackageId: pkg, VersionId: ver, Filename: file,
		}, []byte(pkg+ver+file))
		require.NoError(t, err)
	}

	up("p1", "1", "f")
	up("p1", "2", "f")
	up("p2", "1", "f")
	up("p3", "1", "f")
	up("p3", "2", "f")

	count := func() int {
		return len(listFiles(t, e, &artifactregistrypb.ListFilesRequest{Parent: repoName("cas")}))
	}

	require.Equal(t, 5, count())

	op, err := e.client.DeleteVersion(
		ctx,
		&artifactregistrypb.DeleteVersionRequest{Name: pkgName("cas", "p1") + "/versions/1"},
	)
	require.NoError(t, err)
	require.NoError(t, op.Wait(ctx))
	assert.Equal(t, 4, count())

	pkgOp, err := e.client.DeletePackage(ctx, &artifactregistrypb.DeletePackageRequest{Name: pkgName("cas", "p2")})
	require.NoError(t, err)
	require.NoError(t, pkgOp.Wait(ctx))
	assert.Equal(t, 3, count())

	_, err = e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkgName("cas", "p2")})
	assert.Equal(t, codes.NotFound, status.Code(err))

	batchOp, err := e.client.BatchDeleteVersions(ctx, &artifactregistrypb.BatchDeleteVersionsRequest{
		Parent: pkgName("cas", "p3"),
		Names:  []string{pkgName("cas", "p3") + "/versions/1", pkgName("cas", "p3") + "/versions/2"},
	})
	require.NoError(t, err)
	require.NoError(t, batchOp.Wait(ctx))
	assert.Equal(t, 1, count())

	delOp, err := e.client.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: repoName("cas")})
	require.NoError(t, err)
	require.NoError(t, delOp.Wait(ctx))

	e.createRepo(t, "cas", genericRepo())
	assert.Equal(t, 0, count(), "a recreated repository starts empty")
}

func TestGenericDownloadRule(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "rl", genericRepo())

	svc := restService(t, e)

	for _, pkg := range []string{"open", "closed"} {
		_, err := upload(t, svc, "rl", &arapi.UploadGenericArtifactRequest{
			PackageId: pkg, VersionId: "1", Filename: "f",
		}, []byte(pkg))
		require.NoError(t, err)
	}

	_, err := e.client.CreateRule(t.Context(), &artifactregistrypb.CreateRuleRequest{
		Parent: repoName("rl"), RuleId: "deny-closed", Rule: ruleOf(artifactregistrypb.Rule_DENY, "closed"),
	})
	require.NoError(t, err)

	data, err := download(t, svc, fileName("rl", "open:1:f"))
	require.NoError(t, err)
	assert.Equal(t, []byte("open"), data)

	_, err = download(t, svc, fileName("rl", "closed:1:f"))
	assert.Equal(t, http.StatusForbidden, httpCode(err))
}

func sha256Of(b []byte) string {
	sum := sha256.Sum256(b)

	return "sha256:" + hex.EncodeToString(sum[:])
}
