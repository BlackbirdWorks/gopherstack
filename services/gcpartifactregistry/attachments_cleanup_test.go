package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func (e *env) attach(t *testing.T, repo, id string, att *artifactregistrypb.Attachment) *artifactregistrypb.Attachment {
	t.Helper()

	op, err := e.client.CreateAttachment(t.Context(), &artifactregistrypb.CreateAttachmentRequest{
		Parent: repoName(repo), AttachmentId: id, Attachment: att,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func (e *env) attachmentGone(t *testing.T, repo, id string) bool {
	t.Helper()

	_, err := e.client.GetAttachment(t.Context(), &artifactregistrypb.GetAttachmentRequest{
		Name: repoName(repo) + "/attachments/" + id,
	})

	return status.Code(err) == codes.NotFound
}

func TestGenericAttachmentCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		del  func(t *testing.T, e *env, version, file string)
		name string
		gone []string
	}{
		{
			name: "version",
			del: func(t *testing.T, e *env, version, _ string) {
				t.Helper()

				op, err := e.client.DeleteVersion(t.Context(), &artifactregistrypb.DeleteVersionRequest{Name: version})
				require.NoError(t, err)
				require.NoError(t, op.Wait(t.Context()))
			},
			gone: []string{"on-version", "on-file", "on-repo"},
		},
		{
			name: "package",
			del: func(t *testing.T, e *env, _, _ string) {
				t.Helper()

				op, err := e.client.DeletePackage(t.Context(), &artifactregistrypb.DeletePackageRequest{
					Name: pkgName("gen", "tool"),
				})
				require.NoError(t, err)
				require.NoError(t, op.Wait(t.Context()))
			},
			gone: []string{"on-version", "on-package", "on-file", "on-repo"},
		},
		{
			name: "file",
			del: func(t *testing.T, e *env, _, file string) {
				t.Helper()

				op, err := e.client.DeleteFile(t.Context(), &artifactregistrypb.DeleteFileRequest{Name: file})
				require.NoError(t, err)
				require.NoError(t, op.Wait(t.Context()))
			},
			gone: []string{"on-version", "on-package", "on-repo", "on-file"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, version, file := attachEnv(t, nil)
			pkg := pkgName("gen", "tool")

			e.attach(t, "gen", "on-version", &artifactregistrypb.Attachment{Target: version, Files: []string{file}})
			e.attach(t, "gen", "on-package", &artifactregistrypb.Attachment{Target: pkg, Files: []string{file}})
			e.attach(
				t,
				"gen",
				"on-repo",
				&artifactregistrypb.Attachment{Target: repoName("gen"), Files: []string{file}},
			)
			e.attach(t, "gen", "on-file", &artifactregistrypb.Attachment{Target: version, Files: []string{file}})

			tt.del(t, e, version, file)

			for _, id := range tt.gone {
				assert.True(t, e.attachmentGone(t, "gen", id), id)
			}
		})
	}
}

func TestDockerAttachmentOCIVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "dock", dockerRepo())

	c := e.docker(t)
	img := pushTo(c, "dock", "app", "v1")
	version := pkgName("dock", "app") + "/versions/" + img.Digest
	layer := fileName("dock", ocitest.Digest(img.Layer))

	manifestAtt := e.attach(t, "dock", "sbom", &artifactregistrypb.Attachment{
		Target: version, Type: "application/spdx+json", Files: []string{fileName("dock", img.Digest)},
	})
	assert.Equal(t, version, manifestAtt.GetOciVersionName())

	blobAtt := e.attach(t, "dock", "blob", &artifactregistrypb.Attachment{Target: version, Files: []string{layer}})
	assert.Empty(t, blobAtt.GetOciVersionName())

	del := c.Do(http.MethodDelete, "/v2/"+project+"/dock/app/manifests/"+img.Digest, nil, nil)
	require.Equal(t, http.StatusAccepted, del.Status, string(del.Body))

	assert.True(t, e.attachmentGone(t, "dock", "sbom"))
	assert.True(t, e.attachmentGone(t, "dock", "blob"))
}
