package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func pkgName(repo, image string) string { return repoName(repo) + "/packages/" + image }

type pushed struct {
	first, second ocitest.Image
}

// pushTwo pushes team/app:v1, team/app:v2 and other:latest into repo.
func pushTwo(t *testing.T, e *env, repo string) pushed {
	t.Helper()

	c := e.docker(t)
	base := "/v2/" + project + "/" + repo + "/"
	_ = base

	return pushed{
		first:  pushTo(c, repo, "team/app", "v1"),
		second: pushTo(c, repo, "team/app", "v2"),
	}
}

func pushTo(c ocitest.Client, repo, image, tag string) ocitest.Image {
	img := c.PushImage(project+"/"+repo+"/"+image, tag)

	return img
}

func TestAPISeesPushedImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "images", dockerRepo())

	p := pushTwo(t, e, "images")
	pushTo(e.docker(t), "images", "other", "latest")

	ctx := t.Context()

	t.Run("packages", func(t *testing.T) {
		t.Parallel()

		it := e.client.ListPackages(ctx, &artifactregistrypb.ListPackagesRequest{Parent: repoName("images")})

		var names []string

		for {
			pk, err := it.Next()
			if err != nil {
				require.ErrorIs(t, err, iterator.Done)

				break
			}

			names = append(names, pk.GetName())
		}

		assert.Equal(t, []string{pkgName("images", "other"), pkgName("images", "team%2Fapp")}, names)

		got, err := e.client.GetPackage(
			ctx,
			&artifactregistrypb.GetPackageRequest{Name: pkgName("images", "team%2Fapp")},
		)
		require.NoError(t, err)
		assert.Equal(t, "team/app", got.GetDisplayName())
		assert.NotNil(t, got.GetCreateTime())

		_, err = e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkgName("images", "ghost")})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("versions", func(t *testing.T) {
		t.Parallel()

		parent := pkgName("images", "team%2Fapp")
		it := e.client.ListVersions(ctx, &artifactregistrypb.ListVersionsRequest{
			Parent: parent, View: artifactregistrypb.VersionView_FULL,
		})

		var versions []*artifactregistrypb.Version

		for {
			v, err := it.Next()
			if err != nil {
				break
			}

			versions = append(versions, v)
		}

		require.Len(t, versions, 2)

		byName := map[string]*artifactregistrypb.Version{}
		for _, v := range versions {
			byName[v.GetName()] = v
		}

		v1 := byName[parent+"/versions/"+p.first.Digest]
		require.NotNil(t, v1)
		require.Len(t, v1.GetRelatedTags(), 1)
		assert.Equal(t, parent+"/tags/v1", v1.GetRelatedTags()[0].GetName())
		assert.Equal(t, ocitest.ManifestMediaType, v1.GetMetadata().GetFields()["mediaType"].GetStringValue())
		assert.NotEmpty(t, v1.GetMetadata().GetFields()["imageSizeBytes"].GetStringValue())

		basic, err := e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: v1.GetName()})
		require.NoError(t, err)
		assert.Nil(t, basic.GetMetadata(), "metadata only in the FULL view")

		_, err = e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: parent + "/versions/sha256:none"})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("docker images", func(t *testing.T) {
		t.Parallel()

		it := e.client.ListDockerImages(ctx, &artifactregistrypb.ListDockerImagesRequest{Parent: repoName("images")})

		var images []*artifactregistrypb.DockerImage

		for {
			img, err := it.Next()
			if err != nil {
				break
			}

			images = append(images, img)
		}

		require.Len(t, images, 3)

		byDigest, err := e.client.GetDockerImage(ctx, &artifactregistrypb.GetDockerImageRequest{
			Name: repoName("images") + "/dockerImages/team%2Fapp@" + p.first.Digest,
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"v1"}, byDigest.GetTags())
		assert.Equal(t, "localhost:"+e.port+"/"+project+"/images/team/app@"+p.first.Digest, byDigest.GetUri())
		assert.Equal(t, ocitest.ManifestMediaType, byDigest.GetMediaType())
		assert.Greater(t, byDigest.GetImageSizeBytes(), int64(len(p.first.Layer)))

		byTag, err := e.client.GetDockerImage(ctx, &artifactregistrypb.GetDockerImageRequest{
			Name: repoName("images") + "/dockerImages/team%2Fapp:v2",
		})
		require.NoError(t, err)
		assert.Equal(t, p.second.Digest, byTag.GetUri()[len(byTag.GetUri())-len(p.second.Digest):])

		_, err = e.client.GetDockerImage(ctx, &artifactregistrypb.GetDockerImageRequest{
			Name: repoName("images") + "/dockerImages/team%2Fapp:none",
		})
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("repository size", func(t *testing.T) {
		t.Parallel()

		got, err := e.client.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: repoName("images")})
		require.NoError(t, err)
		assert.Positive(t, got.GetSizeBytes())
	})
}

func TestTagOperations(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "tags", dockerRepo())

	p := pushTwo(t, e, "tags")
	pkg := pkgName("tags", "team%2Fapp")
	v1 := pkg + "/versions/" + p.first.Digest
	v2 := pkg + "/versions/" + p.second.Digest
	ctx := t.Context()

	created, err := e.client.CreateTag(ctx, &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "stable", Tag: &artifactregistrypb.Tag{Version: v1},
	})
	require.NoError(t, err)
	assert.Equal(t, pkg+"/tags/stable", created.GetName())

	// The new tag is visible to docker clients.
	got := e.docker(t).Do(http.MethodGet, "/v2/"+project+"/tags/team/app/manifests/stable", nil, nil)
	assert.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, p.first.Body, got.Body)

	_, err = e.client.CreateTag(ctx, &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "stable", Tag: &artifactregistrypb.Tag{Version: v1},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.CreateTag(ctx, &artifactregistrypb.CreateTagRequest{
		Parent: pkg, TagId: "ghost", Tag: &artifactregistrypb.Tag{Version: pkg + "/versions/sha256:00"},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.CreateTag(ctx, &artifactregistrypb.CreateTagRequest{
		Parent: pkg,
		TagId:  "elsewhere",
		Tag:    &artifactregistrypb.Tag{Version: pkgName("tags", "other") + "/versions/x"},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	moved, err := e.client.UpdateTag(ctx, &artifactregistrypb.UpdateTagRequest{
		Tag:        &artifactregistrypb.Tag{Name: pkg + "/tags/stable", Version: v2},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"version"}},
	})
	require.NoError(t, err)
	assert.Equal(t, v2, moved.GetVersion())

	list := e.client.ListTags(ctx, &artifactregistrypb.ListTagsRequest{Parent: pkg, Filter: `version="` + v2 + `"`})

	var tags []string

	for {
		tg, nextErr := list.Next()
		if nextErr != nil {
			break
		}

		tags = append(tags, tg.GetName())
	}

	assert.Equal(t, []string{pkg + "/tags/stable", pkg + "/tags/v2"}, tags)

	tg, err := e.client.GetTag(ctx, &artifactregistrypb.GetTagRequest{Name: pkg + "/tags/v1"})
	require.NoError(t, err)
	assert.Equal(t, v1, tg.GetVersion())

	require.NoError(t, e.client.DeleteTag(ctx, &artifactregistrypb.DeleteTagRequest{Name: pkg + "/tags/stable"}))

	_, err = e.client.GetTag(ctx, &artifactregistrypb.GetTagRequest{Name: pkg + "/tags/stable"})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, http.StatusNotFound,
		e.docker(t).Do(http.MethodGet, "/v2/"+project+"/tags/team/app/manifests/stable", nil, nil).Status)

	_, err = e.client.ListTags(ctx, &artifactregistrypb.ListTagsRequest{Parent: pkg, Filter: "name=x"}).Next()
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDeleteVersionsAndPackages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "deletes", dockerRepo())

	p := pushTwo(t, e, "deletes")
	pkg := pkgName("deletes", "team%2Fapp")
	ctx := t.Context()

	op, err := e.client.DeleteVersion(
		ctx,
		&artifactregistrypb.DeleteVersionRequest{Name: pkg + "/versions/" + p.first.Digest},
	)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "tagged versions need force")
	assert.Nil(t, op)

	op, err = e.client.DeleteVersion(ctx, &artifactregistrypb.DeleteVersionRequest{
		Name: pkg + "/versions/" + p.first.Digest, Force: true,
	})
	require.NoError(t, err)
	require.NoError(t, op.Wait(ctx))

	assert.Equal(t, http.StatusNotFound,
		e.docker(t).Do(http.MethodGet, "/v2/"+project+"/deletes/team/app/manifests/v1", nil, nil).Status)

	bad, err := e.client.BatchDeleteVersions(ctx, &artifactregistrypb.BatchDeleteVersionsRequest{
		Parent: pkg, Names: []string{pkg + "/versions/sha256:none"},
	})
	require.Error(t, err)
	assert.Nil(t, bad)

	dry, err := e.client.BatchDeleteVersions(ctx, &artifactregistrypb.BatchDeleteVersionsRequest{
		Parent: pkg, Names: []string{pkg + "/versions/" + p.second.Digest}, ValidateOnly: true,
	})
	require.NoError(t, err)
	require.NoError(t, dry.Wait(ctx))

	_, err = e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: pkg + "/versions/" + p.second.Digest})
	require.NoError(t, err, "validate_only deletes nothing")

	batch, err := e.client.BatchDeleteVersions(ctx, &artifactregistrypb.BatchDeleteVersionsRequest{
		Parent: pkg, Names: []string{pkg + "/versions/" + p.second.Digest},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Wait(ctx))

	_, err = e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkg})
	assert.Equal(t, codes.NotFound, status.Code(err), "a package disappears with its last version")

	pushTo(e.docker(t), "deletes", "gone", "x")

	del, err := e.client.DeletePackage(ctx, &artifactregistrypb.DeletePackageRequest{Name: pkgName("deletes", "gone")})
	require.NoError(t, err)
	require.NoError(t, del.Wait(ctx))

	_, err = e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkgName("deletes", "gone")})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDeleteRepositoryRemovesImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "doomed", dockerRepo())
	pushTo(e.docker(t), "doomed", "app", "v1")

	op, err := e.client.DeleteRepository(
		t.Context(),
		&artifactregistrypb.DeleteRepositoryRequest{Name: repoName("doomed")},
	)
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	assert.Equal(t, http.StatusNotFound,
		e.docker(t).Do(http.MethodGet, "/v2/"+project+"/doomed/app/manifests/v1", nil, nil).Status)

	// A repository created again under the same id starts empty.
	e.createRepo(t, "doomed", dockerRepo())

	_, err = e.client.GetPackage(t.Context(), &artifactregistrypb.GetPackageRequest{Name: pkgName("doomed", "app")})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
