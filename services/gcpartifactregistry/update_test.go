package gcpartifactregistry_test

import (
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestUpdatePackageAndVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "upd", dockerRepo())

	img := pushTo(e.docker(t), "upd", "app", "v1")
	pkg := pkgName("upd", "app")
	ver := pkg + "/versions/" + img.Digest
	ctx := t.Context()

	pkgTests := []struct {
		in      *artifactregistrypb.Package
		ann     map[string]string
		name    string
		display string
		mask    []string
		code    codes.Code
	}{
		{name: "display name", in: &artifactregistrypb.Package{Name: pkg, DisplayName: "Application"},
			mask: []string{"display_name"}, display: "Application"},
		{
			name:    "annotations keep display",
			in:      &artifactregistrypb.Package{Name: pkg, Annotations: map[string]string{"a": "b"}},
			mask:    []string{"annotations"},
			display: "Application",
			ann:     map[string]string{"a": "b"},
		},
		{name: "empty mask copies set fields", in: &artifactregistrypb.Package{Name: pkg, DisplayName: "App2"},
			display: "App2", ann: map[string]string{"a": "b"}},
		{name: "bad mask", in: &artifactregistrypb.Package{Name: pkg}, mask: []string{"create_time"},
			code: codes.InvalidArgument},
		{name: "missing", in: &artifactregistrypb.Package{Name: pkgName("upd", "ghost")},
			mask: []string{"annotations"}, code: codes.NotFound},
	}

	for _, tt := range pkgTests {
		got, err := e.client.UpdatePackage(ctx, &artifactregistrypb.UpdatePackageRequest{
			Package: tt.in, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
		})
		require.Equal(t, tt.code, status.Code(err), tt.name)

		if tt.code != codes.OK {
			continue
		}

		assert.Equal(t, tt.display, got.GetDisplayName(), tt.name)
		assert.Equal(t, tt.ann, got.GetAnnotations(), tt.name)

		again, getErr := e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkg})
		require.NoError(t, getErr)
		assert.Equal(t, tt.display, again.GetDisplayName(), tt.name)
	}

	verTests := []struct {
		in   *artifactregistrypb.Version
		ann  map[string]string
		name string
		desc string
		mask []string
		code codes.Code
	}{
		{name: "description", in: &artifactregistrypb.Version{Name: ver, Description: "release"},
			mask: []string{"description"}, desc: "release"},
		{name: "annotations", in: &artifactregistrypb.Version{Name: ver, Annotations: map[string]string{"c": "d"}},
			mask: []string{"annotations"}, desc: "release", ann: map[string]string{"c": "d"}},
		{name: "bad mask", in: &artifactregistrypb.Version{Name: ver}, mask: []string{"name"},
			code: codes.InvalidArgument},
		{name: "missing", in: &artifactregistrypb.Version{Name: pkg + "/versions/sha256:00"},
			mask: []string{"description"}, code: codes.NotFound},
	}

	for _, tt := range verTests {
		got, err := e.client.UpdateVersion(ctx, &artifactregistrypb.UpdateVersionRequest{
			Version: tt.in, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
		})
		require.Equal(t, tt.code, status.Code(err), tt.name)

		if tt.code != codes.OK {
			continue
		}

		assert.Equal(t, tt.desc, got.GetDescription(), tt.name)
		assert.Equal(t, tt.ann, got.GetAnnotations(), tt.name)

		again, getErr := e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: ver})
		require.NoError(t, getErr)
		assert.Equal(t, tt.desc, again.GetDescription(), tt.name)
	}

	_, err := e.client.DeletePackage(ctx, &artifactregistrypb.DeletePackageRequest{Name: pkg})
	require.NoError(t, err)

	pushTo(e.docker(t), "upd", "app", "v2")

	again, err := e.client.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: pkg})
	require.NoError(t, err)
	assert.Equal(t, "app", again.GetDisplayName(), "edits do not outlive the package")
	assert.Empty(t, again.GetAnnotations())
}
