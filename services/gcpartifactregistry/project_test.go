package gcpartifactregistry_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	arapi "google.golang.org/api/artifactregistry/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestProjectSettings(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	ctx, name := t.Context(), "projects/"+project+"/projectSettings"

	def, err := e.client.GetProjectSettings(ctx, &artifactregistrypb.GetProjectSettingsRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, name, def.GetName())
	assert.Equal(
		t,
		artifactregistrypb.ProjectSettings_REDIRECTION_FROM_GCR_IO_DISABLED,
		def.GetLegacyRedirectionState(),
	)

	tests := []struct {
		in    *artifactregistrypb.ProjectSettings
		name  string
		mask  []string
		state artifactregistrypb.ProjectSettings_RedirectionState
		pull  int32
		code  codes.Code
	}{
		{
			&artifactregistrypb.ProjectSettings{
				Name:                   name,
				LegacyRedirectionState: artifactregistrypb.ProjectSettings_REDIRECTION_FROM_GCR_IO_ENABLED,
			},
			"state",
			[]string{"legacy_redirection_state"},
			artifactregistrypb.ProjectSettings_REDIRECTION_FROM_GCR_IO_ENABLED,
			0,
			codes.OK,
		},
		{&artifactregistrypb.ProjectSettings{Name: name, PullPercent: 40}, "pull percent", []string{"pull_percent"},
			artifactregistrypb.ProjectSettings_REDIRECTION_FROM_GCR_IO_ENABLED, 40, codes.OK},
		{&artifactregistrypb.ProjectSettings{Name: name, PullPercent: 101}, "pull too large", []string{"pull_percent"},
			0, 0, codes.InvalidArgument},
		{&artifactregistrypb.ProjectSettings{Name: name, LegacyRedirectionState: 99}, "unknown state", nil,
			0, 0, codes.InvalidArgument},
		{&artifactregistrypb.ProjectSettings{Name: name}, "bad mask", []string{"name"}, 0, 0, codes.InvalidArgument},
		{
			&artifactregistrypb.ProjectSettings{Name: "projects/x/settings"},
			"bad name",
			nil,
			0,
			0,
			codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		got, updErr := e.client.UpdateProjectSettings(ctx, &artifactregistrypb.UpdateProjectSettingsRequest{
			ProjectSettings: tt.in, UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.mask},
		})
		require.Equal(t, tt.code, status.Code(updErr), tt.name)

		if tt.code == codes.OK {
			assert.Equal(t, tt.state, got.GetLegacyRedirectionState(), tt.name)
			assert.Equal(t, tt.pull, got.GetPullPercent(), tt.name)
		}
	}

	again, err := e.client.GetProjectSettings(ctx, &artifactregistrypb.GetProjectSettingsRequest{Name: name})
	require.NoError(t, err)
	assert.EqualValues(t, 40, again.GetPullPercent())
}

func TestVPCSCConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	ctx, name := t.Context(), loc+"/vpcscConfig"

	def, err := e.client.GetVPCSCConfig(ctx, &artifactregistrypb.GetVPCSCConfigRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.VPCSCConfig_ALLOW, def.GetVpcscPolicy())

	got, err := e.client.UpdateVPCSCConfig(ctx, &artifactregistrypb.UpdateVPCSCConfigRequest{
		VpcscConfig: &artifactregistrypb.VPCSCConfig{Name: name, VpcscPolicy: artifactregistrypb.VPCSCConfig_DENY},
		UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"vpcsc_policy"}},
	})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.VPCSCConfig_DENY, got.GetVpcscPolicy())

	again, err := e.client.GetVPCSCConfig(ctx, &artifactregistrypb.GetVPCSCConfigRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.VPCSCConfig_DENY, again.GetVpcscPolicy())

	other, err := e.client.GetVPCSCConfig(ctx, &artifactregistrypb.GetVPCSCConfigRequest{
		Name: "projects/" + project + "/locations/europe-west1/vpcscConfig",
	})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.VPCSCConfig_ALLOW, other.GetVpcscPolicy())

	for _, in := range []*artifactregistrypb.VPCSCConfig{
		{Name: name, VpcscPolicy: 9}, {Name: "projects/p/vpcscConfig"}, {Name: loc + "/vpcscConfig", VpcscPolicy: 0},
	} {
		_, err = e.client.UpdateVPCSCConfig(ctx, &artifactregistrypb.UpdateVPCSCConfigRequest{
			VpcscConfig: in, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"vpcsc_policy"}},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), in.GetName())
	}
}

func TestExtrasArePersisted(t *testing.T) {
	t.Parallel()

	before := newEnv(t, "")
	before.createRepo(t, "gen", genericRepo())
	before.createRepo(t, "img", dockerRepo())

	svc, ctx := restService(t, before), t.Context()
	img := pushTo(before.docker(t), "img", "app", "v1")

	_, err := upload(t, svc, "gen", &arapi.UploadGenericArtifactRequest{
		PackageId: "tool", VersionId: "1", Filename: "f.bin",
	}, []byte("payload"))
	require.NoError(t, err)

	_, err = before.client.UpdateFile(ctx, &artifactregistrypb.UpdateFileRequest{
		File: &artifactregistrypb.File{
			Name:        fileName("gen", "tool:1:f.bin"),
			Annotations: map[string]string{"g": "1"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"annotations"}},
	})
	require.NoError(t, err)

	_, err = before.client.UpdateVersion(ctx, &artifactregistrypb.UpdateVersionRequest{
		Version: &artifactregistrypb.Version{
			Name:        pkgName("img", "app") + "/versions/" + img.Digest,
			Description: "kept",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	require.NoError(t, err)

	_, err = before.client.CreateRule(ctx, &artifactregistrypb.CreateRuleRequest{
		Parent: repoName("gen"), RuleId: "r1", Rule: ruleOf(artifactregistrypb.Rule_DENY, "tool"),
	})
	require.NoError(t, err)

	_, err = before.client.UpdateProjectSettings(ctx, &artifactregistrypb.UpdateProjectSettingsRequest{
		ProjectSettings: &artifactregistrypb.ProjectSettings{
			Name: "projects/" + project + "/projectSettings", PullPercent: 7,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"pull_percent"}},
	})
	require.NoError(t, err)

	_, err = before.client.UpdateVPCSCConfig(ctx, &artifactregistrypb.UpdateVPCSCConfigRequest{
		VpcscConfig: &artifactregistrypb.VPCSCConfig{
			Name:        loc + "/vpcscConfig",
			VpcscPolicy: artifactregistrypb.VPCSCConfig_DENY,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"vpcsc_policy"}},
	})
	require.NoError(t, err)

	after := newEnv(t, "")
	require.NoError(t, after.handler.Restore(ctx, before.handler.Snapshot(ctx)))

	f, err := after.client.GetFile(ctx, &artifactregistrypb.GetFileRequest{Name: fileName("gen", "tool:1:f.bin")})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"g": "1"}, f.GetAnnotations())
	assert.EqualValues(t, len("payload"), f.GetSizeBytes())

	data, err := download(t, restService(t, after), f.GetName())
	require.Error(t, err, "the restored rule still denies the package")
	assert.Nil(t, data)

	require.NoError(
		t,
		after.client.DeleteRule(ctx, &artifactregistrypb.DeleteRuleRequest{Name: repoName("gen") + "/rules/r1"}),
	)

	data, err = download(t, restService(t, after), f.GetName())
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data)

	ver, err := after.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{
		Name: pkgName("img", "app") + "/versions/" + img.Digest,
	})
	require.NoError(t, err)
	assert.Equal(t, "kept", ver.GetDescription())

	ps, err := after.client.GetProjectSettings(ctx, &artifactregistrypb.GetProjectSettingsRequest{
		Name: "projects/" + project + "/projectSettings",
	})
	require.NoError(t, err)
	assert.EqualValues(t, 7, ps.GetPullPercent())

	vpc, err := after.client.GetVPCSCConfig(ctx, &artifactregistrypb.GetVPCSCConfigRequest{Name: loc + "/vpcscConfig"})
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.VPCSCConfig_DENY, vpc.GetVpcscPolicy())

	after.handler.Reset()

	_, err = after.client.GetFile(ctx, &artifactregistrypb.GetFileRequest{Name: fileName("gen", "tool:1:f.bin")})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestDockerFileDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "dl", dockerRepo())

	img := pushTo(e.docker(t), "dl", "app", "v1")
	svc := restService(t, e)

	tests := []struct {
		name string
		id   string
		want []byte
	}{
		{"manifest", img.Digest, img.Body},
		{"layer", ocitest.Digest(img.Layer), img.Layer},
	}

	for _, tt := range tests {
		got, err := download(t, svc, fileName("dl", tt.id))
		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.want, got, tt.name)
	}
}

func TestRESTFilesRulesSettings(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	base := "/v1/projects/p/locations/us-central1"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create", Method: http.MethodPost, Path: base + "/repositories?repositoryId=g1",
			Body: `{"format":"GENERIC"}`, Status: 200, Contains: []string{`"done":true`},
		},
		{
			Name:     "files",
			Method:   http.MethodGet,
			Path:     base + "/repositories/g1/files",
			Status:   200,
			Contains: []string{`{}`},
		},
		{
			Name: "file with colons", Method: http.MethodGet, Path: base + "/repositories/g1/files/pkg:1:f.txt",
			Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "bad filter", Method: http.MethodGet, Path: base + "/repositories/g1/files?filter=nonsense",
			Status: 400, Contains: []string{`"status":"INVALID_ARGUMENT"`},
		},
		{
			Name: "create rule", Method: http.MethodPost, Path: base + "/repositories/g1/rules?ruleId=r1",
			Body: `{"action":"DENY","operation":"DOWNLOAD","packageId":"pkg"}`, Status: 200,
			Contains: []string{`"name":"projects/p/locations/us-central1/repositories/g1/rules/r1"`, `"action":"DENY"`},
		},
		{
			Name: "patch rule", Method: http.MethodPatch, Path: base + "/repositories/g1/rules/r1?updateMask=action",
			Body: `{"action":"ALLOW"}`, Status: 200, Contains: []string{`"action":"ALLOW"`, `"packageId":"pkg"`},
		},
		{
			Name:     "list rules",
			Method:   http.MethodGet,
			Path:     base + "/repositories/g1/rules",
			Status:   200,
			Contains: []string{`"rules":[`},
		},
		{Name: "delete rule", Method: http.MethodDelete, Path: base + "/repositories/g1/rules/r1", Status: 200},
		{
			Name:   "project settings",
			Method: http.MethodGet,
			Path:   "/v1/projects/p/projectSettings",
			Status: 200,
			Contains: []string{
				`"name":"projects/p/projectSettings"`,
				`"legacyRedirectionState":"REDIRECTION_FROM_GCR_IO_DISABLED"`,
			},
		},
		{
			Name: "vpcsc", Method: http.MethodGet, Path: base + "/vpcscConfig", Status: 200,
			Contains: []string{`"vpcscPolicy":"ALLOW"`},
		},
		{
			Name:     "package patch",
			Method:   http.MethodPatch,
			Path:     base + "/repositories/g1/packages/none?updateMask=annotations",
			Body:     `{"annotations":{"a":"b"}}`,
			Status:   404,
			Contains: []string{`"status":"NOT_FOUND"`},
		},
	})
}
