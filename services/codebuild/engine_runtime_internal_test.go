package codebuild

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errAssume = errors.New("AccessDenied")

type fakeIssuer struct {
	err   error
	roles []string
}

func (f *fakeIssuer) IssueBuildCredentials(_ context.Context, role, _ string) (BuildCredentials, error) {
	f.roles = append(f.roles, role)
	if f.err != nil {
		return BuildCredentials{}, f.err
	}

	return BuildCredentials{
		AccessKeyID: "ASIATESTKEY", SecretAccessKey: "sekrit-key", SessionToken: "sekrit-token",
	}, nil
}

func specProject(spec string) ProjectConfig {
	return ProjectConfig{
		Source:      &ProjectSource{Type: "NO_SOURCE", Buildspec: spec},
		ServiceRole: "arn:aws:iam::000000000000:role/svc",
	}
}

func TestEngineInjectsEmulatorEndpointAndRoleCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		issuer     *fakeIssuer
		endpoint   string
		wantStatus string
		wantEnv    []string
		noEnv      []string
	}{
		{
			name: "endpoint and creds", issuer: &fakeIssuer{}, endpoint: "http://172.17.0.1:8000/",
			wantStatus: "SUCCEEDED",
			wantEnv: []string{
				"AWS_ENDPOINT_URL='http://172.17.0.1:8000'", "AWS_ACCESS_KEY_ID='ASIATESTKEY'",
				"AWS_SECRET_ACCESS_KEY='sekrit-key'", "AWS_SESSION_TOKEN='sekrit-token'",
				"AWS_REGION='us-east-1'", "CODEBUILD_BUILD_IMAGE='aws/codebuild/standard:7.0'",
			},
		},
		{
			name: "no endpoint no issuer", wantStatus: "SUCCEEDED",
			noEnv: []string{"AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID"},
		},
		{
			name: "assume role denied", issuer: &fakeIssuer{err: errAssume}, endpoint: "http://h:1",
			wantStatus: "FAILED", noEnv: []string{"AWS_ACCESS_KEY_ID"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime()
			h := newHarness(t, rt)

			svc := EngineServices{Store: h.store, Logs: h.logs, Values: fakeValues{}, Endpoint: tt.endpoint}
			if tt.issuer != nil {
				svc.Credentials = tt.issuer
			}

			h.b.SetEngineServices(svc)
			h.project(t, specProject("version: 0.2\nphases:\n  build:\n    commands: [echo hi]\n"))

			b := h.run(t, StartBuildConfig{})
			assert.Equal(t, tt.wantStatus, b.BuildStatus)

			for _, e := range tt.wantEnv {
				assert.True(t, rt.ran(e), "missing %s", e)
			}

			for _, e := range tt.noEnv {
				assert.False(t, rt.ran(e), "unexpected %s", e)
			}

			if tt.issuer != nil {
				assert.Equal(t, []string{"arn:aws:iam::000000000000:role/svc"}, tt.issuer.roles)
			}
		})
	}
}

func TestEngineMasksRoleCredentialsInLogs(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(fakeRule{match: "echo leak", out: "key=sekrit-key tok=sekrit-token\n"})
	h := newHarness(t, rt)
	h.b.SetEngineServices(EngineServices{
		Store: h.store, Logs: h.logs, Values: fakeValues{}, Credentials: &fakeIssuer{},
	})
	h.project(t, specProject("version: 0.2\nphases:\n  build:\n    commands: [echo leak]\n"))

	require.Equal(t, "SUCCEEDED", h.run(t, StartBuildConfig{}).BuildStatus)
	assert.NotContains(t, h.logs.joined(), "sekrit")
	assert.Contains(t, h.logs.joined(), "key=**** tok=****")
}

func TestEngineExportedVariables(t *testing.T) {
	t.Parallel()

	enc := base64.StdEncoding.EncodeToString([]byte("hello world"))
	spec := `version: 0.2
env:
  variables:
    PLAIN: x
  parameter-store:
    DB_PASS: /db/pass
  exported-variables:
    - MY_VAR
    - AWS_REGION
    - DB_PASS
    - 'BAD;NAME'
phases:
  build:
    commands: [echo hi]
`
	rt := newFakeRuntime(fakeRule{match: "base64", out: "MY_VAR=" + enc + "\n"})
	h := newHarness(t, rt)
	h.project(t, specProject(spec))

	b := h.run(t, StartBuildConfig{})
	require.Equal(t, "SUCCEEDED", b.BuildStatus)
	assert.Equal(t, []ExportedEnvironmentVariable{{Name: "MY_VAR", Value: "hello world"}}, b.ExportedEnvironmentVariables)

	var probe string

	for _, s := range rt.scripts {
		if strings.Contains(s, "base64") {
			probe = s
		}
	}

	assert.Contains(t, probe, "for n in MY_VAR;")
}

func TestEngineRunAsAndShell(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     string
		wantSh   string
		wantUser []string
	}{
		{
			name: "global and phase override",
			spec: "version: 0.2\nrun-as: builder\nphases:\n  install:\n    run-as: installer\n    commands: [echo A]\n" +
				"  build:\n    commands: [echo B]\n",
			wantUser: []string{"installer", "builder"},
		},
		{
			name:     "no run-as",
			spec:     "version: 0.2\nphases:\n  build:\n    commands: [echo B]\n",
			wantUser: []string{""},
		},
		{
			name:     "bin sh",
			spec:     "version: 0.2\nenv:\n  shell: /bin/sh\nphases:\n  build:\n    commands: [echo B]\n",
			wantUser: []string{""}, wantSh: "sh_bin='/bin/sh'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime()
			h := newHarness(t, rt)
			h.project(t, specProject(tt.spec))

			require.Equal(t, "SUCCEEDED", h.run(t, StartBuildConfig{}).BuildStatus)

			var users []string

			for _, c := range rt.cmds {
				if len(c) == 6 && strings.Contains(c[5], "Running command") {
					users = append(users, c[4])

					if tt.wantSh != "" {
						assert.Contains(t, c[2], tt.wantSh)
					}
				}
			}

			assert.Equal(t, tt.wantUser, users)
		})
	}
}

func TestParseBuildspecShellAndRuntimeVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "bash", spec: "version: 0.2\nenv:\n  shell: bash\n"},
		{name: "powershell", spec: "version: 0.2\nenv:\n  shell: powershell.exe\n"},
		{name: "zsh rejected", spec: "version: 0.2\nenv:\n  shell: zsh\n", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseBuildspecFile(tt.spec)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestEngineRuntimeVersionsLog(t *testing.T) {
	t.Parallel()

	spec := "version: 0.2\nenv:\n  variables:\n    PYV: '3.12'\nphases:\n  install:\n    runtime-versions:\n" +
		"      python: $PYV\n    commands: [echo hi]\n"

	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "standard image", image: "aws/codebuild/standard:7.0", want: "Selecting runtime python: 3.12"},
		{
			name: "custom image", image: "alpine:3.20",
			want: "Skipping install of runtimes. Runtime version selection is not supported by this build image",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t, newFakeRuntime())
			cfg := specProject(spec)
			cfg.Environment = &ProjectEnvironment{
				Type: "LINUX_CONTAINER", Image: tt.image, ComputeType: "BUILD_GENERAL1_SMALL",
			}
			h.project(t, cfg)

			require.Equal(t, "SUCCEEDED", h.run(t, StartBuildConfig{}).BuildStatus)
			assert.Contains(t, h.logs.joined(), tt.want)
		})
	}
}

func TestCacheNamespaceSharesScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		cache *ProjectCache
		proj  string
		want  string
	}{
		{name: "namespace", cache: &ProjectCache{CacheNamespace: "shared"}, proj: "a", want: "shared"},
		{name: "project", cache: &ProjectCache{}, proj: "a", want: "a"},
		{name: "nil cache", proj: "a", want: "a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, cacheScope(tt.cache, tt.proj))
		})
	}

	_, keyA, _ := s3CacheLocation("arn:aws:s3:::bkt/pfx", cacheScope(&ProjectCache{CacheNamespace: "ns"}, "a"))
	_, keyB, _ := s3CacheLocation("arn:aws:s3:::bkt/pfx", cacheScope(&ProjectCache{CacheNamespace: "ns"}, "b"))
	assert.Equal(t, keyA, keyB)
}

const exportSpec = "version: 0.2\nenv:\n  exported-variables: [EXP]\nphases:\n  build:\n    commands: [echo hi]\n"

func TestSDKCacheNamespaceAndExportedVariablesWire(t *testing.T) {
	t.Parallel()

	enc := base64.StdEncoding.EncodeToString([]byte("v1"))
	rt := newFakeRuntime(fakeRule{match: "base64", out: "EXP=" + enc + "\n"})
	h := newHarness(t, rt)
	c := sdkClientFor(t, h.b)

	createSDKProject(t, c, sdkProject{
		source: &cbtypes.ProjectSource{
			Type:      cbtypes.SourceTypeNoSource,
			Buildspec: aws.String(exportSpec),
		},
		cache: &cbtypes.ProjectCache{
			Type: cbtypes.CacheTypeLocal, CacheNamespace: aws.String("shared-ns"),
			Modes: []cbtypes.CacheMode{cbtypes.CacheModeLocalCustomCache},
		},
	})

	got, err := c.BatchGetProjects(t.Context(), &codebuildsdk.BatchGetProjectsInput{Names: []string{"proj"}})
	require.NoError(t, err)
	require.Len(t, got.Projects, 1)
	assert.Equal(t, "shared-ns", aws.ToString(got.Projects[0].Cache.CacheNamespace))

	b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
	require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus)
	require.Len(t, b.ExportedEnvironmentVariables, 1)
	assert.Equal(t, "EXP", aws.ToString(b.ExportedEnvironmentVariables[0].Name))
	assert.Equal(t, "v1", aws.ToString(b.ExportedEnvironmentVariables[0].Value))
}
