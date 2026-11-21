package codebuild

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	authSecretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:git-AbCdEf"
	authConnARN   = "arn:aws:codeconnections:us-east-1:000000000000:connection/11111111-2222-3333-4444-555555555555"
)

var errAuthFake = errors.New("not found")

type authValues struct{ secrets map[string]string }

func (authValues) Parameter(context.Context, string, string) (string, error) { return "", errAuthFake }

func (v authValues) SecretValue(_ context.Context, _, id, _, _ string) (string, error) {
	if s, ok := v.secrets[id]; ok {
		return s, nil
	}

	return "", errAuthFake
}

type authConnections map[string]string

func (c authConnections) ConnectionStatus(_ context.Context, _, id string) (string, error) {
	if s, ok := c[id]; ok {
		return s, nil
	}

	return "", errAuthFake
}

func TestValidateSourceAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		auth    SourceAuth
		wantErr bool
	}{
		{name: "empty"},
		{name: "oauth", auth: SourceAuth{Type: "OAUTH"}},
		{name: "secret", auth: SourceAuth{Type: "SECRETS_MANAGER", Resource: authSecretARN}},
		{name: "connection", auth: SourceAuth{Type: "CODECONNECTIONS", Resource: authConnARN}},
		{name: "legacy connection", auth: SourceAuth{
			Type: "CODECONNECTIONS", Resource: "arn:aws:codestar-connections:us-east-1:000000000000:connection/x",
		}},
		{name: "resource without type", auth: SourceAuth{Resource: authSecretARN}, wantErr: true},
		{name: "unknown type", auth: SourceAuth{Type: "BASIC"}, wantErr: true},
		{name: "secret without arn", auth: SourceAuth{Type: "SECRETS_MANAGER", Resource: "git"}, wantErr: true},
		{
			name: "secret with connection arn", wantErr: true,
			auth: SourceAuth{Type: "SECRETS_MANAGER", Resource: authConnARN},
		},
		{name: "connection without arn", auth: SourceAuth{Type: "CODECONNECTIONS"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateSourceAuth("source.auth", tt.auth)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEngineGitSourceAuthSecret(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)
	const token = "ghp_from_secret"

	tests := []struct {
		name     string
		secret   string
		resource string
		wantMsg  string
		wantOK   bool
	}{
		{
			name: "valid secret", wantOK: true,
			secret: `{"ServerType":"GITHUB","AuthType":"PERSONAL_ACCESS_TOKEN","Token":"` + token + `"}`,
		},
		{
			name: "wrong server type", wantMsg: "ServerType",
			secret: `{"ServerType":"BITBUCKET","AuthType":"PERSONAL_ACCESS_TOKEN","Token":"` + token + `"}`,
		},
		{
			name: "bad auth type", wantMsg: "AuthType",
			secret: `{"ServerType":"GITHUB","AuthType":"OAUTH","Token":"` + token + `"}`,
		},
		{name: "missing token", wantMsg: "Token", secret: `{"ServerType":"GITHUB","AuthType":"BASIC_AUTH"}`},
		{name: "not json", wantMsg: "documented JSON", secret: "just-a-token"},
		{
			name: "other region", wantMsg: "Region",
			resource: "arn:aws:secretsmanager:eu-west-1:000000000000:secret:git-AbCdEf",
			secret:   `{"ServerType":"GITHUB","AuthType":"PERSONAL_ACCESS_TOKEN","Token":"` + token + `"}`,
		},
		{
			name: "missing secret", wantMsg: "cannot read secret",
			resource: "arn:aws:secretsmanager:us-east-1:000000000000:secret:gone-AbCdEf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resource := tt.resource
			if resource == "" {
				resource = authSecretARN
			}

			secrets := map[string]string{}
			if tt.secret != "" {
				secrets[authSecretARN], secrets[resource] = tt.secret, tt.secret
			}

			location := gitHTTPServer(t, repo, "x-access-token", token)
			obs, rule := gitObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), nil)
			h.b.SetEngineServices(EngineServices{
				Store: h.store, Logs: h.logs,
				Values: authValues{secrets: secrets},
			})

			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeGithub, Location: aws.String(location),
				Auth: &cbtypes.SourceAuth{Type: cbtypes.SourceAuthTypeSecretsManager, Resource: aws.String(resource)},
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			assert.NotContains(t, h.logs.joined(), token)
			assert.NotContains(t, failedPhaseMessage(b), token)

			if tt.wantOK {
				require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
				obs.lock.RLock("read")
				assert.Equal(t, "two", obs.version)
				obs.lock.RUnlock()

				return
			}

			assert.Equal(t, cbtypes.StatusTypeFailed, b.BuildStatus)
			assert.Contains(t, failedPhaseMessage(b), tt.wantMsg)
		})
	}
}

func TestEngineGitSourceAuthConnection(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)

	tests := []struct {
		name    string
		conns   authConnections
		wantMsg string
		wantOK  bool
	}{
		{name: "available", conns: authConnections{authConnARN: "AVAILABLE"}, wantOK: true},
		{name: "pending", conns: authConnections{authConnARN: "PENDING"}, wantMsg: "PENDING"},
		{name: "unknown", conns: authConnections{}, wantMsg: "connection is unusable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obs, rule := gitObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), func(c *EngineConfig) { c.AllowFileGitSources = true })
			h.b.SetEngineServices(EngineServices{Store: h.store, Logs: h.logs, Connections: tt.conns})

			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeGithub, Location: aws.String("file://" + repo.dir),
				Auth: &cbtypes.SourceAuth{
					Type: cbtypes.SourceAuthTypeCodeconnections, Resource: aws.String(authConnARN),
				},
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			if tt.wantOK {
				require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
				obs.lock.RLock("read")
				assert.Equal(t, "two", obs.version)
				obs.lock.RUnlock()

				return
			}

			assert.Equal(t, cbtypes.StatusTypeFailed, b.BuildStatus)
			assert.Contains(t, failedPhaseMessage(b), tt.wantMsg)
		})
	}
}

func TestImportSourceCredentialsResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		authType     cbtypes.AuthType
		token        string
		wantResource string
		wantErr      bool
	}{
		{name: "connection", authType: cbtypes.AuthTypeCodeconnections, token: authConnARN, wantResource: authConnARN},
		{name: "secret", authType: cbtypes.AuthTypeSecretsManager, token: authSecretARN, wantResource: authSecretARN},
		{name: "connection token not an arn", authType: cbtypes.AuthTypeCodeconnections, token: "abc", wantErr: true},
		{name: "secret token not an arn", authType: cbtypes.AuthTypeSecretsManager, token: authConnARN, wantErr: true},
		{name: "pat has no resource", authType: cbtypes.AuthTypePersonalAccessToken, token: "ghp_x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			c := sdkClientFor(t, b)

			_, err := c.ImportSourceCredentials(t.Context(), &codebuildsdk.ImportSourceCredentialsInput{
				ServerType: cbtypes.ServerTypeGithub, AuthType: tt.authType, Token: aws.String(tt.token),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			listed, err := c.ListSourceCredentials(t.Context(), &codebuildsdk.ListSourceCredentialsInput{})
			require.NoError(t, err)
			require.Len(t, listed.SourceCredentialsInfos, 1)
			assert.Equal(t, tt.wantResource, aws.ToString(listed.SourceCredentialsInfos[0].Resource))

			tok, _, ok := b.sourceToken("GITHUB")
			assert.Equal(t, tt.wantResource == "", ok)

			if ok {
				assert.Equal(t, tt.token, tok)
			}
		})
	}
}

func TestSourceCredentialsResourcePersists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authType string
		token    string
		wantRes  string
		wantTok  string
	}{
		{name: "secret", authType: authTypeSecretsManager, token: authSecretARN, wantRes: authSecretARN},
		{name: "pat", authType: "PERSONAL_ACCESS_TOKEN", token: "ghp_x", wantTok: "ghp_x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := NewInMemoryBackend("000000000000", "us-east-1")
			_, err := src.ImportSourceCredentials(tt.authType, "GITHUB", tt.token, "", true)
			require.NoError(t, err)

			dst := NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

			got := dst.ListSourceCredentials()
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantRes, got[0].Resource)
			assert.Equal(t, tt.wantTok, got[0].Token)
		})
	}
}

func TestEngineGitSourceImportedAuth(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)
	const token = "ghp_from_imported_secret"

	secretBody := `{"ServerType":"GITHUB","AuthType":"PERSONAL_ACCESS_TOKEN","Token":"` + token + `"}`

	tests := []struct {
		name     string
		authType string
		resource string
		auth     *cbtypes.SourceAuth
		conns    authConnections
		wantMsg  string
		wantOK   bool
	}{
		{name: "imported secret", authType: authTypeSecretsManager, resource: authSecretARN, wantOK: true},
		{
			name: "imported connection pending", authType: authTypeCodeConnections, resource: authConnARN,
			conns: authConnections{authConnARN: "PENDING"}, wantMsg: "PENDING",
		},
		{
			name: "secret of another account", authType: authTypeSecretsManager, wantMsg: "account",
			auth: &cbtypes.SourceAuth{
				Type:     cbtypes.SourceAuthTypeSecretsManager,
				Resource: aws.String("arn:aws:secretsmanager:us-east-1:111111111111:secret:git-AbCdEf"),
			},
		},
		{
			name: "connection of another account", authType: authTypeCodeConnections, wantMsg: "account",
			auth: &cbtypes.SourceAuth{
				Type: cbtypes.SourceAuthTypeCodeconnections,
				Resource: aws.String(
					"arn:aws:codeconnections:us-east-1:111111111111:connection/11111111-2222-3333-4444-555555555555"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			location := gitHTTPServer(t, repo, "x-access-token", token)
			obs, rule := gitObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), nil)
			h.b.SetEngineServices(EngineServices{
				Store: h.store, Logs: h.logs, Connections: tt.conns,
				Values: authValues{secrets: map[string]string{authSecretARN: secretBody}},
			})

			if tt.resource != "" {
				_, err := h.b.ImportSourceCredentials(tt.authType, "GITHUB", tt.resource, "", true)
				require.NoError(t, err)
			}

			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeGithub, Location: aws.String(location), Auth: tt.auth,
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			assert.NotContains(t, h.logs.joined(), token)

			if tt.wantOK {
				require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
				obs.lock.RLock("read")
				assert.Equal(t, "two", obs.version)
				obs.lock.RUnlock()

				return
			}

			assert.Equal(t, cbtypes.StatusTypeFailed, b.BuildStatus)
			assert.Contains(t, failedPhaseMessage(b), tt.wantMsg)
		})
	}
}
