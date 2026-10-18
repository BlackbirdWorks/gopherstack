package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

type fakeEKSKeys []iambackend.AccessKey

func (f fakeEKSKeys) ListAllAccessKeys() []iambackend.AccessKey { return f }

type fakeEKSSTS struct {
	sessions map[string]*stsbackend.SessionInfo
	arn      string
}

func (f fakeEKSSTS) GetCallerIdentity(string, string) (*stsbackend.GetCallerIdentityResponse, error) {
	return &stsbackend.GetCallerIdentityResponse{
		GetCallerIdentityResult: stsbackend.GetCallerIdentityResult{Arn: f.arn},
	}, nil
}

func (f fakeEKSSTS) LookupSession(ak, _ string) *stsbackend.SessionInfo { return f.sessions[ak] }

func TestEKSCredentialResolver(t *testing.T) {
	t.Parallel()

	keys := fakeEKSKeys{
		{AccessKeyID: "AKIAUSER", SecretAccessKey: "usersecret", Status: "Active"},
		{AccessKeyID: "AKIAOFF", SecretAccessKey: "off", Status: "Inactive"},
	}
	sess := map[string]*stsbackend.SessionInfo{"ASIASESS": {SecretAccessKey: "sesssecret"}}

	tests := []struct {
		name       string
		accessKey  string
		wantSecret string
	}{
		{name: "iam_user_key", accessKey: "AKIAUSER", wantSecret: "usersecret"},
		{name: "inactive_key_unverifiable", accessKey: "AKIAOFF", wantSecret: ""},
		{name: "session_key", accessKey: "ASIASESS", wantSecret: "sesssecret"},
		{name: "unknown_key_root_fallback", accessKey: "test", wantSecret: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := eksCredentialResolver{iam: keys, sts: fakeEKSSTS{arn: "arn:aws:iam::1:user/x", sessions: sess}}
			id, err := r.ResolveCredential(tt.accessKey, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSecret, id.Secret)
			assert.Equal(t, "arn:aws:iam::1:user/x", id.PrincipalARN)
		})
	}

	_, err := eksCredentialResolver{}.ResolveCredential("x", "")
	require.Error(t, err)
}
