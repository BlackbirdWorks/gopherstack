package app

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var (
	errEKSNoSTS       = errors.New("STS is not available to resolve the access key")
	errEKSKeyInactive = errors.New("access key is not active")
)

type eksIAMKeys interface {
	ListAllAccessKeys() []iambackend.AccessKey
}

type eksSTS interface {
	GetCallerIdentity(accessKeyID, sessionToken string) (*stsbackend.GetCallerIdentityResponse, error)
	LookupSession(accessKeyID, sessionToken string) *stsbackend.SessionInfo
}

// eksCredentialResolver resolves `aws eks get-token` access keys through the emulator's own STS and IAM,
// so a token authenticates as exactly the principal GetCallerIdentity reports for the same key.
type eksCredentialResolver struct {
	iam eksIAMKeys
	sts eksSTS
}

func (r eksCredentialResolver) ResolveCredential(
	accessKeyID, sessionToken string,
) (eksbackend.CredentialIdentity, error) {
	if r.sts == nil {
		return eksbackend.CredentialIdentity{}, errEKSNoSTS
	}

	who, err := r.sts.GetCallerIdentity(accessKeyID, sessionToken)
	if err != nil {
		return eksbackend.CredentialIdentity{}, fmt.Errorf("resolve caller identity: %w", err)
	}

	id := eksbackend.CredentialIdentity{PrincipalARN: who.GetCallerIdentityResult.Arn}

	if s := r.sts.LookupSession(accessKeyID, sessionToken); s != nil {
		id.Secret = s.SecretAccessKey

		return id, nil
	}

	return r.withIAMSecret(id, accessKeyID)
}

func (r eksCredentialResolver) withIAMSecret(
	id eksbackend.CredentialIdentity, accessKeyID string,
) (eksbackend.CredentialIdentity, error) {
	if r.iam == nil {
		return id, nil
	}

	for _, ak := range r.iam.ListAllAccessKeys() {
		if ak.AccessKeyID != accessKeyID {
			continue
		}

		if ak.Status != "" && ak.Status != "Active" {
			return eksbackend.CredentialIdentity{}, errEKSKeyInactive
		}

		id.Secret = ak.SecretAccessKey

		return id, nil
	}

	return id, nil
}

// wireEKSTokenAuth lets the docker-engine EKS endpoint authenticate `aws eks get-token` bearers against IAM and STS.
func wireEKSTokenAuth(byName map[string]service.Registerable) {
	eksH, ok := byName["EKS"].(*eksbackend.Handler)
	if !ok {
		return
	}

	res := eksCredentialResolver{}

	if iamH, found := byName["IAM"].(*iambackend.Handler); found {
		if keys, isKeys := iamH.Backend.(eksIAMKeys); isKeys {
			res.iam = keys
		}
	}

	if stsH, found := byName["STS"].(*stsbackend.Handler); found {
		if s, isSTS := stsH.Backend.(eksSTS); isSTS {
			res.sts = s
		}
	}

	eksH.SetCredentialResolver(res)
}
