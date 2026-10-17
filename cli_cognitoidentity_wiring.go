package main

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// stsRoleAssumer lets Cognito Identity mint credentials through STS AssumeRoleWithWebIdentity.
type stsRoleAssumer struct{ backend stsbackend.StorageBackend }

func (a stsRoleAssumer) AssumeRoleWithWebIdentity(
	roleARN, sessionName, token string,
) (*cognitoidentitybackend.Credentials, error) {
	out, err := a.backend.AssumeRoleWithWebIdentity(&stsbackend.AssumeRoleWithWebIdentityInput{
		RoleArn:          roleARN,
		RoleSessionName:  sessionName,
		WebIdentityToken: token,
	})
	if err != nil {
		return nil, fmt.Errorf("sts assume role with web identity: %w", err)
	}

	creds := out.AssumeRoleWithWebIdentityResult.Credentials

	expiry, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		return nil, fmt.Errorf("parse credential expiration: %w", err)
	}

	return &cognitoidentitybackend.Credentials{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      expiry,
	}, nil
}

// userPoolKeyResolver exposes Cognito User Pool signing keys to Cognito Identity login verification.
type userPoolKeyResolver struct{ idp *cognitoidpbackend.Handler }

func (r userPoolKeyResolver) ResolveTokenKey(issuer, kid string) (*rsa.PublicKey, bool, error) {
	key, err := r.idp.GetJWTPublicKey(issuer, kid)
	if errors.Is(err, cognitoidpbackend.ErrJWTIssuerUnknown) {
		return nil, false, nil
	}

	return key, true, err
}

// wireCognitoIdentity connects Cognito Identity to STS (credentials) and to Cognito IDP (login signatures).
func wireCognitoIdentity(identityReg, stsReg, idpReg service.Registerable) {
	identityH, ok := identityReg.(*cognitoidentitybackend.Handler)
	if !ok {
		return
	}

	if stsH, stsOk := stsReg.(*stsbackend.Handler); stsOk {
		identityH.Backend.SetRoleAssumer(stsRoleAssumer{backend: stsH.Backend})
	}

	if idpH, idpOk := idpReg.(*cognitoidpbackend.Handler); idpOk {
		identityH.Backend.SetTokenKeyResolver(userPoolKeyResolver{idp: idpH})
	}
}
