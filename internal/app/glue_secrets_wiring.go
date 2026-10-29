package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

type glueSecrets struct{ sm *smbackend.InMemoryBackend }

func (g glueSecrets) SecretString(ctx context.Context, region, secretID string) (string, error) {
	out, err := g.sm.GetSecretValue(
		smbackend.WithRegion(ctx, region),
		&smbackend.GetSecretValueInput{SecretID: secretID},
	)
	if err != nil {
		return "", err
	}

	return out.SecretString, nil
}

// wireGlueBackends wires the S3 and Secrets Manager backends crawlers read.
func wireGlueBackends(byName map[string]service.Registerable) {
	wireGlueS3(byName["Glue"], byName["S3"])
	wireGlueSecrets(byName["Glue"], byName["SecretsManager"])
}

// wireGlueSecrets lets Glue crawlers resolve connection SECRET_ID credentials.
func wireGlueSecrets(glueReg, smReg service.Registerable) {
	glueH, ok := glueReg.(*gluebackend.Handler)
	if !ok {
		return
	}

	glueBk, ok := glueH.Backend.(*gluebackend.InMemoryBackend)
	if !ok {
		return
	}

	smH, ok := smReg.(*smbackend.Handler)
	if !ok {
		return
	}

	if smBk, ok2 := smH.Backend.(*smbackend.InMemoryBackend); ok2 {
		glueBk.SetSecretsBackend(glueSecrets{sm: smBk})
	}
}
