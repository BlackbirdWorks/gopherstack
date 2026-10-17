package app

import (
	"context"
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const ssmCallerArnFields = 6

const secretsGetValueAction = "secretsmanager:GetSecretValue"

var errSSMSecretsUnavailable = errors.New("secrets manager backend unavailable")

// ssmSecretsAdapter serves /aws/reference/secretsmanager/* parameters from Secrets Manager.
// Backends are resolved per call, never captured at wiring time.
type ssmSecretsAdapter struct {
	byName     map[string]service.Registerable
	enforceIAM bool
}

func (a *ssmSecretsAdapter) ResolveSecret(
	ctx context.Context, ref ssmbackend.SecretRef,
) (*ssmbackend.SecretValue, error) {
	smH, ok := a.byName["SecretsManager"].(*smbackend.Handler)
	if !ok {
		return nil, errSSMSecretsUnavailable
	}

	smBk, ok := smH.Backend.(*smbackend.InMemoryBackend)
	if !ok {
		return nil, errSSMSecretsUnavailable
	}

	if region := awsmeta.Region(ctx); region != "" {
		ctx = smbackend.WithRegion(ctx, region)
	}

	out, err := smBk.GetSecretValue(ctx, &smbackend.GetSecretValueInput{
		SecretID: ref.ID, VersionID: ref.VersionID, VersionStage: ref.VersionStage,
	})
	if err != nil {
		if errors.Is(err, smbackend.ErrSecretNotFound) || errors.Is(err, smbackend.ErrVersionNotFound) ||
			errors.Is(err, smbackend.ErrSecretDeleted) {
			return nil, ssmbackend.ErrSecretNotFound
		}

		return nil, err
	}

	if !a.allowed(ctx, out.ARN) {
		return nil, ssmbackend.ErrSecretAccessDenied
	}

	return &ssmbackend.SecretValue{
		ARN: out.ARN, Name: out.Name, VersionID: out.VersionID, SecretString: out.SecretString,
		SecretBinary: out.SecretBinary, VersionStages: out.VersionStages, CreatedDate: out.CreatedDate,
	}, nil
}

// allowed evaluates secretsmanager:GetSecretValue for the caller when IAM enforcement is on.
// Callers that are not IAM users or roles (root, unresolved) are not evaluated.
func (a *ssmSecretsAdapter) allowed(ctx context.Context, secretArn string) bool {
	principal := iamPrincipalArn(awsmeta.CallerArn(ctx))
	if !a.enforceIAM || principal == "" {
		return true
	}

	iamH, ok := a.byName["IAM"].(*iambackend.Handler)
	if !ok {
		return true
	}

	iamBk, ok := iamH.Backend.(*iambackend.InMemoryBackend)
	if !ok {
		return true
	}

	results, err := iamBk.SimulatePrincipalPolicy(
		principal, "", "", nil, []string{secretsGetValueAction}, []string{secretArn}, iambackend.ConditionContext{},
	)
	if err != nil {
		return true
	}

	for _, r := range results {
		if r.Decision != "allowed" {
			return false
		}
	}

	return true
}

// iamPrincipalArn maps a caller ARN to the IAM user/role ARN whose policies apply, or "".
func iamPrincipalArn(callerArn string) string {
	parts := strings.SplitN(callerArn, ":", ssmCallerArnFields)
	if len(parts) < ssmCallerArnFields {
		return ""
	}

	account, resource := parts[4], parts[5]

	switch {
	case strings.HasPrefix(resource, "user/"), strings.HasPrefix(resource, "role/"):
		return callerArn
	case parts[2] == "sts" && strings.HasPrefix(resource, "assumed-role/"):
		role, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")

		return "arn:" + parts[1] + ":iam::" + account + ":role/" + role
	default:
		return ""
	}
}

// wireSSMSecrets lets SSM serve /aws/reference/secretsmanager/* from Secrets Manager.
func wireSSMSecrets(cli *CLI, byName map[string]service.Registerable) {
	ssmH, ok := byName["SSM"].(*ssmbackend.Handler)
	if !ok {
		return
	}

	ssmBk, ok := ssmH.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return
	}

	ssmBk.SetSecretResolver(&ssmSecretsAdapter{byName: byName, enforceIAM: cli != nil && cli.EnforceIAM})
}
