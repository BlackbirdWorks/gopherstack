package app

import (
	"context"
	"errors"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var (
	errECSSTSUnavailable     = errors.New("sts backend unavailable")
	errECSSecretsUnavailable = errors.New("secrets backend unavailable")
)

// ecsSecrets adapts Secrets Manager and SSM to ecs.SecretsResolver, resolved at call time.
type ecsSecrets struct {
	byName map[string]service.Registerable
}

func (a *ecsSecrets) SecretString(ctx context.Context, region, secretID, versionStage, versionID string) (string, error) {
	h, ok := a.byName["SecretsManager"].(*smbackend.Handler)
	if !ok {
		return "", errECSSecretsUnavailable
	}

	bk, ok := h.Backend.(*smbackend.InMemoryBackend)
	if !ok {
		return "", errECSSecretsUnavailable
	}

	out, err := bk.GetSecretValue(smbackend.WithRegion(ctx, region), &smbackend.GetSecretValueInput{
		SecretID: secretID, VersionStage: versionStage, VersionID: versionID,
	})
	if err != nil {
		return "", err
	}

	return out.SecretString, nil
}

func (a *ecsSecrets) ParameterValue(ctx context.Context, region, name string) (string, error) {
	h, ok := a.byName["SSM"].(*ssmbackend.Handler)
	if !ok {
		return "", errECSSecretsUnavailable
	}

	bk, ok := h.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return "", errECSSecretsUnavailable
	}

	out, err := bk.GetParameter(ssmbackend.WithRegion(ctx, region), &ssmbackend.GetParameterInput{
		Name: name, WithDecryption: true,
	})
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

// ecsEventPublisher puts ECS state-change events on the default bus. EventBridge
// is resolved at call time, never captured in Provider.Init.
type ecsEventPublisher struct {
	byName map[string]service.Registerable
}

func (p *ecsEventPublisher) PublishEvent(
	ctx context.Context, source, detailType string, resources []string, detail string,
) {
	h, ok := p.byName["EventBridge"].(*ebbackend.Handler)
	if !ok {
		return
	}

	bk, ok := h.Backend.(*ebbackend.InMemoryBackend)
	if !ok {
		return
	}

	_, _ = bk.PutEvents(ctx, []ebbackend.EventEntry{
		{Source: source, DetailType: detailType, Detail: detail, Resources: resources},
	})
}

// ecsTaskCredentials adapts STS to ecs.RoleCredentialIssuer.
type ecsTaskCredentials struct {
	byName map[string]service.Registerable
}

func (a *ecsTaskCredentials) IssueTaskCredentials(roleArn, sessionName string) (ecsbackend.RoleCredentials, error) {
	stsH, ok := a.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return ecsbackend.RoleCredentials{}, errECSSTSUnavailable
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return ecsbackend.RoleCredentials{}, errECSSTSUnavailable
	}

	out, err := stsBk.AssumeRoleForService(ecsbackend.TaskServicePrincipal(), roleArn, sessionName)
	if err != nil {
		return ecsbackend.RoleCredentials{}, err
	}

	c := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		return ecsbackend.RoleCredentials{}, err
	}

	return ecsbackend.RoleCredentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expires: exp,
	}, nil
}

func ecsBackendOf(byName map[string]service.Registerable) *ecsbackend.InMemoryBackend {
	h, ok := byName["ECS"].(*ecsbackend.Handler)
	if !ok {
		return nil
	}

	bk, ok := h.Backend.(*ecsbackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return bk
}

// wireECSRuntime gives ECS the default-bus publisher, task-role credential
// issuer and the emulator endpoint injected into task containers.
func wireECSRuntime(cli *CLI, byName map[string]service.Registerable) {
	bk := ecsBackendOf(byName)
	if bk == nil {
		return
	}

	bk.SetEventPublisher(&ecsEventPublisher{byName: byName})
	bk.SetRoleCredentialIssuer(&ecsTaskCredentials{byName: byName})
	bk.SetSecretsResolver(&ecsSecrets{byName: byName})

	if cli != nil && cli.Port != "" {
		bk.SetEndpointURL(lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port))
	}
}

// registerECSTaskEndpoints serves the task-role credentials and task metadata v4 endpoints.
func registerECSTaskEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		h, ok := svc.(*ecsbackend.Handler)
		if !ok {
			continue
		}

		e.GET("/_gopherstack/ecs/credentials/:task", h.ServeTaskCredentials)
		e.GET("/_gopherstack/ecs/metadata/:task/:container", h.ServeTaskMetadata)
		e.GET("/_gopherstack/ecs/metadata/:task/:container/*", h.ServeTaskMetadata)
	}
}
