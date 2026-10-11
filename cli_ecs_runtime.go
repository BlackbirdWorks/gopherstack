package main

import (
	"context"
	"errors"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var errECSSTSUnavailable = errors.New("sts backend unavailable")

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
