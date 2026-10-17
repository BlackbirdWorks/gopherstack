package app

import (
	"errors"
	"net"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var errLambdaSTSUnavailable = errors.New("sts backend unavailable")

// lambdaRoleCredentials adapts STS to lambda.RoleCredentialIssuer. The STS
// backend is resolved at call time, never captured in Provider.Init.
type lambdaRoleCredentials struct {
	byName map[string]service.Registerable
}

func (a *lambdaRoleCredentials) IssueFunctionCredentials(
	roleArn, sessionName string,
) (lambdabackend.RoleCredentials, error) {
	stsH, ok := a.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return lambdabackend.RoleCredentials{}, errLambdaSTSUnavailable
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return lambdabackend.RoleCredentials{}, errLambdaSTSUnavailable
	}

	out, err := stsBk.AssumeRoleForService(lambdabackend.ServicePrincipal(), roleArn, sessionName)
	if err != nil {
		return lambdabackend.RoleCredentials{}, err
	}

	c := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		return lambdabackend.RoleCredentials{}, err
	}

	return lambdabackend.RoleCredentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expires: exp,
	}, nil
}

// lambdaEndpointURL is the URL function containers use to reach this server.
func lambdaEndpointURL(dockerHost, port string) string {
	return "http://" + net.JoinHostPort(dockerHost, strings.TrimPrefix(port, ":"))
}

// wireLambdaRuntimeEnv gives function containers the emulator endpoint and
// execution-role credentials.
func wireLambdaRuntimeEnv(cli *CLI, byName map[string]service.Registerable) {
	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	if !ok {
		return
	}

	lambdaBk, ok := lambdaH.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return
	}

	lambdaBk.SetRoleCredentialIssuer(&lambdaRoleCredentials{byName: byName})

	if cli != nil && cli.Port != "" {
		lambdaBk.SetEndpointURL(lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port))
	}
}
