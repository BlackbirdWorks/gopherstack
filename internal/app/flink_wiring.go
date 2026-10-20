package app

import (
	"path/filepath"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kav2 "github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

// GetFlinkDataDir persists docker-mode Flink application code and savepoints under the data dir when --persist is
// on (kinesisanalyticsv2.DataDirConfig).
func (c *CLI) GetFlinkDataDir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "flink")
}

// flinkRoleCredentials adapts STS to kinesisanalyticsv2.RoleCredentialIssuer; STS is resolved at call time.
type flinkRoleCredentials struct {
	byName map[string]service.Registerable
}

func (a *flinkRoleCredentials) IssueApplicationCredentials(roleArn, sessionName string) (kav2.RoleCredentials, error) {
	stsH, ok := a.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return kav2.RoleCredentials{}, errLambdaSTSUnavailable
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return kav2.RoleCredentials{}, errLambdaSTSUnavailable
	}

	out, err := stsBk.AssumeRoleForService(kav2.ServicePrincipal(), roleArn, sessionName)
	if err != nil {
		return kav2.RoleCredentials{}, err
	}

	c := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		return kav2.RoleCredentials{}, err
	}

	return kav2.RoleCredentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expires: exp,
	}, nil
}

// wireFlinkRoleCredentials gives Flink containers ServiceExecutionRole credentials issued by STS.
func wireFlinkRoleCredentials(byName map[string]service.Registerable) {
	h, ok := byName["KinesisAnalyticsV2"].(*kav2.Handler)
	if !ok {
		return
	}

	if bk, isMem := h.Backend.(*kav2.InMemoryBackend); isMem {
		bk.SetRoleCredentialIssuer(&flinkRoleCredentials{byName: byName})
	}
}
