package kinesisanalyticsv2

import (
	"fmt"
	"time"
)

const (
	flinkServicePrincipal = "kinesisanalytics.amazonaws.com"
	flinkSessionMaxLen    = 64
)

// RoleCredentials are short-lived credentials for an application's ServiceExecutionRole.
type RoleCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// RoleCredentialIssuer mints ServiceExecutionRole credentials through the emulator's STS.
type RoleCredentialIssuer interface {
	IssueApplicationCredentials(roleArn, sessionName string) (RoleCredentials, error)
}

// ServicePrincipal is the service principal an application's ServiceExecutionRole must trust.
func ServicePrincipal() string { return flinkServicePrincipal }

// SetRoleCredentialIssuer wires STS-backed role credentials into Flink containers.
func (b *InMemoryBackend) SetRoleCredentialIssuer(i RoleCredentialIssuer) {
	b.mu.Lock("SetRoleCredentialIssuer")
	defer b.mu.Unlock()

	b.roleIssuer = i
}

// credentialEnv returns the AWS credential variables for the job: role credentials when an issuer is wired,
// otherwise static test credentials.
func credentialEnv(in flinkLaunch) ([]string, error) {
	if in.issuer == nil || in.roleARN == "" {
		return []string{"AWS_ACCESS_KEY_ID=" + flinkDefaultKey, "AWS_SECRET_ACCESS_KEY=" + flinkDefaultKey}, nil
	}

	session := "kinesisanalytics-" + in.name
	if len(session) > flinkSessionMaxLen {
		session = session[:flinkSessionMaxLen]
	}

	c, err := in.issuer.IssueApplicationCredentials(in.roleARN, session)
	if err != nil {
		return nil, fmt.Errorf("assume %s: %w", in.roleARN, err)
	}

	env := []string{"AWS_ACCESS_KEY_ID=" + c.AccessKeyID, "AWS_SECRET_ACCESS_KEY=" + c.SecretAccessKey}
	if c.SessionToken != "" {
		env = append(env, "AWS_SESSION_TOKEN="+c.SessionToken)
	}

	return env, nil
}
