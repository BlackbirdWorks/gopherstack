package app

import (
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

var errBatchDependencyUnavailable = errors.New("batch execution dependency unavailable")

// batchLogSink writes job output to CloudWatch Logs, resolved per call.
type batchLogSink struct {
	byName map[string]service.Registerable
}

func (s *batchLogSink) adapter(region string) (*cwLogsAdapter, error) {
	h, ok := s.byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	if !ok {
		return nil, errBatchDependencyUnavailable
	}

	bk, ok := h.Backend.(*cwlogsbackend.InMemoryBackend)
	if !ok {
		return nil, errBatchDependencyUnavailable
	}

	return &cwLogsAdapter{backend: bk, region: region}, nil
}

func (s *batchLogSink) EnsureLogGroupAndStream(region, group, stream string) error {
	a, err := s.adapter(region)
	if err != nil {
		return err
	}

	return a.EnsureLogGroupAndStream(group, stream)
}

func (s *batchLogSink) PutLogLines(region, group, stream string, messages []string) error {
	a, err := s.adapter(region)
	if err != nil {
		return err
	}

	return a.PutLogLines(group, stream, messages)
}

// batchJobCredentials issues job-role credentials from STS.
type batchJobCredentials struct {
	byName map[string]service.Registerable
}

func (c *batchJobCredentials) IssueJobCredentials(roleArn, sessionName string) (batchbackend.JobCredentials, error) {
	h, ok := c.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return batchbackend.JobCredentials{}, errBatchDependencyUnavailable
	}

	bk, ok := h.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return batchbackend.JobCredentials{}, errBatchDependencyUnavailable
	}

	out, err := bk.AssumeRoleForService(ecsbackend.TaskServicePrincipal(), roleArn, sessionName)
	if err != nil {
		return batchbackend.JobCredentials{}, err
	}

	creds := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, creds.Expiration)
	if err != nil {
		return batchbackend.JobCredentials{}, err
	}

	return batchbackend.JobCredentials{
		AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey,
		SessionToken: creds.SessionToken, Expires: exp,
	}, nil
}

// wireBatchExec gives Batch's container executor its log sink, job-role
// credential issuer and the emulator endpoint injected into job containers.
func wireBatchExec(cli *CLI, byName map[string]service.Registerable) {
	h, ok := byName["Batch"].(*batchbackend.Handler)
	if !ok {
		return
	}

	h.Backend.SetJobLogSink(&batchLogSink{byName: byName})
	h.Backend.SetJobRoleCredentialIssuer(&batchJobCredentials{byName: byName})

	if cli != nil && cli.Port != "" {
		h.Backend.SetExecutionEndpoint(lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port))
	}
}
