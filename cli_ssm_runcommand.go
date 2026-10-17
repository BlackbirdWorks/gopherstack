package main

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

var errSSMRunCommandDependencyUnavailable = errors.New("ssm run command dependency unavailable")

// ssmContainerInstances lists docker-backed running EC2 instances, resolved per call.
type ssmContainerInstances struct {
	byName map[string]service.Registerable
}

func (s *ssmContainerInstances) ContainerInstances(region string, ids []string) []ssmbackend.ManagedContainer {
	h := ec2HandlerOf(s.byName)
	if h == nil {
		return nil
	}

	lister, ok := h.BackendFor(region).(interface {
		ExecTargets(ids []string) []ec2backend.ExecTarget
	})
	if !ok {
		return nil
	}

	var out []ssmbackend.ManagedContainer

	for _, t := range lister.ExecTargets(ids) {
		if !t.HasInstanceProfile {
			continue
		}

		out = append(out, ssmbackend.ManagedContainer{
			InstanceID: t.InstanceID, ContainerID: t.ContainerID, Tags: t.Tags, RegisteredAt: t.LaunchTime,
		})
	}

	slices.SortFunc(out, func(a, b ssmbackend.ManagedContainer) int {
		return strings.Compare(a.InstanceID, b.InstanceID)
	})

	return out
}

// ssmOutputSink writes Run Command output to S3 and CloudWatch Logs, resolved per call.
type ssmOutputSink struct {
	byName map[string]service.Registerable
	logs   *batchLogSink
}

func (s *ssmOutputSink) PutObject(bucket, key string, body []byte) error {
	h, ok := s.byName["S3"].(*s3backend.S3Handler)
	if !ok {
		return errSSMRunCommandDependencyUnavailable
	}

	bk, ok := h.Backend.(*s3backend.InMemoryBackend)
	if !ok {
		return errSSMRunCommandDependencyUnavailable
	}

	_, err := bk.PutObject(context.Background(), &sdks3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(body),
	})

	return err
}

func (s *ssmOutputSink) PutLogLines(region, group, stream string, lines []string) error {
	if err := s.logs.EnsureLogGroupAndStream(region, group, stream); err != nil {
		return err
	}

	return s.logs.PutLogLines(region, group, stream, lines)
}

// installSSMRunCommand lets SSM SendCommand execute in docker-backed EC2 instances through exec.
func installSSMRunCommand(byName map[string]service.Registerable, exec ssmbackend.ScriptExecutor) {
	h, ok := byName["SSM"].(*ssmbackend.Handler)
	if !ok {
		return
	}

	bk, ok := h.Backend.(*ssmbackend.InMemoryBackend)
	if !ok {
		return
	}

	bk.SetCommandRunner(&ssmbackend.CommandRunner{
		Instances: &ssmContainerInstances{byName: byName},
		Executor:  exec,
		Sink:      &ssmOutputSink{byName: byName, logs: &batchLogSink{byName: byName}},
	})
}

// wireSSMRunCommand enables SendCommand execution when EC2 instances are docker containers.
func wireSSMRunCommand(cli *CLI, byName map[string]service.Registerable) {
	if cli == nil || cli.GetEC2ComputeProvider() != "docker" {
		return
	}

	exec, err := ssmbackend.NewDockerScriptExecutor()
	if err != nil {
		return
	}

	installSSMRunCommand(byName, exec)

	if shells, serr := ssmbackend.NewDockerShellOpener(); serr == nil {
		installSSMShells(byName, shells)
	}
}

// installSSMShells lets Session Manager sessions open interactive shells in docker-backed EC2 instances.
func installSSMShells(byName map[string]service.Registerable, shells ssmbackend.ShellOpener) {
	h, ok := byName["SSM"].(*ssmbackend.Handler)
	if !ok {
		return
	}

	if bk, isMem := h.Backend.(*ssmbackend.InMemoryBackend); isMem {
		bk.SetShellOpener(shells)
	}
}
