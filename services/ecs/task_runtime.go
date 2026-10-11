package ecs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	ecsTasksServicePrincipal = "ecs-tasks.amazonaws.com"

	credentialsPathPrefix = "/_gopherstack/ecs/credentials/" //nolint:gosec // URL path, not a credential
	metadataPathPrefix    = "/_gopherstack/ecs/metadata/"
)

// ErrTaskCredentialsUnavailable is returned when a task has no role or no issuer is wired.
var ErrTaskCredentialsUnavailable = errors.New("task role credentials unavailable")

// RoleCredentials are temporary credentials for a task role.
type RoleCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// RoleCredentialIssuer mints task-role credentials for a task container.
type RoleCredentialIssuer interface {
	IssueTaskCredentials(roleArn, sessionName string) (RoleCredentials, error)
}

// TaskServicePrincipal is the service principal a task role must trust.
func TaskServicePrincipal() string { return ecsTasksServicePrincipal }

// SetRoleCredentialIssuer wires STS-backed task-role credentials into task containers.
func (b *InMemoryBackend) SetRoleCredentialIssuer(i RoleCredentialIssuer) {
	b.mu.Lock("SetRoleCredentialIssuer")
	defer b.mu.Unlock()

	b.credIssuer = i
}

// SetEndpointURL sets the base URL task containers use to reach this server.
func (b *InMemoryBackend) SetEndpointURL(u string) {
	b.mu.Lock("SetEndpointURL")
	defer b.mu.Unlock()

	b.endpointURL = u
}

// EndpointURL returns the base URL task containers use to reach this server.
func (b *InMemoryBackend) EndpointURL() string {
	b.mu.RLock("EndpointURL")
	defer b.mu.RUnlock()

	return b.endpointURL
}

// taskEnvRunner is implemented by runners that launch real containers and can
// take their ECS runtime environment from the backend.
type taskEnvRunner interface {
	SetTaskEnvProvider(fn func(task *Task, containerName string) []string)
}

func executionEnvFor(launchType string) string {
	if launchType == launchTypeFargate {
		return "AWS_ECS_FARGATE"
	}

	return "AWS_ECS_EC2"
}

// containerRuntimeEnv is the environment ECS injects into a task container:
// region, execution env, the emulator endpoint, the task-role credentials URI
// and the task metadata v4 URI.
func (b *InMemoryBackend) containerRuntimeEnv(task *Task, containerName string) []string {
	b.mu.RLock("containerRuntimeEnv")
	endpoint := b.endpointURL
	issuer := b.credIssuer
	b.mu.RUnlock()

	env := []string{
		"AWS_REGION=" + b.region,
		"AWS_DEFAULT_REGION=" + b.region,
		"AWS_EXECUTION_ENV=" + executionEnvFor(task.LaunchType),
	}

	if endpoint == "" {
		return env
	}

	base := strings.TrimSuffix(endpoint, "/")
	taskID := taskIDFromARN(task.TaskArn)

	env = append(env,
		"AWS_ENDPOINT_URL="+base,
		"ECS_CONTAINER_METADATA_URI_V4="+base+metadataPathPrefix+taskID+"/"+containerName,
	)

	if issuer != nil && task.TaskRoleArn != "" {
		env = append(env, "AWS_CONTAINER_CREDENTIALS_FULL_URI="+base+credentialsPathPrefix+taskID)
	}

	return env
}

// issueTaskCredentials mints credentials for the task's role; the session name is the task ID.
func (b *InMemoryBackend) issueTaskCredentials(task Task) (RoleCredentials, error) {
	b.mu.RLock("issueTaskCredentials")
	issuer := b.credIssuer
	b.mu.RUnlock()

	if issuer == nil || task.TaskRoleArn == "" {
		return RoleCredentials{}, ErrTaskCredentialsUnavailable
	}

	creds, err := issuer.IssueTaskCredentials(task.TaskRoleArn, taskIDFromARN(task.TaskArn))
	if err != nil {
		logger.Load(context.Background()).Warn("ecs: task role credentials unavailable",
			"task", task.TaskArn, "role", task.TaskRoleArn, "error", err)

		return RoleCredentials{}, err
	}

	return creds, nil
}
