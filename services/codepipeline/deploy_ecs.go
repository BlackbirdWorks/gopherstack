package codepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const (
	configKeyECSCluster    = "ClusterName"
	configKeyECSService    = "ServiceName"
	configKeyECSFileName   = "FileName"
	configKeyECSTimeout    = "DeploymentTimeout"
	defaultImageDefsFile   = "imagedefinitions.json"
	maxImageDefsBytes      = 100 << 10
	defaultECSDeployWindow = 60 * time.Minute

	ecsRolloutCompleted  = "COMPLETED"
	ecsRolloutFailed     = "FAILED"
	ecsDeploymentActive  = "ACTIVE"
	ecsDeploymentPrimary = "PRIMARY"
)

type imageDefinition struct {
	Name     string `json:"name"`
	ImageURI string `json:"imageUri"`
}

// runECSAction rolls the service onto a new task definition revision built from the image definitions file. The
// emulator's ECS backend reports rollout state per deployment; the action completes when the new revision's
// deployment is the only one left and its rollout is COMPLETED (or, when no rollout state is reported, its
// running count has reached its desired count).
func (b *InMemoryBackend) runECSAction(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	if b.ecsDeployer == nil {
		failActionf(ae, failureConfigurationError, "ECS is not available in this emulator instance")

		return
	}

	ae.Status = statusInProgress

	b.advanceECS(ctx, p, execID, action, ae)
}

func (b *InMemoryBackend) advanceECS(
	ctx context.Context, p *Pipeline, execID string, action Action, ae *ActionExecution,
) {
	cluster, service := action.Configuration[configKeyECSCluster], action.Configuration[configKeyECSService]

	if ae.ExternalExecutionID == "" {
		newTD, err := b.ecsStartDeployment(ctx, p, execID, action, cluster, service)
		if err != nil {
			failActionf(ae, failureJobFailed, "%v", err)

			return
		}

		ae.ExternalExecutionID = newTD
	}

	svc, err := b.ecsDeployer.DescribeService(ctx, cluster, service)

	switch {
	case err != nil:
		failActionf(ae, failureJobFailed, "%v", err)
	case svc == nil:
		failActionf(ae, failureJobFailed, "Service %s was not found in cluster %s", service, cluster)
	default:
		b.ecsEvaluate(action, ae, svc)
	}
}

func (b *InMemoryBackend) ecsStartDeployment(
	ctx context.Context, p *Pipeline, execID string, action Action, cluster, service string,
) (string, error) {
	file := action.Configuration[configKeyECSFileName]
	if file == "" {
		file = defaultImageDefsFile
	}

	in := primaryInputArtifact(action)
	if in == "" {
		return "", fmt.Errorf("%w: action %q has no input artifact", ErrValidation, action.Name)
	}

	body, err := b.artifactFile(ctx, p, execID, action, in, file)
	if err != nil {
		return "", fmt.Errorf("unable to read image definitions file %q: %w", file, err)
	}

	if len(body) > maxImageDefsBytes {
		return "", fmt.Errorf("%w: image definitions file %q exceeds %d bytes", ErrValidation, file, maxImageDefsBytes)
	}

	var defs []imageDefinition
	if err = json.Unmarshal(body, &defs); err != nil || len(defs) == 0 {
		return "", fmt.Errorf(
			"%w: image definitions file %q must be a non-empty JSON array of name/imageUri",
			ErrValidation,
			file,
		)
	}

	images := make(map[string]string, len(defs))
	for _, d := range defs {
		if d.Name == "" || d.ImageURI == "" {
			return "", fmt.Errorf(
				"%w: image definitions file %q has an entry without name or imageUri",
				ErrValidation,
				file,
			)
		}

		images[d.Name] = d.ImageURI
	}

	svc, err := b.ecsDeployer.DescribeService(ctx, cluster, service)
	if err != nil {
		return "", err
	}

	if svc == nil {
		return "", fmt.Errorf("%w: service %s was not found in cluster %s", ErrValidation, service, cluster)
	}

	newTD, err := b.ecsDeployer.RegisterRevisionWithImages(ctx, svc.TaskDefinition, images)
	if err != nil {
		return "", err
	}

	if err = b.ecsDeployer.UpdateServiceTaskDefinition(ctx, cluster, service, newTD); err != nil {
		return "", err
	}

	return newTD, nil
}

func (b *InMemoryBackend) ecsEvaluate(action Action, ae *ActionExecution, svc *ECSServiceState) {
	var primary *ECSDeploymentState

	live := 0

	for i := range svc.Deployments {
		d := &svc.Deployments[i]
		if d.Status == ecsDeploymentActive || d.Status == ecsDeploymentPrimary {
			live++
		}

		if d.Status == ecsDeploymentPrimary && d.TaskDefinition == ae.ExternalExecutionID {
			primary = d
		}
	}

	switch {
	case primary == nil && live > 0 && b.ecsTimedOut(action, ae):
		failActionf(
			ae,
			failureJobFailed,
			"Deployment of %s did not start before the deployment timeout",
			ae.ExternalExecutionID,
		)
	case primary == nil:
		return
	case primary.RolloutState == ecsRolloutFailed:
		failActionf(
			ae,
			failureJobFailed,
			"Deployment of %s failed: %s",
			ae.ExternalExecutionID,
			primary.RolloutStateReason,
		)
	case live == 1 && ecsSteady(primary):
		ae.Status = statusSucceeded
	case b.ecsTimedOut(action, ae):
		failActionf(ae, failureJobFailed, "Service did not reach a steady state within the deployment timeout")
	}
}

func ecsSteady(d *ECSDeploymentState) bool {
	if d.RolloutState != "" {
		return d.RolloutState == ecsRolloutCompleted
	}

	return d.RunningCount >= d.DesiredCount
}

func (b *InMemoryBackend) ecsTimedOut(action Action, ae *ActionExecution) bool {
	window := defaultECSDeployWindow

	if m, err := strconv.Atoi(action.Configuration[configKeyECSTimeout]); err == nil && m > 0 {
		window = time.Duration(m) * time.Minute
	}

	return b.now().Sub(ae.StartTime) > window
}
