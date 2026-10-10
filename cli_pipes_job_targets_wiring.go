package main

import (
	"context"
	"fmt"
	"math"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

type pipesBatchAdapter struct{ backend *batchbackend.InMemoryBackend }

func (a *pipesBatchAdapter) SubmitBatchJob(
	ctx context.Context, queueARN string, p *pipesbackend.BatchJobTargetParameters,
) error {
	if p == nil {
		return fmt.Errorf("%w: batch target %s", errMissingTargetParams, queueARN)
	}

	var retry *batchbackend.RetryStrategy
	if p.RetryStrategy != nil {
		attempts := int32(min(p.RetryStrategy.Attempts, math.MaxInt32)) //nolint:gosec // clamped above
		retry = &batchbackend.RetryStrategy{Attempts: attempts}
	}

	var array *batchbackend.ArrayProperties
	if p.ArrayProperties != nil {
		size := int32(min(p.ArrayProperties.Size, math.MaxInt32)) //nolint:gosec // clamped above
		array = &batchbackend.ArrayProperties{Size: size}
	}

	deps := make([]batchbackend.JobDependency, len(p.DependsOn))
	for i, d := range p.DependsOn {
		deps[i] = batchbackend.JobDependency{JobID: d.JobID, Type: d.Type}
	}

	var overrides *batchbackend.ContainerOverrides

	if co := p.ContainerOverrides; co != nil {
		overrides = &batchbackend.ContainerOverrides{InstanceType: co.InstanceType, Command: co.Command}
		for _, e := range co.Environment {
			overrides.Environment = append(
				overrides.Environment,
				batchbackend.KeyValuePair{Name: e.Name, Value: e.Value},
			)
		}

		for _, rr := range co.ResourceRequirements {
			overrides.ResourceRequirements = append(
				overrides.ResourceRequirements, batchbackend.ResourceRequirement{Type: rr.Type, Value: rr.Value},
			)
		}
	}

	_, err := a.backend.SubmitJob(
		inRegion(ctx, arnRegion(queueARN)),
		p.JobName, queueARN, p.JobDefinition,
		nil, p.Parameters, deps, retry, nil, array, overrides, nil, "", 0, false,
	)

	return err
}

type pipesSageMakerAdapter struct {
	backend *sagemakerbackend.InMemoryBackend
}

func (a *pipesSageMakerAdapter) StartSageMakerPipeline(
	ctx context.Context, pipelineARN string, p *pipesbackend.SageMakerPipelineTargetParameters,
) error {
	params := &ebbackend.SageMakerPipelineParameters{}
	if p != nil {
		for _, kv := range p.PipelineParameterList {
			params.PipelineParameterList = append(
				params.PipelineParameterList, ebbackend.SageMakerPipelineParameter{Name: kv.Name, Value: kv.Value},
			)
		}
	}

	return (&ebSageMakerPipelineAdapter{backend: a.backend}).StartSageMakerPipeline(ctx, pipelineARN, params)
}

type pipesRedshiftAdapter struct {
	backend *redshiftdatabackend.InMemoryBackend
}

func (a *pipesRedshiftAdapter) ExecuteRedshiftStatement(
	ctx context.Context, clusterARN string, p *pipesbackend.RedshiftDataTargetParameters,
) error {
	if p == nil {
		return fmt.Errorf("%w: redshift target %s", errMissingTargetParams, clusterARN)
	}

	return (&ebRedshiftDataAdapter{backend: a.backend}).ExecuteRedshiftStatement(
		ctx, clusterARN, &ebbackend.RedshiftDataParameters{
			Database: p.Database, DBUser: p.DBUser, SecretManagerArn: p.SecretManagerArn,
			StatementName: p.StatementName, Sqls: p.Sqls, WithEvent: p.WithEvent,
		},
	)
}

type pipesECSAdapter struct{ eb *ebECSTaskRunnerAdapter }

func (a *pipesECSAdapter) RunECSTask(
	ctx context.Context, clusterARN string, p *pipesbackend.ECSTaskTargetParameters,
) error {
	if p == nil {
		return fmt.Errorf("%w: ecs target %s", errMissingTargetParams, clusterARN)
	}

	params := &ebbackend.EcsParameters{
		TaskDefinitionArn:    p.TaskDefinitionArn,
		LaunchType:           p.LaunchType,
		Group:                p.Group,
		PlatformVersion:      p.PlatformVersion,
		PropagateTags:        p.PropagateTags,
		EnableECSManagedTags: p.EnableECSManagedTags,
		TaskCount:            int32(min(p.TaskCount, math.MaxInt32)), //nolint:gosec // clamped above
	}

	if nc := p.NetworkConfiguration; nc != nil && nc.AwsvpcConfiguration != nil {
		params.NetworkConfiguration = &ebbackend.NetworkConfiguration{
			AwsvpcConfiguration: &ebbackend.AwsVpcConfiguration{
				AssignPublicIP: nc.AwsvpcConfiguration.AssignPublicIP,
				Subnets:        nc.AwsvpcConfiguration.Subnets,
				SecurityGroups: nc.AwsvpcConfiguration.SecurityGroups,
			},
		}
	}

	return a.eb.RunTaskWithParams(ctx, clusterARN, params, nil)
}

// wirePipesJobTargets lets pipes deliver to Batch, SageMaker pipeline, Redshift Data and ECS targets.
func wirePipesJobTargets(byName map[string]service.Registerable) {
	pipesH, ok := byName["Pipes"].(*pipesbackend.Handler)
	if !ok {
		return
	}

	var jobs pipesbackend.JobTargets

	if h, hok := byName["Batch"].(*batchbackend.Handler); hok {
		jobs.Batch = &pipesBatchAdapter{backend: h.Backend}
	}

	if h, hok := byName["SageMaker"].(*sagemakerbackend.Handler); hok {
		jobs.SageMaker = &pipesSageMakerAdapter{backend: h.Backend}
	}

	if h, hok := byName["RedshiftData"].(*redshiftdatabackend.Handler); hok {
		if bk, bok := h.Backend.(*redshiftdatabackend.InMemoryBackend); bok {
			jobs.Redshift = &pipesRedshiftAdapter{backend: bk}
		}
	}

	if h, hok := byName["ECS"].(*ecsbackend.Handler); hok {
		if bk, bok := h.Backend.(*ecsbackend.InMemoryBackend); bok {
			jobs.ECS = &pipesECSAdapter{eb: &ebECSTaskRunnerAdapter{backend: bk, handler: h}}
		}
	}

	pipesH.GetRunner().SetJobTargets(jobs)
}
