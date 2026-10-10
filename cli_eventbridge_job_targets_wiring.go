package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

var errMissingTargetParams = errors.New("target has no type-specific parameters")

// arnLastSegment returns the final ":" or "/" separated segment of an ARN.
func arnLastSegment(arn string) string {
	if i := strings.LastIndexAny(arn, ":/"); i >= 0 {
		return arn[i+1:]
	}

	return arn
}

type ebBatchAdapter struct{ backend *batchbackend.InMemoryBackend }

func (a *ebBatchAdapter) SubmitBatchJob(
	ctx context.Context, queueARN string, params *ebbackend.BatchParameters, _ string,
) error {
	if params == nil {
		return fmt.Errorf("%w: batch target %s", errMissingTargetParams, queueARN)
	}

	var retry *batchbackend.RetryStrategy
	if params.RetryStrategy != nil {
		retry = &batchbackend.RetryStrategy{Attempts: params.RetryStrategy.Attempts}
	}

	var array *batchbackend.ArrayProperties
	if params.ArrayProperties != nil {
		size := int32(min(params.ArrayProperties.Size, math.MaxInt32)) //nolint:gosec // clamped above
		array = &batchbackend.ArrayProperties{Size: size}
	}

	_, err := a.backend.SubmitJob(
		inRegion(ctx, arnRegion(queueARN)),
		params.JobName, queueARN, params.JobDefinition,
		nil, nil, nil, retry, nil, array, nil, nil, "", 0, false,
	)

	return err
}

type ebCodeBuildAdapter struct{ handler *codebuildbackend.Handler }

func (a *ebCodeBuildAdapter) StartCodeBuild(_ context.Context, projectARN string) error {
	bk := a.handler.RegionHandler(arnRegion(projectARN)).Backend
	_, err := bk.StartBuild(arnLastSegment(projectARN), codebuildbackend.StartBuildConfig{})

	return err
}

type ebCodePipelineAdapter struct {
	backend *codepipelinebackend.InMemoryBackend
}

func (a *ebCodePipelineAdapter) StartCodePipeline(ctx context.Context, pipelineARN string) error {
	_, err := a.backend.StartPipelineExecution(inRegion(ctx, arnRegion(pipelineARN)), arnLastSegment(pipelineARN))

	return err
}

type ebSageMakerPipelineAdapter struct {
	backend *sagemakerbackend.InMemoryBackend
}

func (a *ebSageMakerPipelineAdapter) StartSageMakerPipeline(
	ctx context.Context, pipelineARN string, params *ebbackend.SageMakerPipelineParameters,
) error {
	opts := sagemakerbackend.StartPipelineExecutionOptions{PipelineName: arnLastSegment(pipelineARN)}
	if params != nil {
		for _, p := range params.PipelineParameterList {
			opts.PipelineParameters = append(
				opts.PipelineParameters, sagemakerbackend.PipelineParameter{Name: p.Name, Value: p.Value},
			)
		}
	}

	_, err := a.backend.StartPipelineExecutionFull(inRegion(ctx, arnRegion(pipelineARN)), opts)

	return err
}

type ebRedshiftDataAdapter struct {
	backend *redshiftdatabackend.InMemoryBackend
}

func (a *ebRedshiftDataAdapter) ExecuteRedshiftStatement(
	ctx context.Context, clusterARN string, params *ebbackend.RedshiftDataParameters,
) error {
	if params == nil {
		return fmt.Errorf("%w: redshift target %s", errMissingTargetParams, clusterARN)
	}

	ctx = inRegion(ctx, arnRegion(clusterARN))
	cluster := arnLastSegment(clusterARN)

	if len(params.Sqls) > 0 {
		_, err := a.backend.BatchExecuteStatement(
			ctx, params.Sqls, cluster, "", params.Database, params.DBUser,
			params.SecretManagerArn, params.StatementName, params.WithEvent, "", nil, "", "",
		)

		return err
	}

	_, err := a.backend.ExecuteStatement(
		ctx, params.SQL, cluster, "", params.Database, params.DBUser,
		params.SecretManagerArn, params.StatementName, params.WithEvent, "", nil, "",
	)

	return err
}

// wireEventBridgeJobTargets lets rule targets start Batch jobs, CodeBuild builds, CodePipeline and
// SageMaker pipeline executions, and Redshift Data statements.
func wireEventBridgeJobTargets(byName map[string]service.Registerable) {
	ebH, ok := byName["EventBridge"].(*ebbackend.Handler)
	if !ok {
		return
	}

	ebBk, ok := ebH.Backend.(*ebbackend.InMemoryBackend)
	if !ok {
		return
	}

	ebBk.ConfigureDeliveryTargets(func(dt *ebbackend.DeliveryTargets) {
		if h, hok := byName["Batch"].(*batchbackend.Handler); hok {
			dt.Batch = &ebBatchAdapter{backend: h.Backend}
		}

		if h, hok := byName["CodeBuild"].(*codebuildbackend.Handler); hok {
			dt.CodeBuild = &ebCodeBuildAdapter{handler: h}
		}

		if h, hok := byName["CodePipeline"].(*codepipelinebackend.Handler); hok {
			dt.CodePipeline = &ebCodePipelineAdapter{backend: h.Backend}
		}

		if h, hok := byName["SageMaker"].(*sagemakerbackend.Handler); hok {
			dt.SageMakerPipeline = &ebSageMakerPipelineAdapter{backend: h.Backend}
		}

		if h, hok := byName["RedshiftData"].(*redshiftdatabackend.Handler); hok {
			if bk, bok := h.Backend.(*redshiftdatabackend.InMemoryBackend); bok {
				dt.RedshiftData = &ebRedshiftDataAdapter{backend: bk}
			}
		}
	})
}
