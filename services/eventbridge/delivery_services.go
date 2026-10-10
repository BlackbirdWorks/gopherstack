package eventbridge

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// BatchJobSubmitter submits an AWS Batch job to the queue named by queueARN.
type BatchJobSubmitter interface {
	SubmitBatchJob(ctx context.Context, queueARN string, params *BatchParameters, payload string) error
}

// CodeBuildStarter starts a build of the CodeBuild project named by projectARN.
type CodeBuildStarter interface {
	StartCodeBuild(ctx context.Context, projectARN string) error
}

// CodePipelineStarter starts an execution of the pipeline named by pipelineARN.
type CodePipelineStarter interface {
	StartCodePipeline(ctx context.Context, pipelineARN string) error
}

// SageMakerPipelineStarter starts an execution of the SageMaker pipeline named by pipelineARN.
type SageMakerPipelineStarter interface {
	StartSageMakerPipeline(ctx context.Context, pipelineARN string, params *SageMakerPipelineParameters) error
}

// RedshiftDataExecutor runs the target's SQL against the Redshift cluster named by clusterARN.
type RedshiftDataExecutor interface {
	ExecuteRedshiftStatement(ctx context.Context, clusterARN string, params *RedshiftDataParameters) error
}

// deliverToServiceTarget routes ARNs of the job/pipeline/query-style targets.
// handled is false when arn is not one of them.
func deliverToServiceTarget(
	ctx context.Context,
	target *Target,
	dt DeliveryTargets,
	payload string,
) (bool, bool) {
	arn := target.Arn

	switch {
	case strings.HasPrefix(arn, "arn:aws:batch:") && strings.Contains(arn, ":job-queue/"):
		if dt.Batch == nil {
			return false, true
		}

		return logTargetErr(ctx, "Batch", arn, dt.Batch.SubmitBatchJob(ctx, arn, target.BatchParameters, payload)), true
	case strings.HasPrefix(arn, "arn:aws:codebuild:") && strings.Contains(arn, ":project/"):
		if dt.CodeBuild == nil {
			return false, true
		}

		return logTargetErr(ctx, "CodeBuild", arn, dt.CodeBuild.StartCodeBuild(ctx, arn)), true
	case strings.HasPrefix(arn, "arn:aws:codepipeline:"):
		if dt.CodePipeline == nil {
			return false, true
		}

		return logTargetErr(ctx, "CodePipeline", arn, dt.CodePipeline.StartCodePipeline(ctx, arn)), true
	case strings.HasPrefix(arn, "arn:aws:sagemaker:") && strings.Contains(arn, ":pipeline/"):
		if dt.SageMakerPipeline == nil {
			return false, true
		}

		err := dt.SageMakerPipeline.StartSageMakerPipeline(ctx, arn, target.SageMakerPipelineParameters)

		return logTargetErr(ctx, "SageMaker pipeline", arn, err), true
	case strings.HasPrefix(arn, "arn:aws:redshift:") && strings.Contains(arn, ":cluster:"):
		if dt.RedshiftData == nil {
			return false, true
		}

		err := dt.RedshiftData.ExecuteRedshiftStatement(ctx, arn, target.RedshiftDataParameters)

		return logTargetErr(ctx, "Redshift", arn, err), true
	}

	return false, false
}

func logTargetErr(ctx context.Context, kind, arn string, err error) bool {
	if err == nil {
		return false
	}

	logger.Load(ctx).WarnContext(ctx, "EventBridge failed to deliver to target", "kind", kind, "arn", arn, "error", err)

	return true
}
