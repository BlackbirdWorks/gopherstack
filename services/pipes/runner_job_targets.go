package pipes

import (
	"context"
	"fmt"
	"strings"
)

// PipeBatchSubmitter submits an AWS Batch job to the queue named by queueARN.
type PipeBatchSubmitter interface {
	SubmitBatchJob(ctx context.Context, queueARN string, params *BatchJobTargetParameters) error
}

// PipeSageMakerStarter starts an execution of the SageMaker pipeline named by pipelineARN.
type PipeSageMakerStarter interface {
	StartSageMakerPipeline(ctx context.Context, pipelineARN string, params *SageMakerPipelineTargetParameters) error
}

// PipeRedshiftExecutor runs SQL against the Redshift cluster named by clusterARN.
type PipeRedshiftExecutor interface {
	ExecuteRedshiftStatement(ctx context.Context, clusterARN string, params *RedshiftDataTargetParameters) error
}

// PipeECSRunner runs an ECS task on the cluster named by clusterARN.
type PipeECSRunner interface {
	RunECSTask(ctx context.Context, clusterARN string, params *ECSTaskTargetParameters) error
}

// JobTargets holds the compute/query-style pipe target invokers.
type JobTargets struct {
	Batch     PipeBatchSubmitter
	SageMaker PipeSageMakerStarter
	Redshift  PipeRedshiftExecutor
	ECS       PipeECSRunner
}

// SetJobTargets installs the Batch, SageMaker pipeline, Redshift Data and ECS target invokers.
func (r *Runner) SetJobTargets(j JobTargets) { r.jobs = j }

// dispatchJobTarget routes Batch, SageMaker, Redshift and ECS targets. handled is false for any other ARN.
func (r *Runner) dispatchJobTarget(ctx context.Context, p *Pipe) (bool, error) {
	t := p.Target
	tp := p.TargetParameters

	switch {
	case strings.HasPrefix(t, "arn:aws:batch:"):
		if r.jobs.Batch == nil {
			return true, fmt.Errorf("%w: batch target %q", ErrTargetInvokerUnwired, t)
		}

		var bp *BatchJobTargetParameters
		if tp != nil {
			bp = tp.BatchJobParameters
		}

		return true, r.jobs.Batch.SubmitBatchJob(ctx, t, bp)
	case strings.HasPrefix(t, "arn:aws:sagemaker:"):
		if r.jobs.SageMaker == nil {
			return true, fmt.Errorf("%w: sagemaker target %q", ErrTargetInvokerUnwired, t)
		}

		var sp *SageMakerPipelineTargetParameters
		if tp != nil {
			sp = tp.SageMakerPipelineParameters
		}

		return true, r.jobs.SageMaker.StartSageMakerPipeline(ctx, t, sp)
	case strings.HasPrefix(t, "arn:aws:redshift:"):
		if r.jobs.Redshift == nil {
			return true, fmt.Errorf("%w: redshift target %q", ErrTargetInvokerUnwired, t)
		}

		var rp *RedshiftDataTargetParameters
		if tp != nil {
			rp = tp.RedshiftDataParameters
		}

		return true, r.jobs.Redshift.ExecuteRedshiftStatement(ctx, t, rp)
	case strings.HasPrefix(t, "arn:aws:ecs:"):
		if r.jobs.ECS == nil {
			return true, fmt.Errorf("%w: ecs target %q", ErrTargetInvokerUnwired, t)
		}

		var ep *ECSTaskTargetParameters
		if tp != nil {
			ep = tp.EcsTaskParameters
		}

		return true, r.jobs.ECS.RunECSTask(ctx, t, ep)
	}

	return false, nil
}
