package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var errTargetUnwired = errors.New("scheduler target invoker not wired")

// FirehosePutter puts a record to the Firehose delivery stream named by streamARN.
type FirehosePutter interface {
	PutRecord(ctx context.Context, streamARN string, data []byte) error
}

// CodeBuildStarter starts a build of the CodeBuild project named by projectARN.
type CodeBuildStarter interface {
	StartCodeBuild(ctx context.Context, projectARN string) error
}

// CodePipelineStarter starts an execution of the pipeline named by pipelineARN.
type CodePipelineStarter interface {
	StartCodePipeline(ctx context.Context, pipelineARN string) error
}

// DeliveryTargets holds the Firehose, CodeBuild and CodePipeline target invokers.
type DeliveryTargets struct {
	Firehose     FirehosePutter
	CodeBuild    CodeBuildStarter
	CodePipeline CodePipelineStarter
}

// SetDeliveryTargets installs the Firehose, CodeBuild and CodePipeline target invokers.
func (r *Runner) SetDeliveryTargets(d DeliveryTargets) { r.extra = d }

func (r *Runner) invokeExtraTarget(ctx context.Context, s *Schedule, payload []byte) (bool, error) {
	arn := s.Target.ARN

	switch {
	case strings.HasPrefix(arn, "arn:aws:firehose:"):
		if r.extra.Firehose == nil {
			return true, fmt.Errorf("%w: firehose target %q", errTargetUnwired, arn)
		}

		return true, r.extra.Firehose.PutRecord(ctx, arn, payload)
	case strings.HasPrefix(arn, "arn:aws:codebuild:"):
		if r.extra.CodeBuild == nil {
			return true, fmt.Errorf("%w: codebuild target %q", errTargetUnwired, arn)
		}

		return true, r.extra.CodeBuild.StartCodeBuild(ctx, arn)
	case strings.HasPrefix(arn, "arn:aws:codepipeline:"):
		if r.extra.CodePipeline == nil {
			return true, fmt.Errorf("%w: codepipeline target %q", errTargetUnwired, arn)
		}

		return true, r.extra.CodePipeline.StartCodePipeline(ctx, arn)
	}

	return false, nil
}
