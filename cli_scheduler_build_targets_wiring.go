package main

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
)

type schedFirehoseAdapter struct {
	backend *firehosebackend.InMemoryBackend
}

func (a *schedFirehoseAdapter) PutRecord(ctx context.Context, streamARN string, data []byte) error {
	return a.backend.PutRecord(inRegion(ctx, arnRegion(streamARN)), arnLastSegment(streamARN), data)
}

// wireSchedulerDeliveryTargets lets schedules target Firehose delivery streams, CodeBuild
// projects and CodePipeline pipelines.
func wireSchedulerDeliveryTargets(byName map[string]service.Registerable) {
	schedH, ok := byName["Scheduler"].(*schedulerbackend.Handler)
	if !ok {
		return
	}

	var dt schedulerbackend.DeliveryTargets

	if h, hok := byName["Firehose"].(*firehosebackend.Handler); hok {
		if bk, bok := h.Backend.(*firehosebackend.InMemoryBackend); bok {
			dt.Firehose = &schedFirehoseAdapter{backend: bk}
		}
	}

	if h, hok := byName["CodeBuild"].(*codebuildbackend.Handler); hok {
		dt.CodeBuild = &ebCodeBuildAdapter{handler: h}
	}

	if h, hok := byName["CodePipeline"].(*codepipelinebackend.Handler); hok {
		dt.CodePipeline = &ebCodePipelineAdapter{backend: h.Backend}
	}

	schedH.GetRunner().SetDeliveryTargets(dt)
}
