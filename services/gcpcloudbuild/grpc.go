package gcpcloudbuild

import (
	"context"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	cloudbuildpb.UnimplementedCloudBuildServer
	b *InMemoryBackend
}

func (s *server) CreateBuild(
	_ context.Context,
	in *cloudbuildpb.CreateBuildRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateBuild(in)
}

func (s *server) GetBuild(
	_ context.Context,
	in *cloudbuildpb.GetBuildRequest,
) (*cloudbuildpb.Build, error) {
	return s.b.GetBuild(in)
}

func (s *server) ListBuilds(
	_ context.Context,
	in *cloudbuildpb.ListBuildsRequest,
) (*cloudbuildpb.ListBuildsResponse, error) {
	items, next, err := s.b.ListBuilds(in)
	if err != nil {
		return nil, err
	}

	return &cloudbuildpb.ListBuildsResponse{Builds: items, NextPageToken: next}, nil
}

func (s *server) CancelBuild(
	_ context.Context,
	in *cloudbuildpb.CancelBuildRequest,
) (*cloudbuildpb.Build, error) {
	return s.b.CancelBuild(in)
}

func (s *server) RetryBuild(
	_ context.Context,
	in *cloudbuildpb.RetryBuildRequest,
) (*longrunningpb.Operation, error) {
	return s.b.RetryBuild(in)
}

func (s *server) ApproveBuild(
	_ context.Context,
	in *cloudbuildpb.ApproveBuildRequest,
) (*longrunningpb.Operation, error) {
	return s.b.ApproveBuild(in)
}

func (s *server) CreateBuildTrigger(
	_ context.Context, in *cloudbuildpb.CreateBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	return s.b.CreateBuildTrigger(in)
}

func (s *server) GetBuildTrigger(
	_ context.Context, in *cloudbuildpb.GetBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	return s.b.GetBuildTrigger(in)
}

func (s *server) ListBuildTriggers(
	_ context.Context, in *cloudbuildpb.ListBuildTriggersRequest,
) (*cloudbuildpb.ListBuildTriggersResponse, error) {
	items, next, err := s.b.ListBuildTriggers(in)
	if err != nil {
		return nil, err
	}

	return &cloudbuildpb.ListBuildTriggersResponse{Triggers: items, NextPageToken: next}, nil
}

func (s *server) DeleteBuildTrigger(
	_ context.Context,
	in *cloudbuildpb.DeleteBuildTriggerRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteBuildTrigger(in)
}

func (s *server) UpdateBuildTrigger(
	_ context.Context, in *cloudbuildpb.UpdateBuildTriggerRequest,
) (*cloudbuildpb.BuildTrigger, error) {
	return s.b.UpdateBuildTrigger(in)
}

func (s *server) RunBuildTrigger(
	_ context.Context, in *cloudbuildpb.RunBuildTriggerRequest,
) (*longrunningpb.Operation, error) {
	return s.b.RunBuildTrigger(in)
}

func (s *server) ReceiveTriggerWebhook(
	_ context.Context, in *cloudbuildpb.ReceiveTriggerWebhookRequest,
) (*cloudbuildpb.ReceiveTriggerWebhookResponse, error) {
	return &cloudbuildpb.ReceiveTriggerWebhookResponse{}, s.b.ReceiveTriggerWebhook(in)
}

func (s *server) CreateWorkerPool(
	_ context.Context, in *cloudbuildpb.CreateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateWorkerPool(in)
}

func (s *server) GetWorkerPool(
	_ context.Context,
	in *cloudbuildpb.GetWorkerPoolRequest,
) (*cloudbuildpb.WorkerPool, error) {
	return s.b.GetWorkerPool(in.GetName())
}

func (s *server) DeleteWorkerPool(
	_ context.Context, in *cloudbuildpb.DeleteWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteWorkerPool(in)
}

func (s *server) UpdateWorkerPool(
	_ context.Context, in *cloudbuildpb.UpdateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateWorkerPool(in)
}

func (s *server) ListWorkerPools(
	_ context.Context, in *cloudbuildpb.ListWorkerPoolsRequest,
) (*cloudbuildpb.ListWorkerPoolsResponse, error) {
	items, next, err := s.b.ListWorkerPools(in)
	if err != nil {
		return nil, err
	}

	return &cloudbuildpb.ListWorkerPoolsResponse{WorkerPools: items, NextPageToken: next}, nil
}
