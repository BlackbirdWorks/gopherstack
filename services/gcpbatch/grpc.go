package gcpbatch

import (
	"context"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/protobuf/types/known/emptypb"
)

type batchServer struct {
	batchpb.UnimplementedBatchServiceServer
	b *InMemoryBackend
}

func (s *batchServer) CreateJob(_ context.Context, in *batchpb.CreateJobRequest) (*batchpb.Job, error) {
	return s.b.CreateJob(in)
}

func (s *batchServer) GetJob(_ context.Context, in *batchpb.GetJobRequest) (*batchpb.Job, error) {
	return s.b.GetJob(in.GetName())
}

func (s *batchServer) DeleteJob(
	_ context.Context, in *batchpb.DeleteJobRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteJob(in)
}

func (s *batchServer) CancelJob(
	_ context.Context, in *batchpb.CancelJobRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CancelJob(in)
}

func (s *batchServer) ListJobs(
	_ context.Context, in *batchpb.ListJobsRequest,
) (*batchpb.ListJobsResponse, error) {
	return s.b.ListJobs(in)
}

func (s *batchServer) GetTask(_ context.Context, in *batchpb.GetTaskRequest) (*batchpb.Task, error) {
	return s.b.GetTask(in.GetName())
}

func (s *batchServer) ListTasks(
	_ context.Context, in *batchpb.ListTasksRequest,
) (*batchpb.ListTasksResponse, error) {
	return s.b.ListTasks(in)
}

type locationsServer struct {
	location.UnimplementedLocationsServer
}

func (locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	return listLocations(in)
}

func (locationsServer) GetLocation(_ context.Context, in *location.GetLocationRequest) (*location.Location, error) {
	return getLocation(in.GetName())
}

type operationsServer struct {
	longrunningpb.UnimplementedOperationsServer
	b *InMemoryBackend
}

func (s *operationsServer) GetOperation(
	_ context.Context, in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	return s.b.GetOperation(in.GetName())
}

func (s *operationsServer) WaitOperation(
	ctx context.Context, in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	return s.b.WaitOperation(ctx, in)
}

func (s *operationsServer) ListOperations(
	_ context.Context, in *longrunningpb.ListOperationsRequest,
) (*longrunningpb.ListOperationsResponse, error) {
	return s.b.ListOperations(in)
}

func (s *operationsServer) DeleteOperation(
	_ context.Context, in *longrunningpb.DeleteOperationRequest,
) (*emptypb.Empty, error) {
	return s.b.DeleteOperation(in.GetName())
}
