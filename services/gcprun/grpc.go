package gcprun

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
)

type servicesServer struct {
	runpb.UnimplementedServicesServer
	b *InMemoryBackend
}

func (s *servicesServer) CreateService(
	_ context.Context,
	in *runpb.CreateServiceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateService(in)
}

func (s *servicesServer) GetService(_ context.Context, in *runpb.GetServiceRequest) (*runpb.Service, error) {
	return s.b.GetService(in.GetName())
}

func (s *servicesServer) ListServices(
	_ context.Context,
	in *runpb.ListServicesRequest,
) (*runpb.ListServicesResponse, error) {
	items, next, err := s.b.ListServices(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListServicesResponse{Services: items, NextPageToken: next}, nil
}

func (s *servicesServer) UpdateService(
	_ context.Context,
	in *runpb.UpdateServiceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateService(in)
}

func (s *servicesServer) DeleteService(
	_ context.Context,
	in *runpb.DeleteServiceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteService(in)
}

type revisionsServer struct {
	runpb.UnimplementedRevisionsServer
	b *InMemoryBackend
}

func (s *revisionsServer) GetRevision(_ context.Context, in *runpb.GetRevisionRequest) (*runpb.Revision, error) {
	return s.b.GetRevision(in.GetName())
}

func (s *revisionsServer) ListRevisions(
	_ context.Context,
	in *runpb.ListRevisionsRequest,
) (*runpb.ListRevisionsResponse, error) {
	items, next, err := s.b.ListRevisions(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListRevisionsResponse{Revisions: items, NextPageToken: next}, nil
}

func (s *revisionsServer) DeleteRevision(
	_ context.Context,
	in *runpb.DeleteRevisionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteRevision(in)
}

type jobsServer struct {
	runpb.UnimplementedJobsServer
	b *InMemoryBackend
}

func (s *jobsServer) CreateJob(_ context.Context, in *runpb.CreateJobRequest) (*longrunningpb.Operation, error) {
	return s.b.CreateJob(in)
}

func (s *jobsServer) GetJob(_ context.Context, in *runpb.GetJobRequest) (*runpb.Job, error) {
	return s.b.GetJob(in.GetName())
}

func (s *jobsServer) ListJobs(_ context.Context, in *runpb.ListJobsRequest) (*runpb.ListJobsResponse, error) {
	items, next, err := s.b.ListJobs(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListJobsResponse{Jobs: items, NextPageToken: next}, nil
}

func (s *jobsServer) UpdateJob(_ context.Context, in *runpb.UpdateJobRequest) (*longrunningpb.Operation, error) {
	return s.b.UpdateJob(in)
}

func (s *jobsServer) DeleteJob(_ context.Context, in *runpb.DeleteJobRequest) (*longrunningpb.Operation, error) {
	return s.b.DeleteJob(in)
}

func (s *jobsServer) RunJob(_ context.Context, in *runpb.RunJobRequest) (*longrunningpb.Operation, error) {
	return s.b.RunJob(in)
}

type executionsServer struct {
	runpb.UnimplementedExecutionsServer
	b *InMemoryBackend
}

func (s *executionsServer) GetExecution(_ context.Context, in *runpb.GetExecutionRequest) (*runpb.Execution, error) {
	return s.b.GetExecution(in.GetName())
}

func (s *executionsServer) ListExecutions(
	_ context.Context,
	in *runpb.ListExecutionsRequest,
) (*runpb.ListExecutionsResponse, error) {
	items, next, err := s.b.ListExecutions(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListExecutionsResponse{Executions: items, NextPageToken: next}, nil
}

func (s *executionsServer) DeleteExecution(
	ctx context.Context,
	in *runpb.DeleteExecutionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteExecution(ctx, in)
}

func (s *executionsServer) CancelExecution(
	ctx context.Context,
	in *runpb.CancelExecutionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CancelExecution(ctx, in)
}

type tasksServer struct {
	runpb.UnimplementedTasksServer
	b *InMemoryBackend
}

func (s *tasksServer) GetTask(_ context.Context, in *runpb.GetTaskRequest) (*runpb.Task, error) {
	return s.b.GetTask(in.GetName())
}

func (s *tasksServer) ListTasks(_ context.Context, in *runpb.ListTasksRequest) (*runpb.ListTasksResponse, error) {
	items, next, err := s.b.ListTasks(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListTasksResponse{Tasks: items, NextPageToken: next}, nil
}

type workerPoolsServer struct {
	runpb.UnimplementedWorkerPoolsServer
	b *InMemoryBackend
}

func (s *workerPoolsServer) CreateWorkerPool(
	_ context.Context,
	in *runpb.CreateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateWorkerPool(in)
}

func (s *workerPoolsServer) GetWorkerPool(
	_ context.Context,
	in *runpb.GetWorkerPoolRequest,
) (*runpb.WorkerPool, error) {
	return s.b.GetWorkerPool(in.GetName())
}

func (s *workerPoolsServer) ListWorkerPools(
	_ context.Context,
	in *runpb.ListWorkerPoolsRequest,
) (*runpb.ListWorkerPoolsResponse, error) {
	items, next, err := s.b.ListWorkerPools(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListWorkerPoolsResponse{WorkerPools: items, NextPageToken: next}, nil
}

func (s *workerPoolsServer) UpdateWorkerPool(
	_ context.Context,
	in *runpb.UpdateWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateWorkerPool(in)
}

func (s *workerPoolsServer) DeleteWorkerPool(
	_ context.Context,
	in *runpb.DeleteWorkerPoolRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteWorkerPool(in)
}

type instancesServer struct {
	runpb.UnimplementedInstancesServer
	b *InMemoryBackend
}

func (s *instancesServer) CreateInstance(
	_ context.Context,
	in *runpb.CreateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateInstance(in)
}

func (s *instancesServer) GetInstance(_ context.Context, in *runpb.GetInstanceRequest) (*runpb.Instance, error) {
	return s.b.GetInstance(in.GetName())
}

func (s *instancesServer) ListInstances(
	_ context.Context,
	in *runpb.ListInstancesRequest,
) (*runpb.ListInstancesResponse, error) {
	items, next, err := s.b.ListInstances(in)
	if err != nil {
		return nil, err
	}

	return &runpb.ListInstancesResponse{Instances: items, NextPageToken: next}, nil
}

func (s *instancesServer) DeleteInstance(
	_ context.Context,
	in *runpb.DeleteInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteInstance(in)
}

func (s *instancesServer) StopInstance(
	_ context.Context,
	in *runpb.StopInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.StopInstance(in)
}

func (s *instancesServer) StartInstance(
	_ context.Context,
	in *runpb.StartInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.StartInstance(in)
}

type buildsServer struct {
	runpb.UnimplementedBuildsServer
	b *InMemoryBackend
}

func (s *buildsServer) SubmitBuild(
	_ context.Context,
	in *runpb.SubmitBuildRequest,
) (*runpb.SubmitBuildResponse, error) {
	return s.b.SubmitBuild(in)
}
