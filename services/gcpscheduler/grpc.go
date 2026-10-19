package gcpscheduler

import (
	"context"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	schedulerpb.UnimplementedCloudSchedulerServer
	b *InMemoryBackend
}

func (s *server) ListJobs(
	_ context.Context, in *schedulerpb.ListJobsRequest,
) (*schedulerpb.ListJobsResponse, error) {
	items, next, err := s.b.ListJobs(in)
	if err != nil {
		return nil, err
	}

	return &schedulerpb.ListJobsResponse{Jobs: items, NextPageToken: next}, nil
}

func (s *server) GetJob(_ context.Context, in *schedulerpb.GetJobRequest) (*schedulerpb.Job, error) {
	return s.b.GetJob(in.GetName())
}

func (s *server) CreateJob(_ context.Context, in *schedulerpb.CreateJobRequest) (*schedulerpb.Job, error) {
	return s.b.CreateJob(in)
}

func (s *server) UpdateJob(_ context.Context, in *schedulerpb.UpdateJobRequest) (*schedulerpb.Job, error) {
	return s.b.UpdateJob(in)
}

func (s *server) DeleteJob(_ context.Context, in *schedulerpb.DeleteJobRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteJob(in.GetName())
}

func (s *server) PauseJob(_ context.Context, in *schedulerpb.PauseJobRequest) (*schedulerpb.Job, error) {
	return s.b.PauseJob(in.GetName())
}

func (s *server) ResumeJob(_ context.Context, in *schedulerpb.ResumeJobRequest) (*schedulerpb.Job, error) {
	return s.b.ResumeJob(in.GetName())
}

func (s *server) RunJob(_ context.Context, in *schedulerpb.RunJobRequest) (*schedulerpb.Job, error) {
	return s.b.RunJob(in.GetName())
}

type locationsServer struct {
	location.UnimplementedLocationsServer
}

func (locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	items, next, err := ListLocations(in)
	if err != nil {
		return nil, err
	}

	return &location.ListLocationsResponse{Locations: items, NextPageToken: next}, nil
}

func (locationsServer) GetLocation(_ context.Context, in *location.GetLocationRequest) (*location.Location, error) {
	return GetLocation(in)
}
