package gcpredis

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/genproto/googleapis/cloud/location"
)

type server struct {
	redispb.UnimplementedCloudRedisServer
	b *InMemoryBackend
}

func (s *server) ListInstances(
	_ context.Context, in *redispb.ListInstancesRequest,
) (*redispb.ListInstancesResponse, error) {
	items, next, err := s.b.ListInstances(in)
	if err != nil {
		return nil, err
	}

	return &redispb.ListInstancesResponse{Instances: items, NextPageToken: next}, nil
}

func (s *server) GetInstance(_ context.Context, in *redispb.GetInstanceRequest) (*redispb.Instance, error) {
	return s.b.GetInstance(in.GetName())
}

func (s *server) GetInstanceAuthString(
	_ context.Context, in *redispb.GetInstanceAuthStringRequest,
) (*redispb.InstanceAuthString, error) {
	return s.b.GetInstanceAuthString(in.GetName())
}

func (s *server) CreateInstance(
	_ context.Context, in *redispb.CreateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateInstance(in)
}

func (s *server) UpdateInstance(
	_ context.Context, in *redispb.UpdateInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateInstance(in)
}

func (s *server) UpgradeInstance(
	_ context.Context, in *redispb.UpgradeInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpgradeInstance(in)
}

func (s *server) ImportInstance(
	_ context.Context, in *redispb.ImportInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.ImportInstance(in)
}

func (s *server) ExportInstance(
	_ context.Context, in *redispb.ExportInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.ExportInstance(in)
}

func (s *server) FailoverInstance(
	_ context.Context, in *redispb.FailoverInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.FailoverInstance(in)
}

func (s *server) DeleteInstance(
	_ context.Context, in *redispb.DeleteInstanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteInstance(in.GetName())
}

func (s *server) RescheduleMaintenance(
	_ context.Context, in *redispb.RescheduleMaintenanceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.RescheduleMaintenance(in)
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
