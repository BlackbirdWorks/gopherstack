package gcpmonitoring

import (
	"context"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type groupServer struct {
	monitoringpb.UnimplementedGroupServiceServer
	b *InMemoryBackend
}

func (s *groupServer) ListGroups(
	_ context.Context, in *monitoringpb.ListGroupsRequest,
) (*monitoringpb.ListGroupsResponse, error) {
	gs, next, err := s.b.ListGroups(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListGroupsResponse{Group: gs, NextPageToken: next}, nil
}

func (s *groupServer) GetGroup(_ context.Context, in *monitoringpb.GetGroupRequest) (*monitoringpb.Group, error) {
	return s.b.GetGroup(in.GetName())
}

func (s *groupServer) CreateGroup(_ context.Context, in *monitoringpb.CreateGroupRequest) (*monitoringpb.Group, error) {
	return s.b.CreateGroup(in)
}

func (s *groupServer) UpdateGroup(_ context.Context, in *monitoringpb.UpdateGroupRequest) (*monitoringpb.Group, error) {
	return s.b.UpdateGroup(in)
}

func (s *groupServer) DeleteGroup(_ context.Context, in *monitoringpb.DeleteGroupRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteGroup(in)
}

func (s *groupServer) ListGroupMembers(
	_ context.Context, in *monitoringpb.ListGroupMembersRequest,
) (*monitoringpb.ListGroupMembersResponse, error) {
	ms, next, total, err := s.b.ListGroupMembers(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListGroupMembersResponse{Members: ms, NextPageToken: next, TotalSize: total}, nil
}

type uptimeServer struct {
	monitoringpb.UnimplementedUptimeCheckServiceServer
	b *InMemoryBackend
}

func (s *uptimeServer) ListUptimeCheckConfigs(
	_ context.Context, in *monitoringpb.ListUptimeCheckConfigsRequest,
) (*monitoringpb.ListUptimeCheckConfigsResponse, error) {
	cs, next, total, err := s.b.ListUptimeCheckConfigs(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListUptimeCheckConfigsResponse{
		UptimeCheckConfigs: cs,
		NextPageToken:      next,
		TotalSize:          total,
	}, nil
}

func (s *uptimeServer) GetUptimeCheckConfig(
	_ context.Context, in *monitoringpb.GetUptimeCheckConfigRequest,
) (*monitoringpb.UptimeCheckConfig, error) {
	return s.b.GetUptimeCheckConfig(in.GetName())
}

func (s *uptimeServer) CreateUptimeCheckConfig(
	_ context.Context, in *monitoringpb.CreateUptimeCheckConfigRequest,
) (*monitoringpb.UptimeCheckConfig, error) {
	return s.b.CreateUptimeCheckConfig(in)
}

func (s *uptimeServer) UpdateUptimeCheckConfig(
	_ context.Context, in *monitoringpb.UpdateUptimeCheckConfigRequest,
) (*monitoringpb.UptimeCheckConfig, error) {
	return s.b.UpdateUptimeCheckConfig(in)
}

func (s *uptimeServer) DeleteUptimeCheckConfig(
	_ context.Context, in *monitoringpb.DeleteUptimeCheckConfigRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteUptimeCheckConfig(in.GetName())
}

func (s *uptimeServer) ListUptimeCheckIps(
	_ context.Context, in *monitoringpb.ListUptimeCheckIpsRequest,
) (*monitoringpb.ListUptimeCheckIpsResponse, error) {
	if err := pageTokenOK(in.GetPageToken()); err != nil {
		return nil, err
	}

	pg := page.New(s.b.ListUptimeCheckIps(), in.GetPageToken(), clampPage(in.GetPageSize()), defaultPage)

	return &monitoringpb.ListUptimeCheckIpsResponse{UptimeCheckIps: pg.Data, NextPageToken: pg.Next}, nil
}
