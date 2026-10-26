package gcpmonitoring

import (
	"context"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type alertServer struct {
	monitoringpb.UnimplementedAlertPolicyServiceServer
	b *InMemoryBackend
}

func (s *alertServer) ListAlertPolicies(
	_ context.Context, in *monitoringpb.ListAlertPoliciesRequest,
) (*monitoringpb.ListAlertPoliciesResponse, error) {
	ps, next, total, err := s.b.ListAlertPolicies(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListAlertPoliciesResponse{AlertPolicies: ps, NextPageToken: next, TotalSize: total}, nil
}

func (s *alertServer) GetAlertPolicy(
	_ context.Context, in *monitoringpb.GetAlertPolicyRequest,
) (*monitoringpb.AlertPolicy, error) {
	return s.b.GetAlertPolicy(in.GetName())
}

func (s *alertServer) CreateAlertPolicy(
	_ context.Context, in *monitoringpb.CreateAlertPolicyRequest,
) (*monitoringpb.AlertPolicy, error) {
	return s.b.CreateAlertPolicy(in)
}

func (s *alertServer) UpdateAlertPolicy(
	_ context.Context, in *monitoringpb.UpdateAlertPolicyRequest,
) (*monitoringpb.AlertPolicy, error) {
	return s.b.UpdateAlertPolicy(in)
}

func (s *alertServer) DeleteAlertPolicy(
	_ context.Context, in *monitoringpb.DeleteAlertPolicyRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteAlertPolicy(in.GetName())
}

type channelServer struct {
	monitoringpb.UnimplementedNotificationChannelServiceServer
	b *InMemoryBackend
}

func (s *channelServer) ListNotificationChannelDescriptors(
	_ context.Context, in *monitoringpb.ListNotificationChannelDescriptorsRequest,
) (*monitoringpb.ListNotificationChannelDescriptorsResponse, error) {
	ds, next, err := s.b.ListNotificationChannelDescriptors(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListNotificationChannelDescriptorsResponse{ChannelDescriptors: ds, NextPageToken: next}, nil
}

func (s *channelServer) GetNotificationChannelDescriptor(
	_ context.Context, in *monitoringpb.GetNotificationChannelDescriptorRequest,
) (*monitoringpb.NotificationChannelDescriptor, error) {
	return s.b.GetNotificationChannelDescriptor(in.GetName())
}

func (s *channelServer) ListNotificationChannels(
	_ context.Context, in *monitoringpb.ListNotificationChannelsRequest,
) (*monitoringpb.ListNotificationChannelsResponse, error) {
	cs, next, total, err := s.b.ListNotificationChannels(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListNotificationChannelsResponse{
		NotificationChannels: cs, NextPageToken: next, TotalSize: total,
	}, nil
}

func (s *channelServer) GetNotificationChannel(
	_ context.Context, in *monitoringpb.GetNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	return s.b.GetNotificationChannel(in.GetName())
}

func (s *channelServer) CreateNotificationChannel(
	_ context.Context, in *monitoringpb.CreateNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	return s.b.CreateNotificationChannel(in)
}

func (s *channelServer) UpdateNotificationChannel(
	_ context.Context, in *monitoringpb.UpdateNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	return s.b.UpdateNotificationChannel(in)
}

func (s *channelServer) DeleteNotificationChannel(
	_ context.Context, in *monitoringpb.DeleteNotificationChannelRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteNotificationChannel(in)
}

func (s *channelServer) SendNotificationChannelVerificationCode(
	_ context.Context, in *monitoringpb.SendNotificationChannelVerificationCodeRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.SendNotificationChannelVerificationCode(in.GetName())
}

func (s *channelServer) GetNotificationChannelVerificationCode(
	_ context.Context, in *monitoringpb.GetNotificationChannelVerificationCodeRequest,
) (*monitoringpb.GetNotificationChannelVerificationCodeResponse, error) {
	return s.b.GetNotificationChannelVerificationCode(in)
}

func (s *channelServer) VerifyNotificationChannel(
	_ context.Context, in *monitoringpb.VerifyNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	return s.b.VerifyNotificationChannel(in)
}

type snoozeServer struct {
	monitoringpb.UnimplementedSnoozeServiceServer
	b *InMemoryBackend
}

func (s *snoozeServer) CreateSnooze(
	_ context.Context, in *monitoringpb.CreateSnoozeRequest,
) (*monitoringpb.Snooze, error) {
	return s.b.CreateSnooze(in)
}

func (s *snoozeServer) GetSnooze(
	_ context.Context, in *monitoringpb.GetSnoozeRequest,
) (*monitoringpb.Snooze, error) {
	return s.b.GetSnooze(in.GetName())
}

func (s *snoozeServer) ListSnoozes(
	_ context.Context, in *monitoringpb.ListSnoozesRequest,
) (*monitoringpb.ListSnoozesResponse, error) {
	ss, next, err := s.b.ListSnoozes(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListSnoozesResponse{Snoozes: ss, NextPageToken: next}, nil
}

func (s *snoozeServer) UpdateSnooze(
	_ context.Context, in *monitoringpb.UpdateSnoozeRequest,
) (*monitoringpb.Snooze, error) {
	return s.b.UpdateSnooze(in)
}

type serviceServer struct {
	monitoringpb.UnimplementedServiceMonitoringServiceServer
	b *InMemoryBackend
}

func (s *serviceServer) CreateService(
	_ context.Context, in *monitoringpb.CreateServiceRequest,
) (*monitoringpb.Service, error) {
	return s.b.CreateService(in)
}

func (s *serviceServer) GetService(
	_ context.Context, in *monitoringpb.GetServiceRequest,
) (*monitoringpb.Service, error) {
	return s.b.GetService(in.GetName())
}

func (s *serviceServer) ListServices(
	_ context.Context, in *monitoringpb.ListServicesRequest,
) (*monitoringpb.ListServicesResponse, error) {
	ss, next, err := s.b.ListServices(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListServicesResponse{Services: ss, NextPageToken: next}, nil
}

func (s *serviceServer) UpdateService(
	_ context.Context, in *monitoringpb.UpdateServiceRequest,
) (*monitoringpb.Service, error) {
	return s.b.UpdateService(in)
}

func (s *serviceServer) DeleteService(
	_ context.Context, in *monitoringpb.DeleteServiceRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteService(in.GetName())
}

func (s *serviceServer) CreateServiceLevelObjective(
	_ context.Context, in *monitoringpb.CreateServiceLevelObjectiveRequest,
) (*monitoringpb.ServiceLevelObjective, error) {
	return s.b.CreateServiceLevelObjective(in)
}

func (s *serviceServer) GetServiceLevelObjective(
	_ context.Context, in *monitoringpb.GetServiceLevelObjectiveRequest,
) (*monitoringpb.ServiceLevelObjective, error) {
	return s.b.GetServiceLevelObjective(in.GetName())
}

func (s *serviceServer) ListServiceLevelObjectives(
	_ context.Context, in *monitoringpb.ListServiceLevelObjectivesRequest,
) (*monitoringpb.ListServiceLevelObjectivesResponse, error) {
	ss, next, err := s.b.ListServiceLevelObjectives(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListServiceLevelObjectivesResponse{ServiceLevelObjectives: ss, NextPageToken: next}, nil
}

func (s *serviceServer) UpdateServiceLevelObjective(
	_ context.Context, in *monitoringpb.UpdateServiceLevelObjectiveRequest,
) (*monitoringpb.ServiceLevelObjective, error) {
	return s.b.UpdateServiceLevelObjective(in)
}

func (s *serviceServer) DeleteServiceLevelObjective(
	_ context.Context, in *monitoringpb.DeleteServiceLevelObjectiveRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteServiceLevelObjective(in.GetName())
}
