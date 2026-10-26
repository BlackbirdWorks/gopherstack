package gcpeventarc

import (
	"context"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/cloud/location"
)

type eventarcServer struct {
	eventarcpb.UnimplementedEventarcServer
	b *InMemoryBackend
}

func (s *eventarcServer) GetTrigger(_ context.Context, in *eventarcpb.GetTriggerRequest) (*eventarcpb.Trigger, error) {
	return s.b.GetTrigger(in.GetName())
}

func (s *eventarcServer) ListTriggers(
	_ context.Context, in *eventarcpb.ListTriggersRequest,
) (*eventarcpb.ListTriggersResponse, error) {
	return s.b.ListTriggers(in)
}

func (s *eventarcServer) CreateTrigger(
	_ context.Context, in *eventarcpb.CreateTriggerRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateTrigger(in)
}

func (s *eventarcServer) UpdateTrigger(
	_ context.Context, in *eventarcpb.UpdateTriggerRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateTrigger(in)
}

func (s *eventarcServer) DeleteTrigger(
	_ context.Context, in *eventarcpb.DeleteTriggerRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteTrigger(in)
}

func (s *eventarcServer) GetChannel(_ context.Context, in *eventarcpb.GetChannelRequest) (*eventarcpb.Channel, error) {
	return s.b.GetChannel(in.GetName())
}

func (s *eventarcServer) ListChannels(
	_ context.Context, in *eventarcpb.ListChannelsRequest,
) (*eventarcpb.ListChannelsResponse, error) {
	return s.b.ListChannels(in)
}

func (s *eventarcServer) CreateChannel(
	_ context.Context, in *eventarcpb.CreateChannelRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateChannel(in)
}

func (s *eventarcServer) UpdateChannel(
	_ context.Context, in *eventarcpb.UpdateChannelRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateChannel(in)
}

func (s *eventarcServer) DeleteChannel(
	_ context.Context, in *eventarcpb.DeleteChannelRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteChannel(in)
}

func (s *eventarcServer) GetProvider(
	_ context.Context, in *eventarcpb.GetProviderRequest,
) (*eventarcpb.Provider, error) {
	return s.b.GetProvider(in.GetName())
}

func (s *eventarcServer) ListProviders(
	_ context.Context, in *eventarcpb.ListProvidersRequest,
) (*eventarcpb.ListProvidersResponse, error) {
	return s.b.ListProviders(in)
}

func (s *eventarcServer) GetChannelConnection(
	_ context.Context, in *eventarcpb.GetChannelConnectionRequest,
) (*eventarcpb.ChannelConnection, error) {
	return s.b.GetChannelConnection(in.GetName())
}

func (s *eventarcServer) ListChannelConnections(
	_ context.Context, in *eventarcpb.ListChannelConnectionsRequest,
) (*eventarcpb.ListChannelConnectionsResponse, error) {
	return s.b.ListChannelConnections(in)
}

func (s *eventarcServer) CreateChannelConnection(
	_ context.Context, in *eventarcpb.CreateChannelConnectionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateChannelConnection(in)
}

func (s *eventarcServer) DeleteChannelConnection(
	_ context.Context, in *eventarcpb.DeleteChannelConnectionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteChannelConnection(in)
}

func (s *eventarcServer) GetGoogleChannelConfig(
	_ context.Context, in *eventarcpb.GetGoogleChannelConfigRequest,
) (*eventarcpb.GoogleChannelConfig, error) {
	return s.b.GetGoogleChannelConfig(in.GetName())
}

func (s *eventarcServer) UpdateGoogleChannelConfig(
	_ context.Context, in *eventarcpb.UpdateGoogleChannelConfigRequest,
) (*eventarcpb.GoogleChannelConfig, error) {
	return s.b.UpdateGoogleChannelConfig(in)
}

type iamServer struct {
	iampb.UnimplementedIAMPolicyServer
	b *InMemoryBackend
}

func (s *iamServer) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource(), in.GetOptions().GetRequestedPolicyVersion())
}

func (s *iamServer) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *iamServer) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

type locationsServer struct {
	location.UnimplementedLocationsServer
	b *InMemoryBackend
}

func (s locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	return s.b.ListLocations(in)
}

func (s locationsServer) GetLocation(_ context.Context, in *location.GetLocationRequest) (*location.Location, error) {
	return s.b.GetLocation(in.GetName())
}
