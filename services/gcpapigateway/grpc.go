package gcpapigateway

import (
	"context"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/protobuf/types/known/emptypb"
)

type apiGatewayServer struct {
	apigatewaypb.UnimplementedApiGatewayServiceServer
	b *InMemoryBackend
}

func (s *apiGatewayServer) ListGateways(
	_ context.Context, in *apigatewaypb.ListGatewaysRequest,
) (*apigatewaypb.ListGatewaysResponse, error) {
	return s.b.ListGateways(in)
}

func (s *apiGatewayServer) GetGateway(
	_ context.Context, in *apigatewaypb.GetGatewayRequest,
) (*apigatewaypb.Gateway, error) {
	return s.b.GetGateway(in.GetName())
}

func (s *apiGatewayServer) CreateGateway(
	_ context.Context, in *apigatewaypb.CreateGatewayRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateGateway(in)
}

func (s *apiGatewayServer) UpdateGateway(
	_ context.Context, in *apigatewaypb.UpdateGatewayRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateGateway(in)
}

func (s *apiGatewayServer) DeleteGateway(
	_ context.Context, in *apigatewaypb.DeleteGatewayRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteGateway(in)
}

func (s *apiGatewayServer) ListApis(
	_ context.Context, in *apigatewaypb.ListApisRequest,
) (*apigatewaypb.ListApisResponse, error) {
	return s.b.ListApis(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) GetApi(_ context.Context, in *apigatewaypb.GetApiRequest) (*apigatewaypb.Api, error) {
	return s.b.GetAPI(in.GetName())
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) CreateApi(
	_ context.Context, in *apigatewaypb.CreateApiRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateAPI(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) UpdateApi(
	_ context.Context, in *apigatewaypb.UpdateApiRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateAPI(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) DeleteApi(
	_ context.Context, in *apigatewaypb.DeleteApiRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteAPI(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) ListApiConfigs(
	_ context.Context, in *apigatewaypb.ListApiConfigsRequest,
) (*apigatewaypb.ListApiConfigsResponse, error) {
	return s.b.ListAPIConfigs(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) GetApiConfig(
	_ context.Context, in *apigatewaypb.GetApiConfigRequest,
) (*apigatewaypb.ApiConfig, error) {
	return s.b.GetAPIConfig(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) CreateApiConfig(
	_ context.Context, in *apigatewaypb.CreateApiConfigRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateAPIConfig(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) UpdateApiConfig(
	_ context.Context, in *apigatewaypb.UpdateApiConfigRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateAPIConfig(in)
}

//nolint:revive,staticcheck,nolintlint // method name fixed by the generated gRPC interface
func (s *apiGatewayServer) DeleteApiConfig(
	_ context.Context, in *apigatewaypb.DeleteApiConfigRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteAPIConfig(in)
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

func (s *operationsServer) CancelOperation(
	_ context.Context, in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	return s.b.CancelOperation(in.GetName())
}
