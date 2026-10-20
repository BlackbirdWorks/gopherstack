package gcpfunctions

import (
	"context"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

type server struct {
	functionspb.UnimplementedFunctionServiceServer
	b *InMemoryBackend
}

func (s *server) GetFunction(_ context.Context, in *functionspb.GetFunctionRequest) (*functionspb.Function, error) {
	if in.GetRevision() != "" {
		return nil, errInvalid("Fetching a specific revision is not supported by the emulator.")
	}

	return s.b.GetFunction(in.GetName())
}

func (s *server) ListFunctions(
	_ context.Context, in *functionspb.ListFunctionsRequest,
) (*functionspb.ListFunctionsResponse, error) {
	items, next, err := s.b.ListFunctions(in)
	if err != nil {
		return nil, err
	}

	return &functionspb.ListFunctionsResponse{Functions: items, NextPageToken: next}, nil
}

func (s *server) CreateFunction(
	_ context.Context, in *functionspb.CreateFunctionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateFunction(in)
}

func (s *server) UpdateFunction(
	_ context.Context, in *functionspb.UpdateFunctionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateFunction(in)
}

func (s *server) DeleteFunction(
	_ context.Context, in *functionspb.DeleteFunctionRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteFunction(in)
}

//nolint:revive,staticcheck // name fixed by the generated gRPC interface
func (s *server) GenerateUploadUrl(
	_ context.Context, in *functionspb.GenerateUploadUrlRequest,
) (*functionspb.GenerateUploadUrlResponse, error) {
	return s.b.GenerateUploadURL(in)
}

//nolint:revive,staticcheck // name fixed by the generated gRPC interface
func (s *server) GenerateDownloadUrl(
	_ context.Context, in *functionspb.GenerateDownloadUrlRequest,
) (*functionspb.GenerateDownloadUrlResponse, error) {
	return s.b.GenerateDownloadURL(in)
}

func (s *server) ListRuntimes(
	_ context.Context, in *functionspb.ListRuntimesRequest,
) (*functionspb.ListRuntimesResponse, error) {
	return s.b.ListRuntimes(in)
}
