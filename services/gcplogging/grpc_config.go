package gcplogging

import (
	"context"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type configServer struct {
	loggingpb.UnimplementedConfigServiceV2Server
	b *InMemoryBackend
}

func (s *configServer) ListBuckets(
	_ context.Context, in *loggingpb.ListBucketsRequest,
) (*loggingpb.ListBucketsResponse, error) {
	bs, next, err := s.b.ListBuckets(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListBucketsResponse{Buckets: bs, NextPageToken: next}, nil
}

func (s *configServer) GetBucket(_ context.Context, in *loggingpb.GetBucketRequest) (*loggingpb.LogBucket, error) {
	return s.b.GetBucket(in.GetName())
}

func (s *configServer) CreateBucket(
	_ context.Context,
	in *loggingpb.CreateBucketRequest,
) (*loggingpb.LogBucket, error) {
	return s.b.CreateBucket(in)
}

func (s *configServer) UpdateBucket(
	_ context.Context,
	in *loggingpb.UpdateBucketRequest,
) (*loggingpb.LogBucket, error) {
	return s.b.UpdateBucket(in)
}

func (s *configServer) DeleteBucket(_ context.Context, in *loggingpb.DeleteBucketRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteBucket(in.GetName())
}

func (s *configServer) UndeleteBucket(_ context.Context, in *loggingpb.UndeleteBucketRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.UndeleteBucket(in.GetName())
}

func (s *configServer) CreateBucketAsync(
	_ context.Context, in *loggingpb.CreateBucketRequest,
) (*longrunningpb.Operation, error) {
	return s.b.createBucketOp(in)
}

func (s *configServer) UpdateBucketAsync(
	_ context.Context, in *loggingpb.UpdateBucketRequest,
) (*longrunningpb.Operation, error) {
	return s.b.updateBucketOp(in)
}

func (s *configServer) ListViews(
	_ context.Context,
	in *loggingpb.ListViewsRequest,
) (*loggingpb.ListViewsResponse, error) {
	vs, next, err := s.b.ListViews(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListViewsResponse{Views: vs, NextPageToken: next}, nil
}

func (s *configServer) GetView(_ context.Context, in *loggingpb.GetViewRequest) (*loggingpb.LogView, error) {
	return s.b.GetView(in.GetName())
}

func (s *configServer) CreateView(_ context.Context, in *loggingpb.CreateViewRequest) (*loggingpb.LogView, error) {
	return s.b.CreateView(in)
}

func (s *configServer) UpdateView(_ context.Context, in *loggingpb.UpdateViewRequest) (*loggingpb.LogView, error) {
	return s.b.UpdateView(in)
}

func (s *configServer) DeleteView(_ context.Context, in *loggingpb.DeleteViewRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteView(in.GetName())
}

func (s *configServer) ListSinks(
	_ context.Context,
	in *loggingpb.ListSinksRequest,
) (*loggingpb.ListSinksResponse, error) {
	ss, next, err := s.b.ListSinks(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListSinksResponse{Sinks: ss, NextPageToken: next}, nil
}

func (s *configServer) GetSink(_ context.Context, in *loggingpb.GetSinkRequest) (*loggingpb.LogSink, error) {
	return s.b.GetSink(in.GetSinkName())
}

func (s *configServer) CreateSink(_ context.Context, in *loggingpb.CreateSinkRequest) (*loggingpb.LogSink, error) {
	return s.b.CreateSink(in)
}

func (s *configServer) UpdateSink(_ context.Context, in *loggingpb.UpdateSinkRequest) (*loggingpb.LogSink, error) {
	return s.b.UpdateSink(in)
}

func (s *configServer) DeleteSink(_ context.Context, in *loggingpb.DeleteSinkRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteSink(in.GetSinkName())
}

func (s *configServer) ListExclusions(
	_ context.Context, in *loggingpb.ListExclusionsRequest,
) (*loggingpb.ListExclusionsResponse, error) {
	xs, next, err := s.b.ListExclusions(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListExclusionsResponse{Exclusions: xs, NextPageToken: next}, nil
}

func (s *configServer) GetExclusion(
	_ context.Context,
	in *loggingpb.GetExclusionRequest,
) (*loggingpb.LogExclusion, error) {
	return s.b.GetExclusion(in.GetName())
}

func (s *configServer) CreateExclusion(
	_ context.Context, in *loggingpb.CreateExclusionRequest,
) (*loggingpb.LogExclusion, error) {
	return s.b.CreateExclusion(in)
}

func (s *configServer) UpdateExclusion(
	_ context.Context, in *loggingpb.UpdateExclusionRequest,
) (*loggingpb.LogExclusion, error) {
	return s.b.UpdateExclusion(in)
}

func (s *configServer) DeleteExclusion(
	_ context.Context,
	in *loggingpb.DeleteExclusionRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteExclusion(in.GetName())
}

func (s *configServer) GetCmekSettings(
	_ context.Context, in *loggingpb.GetCmekSettingsRequest,
) (*loggingpb.CmekSettings, error) {
	return s.b.GetCmekSettings(in.GetName())
}

func (s *configServer) UpdateCmekSettings(
	_ context.Context, in *loggingpb.UpdateCmekSettingsRequest,
) (*loggingpb.CmekSettings, error) {
	return s.b.UpdateCmekSettings(in)
}

func (s *configServer) GetSettings(_ context.Context, in *loggingpb.GetSettingsRequest) (*loggingpb.Settings, error) {
	return s.b.GetSettings(in.GetName())
}

func (s *configServer) UpdateSettings(
	_ context.Context,
	in *loggingpb.UpdateSettingsRequest,
) (*loggingpb.Settings, error) {
	return s.b.UpdateSettings(in)
}
