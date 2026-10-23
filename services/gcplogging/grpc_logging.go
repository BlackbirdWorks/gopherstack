package gcplogging

import (
	"context"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type loggingServer struct {
	loggingpb.UnimplementedLoggingServiceV2Server
	b *InMemoryBackend
}

func (s *loggingServer) WriteLogEntries(
	ctx context.Context, in *loggingpb.WriteLogEntriesRequest,
) (*loggingpb.WriteLogEntriesResponse, error) {
	if err := s.b.WriteLogEntries(ctx, in); err != nil {
		return nil, err
	}

	return &loggingpb.WriteLogEntriesResponse{}, nil
}

func (s *loggingServer) ListLogEntries(
	_ context.Context, in *loggingpb.ListLogEntriesRequest,
) (*loggingpb.ListLogEntriesResponse, error) {
	entries, next, err := s.b.ListLogEntries(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListLogEntriesResponse{Entries: entries, NextPageToken: next}, nil
}

func (s *loggingServer) ListLogs(
	_ context.Context,
	in *loggingpb.ListLogsRequest,
) (*loggingpb.ListLogsResponse, error) {
	names, next, err := s.b.ListLogs(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListLogsResponse{LogNames: names, NextPageToken: next}, nil
}

func (s *loggingServer) DeleteLog(_ context.Context, in *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteLog(in.GetLogName())
}

func (s *loggingServer) ListMonitoredResourceDescriptors(
	_ context.Context, in *loggingpb.ListMonitoredResourceDescriptorsRequest,
) (*loggingpb.ListMonitoredResourceDescriptorsResponse, error) {
	ds, next, err := s.b.ListMonitoredResourceDescriptors(in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: ds, NextPageToken: next}, nil
}

func (s *loggingServer) TailLogEntries(stream loggingpb.LoggingServiceV2_TailLogEntriesServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	return s.b.Tail(stream, req)
}
