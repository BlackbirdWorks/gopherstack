package gcplogging

import (
	"context"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type metricsServer struct {
	loggingpb.UnimplementedMetricsServiceV2Server
	b *InMemoryBackend
}

func (s *metricsServer) ListLogMetrics(
	_ context.Context, in *loggingpb.ListLogMetricsRequest,
) (*loggingpb.ListLogMetricsResponse, error) {
	ms, next, err := s.b.ListLogMetrics(in)
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListLogMetricsResponse{Metrics: ms, NextPageToken: next}, nil
}

func (s *metricsServer) GetLogMetric(
	_ context.Context,
	in *loggingpb.GetLogMetricRequest,
) (*loggingpb.LogMetric, error) {
	return s.b.GetLogMetric(in.GetMetricName())
}

func (s *metricsServer) CreateLogMetric(
	_ context.Context, in *loggingpb.CreateLogMetricRequest,
) (*loggingpb.LogMetric, error) {
	return s.b.CreateLogMetric(in)
}

func (s *metricsServer) UpdateLogMetric(
	_ context.Context, in *loggingpb.UpdateLogMetricRequest,
) (*loggingpb.LogMetric, error) {
	return s.b.UpdateLogMetric(in)
}

func (s *metricsServer) DeleteLogMetric(
	_ context.Context,
	in *loggingpb.DeleteLogMetricRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteLogMetric(in.GetMetricName())
}
