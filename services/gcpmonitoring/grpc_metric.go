package gcpmonitoring

import (
	"context"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/emptypb"
)

type metricServer struct {
	monitoringpb.UnimplementedMetricServiceServer
	b *InMemoryBackend
}

func (s *metricServer) ListMonitoredResourceDescriptors(
	_ context.Context, in *monitoringpb.ListMonitoredResourceDescriptorsRequest,
) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	ds, next, err := s.b.ListMonitoredResourceDescriptors(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: ds, NextPageToken: next}, nil
}

func (s *metricServer) GetMonitoredResourceDescriptor(
	_ context.Context, in *monitoringpb.GetMonitoredResourceDescriptorRequest,
) (*monitoredres.MonitoredResourceDescriptor, error) {
	return s.b.GetMonitoredResourceDescriptor(in.GetName())
}

func (s *metricServer) ListMetricDescriptors(
	_ context.Context, in *monitoringpb.ListMetricDescriptorsRequest,
) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	ds, next, err := s.b.ListMetricDescriptors(in)
	if err != nil {
		return nil, err
	}

	return &monitoringpb.ListMetricDescriptorsResponse{MetricDescriptors: ds, NextPageToken: next}, nil
}

func (s *metricServer) GetMetricDescriptor(
	_ context.Context, in *monitoringpb.GetMetricDescriptorRequest,
) (*metricpb.MetricDescriptor, error) {
	return s.b.GetMetricDescriptor(in.GetName())
}

func (s *metricServer) CreateMetricDescriptor(
	_ context.Context, in *monitoringpb.CreateMetricDescriptorRequest,
) (*metricpb.MetricDescriptor, error) {
	return s.b.CreateMetricDescriptor(in)
}

func (s *metricServer) DeleteMetricDescriptor(
	_ context.Context, in *monitoringpb.DeleteMetricDescriptorRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteMetricDescriptor(in.GetName())
}

func (s *metricServer) ListTimeSeries(
	_ context.Context, in *monitoringpb.ListTimeSeriesRequest,
) (*monitoringpb.ListTimeSeriesResponse, error) {
	return s.b.ListTimeSeries(in)
}

func (s *metricServer) CreateTimeSeries(
	_ context.Context, in *monitoringpb.CreateTimeSeriesRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.CreateTimeSeries(in)
}

func (s *metricServer) CreateServiceTimeSeries(
	_ context.Context, in *monitoringpb.CreateTimeSeriesRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.CreateServiceTimeSeries(in)
}
