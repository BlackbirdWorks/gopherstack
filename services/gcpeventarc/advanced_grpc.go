package gcpeventarc

import (
	"context"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
)

func listed[T proto.Message](items []proto.Message) []T {
	out := make([]T, 0, len(items))

	for _, it := range items {
		if t, ok := it.(T); ok {
			out = append(out, t)
		}
	}

	return out
}

func (s *eventarcServer) GetMessageBus(
	_ context.Context,
	in *eventarcpb.GetMessageBusRequest,
) (*eventarcpb.MessageBus, error) {
	m, err := s.b.advGet(busKind(), in.GetName())
	if err != nil {
		return nil, err
	}

	bus, _ := m.(*eventarcpb.MessageBus)

	return bus, nil
}

func (s *eventarcServer) ListMessageBuses(
	_ context.Context, in *eventarcpb.ListMessageBusesRequest,
) (*eventarcpb.ListMessageBusesResponse, error) {
	items, next, err := s.b.advList(
		busKind(),
		in.GetParent(),
		in.GetOrderBy(),
		in.GetFilter(),
		in.GetPageSize(),
		in.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}

	return &eventarcpb.ListMessageBusesResponse{
		MessageBuses:  listed[*eventarcpb.MessageBus](items),
		NextPageToken: next,
	}, nil
}

func (s *eventarcServer) ListMessageBusEnrollments(
	_ context.Context, in *eventarcpb.ListMessageBusEnrollmentsRequest,
) (*eventarcpb.ListMessageBusEnrollmentsResponse, error) {
	return s.b.ListMessageBusEnrollments(in)
}

func (s *eventarcServer) CreateMessageBus(
	_ context.Context, in *eventarcpb.CreateMessageBusRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advCreate(
		busKind(),
		in.GetParent(),
		in.GetMessageBusId(),
		in.GetMessageBus(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) UpdateMessageBus(
	_ context.Context, in *eventarcpb.UpdateMessageBusRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advUpdate(
		busKind(),
		in.GetMessageBus(),
		in.GetUpdateMask(),
		in.GetAllowMissing(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) DeleteMessageBus(
	_ context.Context, in *eventarcpb.DeleteMessageBusRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advDelete(busKind(), in.GetName(), in.GetEtag(), in.GetAllowMissing(), in.GetValidateOnly())
}

func (s *eventarcServer) GetEnrollment(
	_ context.Context,
	in *eventarcpb.GetEnrollmentRequest,
) (*eventarcpb.Enrollment, error) {
	m, err := s.b.advGet(enrollKind(), in.GetName())
	if err != nil {
		return nil, err
	}

	e, _ := m.(*eventarcpb.Enrollment)

	return e, nil
}

func (s *eventarcServer) ListEnrollments(
	_ context.Context, in *eventarcpb.ListEnrollmentsRequest,
) (*eventarcpb.ListEnrollmentsResponse, error) {
	items, next, err := s.b.advList(
		enrollKind(),
		in.GetParent(),
		in.GetOrderBy(),
		in.GetFilter(),
		in.GetPageSize(),
		in.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}

	return &eventarcpb.ListEnrollmentsResponse{
		Enrollments:   listed[*eventarcpb.Enrollment](items),
		NextPageToken: next,
	}, nil
}

func (s *eventarcServer) CreateEnrollment(
	_ context.Context, in *eventarcpb.CreateEnrollmentRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advCreate(
		enrollKind(),
		in.GetParent(),
		in.GetEnrollmentId(),
		in.GetEnrollment(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) UpdateEnrollment(
	_ context.Context, in *eventarcpb.UpdateEnrollmentRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advUpdate(
		enrollKind(),
		in.GetEnrollment(),
		in.GetUpdateMask(),
		in.GetAllowMissing(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) DeleteEnrollment(
	_ context.Context, in *eventarcpb.DeleteEnrollmentRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advDelete(enrollKind(), in.GetName(), in.GetEtag(), in.GetAllowMissing(), in.GetValidateOnly())
}

func (s *eventarcServer) GetPipeline(
	_ context.Context,
	in *eventarcpb.GetPipelineRequest,
) (*eventarcpb.Pipeline, error) {
	m, err := s.b.advGet(pipelineKind(), in.GetName())
	if err != nil {
		return nil, err
	}

	p, _ := m.(*eventarcpb.Pipeline)

	return p, nil
}

func (s *eventarcServer) ListPipelines(
	_ context.Context, in *eventarcpb.ListPipelinesRequest,
) (*eventarcpb.ListPipelinesResponse, error) {
	items, next, err := s.b.advList(
		pipelineKind(),
		in.GetParent(),
		in.GetOrderBy(),
		in.GetFilter(),
		in.GetPageSize(),
		in.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}

	return &eventarcpb.ListPipelinesResponse{Pipelines: listed[*eventarcpb.Pipeline](items), NextPageToken: next}, nil
}

func (s *eventarcServer) CreatePipeline(
	_ context.Context, in *eventarcpb.CreatePipelineRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advCreate(
		pipelineKind(),
		in.GetParent(),
		in.GetPipelineId(),
		in.GetPipeline(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) UpdatePipeline(
	_ context.Context, in *eventarcpb.UpdatePipelineRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advUpdate(
		pipelineKind(),
		in.GetPipeline(),
		in.GetUpdateMask(),
		in.GetAllowMissing(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

func (s *eventarcServer) DeletePipeline(
	_ context.Context, in *eventarcpb.DeletePipelineRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advDelete(pipelineKind(), in.GetName(), in.GetEtag(), in.GetAllowMissing(), in.GetValidateOnly())
}

//nolint:revive,staticcheck // method name fixed by the generated EventarcServer interface
func (s *eventarcServer) GetGoogleApiSource(
	_ context.Context, in *eventarcpb.GetGoogleApiSourceRequest,
) (*eventarcpb.GoogleApiSource, error) {
	m, err := s.b.advGet(sourceKind(), in.GetName())
	if err != nil {
		return nil, err
	}

	g, _ := m.(*eventarcpb.GoogleApiSource)

	return g, nil
}

//nolint:revive,staticcheck // method name fixed by the generated EventarcServer interface
func (s *eventarcServer) ListGoogleApiSources(
	_ context.Context, in *eventarcpb.ListGoogleApiSourcesRequest,
) (*eventarcpb.ListGoogleApiSourcesResponse, error) {
	items, next, err := s.b.advList(
		sourceKind(),
		in.GetParent(),
		in.GetOrderBy(),
		in.GetFilter(),
		in.GetPageSize(),
		in.GetPageToken(),
	)
	if err != nil {
		return nil, err
	}

	return &eventarcpb.ListGoogleApiSourcesResponse{
		GoogleApiSources: listed[*eventarcpb.GoogleApiSource](items), NextPageToken: next,
	}, nil
}

//nolint:revive,staticcheck // method name fixed by the generated EventarcServer interface
func (s *eventarcServer) CreateGoogleApiSource(
	_ context.Context, in *eventarcpb.CreateGoogleApiSourceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advCreate(
		sourceKind(),
		in.GetParent(),
		in.GetGoogleApiSourceId(),
		in.GetGoogleApiSource(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

//nolint:revive,staticcheck // method name fixed by the generated EventarcServer interface
func (s *eventarcServer) UpdateGoogleApiSource(
	_ context.Context, in *eventarcpb.UpdateGoogleApiSourceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advUpdate(
		sourceKind(),
		in.GetGoogleApiSource(),
		in.GetUpdateMask(),
		in.GetAllowMissing(),
		in.GetValidateOnly(),
		s.b.validateAdvanced,
	)
}

//nolint:revive,staticcheck // method name fixed by the generated EventarcServer interface
func (s *eventarcServer) DeleteGoogleApiSource(
	_ context.Context, in *eventarcpb.DeleteGoogleApiSourceRequest,
) (*longrunningpb.Operation, error) {
	return s.b.advDelete(sourceKind(), in.GetName(), in.GetEtag(), in.GetAllowMissing(), in.GetValidateOnly())
}
