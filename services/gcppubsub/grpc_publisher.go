package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type publisherServer struct {
	pubsubpb.UnimplementedPublisherServer
	b *InMemoryBackend
}

func (s *publisherServer) CreateTopic(_ context.Context, in *pubsubpb.Topic) (*pubsubpb.Topic, error) {
	return s.b.CreateTopic(in)
}

func (s *publisherServer) UpdateTopic(_ context.Context, in *pubsubpb.UpdateTopicRequest) (*pubsubpb.Topic, error) {
	return s.b.UpdateTopic(in)
}

func (s *publisherServer) Publish(_ context.Context, in *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	ids, err := s.b.Publish(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.PublishResponse{MessageIds: ids}, nil
}

func (s *publisherServer) GetTopic(_ context.Context, in *pubsubpb.GetTopicRequest) (*pubsubpb.Topic, error) {
	return s.b.GetTopic(in.GetTopic())
}

func (s *publisherServer) ListTopics(
	_ context.Context,
	in *pubsubpb.ListTopicsRequest,
) (*pubsubpb.ListTopicsResponse, error) {
	topics, next, err := s.b.ListTopics(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListTopicsResponse{Topics: topics, NextPageToken: next}, nil
}

func (s *publisherServer) ListTopicSubscriptions(
	_ context.Context,
	in *pubsubpb.ListTopicSubscriptionsRequest,
) (*pubsubpb.ListTopicSubscriptionsResponse, error) {
	names, next, err := s.b.ListTopicSubscriptions(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListTopicSubscriptionsResponse{Subscriptions: names, NextPageToken: next}, nil
}

func (s *publisherServer) ListTopicSnapshots(
	_ context.Context,
	in *pubsubpb.ListTopicSnapshotsRequest,
) (*pubsubpb.ListTopicSnapshotsResponse, error) {
	names, next, err := s.b.ListTopicSnapshots(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListTopicSnapshotsResponse{Snapshots: names, NextPageToken: next}, nil
}

func (s *publisherServer) DeleteTopic(_ context.Context, in *pubsubpb.DeleteTopicRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteTopic(in.GetTopic())
}

func (s *publisherServer) DetachSubscription(
	_ context.Context,
	in *pubsubpb.DetachSubscriptionRequest,
) (*pubsubpb.DetachSubscriptionResponse, error) {
	return &pubsubpb.DetachSubscriptionResponse{}, s.b.DetachSubscription(in.GetSubscription())
}

type schemaServer struct {
	pubsubpb.UnimplementedSchemaServiceServer
	b *InMemoryBackend
}

func (s *schemaServer) CreateSchema(_ context.Context, in *pubsubpb.CreateSchemaRequest) (*pubsubpb.Schema, error) {
	return s.b.CreateSchema(in)
}

func (s *schemaServer) GetSchema(_ context.Context, in *pubsubpb.GetSchemaRequest) (*pubsubpb.Schema, error) {
	return s.b.GetSchema(in)
}

func (s *schemaServer) ListSchemas(
	_ context.Context,
	in *pubsubpb.ListSchemasRequest,
) (*pubsubpb.ListSchemasResponse, error) {
	schemas, next, err := s.b.ListSchemas(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListSchemasResponse{Schemas: schemas, NextPageToken: next}, nil
}

func (s *schemaServer) ListSchemaRevisions(
	_ context.Context,
	in *pubsubpb.ListSchemaRevisionsRequest,
) (*pubsubpb.ListSchemaRevisionsResponse, error) {
	schemas, next, err := s.b.ListSchemaRevisions(in)
	if err != nil {
		return nil, err
	}

	return &pubsubpb.ListSchemaRevisionsResponse{Schemas: schemas, NextPageToken: next}, nil
}

func (s *schemaServer) CommitSchema(_ context.Context, in *pubsubpb.CommitSchemaRequest) (*pubsubpb.Schema, error) {
	return s.b.CommitSchema(in)
}

func (s *schemaServer) RollbackSchema(_ context.Context, in *pubsubpb.RollbackSchemaRequest) (*pubsubpb.Schema, error) {
	return s.b.RollbackSchema(in)
}

func (s *schemaServer) DeleteSchemaRevision(
	_ context.Context,
	in *pubsubpb.DeleteSchemaRevisionRequest,
) (*pubsubpb.Schema, error) {
	return s.b.DeleteSchemaRevision(in)
}

func (s *schemaServer) DeleteSchema(_ context.Context, in *pubsubpb.DeleteSchemaRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteSchema(in.GetName())
}

func (s *schemaServer) ValidateSchema(
	_ context.Context,
	in *pubsubpb.ValidateSchemaRequest,
) (*pubsubpb.ValidateSchemaResponse, error) {
	if err := checkProject(in.GetParent()); err != nil {
		return nil, err
	}

	if in.GetSchema() == nil {
		return nil, errInvalid("schema must be provided")
	}

	if err := validateDefinition(in.GetSchema().GetType(), in.GetSchema().GetDefinition()); err != nil {
		return nil, err
	}

	return &pubsubpb.ValidateSchemaResponse{}, nil
}

func (s *schemaServer) ValidateMessage(
	_ context.Context,
	in *pubsubpb.ValidateMessageRequest,
) (*pubsubpb.ValidateMessageResponse, error) {
	if err := s.b.ValidateMessage(in); err != nil {
		return nil, err
	}

	return &pubsubpb.ValidateMessageResponse{}, nil
}
