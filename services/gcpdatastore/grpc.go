package gcpdatastore

import (
	"context"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
)

type server struct {
	datastorepb.UnimplementedDatastoreServer
	b *InMemoryBackend
}

func (s *server) Lookup(_ context.Context, in *datastorepb.LookupRequest) (*datastorepb.LookupResponse, error) {
	return s.b.Lookup(in)
}

func (s *server) RunQuery(_ context.Context, in *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	return s.b.RunQuery(in)
}

func (s *server) RunAggregationQuery(
	_ context.Context, in *datastorepb.RunAggregationQueryRequest,
) (*datastorepb.RunAggregationQueryResponse, error) {
	return s.b.RunAggregationQuery(in)
}

func (s *server) BeginTransaction(
	_ context.Context, in *datastorepb.BeginTransactionRequest,
) (*datastorepb.BeginTransactionResponse, error) {
	return s.b.BeginTransaction(in)
}

func (s *server) Commit(_ context.Context, in *datastorepb.CommitRequest) (*datastorepb.CommitResponse, error) {
	return s.b.Commit(in)
}

func (s *server) Rollback(_ context.Context, in *datastorepb.RollbackRequest) (*datastorepb.RollbackResponse, error) {
	return s.b.Rollback(in)
}

func (s *server) AllocateIds( //nolint:revive // name fixed by the generated gRPC interface
	_ context.Context, in *datastorepb.AllocateIdsRequest,
) (*datastorepb.AllocateIdsResponse, error) {
	return s.b.AllocateIDs(in)
}

func (s *server) ReserveIds( //nolint:revive // name fixed by the generated gRPC interface
	_ context.Context, in *datastorepb.ReserveIdsRequest,
) (*datastorepb.ReserveIdsResponse, error) {
	return s.b.ReserveIDs(in)
}
