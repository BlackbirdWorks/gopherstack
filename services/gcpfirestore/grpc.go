package gcpfirestore

import (
	"context"
	"math"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	firestorepb.UnimplementedFirestoreServer
	b *InMemoryBackend
}

func (s *server) GetDocument(_ context.Context, in *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	return s.b.GetDocument(in)
}

func (s *server) ListDocuments(
	_ context.Context, in *firestorepb.ListDocumentsRequest,
) (*firestorepb.ListDocumentsResponse, error) {
	docs, next, err := s.b.ListDocuments(in)
	if err != nil {
		return nil, err
	}

	return &firestorepb.ListDocumentsResponse{Documents: docs, NextPageToken: next}, nil
}

func (s *server) CreateDocument(
	_ context.Context, in *firestorepb.CreateDocumentRequest,
) (*firestorepb.Document, error) {
	return s.b.CreateDocument(in)
}

func (s *server) UpdateDocument(
	_ context.Context, in *firestorepb.UpdateDocumentRequest,
) (*firestorepb.Document, error) {
	return s.b.UpdateDocument(in)
}

func (s *server) DeleteDocument(_ context.Context, in *firestorepb.DeleteDocumentRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteDocument(in)
}

func (s *server) BatchGetDocuments(
	in *firestorepb.BatchGetDocumentsRequest, stream firestorepb.Firestore_BatchGetDocumentsServer,
) error {
	results, txn, rt, err := s.b.BatchGetDocuments(in)
	if err != nil {
		return err
	}

	for i, r := range results {
		resp := &firestorepb.BatchGetDocumentsResponse{ReadTime: rt}
		if i == 0 {
			resp.Transaction = txn
		}

		if r.Found != nil {
			resp.Result = &firestorepb.BatchGetDocumentsResponse_Found{Found: r.Found}
		} else {
			resp.Result = &firestorepb.BatchGetDocumentsResponse_Missing{Missing: r.Missing}
		}

		if serr := stream.Send(resp); serr != nil {
			return serr
		}
	}

	if len(results) == 0 && len(txn) > 0 {
		return stream.Send(&firestorepb.BatchGetDocumentsResponse{Transaction: txn, ReadTime: rt})
	}

	return nil
}

func (s *server) BeginTransaction(
	_ context.Context, in *firestorepb.BeginTransactionRequest,
) (*firestorepb.BeginTransactionResponse, error) {
	id, err := s.b.BeginTransaction(in)
	if err != nil {
		return nil, err
	}

	return &firestorepb.BeginTransactionResponse{Transaction: id}, nil
}

func (s *server) Commit(_ context.Context, in *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	return s.b.Commit(in)
}

func (s *server) Rollback(_ context.Context, in *firestorepb.RollbackRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.Rollback(in)
}

func (s *server) BatchWrite(
	_ context.Context, in *firestorepb.BatchWriteRequest,
) (*firestorepb.BatchWriteResponse, error) {
	return s.b.BatchWrite(in)
}

func (s *server) RunQuery(in *firestorepb.RunQueryRequest, stream firestorepb.Firestore_RunQueryServer) error {
	q := in.GetStructuredQuery()
	if q == nil {
		return errInvalid("structured_query is required.")
	}

	var (
		docs    []*firestorepb.Document
		skipped int
		txn     []byte
		rt      *timestamppb.Timestamp
	)

	err := s.b.read(selOf(in), func(rv *readView) error {
		var qerr error

		docs, skipped, qerr = s.b.runQuery(in.GetParent(), q, rv)
		txn, rt = rv.newTxnID, timestamppb.New(rv.readTime)

		return qerr
	})
	if err != nil {
		return err
	}

	if len(docs) == 0 {
		return stream.Send(
			&firestorepb.RunQueryResponse{Transaction: txn, ReadTime: rt, SkippedResults: count32(skipped)})
	}

	for i, d := range docs {
		resp := &firestorepb.RunQueryResponse{Document: project(d, q.GetSelect()), ReadTime: rt}
		if i == 0 {
			resp.Transaction = txn
			resp.SkippedResults = count32(skipped)
		}

		if serr := stream.Send(resp); serr != nil {
			return serr
		}
	}

	return nil
}

func (s *server) RunAggregationQuery(
	in *firestorepb.RunAggregationQueryRequest, stream firestorepb.Firestore_RunAggregationQueryServer,
) error {
	var (
		fields map[string]*firestorepb.Value
		txn    []byte
		rt     *timestamppb.Timestamp
	)

	err := s.b.read(selOf(in), func(rv *readView) error {
		var aerr error

		fields, aerr = s.b.aggregate(in.GetParent(), in.GetStructuredAggregationQuery(), rv)
		txn, rt = rv.newTxnID, timestamppb.New(rv.readTime)

		return aerr
	})
	if err != nil {
		return err
	}

	return stream.Send(&firestorepb.RunAggregationQueryResponse{
		Result:      &firestorepb.AggregationResult{AggregateFields: fields},
		Transaction: txn,
		ReadTime:    rt,
	})
}

func (s *server) PartitionQuery(
	_ context.Context, in *firestorepb.PartitionQueryRequest,
) (*firestorepb.PartitionQueryResponse, error) {
	cursors, next, err := s.b.PartitionQuery(in)
	if err != nil {
		return nil, err
	}

	return &firestorepb.PartitionQueryResponse{Partitions: cursors, NextPageToken: next}, nil
}

func (s *server) ListCollectionIds( //nolint:revive // name fixed by the generated gRPC interface
	_ context.Context, in *firestorepb.ListCollectionIdsRequest,
) (*firestorepb.ListCollectionIdsResponse, error) {
	ids, next, err := s.b.ListCollectionIDs(in)
	if err != nil {
		return nil, err
	}

	return &firestorepb.ListCollectionIdsResponse{CollectionIds: ids, NextPageToken: next}, nil
}

func count32(n int) int32 {
	return int32(min(n, math.MaxInt32)) //nolint:gosec // clamped to the int32 range
}
