package gcpfirestore

import (
	"context"
	"strings"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	gsScheme     = "gs://"
	metadataSfx  = ".overall_export_metadata"
	writeBatch   = 500
	exportDirFmt = "2006-01-02T15-04-05"
)

type operationsServer struct {
	longrunningpb.UnimplementedOperationsServer
	b *InMemoryBackend
}

func (s *operationsServer) GetOperation(
	_ context.Context,
	in *longrunningpb.GetOperationRequest,
) (*longrunningpb.Operation, error) {
	s.b.mu.RLock("GetOperation")
	defer s.b.mu.RUnlock()

	op, ok := s.b.admin.ops[in.GetName()]
	if !ok {
		return nil, errNotFound("Operation %q does not exist.", in.GetName())
	}

	return cloneMsg(op), nil
}

func (s *operationsServer) WaitOperation(
	ctx context.Context,
	in *longrunningpb.WaitOperationRequest,
) (*longrunningpb.Operation, error) {
	return s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: in.GetName()})
}

func (s *operationsServer) ListOperations(
	_ context.Context, in *longrunningpb.ListOperationsRequest,
) (*longrunningpb.ListOperationsResponse, error) {
	if err := validateDatabase(in.GetName()); err != nil {
		return nil, err
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("ListOperations filters are not supported.")
	}

	s.b.mu.RLock("ListOperations")
	all := sortedByName(s.b.admin.ops, in.GetName()+"/operations/")
	s.b.mu.RUnlock()

	pg, err := pageOf(all, in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	out := make([]*longrunningpb.Operation, len(pg.Data))
	for i, op := range pg.Data {
		out[i] = cloneMsg(op)
	}

	return &longrunningpb.ListOperationsResponse{Operations: out, NextPageToken: pg.Next}, nil
}

func (s *operationsServer) DeleteOperation(
	_ context.Context,
	in *longrunningpb.DeleteOperationRequest,
) (*emptypb.Empty, error) {
	s.b.mu.Lock("DeleteOperation")
	defer s.b.mu.Unlock()

	if _, ok := s.b.admin.ops[in.GetName()]; !ok {
		return nil, errNotFound("Operation %q does not exist.", in.GetName())
	}

	delete(s.b.admin.ops, in.GetName())

	return &emptypb.Empty{}, nil
}

// CancelOperation succeeds for any known operation: every operation has already finished.
func (s *operationsServer) CancelOperation(
	_ context.Context,
	in *longrunningpb.CancelOperationRequest,
) (*emptypb.Empty, error) {
	s.b.mu.RLock("CancelOperation")
	defer s.b.mu.RUnlock()

	if _, ok := s.b.admin.ops[in.GetName()]; !ok {
		return nil, errNotFound("Operation %q does not exist.", in.GetName())
	}

	return &emptypb.Empty{}, nil
}

func (b *InMemoryBackend) docsInLocked(db string, collections []string) []*firestorepb.Document {
	var out []*firestorepb.Document

	for name, rec := range b.docs {
		d := rec.cur()
		if d == nil || !strings.HasPrefix(name, db+"/documents/") {
			continue
		}

		if len(collections) == 0 || inAnyCollection(name, db, collections) {
			out = append(
				out,
				cloneMsg(d),
			)
		}
	}

	return out
}

func inAnyCollection(name, db string, collections []string) bool {
	for _, c := range collections {
		if matchesCollection(name, db+"/documents", c, true) {
			return true
		}
	}

	return false
}

func validGCSPrefix(uri string) bool {
	rest, ok := strings.CutPrefix(uri, gsScheme)

	return ok && rest != "" && !strings.HasPrefix(rest, "/")
}

func (s *adminServer) ExportDocuments(
	ctx context.Context,
	in *adminpb.ExportDocumentsRequest,
) (*longrunningpb.Operation, error) {
	if !validGCSPrefix(in.GetOutputUriPrefix()) {
		return nil, errInvalid("output_uri_prefix must be a gs://bucket[/path] URI.")
	}

	b := s.b
	b.mu.Lock("ExportDocuments")
	defer b.mu.Unlock()

	db, err := b.databaseLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	started := b.nowTS()
	docs := b.docsInLocked(db.GetName(), in.GetCollectionIds())
	dir := strings.TrimSuffix(in.GetOutputUriPrefix(), "/") + "/" + started.AsTime().Format(exportDirFmt)
	uri := dir + "/" + started.AsTime().Format(exportDirFmt) + metadataSfx

	b.admin.exports[uri] = &exportDocs{docs: docs}
	b.admin.expOrder = append(b.admin.expOrder, uri)

	for len(b.admin.expOrder) > maxExports {
		delete(b.admin.exports, b.admin.expOrder[0])
		b.admin.expOrder = b.admin.expOrder[1:]
	}

	logger.Load(ctx).InfoContext(ctx, "gcpfirestore: would write export files to Cloud Storage (kept in memory)",
		"uri", uri, "documents", len(docs))

	n := int64(len(docs))

	return b.finishOpLocked(db.GetName(), &adminpb.ExportDocumentsMetadata{
		StartTime: started, EndTime: b.nowTS(), OperationState: adminpb.OperationState_SUCCESSFUL,
		ProgressDocuments: &adminpb.Progress{EstimatedWork: n, CompletedWork: n},
		CollectionIds:     in.GetCollectionIds(), OutputUriPrefix: in.GetOutputUriPrefix(),
	}, &adminpb.ExportDocumentsResponse{OutputUriPrefix: uri})
}

func (s *adminServer) ImportDocuments(
	_ context.Context,
	in *adminpb.ImportDocumentsRequest,
) (*longrunningpb.Operation, error) {
	b := s.b

	b.mu.Lock("ImportDocuments")
	db, err := b.databaseLocked(in.GetName())

	var docs []*firestorepb.Document

	if err == nil {
		exp, ok := b.admin.exports[in.GetInputUriPrefix()]
		if !ok {
			err = errNotFound("No export found at %q.", in.GetInputUriPrefix())
		} else {
			docs = exp.docs
		}
	}

	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	started := b.snapshotTime()

	var writes []*firestorepb.Write

	for _, d := range docs {
		if len(in.GetCollectionIds()) > 0 && !inAnyCollection(d.GetName(), db.GetName(), in.GetCollectionIds()) {
			continue
		}

		name := db.GetName() + strings.TrimPrefix(d.GetName(), dbOf(d.GetName()))
		cp := cloneMsg(d)
		cp.Name, cp.CreateTime, cp.UpdateTime = name, nil, nil
		writes = append(writes, &firestorepb.Write{Operation: &firestorepb.Write_Update{Update: cp}})
	}

	if err = b.commitInBatches(db.GetName(), writes); err != nil {
		return nil, err
	}

	b.mu.Lock("ImportDocuments")
	defer b.mu.Unlock()

	n := int64(len(writes))

	return b.finishOpLocked(db.GetName(), &adminpb.ImportDocumentsMetadata{
		StartTime: started, EndTime: b.nowTS(), OperationState: adminpb.OperationState_SUCCESSFUL,
		ProgressDocuments: &adminpb.Progress{EstimatedWork: n, CompletedWork: n},
		CollectionIds:     in.GetCollectionIds(), InputUriPrefix: in.GetInputUriPrefix(),
	}, nil)
}

func (b *InMemoryBackend) snapshotTime() *timestamppb.Timestamp {
	b.mu.RLock("snapshotTime")
	defer b.mu.RUnlock()

	return b.nowTS()
}

func (b *InMemoryBackend) commitInBatches(db string, writes []*firestorepb.Write) error {
	for len(writes) > 0 {
		n := min(writeBatch, len(writes))

		if _, err := b.Commit(&firestorepb.CommitRequest{Database: db, Writes: writes[:n]}); err != nil {
			return err
		}

		writes = writes[n:]
	}

	return nil
}

func (s *adminServer) BulkDeleteDocuments(
	_ context.Context, in *adminpb.BulkDeleteDocumentsRequest,
) (*longrunningpb.Operation, error) {
	b := s.b

	b.mu.Lock("BulkDeleteDocuments")
	db, err := b.databaseLocked(in.GetName())

	var names []string

	if err == nil {
		for _, d := range b.docsInLocked(db.GetName(), in.GetCollectionIds()) {
			names = append(names, d.GetName())
		}
	}

	b.mu.Unlock()

	if err != nil {
		return nil, err
	}

	started := b.snapshotTime()
	writes := make([]*firestorepb.Write, len(names))

	for i, n := range names {
		writes[i] = &firestorepb.Write{Operation: &firestorepb.Write_Delete{Delete: n}}
	}

	if err = b.commitInBatches(db.GetName(), writes); err != nil {
		return nil, err
	}

	b.mu.Lock("BulkDeleteDocuments")
	defer b.mu.Unlock()

	n := int64(len(names))

	return b.finishOpLocked(db.GetName(), &adminpb.BulkDeleteDocumentsMetadata{
		StartTime: started, EndTime: b.nowTS(), OperationState: adminpb.OperationState_SUCCESSFUL,
		ProgressDocuments: &adminpb.Progress{EstimatedWork: n, CompletedWork: n}, CollectionIds: in.GetCollectionIds(),
	}, &adminpb.BulkDeleteDocumentsResponse{})
}
