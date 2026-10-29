package dashboard

import (
	"context"
	"net/http"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	gcpFirestoreProxyPrefix = "/dashboard/api/v1/gcp/firestore"
	gcpFirestoreDBSegs      = 4
	gcpFirestoreDocsSegs    = 5
	gcpFirestoreProjectIdx  = 0
	gcpFirestoreDatabaseIdx = 2
	gcpFirestoreDocumentsIx = 4
)

// registerGCPFirestoreRoutes exposes Firestore to the SPA as the REST JSON shapes of
// projects.databases.documents, calling the gRPC backend in-process.
func (h *DashboardHandler) registerGCPFirestoreRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPFirestoreOps; ops != nil {
		route = (&gcpFirestoreAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpFirestoreProxyPrefix, route)
}

type gcpFirestoreAPI struct {
	api firestorepb.FirestoreServer
}

// gcpFirestoreStream collects server-streaming responses so they can be returned as a JSON array.
type gcpFirestoreStream[T proto.Message] struct {
	grpc.ServerStream

	ctx context.Context //nolint:containedctx // adapts a gRPC stream to a unary call
	out []T
}

func (s *gcpFirestoreStream[T]) Send(m T) error {
	s.out = append(s.out, m)

	return nil
}

func (s *gcpFirestoreStream[T]) Context() context.Context { return s.ctx }

func gcpStreamToList[T proto.Message](items []T) (*structpb.ListValue, error) {
	out := &structpb.ListValue{Values: make([]*structpb.Value, 0, len(items))}

	for _, m := range items {
		b, err := protojson.Marshal(m)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		v := &structpb.Value{}
		if err = protojson.Unmarshal(b, v); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		out.Values = append(out.Values, v)
	}

	return out, nil
}

func (a *gcpFirestoreAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) < gcpFirestoreDocsSegs ||
		r.segs[gcpFirestoreProjectIdx] != gcpRESTProjectsSeg ||
		r.segs[gcpFirestoreDatabaseIdx] != "databases" ||
		r.segs[gcpFirestoreDocumentsIx] != "documents" {
		return nil, errGCPUnsupported()
	}

	if r.verb != "" {
		if r.method != http.MethodPost {
			return nil, errGCPUnsupported()
		}

		return a.verbCall(ctx, r)
	}

	rel := len(r.segs) - gcpFirestoreDocsSegs

	switch {
	case rel == 0:
		return nil, errGCPUnsupported()
	case rel%2 == 1:
		return a.collection(ctx, r)
	default:
		return a.document(ctx, r)
	}
}

func (a *gcpFirestoreAPI) collection(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	last := len(r.segs) - 1
	parent, id := r.name(last), r.segs[last]

	switch r.method {
	case http.MethodGet:
		return a.api.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{
			Parent: parent, CollectionId: id, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			OrderBy: r.query.Get("orderBy"), ShowMissing: r.query.Get("showMissing") == queryTrue,
		})
	case http.MethodPost:
		doc := &firestorepb.Document{}
		if err := r.unmarshal(doc); err != nil {
			return nil, err
		}

		return a.api.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
			Parent: parent, CollectionId: id, DocumentId: r.query.Get("documentId"), Document: doc,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpFirestoreAPI) document(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(len(r.segs))

	switch r.method {
	case http.MethodGet:
		return a.api.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	case http.MethodPatch:
		doc := &firestorepb.Document{}
		if err := r.unmarshal(doc); err != nil {
			return nil, err
		}

		doc.Name = name

		pre, err := gcpFirestorePrecondition(r)
		if err != nil {
			return nil, err
		}

		req := &firestorepb.UpdateDocumentRequest{Document: doc, CurrentDocument: pre}
		if paths := r.query["updateMask.fieldPaths"]; len(paths) > 0 {
			req.UpdateMask = &firestorepb.DocumentMask{FieldPaths: paths}
		}

		return a.api.UpdateDocument(ctx, req)
	case http.MethodDelete:
		pre, err := gcpFirestorePrecondition(r)
		if err != nil {
			return nil, err
		}

		return a.api.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name, CurrentDocument: pre})
	}

	return nil, errGCPUnsupported()
}

func gcpFirestorePrecondition(r gcpRESTCall) (*firestorepb.Precondition, error) {
	switch {
	case r.query.Has("currentDocument.exists"):
		return &firestorepb.Precondition{ConditionType: &firestorepb.Precondition_Exists{
			Exists: r.query.Get("currentDocument.exists") == "true",
		}}, nil
	case r.query.Has("currentDocument.updateTime"):
		t, err := time.Parse(time.RFC3339Nano, r.query.Get("currentDocument.updateTime"))
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid currentDocument.updateTime: "+err.Error())
		}

		return &firestorepb.Precondition{ConditionType: &firestorepb.Precondition_UpdateTime{
			UpdateTime: timestamppb.New(t),
		}}, nil
	}

	return nil, nil //nolint:nilnil // no precondition requested
}

func (a *gcpFirestoreAPI) verbCall(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(len(r.segs))

	switch r.verb {
	case "runQuery":
		req := &firestorepb.RunQueryRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = parent
		st := &gcpFirestoreStream[*firestorepb.RunQueryResponse]{ctx: ctx}

		if err := a.api.RunQuery(req, st); err != nil {
			return nil, err
		}

		return gcpStreamToList(st.out)
	case "runAggregationQuery":
		req := &firestorepb.RunAggregationQueryRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = parent
		st := &gcpFirestoreStream[*firestorepb.RunAggregationQueryResponse]{ctx: ctx}

		if err := a.api.RunAggregationQuery(req, st); err != nil {
			return nil, err
		}

		return gcpStreamToList(st.out)
	case "listCollectionIds":
		req := &firestorepb.ListCollectionIdsRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = parent

		return a.api.ListCollectionIds(ctx, req)
	case "batchGet", "commit":
		if len(r.segs) != gcpFirestoreDocsSegs {
			return nil, errGCPUnsupported()
		}

		return a.databaseVerb(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpFirestoreAPI) databaseVerb(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	db := r.name(gcpFirestoreDBSegs)

	if r.verb == "commit" {
		req := &firestorepb.CommitRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Database = db

		return a.api.Commit(ctx, req)
	}

	req := &firestorepb.BatchGetDocumentsRequest{}
	if err := r.unmarshal(req); err != nil {
		return nil, err
	}

	req.Database = db
	st := &gcpFirestoreStream[*firestorepb.BatchGetDocumentsResponse]{ctx: ctx}

	if err := a.api.BatchGetDocuments(req, st); err != nil {
		return nil, err
	}

	return gcpStreamToList(st.out)
}
