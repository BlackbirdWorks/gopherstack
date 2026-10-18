package gcpfirestore_test

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpfirestore"
)

const (
	project = "test-project"
	dbRoot  = "projects/" + project + "/databases/(default)"
	docRoot = dbRoot + "/documents"
)

type env struct {
	client  *firestore.Client
	raw     firestorepb.FirestoreClient
	handler *gcpfirestore.Handler
	conn    *grpc.ClientConn
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpfirestore.NewHandler(gcpfirestore.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := firestore.NewClientWithDatabase(t.Context(), project, "(default)", option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, raw: firestorepb.NewFirestoreClient(conn), handler: h, conn: conn}
}

func (e *env) seed(t *testing.T, coll string, docs map[string]map[string]any) {
	t.Helper()

	for id, d := range docs {
		_, err := e.client.Collection(coll).Doc(id).Set(t.Context(), d)
		require.NoError(t, err)
	}
}

func ids(t *testing.T, it *firestore.DocumentIterator) []string {
	t.Helper()

	snaps, err := it.GetAll()
	require.NoError(t, err)

	out := make([]string, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, s.Ref.ID)
	}

	return out
}

func code(err error) codes.Code { return status.Code(err) }

func TestDocumentCRUD(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	ref := e.client.Collection("users").Doc("alice")

	wr, err := ref.Create(
		ctx,
		map[string]any{"name": "Alice", "age": 30, "tags": []string{"a", "b"}, "addr": map[string]any{"city": "X"}},
	)
	require.NoError(t, err)

	_, err = ref.Create(ctx, map[string]any{"name": "dup"})
	assert.Equal(t, codes.AlreadyExists, code(err))

	snap, err := ref.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Alice", snap.Data()["name"])
	assert.Equal(t, int64(30), snap.Data()["age"])
	assert.True(t, snap.UpdateTime.Equal(wr.UpdateTime))
	assert.True(t, snap.CreateTime.Equal(wr.UpdateTime))

	_, err = ref.Update(
		ctx,
		[]firestore.Update{{Path: "addr.city", Value: "Y"}, {Path: "age", Value: firestore.Delete}},
	)
	require.NoError(t, err)

	snap, err = ref.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"city": "Y"}, snap.Data()["addr"])
	assert.NotContains(t, snap.Data(), "age")
	assert.True(t, snap.CreateTime.Equal(wr.UpdateTime))

	_, err = ref.Set(ctx, map[string]any{"name": "merged"}, firestore.MergeAll)
	require.NoError(t, err)
	snap, _ = ref.Get(ctx)
	assert.Equal(t, "merged", snap.Data()["name"])
	assert.Contains(t, snap.Data(), "tags")

	_, err = e.client.Collection("users").Doc("nobody").Update(ctx, []firestore.Update{{Path: "a", Value: 1}})
	assert.Equal(t, codes.NotFound, code(err))

	_, err = ref.Delete(ctx, firestore.LastUpdateTime(wr.UpdateTime))
	assert.Equal(t, codes.FailedPrecondition, code(err))

	_, err = ref.Delete(ctx)
	require.NoError(t, err)

	_, err = ref.Get(ctx)
	assert.Equal(t, codes.NotFound, code(err))
}

func TestRawDocumentRPCs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	created, err := e.raw.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent: docRoot, CollectionId: "items", DocumentId: "one",
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"a": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 1}},
			"b": {ValueType: &firestorepb.Value_StringValue{StringValue: "x"}},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, docRoot+"/items/one", created.GetName())

	auto, err := e.raw.CreateDocument(
		ctx,
		&firestorepb.CreateDocumentRequest{Parent: docRoot, CollectionId: "items", Document: &firestorepb.Document{}},
	)
	require.NoError(t, err)
	assert.Len(t, auto.GetName(), len(docRoot+"/items/")+20)

	_, err = e.raw.CreateDocument(
		ctx,
		&firestorepb.CreateDocumentRequest{
			Parent:       docRoot,
			CollectionId: "items",
			DocumentId:   "one",
			Document:     &firestorepb.Document{},
		},
	)
	assert.Equal(t, codes.AlreadyExists, code(err))

	upd, err := e.raw.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   created.GetName(),
			Fields: map[string]*firestorepb.Value{"b": {ValueType: &firestorepb.Value_StringValue{StringValue: "y"}}},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"b"}},
		Mask:       &firestorepb.DocumentMask{FieldPaths: []string{"b"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "y", upd.GetFields()["b"].GetStringValue())
	assert.NotContains(t, upd.GetFields(), "a")

	got, err := e.raw.GetDocument(
		ctx,
		&firestorepb.GetDocumentRequest{
			Name: created.GetName(),
			Mask: &firestorepb.DocumentMask{FieldPaths: []string{"a"}},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.GetFields()["a"].GetIntegerValue())
	assert.NotContains(t, got.GetFields(), "b")

	list, err := e.raw.ListDocuments(
		ctx,
		&firestorepb.ListDocumentsRequest{Parent: docRoot, CollectionId: "items", PageSize: 1},
	)
	require.NoError(t, err)
	assert.Len(t, list.GetDocuments(), 1)
	assert.NotEmpty(t, list.GetNextPageToken())

	list, err = e.raw.ListDocuments(
		ctx,
		&firestorepb.ListDocumentsRequest{
			Parent:       docRoot,
			CollectionId: "items",
			PageSize:     1,
			PageToken:    list.GetNextPageToken(),
		},
	)
	require.NoError(t, err)
	assert.Len(t, list.GetDocuments(), 1)
	assert.Empty(t, list.GetNextPageToken())

	_, err = e.raw.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: created.GetName()})
	require.NoError(t, err)

	_, err = e.raw.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: "bogus"})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestTransformsViaClient(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	ref := e.client.Collection("t").Doc("d")

	_, err := ref.Set(ctx, map[string]any{"n": 1, "f": 1.5, "arr": []any{"a", "b"}})
	require.NoError(t, err)

	wr, err := ref.Update(ctx, []firestore.Update{
		{Path: "n", Value: firestore.Increment(41)},
		{Path: "f", Value: firestore.Increment(0.25)},
		{Path: "arr", Value: firestore.ArrayUnion("b", "c")},
		{Path: "ts", Value: firestore.ServerTimestamp},
		{Path: "created", Value: firestore.ServerTimestamp},
	})
	require.NoError(t, err)

	snap, err := ref.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(42), snap.Data()["n"])
	assert.InDelta(t, 1.75, snap.Data()["f"], 1e-9)
	assert.Equal(t, []any{"a", "b", "c"}, snap.Data()["arr"])
	assert.True(t, snap.Data()["ts"].(time.Time).Equal(wr.UpdateTime))

	_, err = ref.Update(ctx, []firestore.Update{{Path: "arr", Value: firestore.ArrayRemove("a", "c")}})
	require.NoError(t, err)
	snap, _ = ref.Get(ctx)
	assert.Equal(t, []any{"b"}, snap.Data()["arr"])

	_, err = e.client.Collection("t").
		Doc("new").
		Set(ctx, map[string]any{"v": firestore.Increment(5), "at": firestore.ServerTimestamp})
	require.NoError(t, err)
	snap, err = e.client.Collection("t").Doc("new").Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), snap.Data()["v"])
}

func fieldTransform(
	path string,
	tt func(*firestorepb.DocumentTransform_FieldTransform),
) *firestorepb.DocumentTransform_FieldTransform {
	ft := &firestorepb.DocumentTransform_FieldTransform{FieldPath: path}
	tt(ft)

	return ft
}

func TestMaxMinTransformsRaw(t *testing.T) {
	t.Parallel()

	i := func(v int64) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}}
	}
	d := func(v float64) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: v}}
	}
	s := func(v string) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: v}}
	}

	tests := []struct {
		start *firestorepb.Value
		xf    func(*firestorepb.DocumentTransform_FieldTransform)
		want  *firestorepb.Value
		name  string
	}{
		{i(3), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: i(9)}
		}, i(9), "max int larger"},
		{i(10), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: i(9)}
		}, i(10), "max keeps stored"},
		{i(3), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: d(3.5)}
		}, d(3.5), "max mixed takes larger type"},
		{d(3.5), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Minimum{Minimum: i(3)}
		}, i(3), "min mixed"},
		{s("x"), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Minimum{Minimum: i(7)}
		}, i(7), "min non-numeric replaced"},
		{i(3), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: d(math.NaN())}
		}, d(math.NaN()), "max nan wins"},
		{i(math.MaxInt64), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Increment{Increment: i(5)}
		}, i(math.MaxInt64), "increment overflow saturates"},
		{i(2), func(f *firestorepb.DocumentTransform_FieldTransform) {
			f.TransformType = &firestorepb.DocumentTransform_FieldTransform_Increment{Increment: d(0.5)}
		}, d(2.5), "increment int plus double"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			name := docRoot + "/m/doc"

			_, err := e.raw.Commit(
				t.Context(),
				&firestorepb.CommitRequest{Database: dbRoot, Writes: []*firestorepb.Write{
					{
						Operation: &firestorepb.Write_Update{
							Update: &firestorepb.Document{
								Name:   name,
								Fields: map[string]*firestorepb.Value{"x": tt.start},
							},
						},
					},
				}},
			)
			require.NoError(t, err)

			resp, err := e.raw.Commit(
				t.Context(),
				&firestorepb.CommitRequest{Database: dbRoot, Writes: []*firestorepb.Write{{
					Operation:        &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name}},
					UpdateMask:       &firestorepb.DocumentMask{},
					UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{fieldTransform("x", tt.xf)},
				}}},
			)
			require.NoError(t, err)
			require.Len(t, resp.GetWriteResults()[0].GetTransformResults(), 1)

			got, err := e.raw.GetDocument(t.Context(), &firestorepb.GetDocumentRequest{Name: name})
			require.NoError(t, err)

			if math.IsNaN(tt.want.GetDoubleValue()) {
				assert.True(t, math.IsNaN(got.GetFields()["x"].GetDoubleValue()))

				return
			}

			assert.Equal(t, tt.want.String(), got.GetFields()["x"].String())
		})
	}
}

func TestTransactions(t *testing.T) {
	t.Parallel()

	t.Run("read write commits", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ref := e.client.Collection("acct").Doc("a")
		e.seed(t, "acct", map[string]map[string]any{"a": {"bal": 10}})

		err := e.client.RunTransaction(t.Context(), func(_ context.Context, tx *firestore.Transaction) error {
			s, err := tx.Get(ref)
			if err != nil {
				return err
			}

			return tx.Update(ref, []firestore.Update{{Path: "bal", Value: s.Data()["bal"].(int64) + 5}})
		})
		require.NoError(t, err)

		s, _ := ref.Get(t.Context())
		assert.Equal(t, int64(15), s.Data()["bal"])
	})

	t.Run("conflict retries", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ref := e.client.Collection("acct").Doc("a")
		e.seed(t, "acct", map[string]map[string]any{"a": {"bal": 10}})

		var attempts atomic.Int32

		err := e.client.RunTransaction(t.Context(), func(ctx context.Context, tx *firestore.Transaction) error {
			s, err := tx.Get(ref)
			if err != nil {
				return err
			}

			if attempts.Add(1) == 1 {
				if _, werr := ref.Update(ctx, []firestore.Update{{Path: "bal", Value: 100}}); werr != nil {
					return werr
				}
			}

			return tx.Update(ref, []firestore.Update{{Path: "bal", Value: s.Data()["bal"].(int64) + 1}})
		})
		require.NoError(t, err)
		assert.EqualValues(t, 2, attempts.Load())

		s, _ := ref.Get(t.Context())
		assert.Equal(t, int64(101), s.Data()["bal"])
	})

	t.Run("conflict aborts when attempts exhausted", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ref := e.client.Collection("acct").Doc("a")
		e.seed(t, "acct", map[string]map[string]any{"a": {"bal": 10}})

		err := e.client.RunTransaction(t.Context(), func(ctx context.Context, tx *firestore.Transaction) error {
			if _, err := tx.Get(ref); err != nil {
				return err
			}

			if _, werr := ref.Update(ctx, []firestore.Update{{Path: "bal", Value: 100}}); werr != nil {
				return werr
			}

			return tx.Update(ref, []firestore.Update{{Path: "bal", Value: 1}})
		}, firestore.MaxAttempts(1))
		assert.Equal(t, codes.Aborted, code(err))
	})

	t.Run("query read conflicts with new document", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		coll := e.client.Collection("q")
		e.seed(t, "q", map[string]map[string]any{"a": {"v": 1}})

		var attempts atomic.Int32

		err := e.client.RunTransaction(t.Context(), func(ctx context.Context, tx *firestore.Transaction) error {
			docs, err := tx.Documents(coll).GetAll()
			if err != nil {
				return err
			}

			if attempts.Add(1) == 1 {
				if _, werr := coll.Doc("b").Set(ctx, map[string]any{"v": 2}); werr != nil {
					return werr
				}
			}

			return tx.Set(coll.Doc("count"), map[string]any{"n": len(docs)})
		})
		require.NoError(t, err)
		assert.EqualValues(t, 2, attempts.Load())

		s, _ := coll.Doc("count").Get(t.Context())
		assert.Equal(t, int64(2), s.Data()["n"])
	})

	t.Run("rollback and read only", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ctx := t.Context()
		ref := e.client.Collection("c").Doc("d")
		e.seed(t, "c", map[string]map[string]any{"d": {"v": 1}})

		tx, err := e.raw.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{
			Database: dbRoot,
			Options: &firestorepb.TransactionOptions{
				Mode: &firestorepb.TransactionOptions_ReadOnly_{ReadOnly: &firestorepb.TransactionOptions_ReadOnly{}},
			},
		})
		require.NoError(t, err)

		_, err = ref.Set(ctx, map[string]any{"v": 2})
		require.NoError(t, err)

		got, err := e.raw.GetDocument(ctx, &firestorepb.GetDocumentRequest{
			Name:                docRoot + "/c/d",
			ConsistencySelector: &firestorepb.GetDocumentRequest_Transaction{Transaction: tx.GetTransaction()},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), got.GetFields()["v"].GetIntegerValue())

		_, err = e.raw.Commit(
			ctx,
			&firestorepb.CommitRequest{
				Database:    dbRoot,
				Transaction: tx.GetTransaction(),
				Writes: []*firestorepb.Write{{
					Operation: &firestorepb.Write_Delete{Delete: docRoot + "/c/d"},
				}},
			},
		)
		assert.Equal(t, codes.InvalidArgument, code(err))

		rw, err := e.raw.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: dbRoot})
		require.NoError(t, err)
		_, err = e.raw.Rollback(ctx, &firestorepb.RollbackRequest{Database: dbRoot, Transaction: rw.GetTransaction()})
		require.NoError(t, err)
		_, err = e.raw.Commit(ctx, &firestorepb.CommitRequest{Database: dbRoot, Transaction: rw.GetTransaction()})
		assert.Equal(t, codes.InvalidArgument, code(err))
	})
}

func TestReadTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	ref := e.client.Collection("h").Doc("d")

	w1, err := ref.Set(ctx, map[string]any{"v": 1})
	require.NoError(t, err)
	_, err = ref.Set(ctx, map[string]any{"v": 2})
	require.NoError(t, err)

	resp, err := e.raw.BatchGetDocuments(ctx, &firestorepb.BatchGetDocumentsRequest{
		Database: dbRoot, Documents: []string{docRoot + "/h/d", docRoot + "/h/none"},
		ConsistencySelector: &firestorepb.BatchGetDocumentsRequest_ReadTime{ReadTime: timestamppb.New(w1.UpdateTime)},
	})
	require.NoError(t, err)

	var found, missing int

	for {
		r, rerr := resp.Recv()
		if rerr != nil {
			break
		}

		if f := r.GetFound(); f != nil {
			found++

			assert.Equal(t, int64(1), f.GetFields()["v"].GetIntegerValue())
		} else {
			missing++

			assert.Equal(t, docRoot+"/h/none", r.GetMissing())
		}
	}

	assert.Equal(t, 1, found)
	assert.Equal(t, 1, missing)
}

func TestQueries(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "city", map[string]map[string]any{
		"sf":  {"name": "SF", "state": "CA", "pop": 870, "tags": []string{"coast", "tech"}, "cap": false, "rank": nil},
		"la":  {"name": "LA", "state": "CA", "pop": 3900, "tags": []string{"coast"}, "cap": false},
		"dc":  {"name": "DC", "state": "DC", "pop": 680, "tags": []string{"gov"}, "cap": true},
		"tok": {"name": "Tokyo", "state": "JP", "pop": 37400, "tags": []string{"coast", "gov"}, "cap": true},
		"bj":  {"name": "Beijing", "state": "CN", "pop": 21500.5, "cap": true, "nan": math.NaN()},
	})

	coll := e.client.Collection("city")

	tests := []struct {
		name string
		q    firestore.Query
		want []string
	}{
		{"equality", coll.Where("state", "==", "CA").OrderBy("pop", firestore.Asc), []string{"sf", "la"}},
		{"not equal", coll.Where("state", "!=", "CA").OrderBy("state", firestore.Asc), []string{"bj", "dc", "tok"}},
		{
			"range mixed numbers",
			coll.Where("pop", ">", 1000).OrderBy("pop", firestore.Asc),
			[]string{"la", "bj", "tok"},
		},
		{"range and eq composite", coll.Where("state", "==", "CA").Where("pop", ">=", 1000), []string{"la"}},
		{"or composite", coll.WhereEntity(firestore.OrFilter{Filters: []firestore.EntityFilter{
			firestore.PropertyFilter{Path: "state", Operator: "==", Value: "DC"},
			firestore.PropertyFilter{Path: "pop", Operator: ">", Value: 30000},
		}}).OrderBy("pop", firestore.Asc), []string{"dc", "tok"}},
		{
			"array contains",
			coll.Where("tags", "array-contains", "gov").OrderBy("pop", firestore.Asc),
			[]string{"dc", "tok"},
		},
		{
			"array contains any",
			coll.Where("tags", "array-contains-any", []string{"tech", "gov"}).OrderBy("pop", firestore.Asc),
			[]string{"dc", "sf", "tok"},
		},
		{"in", coll.Where("state", "in", []string{"JP", "DC"}).OrderBy("state", firestore.Asc), []string{"dc", "tok"}},
		{
			"not in",
			coll.Where("state", "not-in", []string{"JP", "DC", "CN"}).OrderBy("pop", firestore.Asc),
			[]string{"sf", "la"},
		},
		{"is null", coll.Where("rank", "==", nil), []string{"sf"}},
		{"is nan", coll.Where("nan", "==", math.NaN()), []string{"bj"}},
		{
			"is not null",
			coll.Where("cap", "!=", nil).Where("cap", "==", true).OrderBy("pop", firestore.Asc),
			[]string{"dc", "bj", "tok"},
		},
		{"order desc limit offset", coll.OrderBy("pop", firestore.Desc).Offset(1).Limit(2), []string{"bj", "la"}},
		{"start at after", coll.OrderBy("pop", firestore.Asc).StartAfter(870).EndAt(21500.5), []string{"la", "bj"}},
		{
			"start at inclusive end before",
			coll.OrderBy("pop", firestore.Asc).StartAt(870).EndBefore(21500.5),
			[]string{"sf", "la"},
		},
		{
			"cursor by snapshot",
			coll.OrderBy("state", firestore.Asc).OrderBy("pop", firestore.Desc),
			[]string{"la", "sf", "bj", "dc", "tok"},
		},
		{"document id order", coll.OrderBy(firestore.DocumentID, firestore.Desc).Limit(2), []string{"tok", "sf"}},
		{"limit to last", coll.OrderBy("pop", firestore.Asc).LimitToLast(2), []string{"bj", "tok"}},
		{"missing order field excluded", coll.OrderBy("nan", firestore.Asc), []string{"bj"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, ids(t, tt.q.Documents(t.Context())))
		})
	}

	t.Run("select projection", func(t *testing.T) {
		t.Parallel()

		snaps, err := coll.Select("name").OrderBy("pop", firestore.Asc).Limit(1).Documents(t.Context()).GetAll()
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		assert.Equal(t, map[string]any{"name": "DC"}, snaps[0].Data())
	})

	t.Run("start after snapshot", func(t *testing.T) {
		t.Parallel()

		s, err := coll.Doc("la").Get(t.Context())
		require.NoError(t, err)
		assert.Equal(
			t,
			[]string{"bj", "tok"},
			ids(t, coll.OrderBy("pop", firestore.Asc).StartAfter(s).Documents(t.Context())),
		)
	})
}

func TestCrossTypeOrdering(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ts := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	ref := e.client.Collection("mix").Doc("ref")

	e.seed(t, "mix", map[string]map[string]any{
		"null":  {"v": nil},
		"false": {"v": false},
		"true":  {"v": true},
		"nan":   {"v": math.NaN()},
		"num1":  {"v": -3},
		"num2":  {"v": 2.5},
		"num3":  {"v": 10},
		"time":  {"v": ts},
		"str1":  {"v": "a"},
		"str2":  {"v": "b"},
		"bytes": {"v": []byte{1, 2}},
		"ref":   {"v": ref},
		"geo":   {"v": &latlng.LatLng{Latitude: 1, Longitude: 2}},
		"arr1":  {"v": []any{1}},
		"arr2":  {"v": []any{1, 2}},
		"map1":  {"v": map[string]any{"a": 1}},
		"map2":  {"v": map[string]any{"b": 0}},
	})

	want := []string{
		"null",
		"false",
		"true",
		"nan",
		"num1",
		"num2",
		"num3",
		"time",
		"str1",
		"str2",
		"bytes",
		"ref",
		"geo",
		"arr1",
		"arr2",
		"map1",
		"map2",
	}
	assert.Equal(t, want, ids(t, e.client.Collection("mix").OrderBy("v", firestore.Asc).Documents(t.Context())))
}

func TestAggregations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "sales", map[string]map[string]any{
		"a": {"region": "w", "amt": 10, "w": 1.5},
		"b": {"region": "w", "amt": 20, "w": 2.5},
		"c": {"region": "e", "amt": 30},
		"d": {"region": "e", "amt": "n/a"},
	})

	coll := e.client.Collection("sales")

	res, err := coll.NewAggregationQuery().
		WithCount("n").
		WithSum("amt", "total").
		WithAvg("amt", "mean").
		Get(t.Context())
	require.NoError(t, err)

	data := res.Data()
	assert.EqualValues(t, 4, data["n"])
	assert.EqualValues(t, 60, data["total"])
	assert.InDelta(t, 20.0, data["mean"], 1e-9)

	west := coll.Where("region", "==", "w")
	res, err = west.NewAggregationQuery().WithCount("n").WithSum("w", "ws").Get(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 2, res.Data()["n"])
	assert.InDelta(t, 4.0, res.Data()["ws"], 1e-9)

	none := coll.Where("region", "==", "none")
	res, err = none.NewAggregationQuery().WithCount("n").WithAvg("amt", "m").WithSum("amt", "s").Get(t.Context())
	require.NoError(t, err)
	assert.EqualValues(t, 0, res.Data()["n"])
	assert.Nil(t, res.Data()["m"])
	assert.EqualValues(t, 0, res.Data()["s"])
}

func TestCollectionGroupsAndListing(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	docPaths := []string{
		"cities/sf/landmarks/bridge",
		"cities/ny/landmarks/statue",
		"other/x/landmarks/y",
		"landmarks/top",
	}
	for _, p := range docPaths {
		_, err := e.client.Doc(p).Set(ctx, map[string]any{"p": p})
		require.NoError(t, err)
	}

	snaps, err := e.client.CollectionGroup("landmarks").
		OrderBy(firestore.DocumentID, firestore.Asc).
		Documents(ctx).
		GetAll()
	require.NoError(t, err)

	paths := make([]string, 0, len(snaps))
	for _, s := range snaps {
		paths = append(paths, s.Ref.Path)
	}

	assert.Equal(t, []string{
		docRoot + "/cities/ny/landmarks/statue", docRoot + "/cities/sf/landmarks/bridge",
		docRoot + "/landmarks/top", docRoot + "/other/x/landmarks/y",
	}, paths)

	sub, err := e.client.Collection("cities").Doc("sf").Collection("landmarks").Documents(ctx).GetAll()
	require.NoError(t, err)
	assert.Len(t, sub, 1)

	var colls []string

	it := e.client.Collections(ctx)
	for {
		c, cerr := it.Next()
		if errors.Is(cerr, iterator.Done) {
			break
		}

		require.NoError(t, cerr)

		colls = append(colls, c.ID)
	}

	assert.Equal(t, []string{"cities", "landmarks", "other"}, colls)

	var subColls []string

	sit := e.client.Doc("cities/sf").Collections(ctx)
	for {
		c, cerr := sit.Next()
		if errors.Is(cerr, iterator.Done) {
			break
		}

		require.NoError(t, cerr)

		subColls = append(subColls, c.ID)
	}

	assert.Equal(t, []string{"landmarks"}, subColls)

	refs, err := e.client.Collection("cities").DocumentRefs(ctx).GetAll()
	require.NoError(t, err)

	refIDs := make([]string, 0, len(refs))
	for _, r := range refs {
		refIDs = append(refIDs, r.ID)
	}

	assert.Equal(t, []string{"ny", "sf"}, refIDs, "show_missing lists documents that only have sub-collections")

	list, err := e.raw.ListDocuments(ctx, &firestorepb.ListDocumentsRequest{Parent: docRoot, CollectionId: "cities"})
	require.NoError(t, err)
	assert.Empty(t, list.GetDocuments(), "without show_missing only real documents are listed")

	part, err := e.raw.PartitionQuery(ctx, &firestorepb.PartitionQueryRequest{
		Parent: docRoot, PartitionCount: 3,
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "landmarks", AllDescendants: true}},
		}},
	})
	require.NoError(t, err)
	assert.Len(t, part.GetPartitions(), 2)

	_, err = e.raw.PartitionQuery(ctx, &firestorepb.PartitionQueryRequest{
		Parent: docRoot, PartitionCount: 2,
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{StructuredQuery: &firestorepb.StructuredQuery{
			From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: "landmarks"}},
		}},
	})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestGetAllAndBulkWriter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()

	bw := e.client.BulkWriter(ctx)
	jobs := make([]*firestore.BulkWriterJob, 0, 5)

	for _, id := range []string{"a", "b", "c", "d", "e"} {
		j, err := bw.Set(e.client.Collection("bulk").Doc(id), map[string]any{"id": id})
		require.NoError(t, err)

		jobs = append(jobs, j)
	}

	bw.End()

	for _, j := range jobs {
		_, err := j.Results()
		require.NoError(t, err)
	}

	snaps, err := e.client.GetAll(ctx, []*firestore.DocumentRef{
		e.client.Collection("bulk").
			Doc("a"),
		e.client.Collection("bulk").Doc("zz"),
		e.client.Collection("bulk").Doc("e"),
	})
	require.NoError(t, err)
	require.Len(t, snaps, 3)
	assert.True(t, snaps[0].Exists())
	assert.False(t, snaps[1].Exists())
	assert.True(t, snaps[2].Exists())

	_, err = e.raw.Commit(ctx, &firestorepb.CommitRequest{Database: dbRoot, Writes: []*firestorepb.Write{
		{Operation: &firestorepb.Write_Delete{Delete: docRoot + "/bulk/a"}},
		{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
			Name:   docRoot + "/bulk/f",
			Fields: map[string]*firestorepb.Value{"id": {ValueType: &firestorepb.Value_StringValue{StringValue: "f"}}},
		}}},
	}})
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "c", "d", "e", "f"}, ids(t, e.client.Collection("bulk").Documents(ctx)))

	resp, err := e.raw.BatchWrite(ctx, &firestorepb.BatchWriteRequest{Database: dbRoot, Writes: []*firestorepb.Write{
		{
			Operation:       &firestorepb.Write_Update{Update: &firestorepb.Document{Name: docRoot + "/bulk/b"}},
			CurrentDocument: &firestorepb.Precondition{ConditionType: &firestorepb.Precondition_Exists{Exists: false}},
		},
		{Operation: &firestorepb.Write_Delete{Delete: docRoot + "/bulk/c"}},
	}})
	require.NoError(t, err)
	require.Len(t, resp.GetStatus(), 2)
	assert.EqualValues(t, codes.AlreadyExists, resp.GetStatus()[0].GetCode())
	assert.EqualValues(t, codes.OK, resp.GetStatus()[1].GetCode())

	_, err = e.raw.BatchWrite(ctx, &firestorepb.BatchWriteRequest{Database: dbRoot, Writes: []*firestorepb.Write{
		{Operation: &firestorepb.Write_Delete{Delete: docRoot + "/bulk/b"}},
		{Operation: &firestorepb.Write_Delete{Delete: docRoot + "/bulk/b"}},
	}})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestSnapshotListeners(t *testing.T) {
	t.Parallel()

	t.Run("document", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ref := e.client.Collection("w").Doc("d")
		it := ref.Snapshots(ctx)
		defer it.Stop()

		s, err := it.Next()
		require.NoError(t, err)
		assert.False(t, s.Exists())

		_, err = ref.Set(ctx, map[string]any{"v": 1})
		require.NoError(t, err)

		s, err = it.Next()
		require.NoError(t, err)
		assert.Equal(t, int64(1), s.Data()["v"])

		_, err = ref.Update(ctx, []firestore.Update{{Path: "v", Value: 2}})
		require.NoError(t, err)

		s, err = it.Next()
		require.NoError(t, err)
		assert.Equal(t, int64(2), s.Data()["v"])

		_, err = ref.Delete(ctx)
		require.NoError(t, err)

		s, err = it.Next()
		require.NoError(t, err)
		assert.False(t, s.Exists())
	})

	t.Run("query", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		coll := e.client.Collection("w")
		e.seed(t, "w", map[string]map[string]any{"a": {"v": 1}, "b": {"v": 5}})

		it := coll.Where("v", ">", 2).OrderBy("v", firestore.Asc).Snapshots(ctx)
		defer it.Stop()

		snap, err := it.Next()
		require.NoError(t, err)
		assert.Equal(t, 1, snap.Size)
		require.Len(t, snap.Changes, 1)
		assert.Equal(t, firestore.DocumentAdded, snap.Changes[0].Kind)

		_, err = coll.Doc("c").Set(ctx, map[string]any{"v": 9})
		require.NoError(t, err)

		snap, err = it.Next()
		require.NoError(t, err)
		assert.Equal(t, 2, snap.Size)
		require.Len(t, snap.Changes, 1)
		assert.Equal(t, "c", snap.Changes[0].Doc.Ref.ID)
		assert.Equal(t, firestore.DocumentAdded, snap.Changes[0].Kind)

		_, err = coll.Doc("b").Update(ctx, []firestore.Update{{Path: "v", Value: 6}})
		require.NoError(t, err)

		snap, err = it.Next()
		require.NoError(t, err)
		require.Len(t, snap.Changes, 1)
		assert.Equal(t, firestore.DocumentModified, snap.Changes[0].Kind)

		_, err = coll.Doc("b").Update(ctx, []firestore.Update{{Path: "v", Value: 0}})
		require.NoError(t, err)

		snap, err = it.Next()
		require.NoError(t, err)
		assert.Equal(t, 1, snap.Size)
		require.Len(t, snap.Changes, 1)
		assert.Equal(t, firestore.DocumentRemoved, snap.Changes[0].Kind)

		_, err = coll.Doc("c").Delete(ctx)
		require.NoError(t, err)

		snap, err = it.Next()
		require.NoError(t, err)
		assert.Equal(t, 0, snap.Size)
		require.Len(t, snap.Changes, 1)
		assert.Equal(t, firestore.DocumentRemoved, snap.Changes[0].Kind)
	})
}

func TestPersistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	src.seed(t, "p", map[string]map[string]any{"a": {"v": 1, "m": map[string]any{"x": []any{1, "s"}}}})

	_, err := src.client.Collection("p").
		Doc("a").
		Update(t.Context(), []firestore.Update{{Path: "t", Value: firestore.ServerTimestamp}})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	want, err := src.client.Collection("p").Doc("a").Get(t.Context())
	require.NoError(t, err)

	got, err := dst.client.Collection("p").Doc("a").Get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, want.Data(), got.Data())
	assert.True(t, want.UpdateTime.Equal(got.UpdateTime))

	w, err := dst.client.Collection("p").Doc("a").Update(t.Context(), []firestore.Update{{Path: "v", Value: 2}})
	require.NoError(t, err)
	assert.True(t, w.UpdateTime.After(got.UpdateTime))

	dst.handler.Reset()

	_, err = dst.client.Collection("p").Doc("a").Get(t.Context())
	assert.Equal(t, codes.NotFound, code(err))
}

func TestEmulatorHostEnv(t *testing.T) {
	h := gcpfirestore.NewHandler(gcpfirestore.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	t.Setenv("FIRESTORE_EMULATOR_HOST", h.Addr())

	client, err := firestore.NewClient(t.Context(), project)
	require.NoError(t, err)

	defer func() { _ = client.Close() }()

	_, err = client.Collection("e").Doc("d").Set(t.Context(), map[string]any{"ok": true})
	require.NoError(t, err)

	s, err := client.Collection("e").Doc("d").Get(t.Context())
	require.NoError(t, err)
	assert.Equal(t, true, s.Data()["ok"])
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	_, err := (&gcpfirestore.Provider{}).Init(nil)
	require.ErrorIs(t, err, gcpfirestore.ErrNilAppContext)
	assert.Equal(t, "GCPFirestore", (&gcpfirestore.Provider{}).Name())
	assert.Equal(t, 8080, gcpfirestore.DefaultSettings().Port)
}
