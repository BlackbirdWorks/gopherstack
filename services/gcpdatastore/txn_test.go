package gcpdatastore_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type counter struct{ N int }

func TestTransactionConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want error
		run  func(t *testing.T, e *env, tx *datastore.Transaction)
		name string
	}{
		{datastore.ErrConcurrentTransaction, func(t *testing.T, e *env, tx *datastore.Transaction) {
			t.Helper()

			var c counter
			require.NoError(t, tx.Get(datastore.NameKey("C", "k", nil), &c))
			_, err := e.client.Put(t.Context(), datastore.NameKey("C", "k", nil), &counter{N: 9})
			require.NoError(t, err)
			_, err = tx.Put(datastore.NameKey("C", "other", nil), &counter{N: 1})
			require.NoError(t, err)
		}, "read entity changed outside"},
		{nil, func(t *testing.T, e *env, tx *datastore.Transaction) {
			t.Helper()

			var c counter
			require.NoError(t, tx.Get(datastore.NameKey("C", "k", nil), &c))
			_, err := e.client.Put(t.Context(), datastore.NameKey("C", "unrelated", nil), &counter{N: 9})
			require.NoError(t, err)
			_, err = tx.Put(datastore.NameKey("C", "k", nil), &counter{N: 2})
			require.NoError(t, err)
		}, "unrelated entity changed outside"},
		{datastore.ErrConcurrentTransaction, func(t *testing.T, e *env, tx *datastore.Transaction) {
			t.Helper()

			var c counter
			require.NoError(t, tx.Get(datastore.NameKey("C", "k", nil), &c))
			_, err := e.client.Put(t.Context(), datastore.NameKey("C", "blind", nil), &counter{N: 1})
			require.NoError(t, err)
			_, err = tx.Put(datastore.NameKey("C", "blind", nil), &counter{N: 2})
			require.NoError(t, err)
		}, "write write on same entity"},
		{datastore.ErrConcurrentTransaction, func(t *testing.T, e *env, tx *datastore.Transaction) {
			t.Helper()

			var out []counter
			_, err := e.client.GetAll(t.Context(), datastore.NewQuery("C").Transaction(tx), &out)
			require.NoError(t, err)
			_, err = e.client.Put(t.Context(), datastore.NameKey("C", "late", nil), &counter{N: 3})
			require.NoError(t, err)
			_, err = tx.Put(datastore.NameKey("Elsewhere", "x", nil), &counter{N: 1})
			require.NoError(t, err)
		}, "query scope gains an entity"},
		{nil, func(t *testing.T, e *env, tx *datastore.Transaction) {
			t.Helper()

			var out []counter
			_, err := e.client.GetAll(t.Context(), datastore.NewQuery("C").Transaction(tx), &out)
			require.NoError(t, err)
			_, err = e.client.Put(t.Context(), datastore.NameKey("Elsewhere", "y", nil), &counter{N: 3})
			require.NoError(t, err)
			_, err = tx.Put(datastore.NameKey("Elsewhere", "x", nil), &counter{N: 1})
			require.NoError(t, err)
		}, "query scope of another kind"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.handler.Backend.SetEnforceIndexes(false)

			_, err := e.client.Put(t.Context(), datastore.NameKey("C", "k", nil), &counter{N: 1})
			require.NoError(t, err)

			tx, err := e.client.NewTransaction(t.Context())
			require.NoError(t, err)

			tt.run(t, e, tx)

			_, err = tx.Commit()
			if tt.want == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.want)
			}
		})
	}
}

func TestTransactionLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("rollback discards writes", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		tx, err := e.client.NewTransaction(t.Context())
		require.NoError(t, err)

		_, err = tx.Put(datastore.NameKey("C", "k", nil), &counter{N: 1})
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		var c counter
		assert.ErrorIs(t, e.client.Get(t.Context(), datastore.NameKey("C", "k", nil), &c), datastore.ErrNoSuchEntity)
	})

	t.Run("read only transactions see a snapshot and reject writes", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		key := datastore.NameKey("C", "k", nil)
		_, err := e.client.Put(t.Context(), key, &counter{N: 1})
		require.NoError(t, err)

		tx, err := e.client.NewTransaction(t.Context(), datastore.ReadOnly)
		require.NoError(t, err)

		_, err = e.client.Put(t.Context(), key, &counter{N: 2})
		require.NoError(t, err)

		var c counter
		require.NoError(t, tx.Get(key, &c))
		assert.Equal(t, 1, c.N)

		_, err = tx.Commit()
		require.NoError(t, err)

		ro, err := e.client.NewTransaction(t.Context(), datastore.ReadOnly)
		require.NoError(t, err)

		_, err = ro.Put(key, &counter{N: 3})
		require.NoError(t, err)

		_, err = ro.Commit()
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("unknown transaction id", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.raw.Commit(t.Context(), &datastorepb.CommitRequest{
			ProjectId:           project,
			TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: []byte("nope")},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))

		_, err = e.raw.Rollback(
			t.Context(),
			&datastorepb.RollbackRequest{ProjectId: project, Transaction: []byte("nope")},
		)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("transactional commit needs a transaction", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.raw.Commit(t.Context(), &datastorepb.CommitRequest{ProjectId: project})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("lookup can begin a transaction", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		resp, err := e.raw.Lookup(t.Context(), &datastorepb.LookupRequest{
			ProjectId: project,
			Keys: []*datastorepb.Key{
				{
					Path: []*datastorepb.Key_PathElement{
						{Kind: "C", IdType: &datastorepb.Key_PathElement_Name{Name: "k"}},
					},
				},
			},
			ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_NewTransaction{
				NewTransaction: &datastorepb.TransactionOptions{},
			}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetTransaction())
		assert.Len(t, resp.GetMissing(), 1)

		_, err = e.raw.Commit(t.Context(), &datastorepb.CommitRequest{
			ProjectId:           project,
			TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: resp.GetTransaction()},
		})
		assert.NoError(t, err)
	})
}

func TestRunInTransactionRetries(t *testing.T) {
	t.Parallel()

	t.Run("retries after a conflict", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		key := datastore.NameKey("C", "k", nil)
		_, err := e.client.Put(t.Context(), key, &counter{N: 1})
		require.NoError(t, err)

		var attempts atomic.Int32

		_, err = e.client.RunInTransaction(t.Context(), func(tx *datastore.Transaction) error {
			var c counter
			if gerr := tx.Get(key, &c); gerr != nil {
				return gerr
			}

			if attempts.Add(1) == 1 {
				if _, perr := e.client.Put(t.Context(), key, &counter{N: 10}); perr != nil {
					return perr
				}
			}

			c.N++
			_, perr := tx.Put(key, &c)

			return perr
		})
		require.NoError(t, err)
		assert.EqualValues(t, 2, attempts.Load())

		var c counter
		require.NoError(t, e.client.Get(t.Context(), key, &c))
		assert.Equal(t, 11, c.N)
	})

	t.Run("concurrent increments are serialisable", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		key := datastore.NameKey("C", "k", nil)
		_, err := e.client.Put(t.Context(), key, &counter{})
		require.NoError(t, err)

		const workers = 8

		var wg sync.WaitGroup

		errs := make(chan error, workers)

		for range workers {
			wg.Go(func() {
				_, terr := e.client.RunInTransaction(t.Context(), func(tx *datastore.Transaction) error {
					var c counter
					if gerr := tx.Get(key, &c); gerr != nil {
						return gerr
					}

					c.N++
					_, perr := tx.Put(key, &c)

					return perr
				}, datastore.MaxAttempts(50))
				errs <- terr
			})
		}

		wg.Wait()
		close(errs)

		for terr := range errs {
			require.NoError(t, terr)
		}

		var c counter
		require.NoError(t, e.client.Get(t.Context(), key, &c))
		assert.Equal(t, workers, c.N)
	})
}

func nameKey(name string) *datastorepb.Key {
	return &datastorepb.Key{
		Path: []*datastorepb.Key_PathElement{{Kind: "K", IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
	}
}

func strEntity(name string, props map[string]*datastorepb.Value) *datastorepb.Entity {
	return &datastorepb.Entity{Key: nameKey(name), Properties: props}
}

func intVal(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}

func strVal(s string) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: s}}
}

func upsert(e *datastorepb.Entity) *datastorepb.Mutation {
	return &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: e}}
}

func (e *env) commit(t *testing.T, muts ...*datastorepb.Mutation) (*datastorepb.CommitResponse, error) {
	t.Helper()

	return e.raw.Commit(t.Context(), &datastorepb.CommitRequest{
		ProjectId: project, Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL, Mutations: muts,
	})
}

func TestMutationSemantics(t *testing.T) {
	t.Parallel()

	t.Run("insert update and versions", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		ins := &datastorepb.Mutation{
			Operation: &datastorepb.Mutation_Insert{
				Insert: strEntity("a", map[string]*datastorepb.Value{"n": intVal(1)}),
			},
		}

		r1, err := e.commit(t, ins)
		require.NoError(t, err)
		assert.NotZero(t, r1.GetMutationResults()[0].GetVersion())
		assert.NotNil(t, r1.GetCommitTime())

		_, err = e.commit(t, ins)
		assert.Equal(t, codes.AlreadyExists, status.Code(err))

		upd := &datastorepb.Mutation{
			Operation: &datastorepb.Mutation_Update{
				Update: strEntity("a", map[string]*datastorepb.Value{"n": intVal(2)}),
			},
		}
		r2, err := e.commit(t, upd)
		require.NoError(t, err)
		assert.Greater(t, r2.GetMutationResults()[0].GetVersion(), r1.GetMutationResults()[0].GetVersion())
		assert.Equal(
			t,
			r1.GetMutationResults()[0].GetCreateTime().AsTime(),
			r2.GetMutationResults()[0].GetCreateTime().AsTime(),
		)

		missing := &datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: strEntity("zz", nil)}}
		_, err = e.commit(t, missing)
		assert.Equal(t, codes.NotFound, status.Code(err))
	})

	t.Run("base version conflicts", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		r1, err := e.commit(t, upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(1)})))
		require.NoError(t, err)

		v1 := r1.GetMutationResults()[0].GetVersion()

		guarded := func(base int64, strat datastorepb.Mutation_ConflictResolutionStrategy, n int64) *datastorepb.Mutation {
			m := upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(n)}))
			m.ConflictDetectionStrategy = &datastorepb.Mutation_BaseVersion{BaseVersion: base}
			m.ConflictResolutionStrategy = strat

			return m
		}

		r2, err := e.commit(t, guarded(v1, datastorepb.Mutation_STRATEGY_UNSPECIFIED, 2))
		require.NoError(t, err)
		assert.False(t, r2.GetMutationResults()[0].GetConflictDetected())

		r3, err := e.commit(t, guarded(v1, datastorepb.Mutation_SERVER_VALUE, 3))
		require.NoError(t, err)
		assert.True(t, r3.GetMutationResults()[0].GetConflictDetected())
		assert.Equal(t, r2.GetMutationResults()[0].GetVersion(), r3.GetMutationResults()[0].GetVersion())

		_, err = e.commit(t, guarded(v1, datastorepb.Mutation_FAIL, 4))
		assert.Equal(t, codes.Aborted, status.Code(err))

		var got struct{ N int }
		require.NoError(t, e.client.Get(t.Context(), datastore.NameKey("K", "a", nil), &got))
		assert.Equal(t, 2, got.N)
	})

	t.Run("property mask updates only masked paths", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.commit(
			t,
			upsert(
				strEntity(
					"a",
					map[string]*datastorepb.Value{"keep": intVal(1), "change": intVal(1), "drop": intVal(1)},
				),
			),
		)
		require.NoError(t, err)

		m := upsert(strEntity("a", map[string]*datastorepb.Value{"change": intVal(2), "ignored": intVal(9)}))
		m.PropertyMask = &datastorepb.PropertyMask{Paths: []string{"change", "drop"}}
		_, err = e.commit(t, m)
		require.NoError(t, err)

		resp, err := e.raw.Lookup(
			t.Context(),
			&datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{nameKey("a")}},
		)
		require.NoError(t, err)

		props := resp.GetFound()[0].GetEntity().GetProperties()
		assert.EqualValues(t, 1, props["keep"].GetIntegerValue())
		assert.EqualValues(t, 2, props["change"].GetIntegerValue())
		assert.NotContains(t, props, "drop")
		assert.NotContains(t, props, "ignored")
	})

	t.Run("property transforms", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.commit(t, upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(5)})))
		require.NoError(t, err)

		m := upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(5)}))
		m.PropertyTransforms = []*datastorepb.PropertyTransform{
			{Property: "n", TransformType: &datastorepb.PropertyTransform_Increment{Increment: intVal(3)}},
			{Property: "hi", TransformType: &datastorepb.PropertyTransform_Maximum{Maximum: intVal(10)}},
			{
				Property: "stamp",
				TransformType: &datastorepb.PropertyTransform_SetToServerValue{
					SetToServerValue: datastorepb.PropertyTransform_REQUEST_TIME,
				},
			},
			{
				Property: "tags",
				TransformType: &datastorepb.PropertyTransform_AppendMissingElements{
					AppendMissingElements: &datastorepb.ArrayValue{
						Values: []*datastorepb.Value{strVal("a"), strVal("b")},
					},
				},
			},
		}

		resp, err := e.commit(t, m)
		require.NoError(t, err)
		assert.EqualValues(t, 8, resp.GetMutationResults()[0].GetTransformResults()[0].GetIntegerValue())

		got, err := e.raw.Lookup(
			t.Context(),
			&datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{nameKey("a")}},
		)
		require.NoError(t, err)

		props := got.GetFound()[0].GetEntity().GetProperties()
		assert.EqualValues(t, 8, props["n"].GetIntegerValue())
		assert.EqualValues(t, 10, props["hi"].GetIntegerValue())
		assert.NotNil(t, props["stamp"].GetTimestampValue())
		assert.Len(t, props["tags"].GetArrayValue().GetValues(), 2)
	})

	t.Run("non transactional commit rejects duplicate entities", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.commit(t, upsert(strEntity("a", nil)), upsert(strEntity("a", nil)))
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("failed commit applies nothing", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		bad := &datastorepb.Mutation{Operation: &datastorepb.Mutation_Update{Update: strEntity("missing", nil)}}
		_, err := e.commit(t, upsert(strEntity("a", nil)), bad)
		require.Error(t, err)

		var got struct{}
		assert.ErrorIs(t, e.client.Get(t.Context(), datastore.NameKey("K", "a", nil), &got), datastore.ErrNoSuchEntity)
	})
}

func TestReadTimeAndIDs(t *testing.T) {
	t.Parallel()

	t.Run("read time sees history", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		r1, err := e.commit(t, upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(1)})))
		require.NoError(t, err)

		_, err = e.commit(t, upsert(strEntity("a", map[string]*datastorepb.Value{"n": intVal(2)})))
		require.NoError(t, err)

		at := func(ts *timestamppb.Timestamp) (*datastorepb.LookupResponse, error) {
			return e.raw.Lookup(t.Context(), &datastorepb.LookupRequest{
				ProjectId: project, Keys: []*datastorepb.Key{nameKey("a")},
				ReadOptions: &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_ReadTime{ReadTime: ts}},
			})
		}

		old, err := at(r1.GetCommitTime())
		require.NoError(t, err)
		assert.EqualValues(t, 1, old.GetFound()[0].GetEntity().GetProperties()["n"].GetIntegerValue())

		before, err := at(timestamppb.New(r1.GetCommitTime().AsTime().Add(-time.Millisecond)))
		require.NoError(t, err)
		assert.Len(t, before.GetMissing(), 1)

		_, err = at(timestamppb.New(time.Now().Add(-2 * time.Hour)))
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("allocate ids", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		parent := datastore.NameKey("P", "p", nil)
		keys, err := e.client.AllocateIDs(t.Context(), []*datastore.Key{
			datastore.IncompleteKey(
				"K",
				parent,
			),
			datastore.IncompleteKey("K", parent),
			datastore.IncompleteKey("K", nil),
		})
		require.NoError(t, err)
		require.Len(t, keys, 3)

		seen := map[int64]bool{}
		for _, k := range keys {
			assert.NotZero(t, k.ID)
			assert.False(t, seen[k.ID])
			seen[k.ID] = true
		}

		assert.Equal(t, "p", keys[0].Parent.Name)

		_, err = e.client.AllocateIDs(t.Context(), []*datastore.Key{datastore.NameKey("K", "n", nil)})
		assert.Error(t, err)
	})

	t.Run("reserve ids and explicit ids", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t)
		_, err := e.raw.ReserveIds(t.Context(), &datastorepb.ReserveIdsRequest{
			ProjectId: project,
			Keys: []*datastorepb.Key{
				{Path: []*datastorepb.Key_PathElement{{Kind: "K", IdType: &datastorepb.Key_PathElement_Id{Id: 77}}}},
			},
		})
		require.NoError(t, err)

		_, err = e.raw.ReserveIds(t.Context(), &datastorepb.ReserveIdsRequest{
			ProjectId: project, Keys: []*datastorepb.Key{{Path: []*datastorepb.Key_PathElement{{Kind: "K"}}}},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))

		k, err := e.client.Put(t.Context(), datastore.IDKey("K", 77, nil), &task{Name: "x"})
		require.NoError(t, err)
		assert.EqualValues(t, 77, k.ID)
	})
}
