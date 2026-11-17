package gcpdatastore_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type memObjects struct {
	objs map[string][]byte
	mu   sync.Mutex
}

var errObjectMissing = errors.New("object missing")

func (m *memObjects) ListObjects(bucket, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []string

	for k := range m.objs {
		if n, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}

	slices.Sort(out)

	return out, nil
}

func (m *memObjects) ReadObject(bucket, name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, ok := m.objs[bucket+"/"+name]
	if !ok {
		return nil, errObjectMissing
	}

	return slices.Clone(d), nil
}

func (m *memObjects) WriteObject(bucket, name string, data []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objs[bucket+"/"+name] = slices.Clone(data)

	return nil
}

func (m *memObjects) corrupt(suffix string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.objs {
		if strings.HasSuffix(k, suffix) {
			m.objs[k] = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		}
	}
}

func exportEnv(t *testing.T) (*env, *memObjects) {
	t.Helper()

	e := newEnv(t)
	store := &memObjects{objs: map[string][]byte{}}
	e.handler.Backend.SetObjectStore(store)

	for _, kind := range []string{"Job", "Other"} {
		e.seed(t, kind, seedTasks())
	}

	k := datastore.NameKey("Job", "ns", nil)
	k.Namespace = "tenant"
	_, err := e.client.Put(t.Context(), k, &task{Name: "ns", Priority: 9})
	require.NoError(t, err)

	return e, store
}

func (e *env) count(t *testing.T, kind, ns string) int {
	t.Helper()

	q := datastore.NewQuery(kind).Namespace(ns).KeysOnly()
	keys, err := e.client.GetAll(t.Context(), q, nil)
	require.NoError(t, err)

	return len(keys)
}

func (e *env) exportURL(t *testing.T, f *adminpb.EntityFilter) string {
	t.Helper()

	op, err := e.admin.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
		ProjectId: project, OutputUrlPrefix: "gs://bkt/exports/", EntityFilter: f,
	})
	require.NoError(t, err)

	resp := &adminpb.ExportEntitiesResponse{}
	require.NoError(t, op.GetResponse().UnmarshalTo(resp))
	require.True(t, op.GetDone())
	require.True(t, strings.HasPrefix(resp.GetOutputUrl(), "gs://bkt/exports/"))
	require.True(t, strings.HasSuffix(resp.GetOutputUrl(), ".overall_export_metadata"))

	return resp.GetOutputUrl()
}

func (e *env) wipe(t *testing.T) {
	t.Helper()

	for _, ns := range []string{"", "tenant"} {
		for _, kind := range []string{"Job", "Other"} {
			keys, err := e.client.GetAll(t.Context(), datastore.NewQuery(kind).Namespace(ns).KeysOnly(), nil)
			require.NoError(t, err)
			require.NoError(t, e.client.DeleteMulti(t.Context(), keys))
		}
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		export  *adminpb.EntityFilter
		imp     *adminpb.EntityFilter
		name    string
		wantJob int
		wantOth int
		wantNs  int
	}{
		{name: "everything", wantJob: 5, wantOth: 5, wantNs: 1},
		{name: "kind filter on export", export: &adminpb.EntityFilter{Kinds: []string{"Job"}}, wantJob: 5, wantNs: 1},
		{
			name:    "default namespace only",
			export:  &adminpb.EntityFilter{NamespaceIds: []string{""}},
			wantJob: 5, wantOth: 5,
		},
		{name: "kind filter on import", imp: &adminpb.EntityFilter{Kinds: []string{"Other"}}, wantOth: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := exportEnv(t)
			url := e.exportURL(t, tt.export)
			e.wipe(t)
			require.Zero(t, e.count(t, "Job", ""))

			op, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
				ProjectId: project, InputUrl: url, EntityFilter: tt.imp,
			})
			require.NoError(t, err)
			assert.True(t, op.GetDone())

			assert.Equal(t, tt.wantJob, e.count(t, "Job", ""))
			assert.Equal(t, tt.wantOth, e.count(t, "Other", ""))
			assert.Equal(t, tt.wantNs, e.count(t, "Job", "tenant"))

			if tt.wantJob > 0 {
				var got task

				require.NoError(t, e.client.Get(t.Context(), datastore.NameKey("Job", "c", nil), &got))
				assert.Equal(t, 3, got.Priority)
				assert.Equal(t, []string{"z"}, got.Tags)
			}
		})
	}
}

func TestExportImportErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *env, store *memObjects) error
		name string
		want codes.Code
	}{
		{
			name: "bad export url",
			run: func(t *testing.T, e *env, _ *memObjects) error {
				t.Helper()

				_, err := e.admin.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
					ProjectId: project, OutputUrlPrefix: "s3://x",
				})

				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "duplicate namespace in filter",
			run: func(t *testing.T, e *env, _ *memObjects) error {
				t.Helper()

				_, err := e.admin.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
					ProjectId: project, OutputUrlPrefix: "gs://bkt",
					EntityFilter: &adminpb.EntityFilter{NamespaceIds: []string{"a", "a"}},
				})

				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "import url without metadata suffix",
			run: func(t *testing.T, e *env, _ *memObjects) error {
				t.Helper()

				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: "gs://bkt/x/output-0",
				})

				return err
			},
			want: codes.InvalidArgument,
		},
		{
			name: "import missing metadata",
			run: func(t *testing.T, e *env, _ *memObjects) error {
				t.Helper()

				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: "gs://bkt/x/y.overall_export_metadata",
				})

				return err
			},
			want: codes.NotFound,
		},
		{
			name: "import filter not a subset",
			run: func(t *testing.T, e *env, _ *memObjects) error {
				t.Helper()

				url := e.exportURL(t, &adminpb.EntityFilter{Kinds: []string{"Job"}})
				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: url, EntityFilter: &adminpb.EntityFilter{Kinds: []string{"Other"}},
				})

				return err
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "corrupt data file leaves state untouched",
			run: func(t *testing.T, e *env, store *memObjects) error {
				t.Helper()

				url := e.exportURL(t, nil)
				e.wipe(t)
				store.corrupt("/output-0")

				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: url,
				})
				assert.Zero(t, e.count(t, "Job", ""))

				return err
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "metadata is not an export",
			run: func(t *testing.T, e *env, store *memObjects) error {
				t.Helper()

				meta := []byte(`{"format":"x"}`)
				require.NoError(t, store.WriteObject("bkt", "z/q.overall_export_metadata", meta, ""))
				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: "gs://bkt/z/q.overall_export_metadata",
				})

				return err
			},
			want: codes.FailedPrecondition,
		},
		{
			name: "metadata names a file outside the export",
			run: func(t *testing.T, e *env, store *memObjects) error {
				t.Helper()

				meta := `{"format":"gopherstack-datastore-export-v1","groups":[{"kind":"Job","dataFile":"other/output-0"}]}`
				require.NoError(t, store.WriteObject("bkt", "z/q.overall_export_metadata", []byte(meta), ""))
				_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
					ProjectId: project, InputUrl: "gs://bkt/z/q.overall_export_metadata",
				})

				return err
			},
			want: codes.FailedPrecondition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, store := exportEnv(t)
			err := tt.run(t, e, store)
			require.Error(t, err)
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}

func TestExportWithoutStorage(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.admin.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
		ProjectId: project, OutputUrlPrefix: "gs://bkt",
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
