package gcpdatastore_test

import (
	"encoding/binary"
	"strings"
	"testing"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const craftedMeta = "z/q.overall_export_metadata"

func jobEntity(name string) *datastorepb.Entity {
	return &datastorepb.Entity{
		Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{
			Kind: "Job", IdType: &datastorepb.Key_PathElement_Name{Name: name},
		}}},
		Properties: map[string]*datastorepb.Value{"n": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}},
	}
}

func frame(t *testing.T, ents ...*datastorepb.Entity) []byte {
	t.Helper()

	var out []byte

	for _, e := range ents {
		raw, err := proto.Marshal(e)
		require.NoError(t, err)

		out = append(binary.AppendUvarint(out, uint64(len(raw))), raw...)
	}

	return out
}

func (e *env) importCrafted(t *testing.T, store *memObjects, meta string, data []byte, f *adminpb.EntityFilter) error {
	t.Helper()

	require.NoError(t, store.WriteObject("bkt", craftedMeta, []byte(meta), ""))
	require.NoError(t, store.WriteObject("bkt", "z/Job/output-0", data, ""))

	_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{
		ProjectId: project, InputUrl: "gs://bkt/" + craftedMeta, EntityFilter: f,
	})

	return err
}

func groupMeta(entities string) string {
	return `{"format":"gopherstack-datastore-export-v1","groups":[{"kind":"Job","dataFile":"z/Job/output-0","entities":` +
		entities + `}]}`
}

func TestImportMalformedDataLeavesNothing(t *testing.T) {
	t.Parallel()

	noKey := &datastorepb.Entity{Properties: jobEntity("a").GetProperties()}
	emptyPath := &datastorepb.Entity{Key: &datastorepb.Key{}}
	incomplete := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{Kind: "Job"}}}}
	noKind := &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{
		IdType: &datastorepb.Key_PathElement_Name{Name: "x"},
	}}}}
	badNS := jobEntity("ns")
	badNS.Key.PartitionId = &datastorepb.PartitionId{NamespaceId: "__reserved__"}
	longKind := jobEntity("lk")
	longKind.Key.Path[0].Kind = strings.Repeat("k", 2000)

	deep := &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: 1}}
	for range 200 {
		deep = &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{
			EntityValue: &datastorepb.Entity{Properties: map[string]*datastorepb.Value{"x": deep}},
		}}
	}

	deepEnt := jobEntity("deep")
	deepEnt.Properties["d"] = deep

	tests := []struct {
		name string
		n    string
		data []byte
	}{
		{name: "truncated varint", data: []byte{0x80}, n: "1"},
		{name: "size beyond data", data: []byte{0x7f, 0x01}, n: "1"},
		{name: "size beyond entity cap", data: binary.AppendUvarint(nil, 2<<20), n: "1"},
		{
			name: "varint overflow",
			data: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
			n:    "1",
		},
		{name: "garbage proto", data: append([]byte{0x03}, 0xff, 0xff, 0xff), n: "1"},
		{name: "entity without key", data: frame(t, noKey), n: "1"},
		{name: "key without path", data: frame(t, emptyPath), n: "1"},
		{name: "incomplete key", data: frame(t, incomplete), n: "1"},
		{name: "key without kind", data: frame(t, noKind), n: "1"},
		{name: "reserved namespace", data: frame(t, badNS), n: "1"},
		{name: "oversized kind", data: frame(t, longKind), n: "1"},
		{name: "too deep", data: frame(t, deepEnt), n: "1"},
		{name: "good then bad", data: append(frame(t, jobEntity("ok1"), jobEntity("ok2")), frame(t, noKey)...), n: "3"},
		{name: "good then truncated", data: append(frame(t, jobEntity("ok1")), 0x05, 0x01), n: "2"},
		{name: "count mismatch", data: frame(t, jobEntity("ok1")), n: "5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, store := exportEnv(t)
			e.wipe(t)

			err := e.importCrafted(t, store, groupMeta(tt.n), tt.data, nil)
			require.Error(t, err)
			assert.Contains(
				t,
				[]codes.Code{codes.FailedPrecondition, codes.InvalidArgument},
				status.Code(err),
				err.Error(),
			)
			assert.Zero(t, e.count(t, "Job", ""))
		})
	}
}

func TestImportEntityCapStopsStreaming(t *testing.T) {
	t.Parallel()

	e, store := exportEnv(t)
	e.wipe(t)

	one := frame(t, &datastorepb.Entity{Key: &datastorepb.Key{Path: []*datastorepb.Key_PathElement{{
		Kind: "Job", IdType: &datastorepb.Key_PathElement_Id{Id: 1},
	}}}})
	data := []byte(strings.Repeat(string(one), 200_001))

	err := e.importCrafted(t, store, groupMeta("200001"), data, nil)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Zero(t, e.count(t, "Job", ""))
}

func TestImportMetadataPathRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		meta string
		url  string
		want codes.Code
	}{
		{
			name: "dot dot in data file",
			meta: strings.Replace(
				groupMeta("0"),
				"z/Job/output-0",
				"z/../other/output-0",
				1,
			), want: codes.FailedPrecondition,
		},
		{
			name: "double slash in data file",
			meta: strings.Replace(
				groupMeta("0"),
				"z/Job/output-0",
				"z//Job/output-0",
				1,
			), want: codes.FailedPrecondition,
		},
		{
			name: "absolute data file",
			meta: strings.Replace(groupMeta("0"), "z/Job/output-0", "/etc/passwd", 1), want: codes.FailedPrecondition,
		},
		{
			name: "other bucket url as data file",
			meta: strings.Replace(
				groupMeta("0"),
				"z/Job/output-0",
				"gs://other/z/Job/output-0",
				1,
			), want: codes.FailedPrecondition,
		},
		{
			name: "root metadata names any object",
			meta: strings.Replace(groupMeta("0"), "z/Job/output-0", "secret/output-0", 1),
			url:  "gs://bkt/q.overall_export_metadata", want: codes.FailedPrecondition,
		},
		{
			name: "newer format",
			meta: strings.Replace(groupMeta("0"), "export-v1", "export-v2", 1), want: codes.FailedPrecondition,
		},
		{
			name: "older format",
			meta: strings.Replace(groupMeta("0"), "export-v1", "export-v0", 1),
			want: codes.FailedPrecondition,
		},
		{name: "not json", meta: "{{{", want: codes.FailedPrecondition},
		{name: "traversal url", url: "gs://bkt/z/../z/q.overall_export_metadata", want: codes.InvalidArgument},
		{name: "double slash url", url: "gs://bkt/z//q.overall_export_metadata", want: codes.InvalidArgument},
		{name: "bad bucket url", url: "gs://B/z/q.overall_export_metadata", want: codes.InvalidArgument},
		{name: "valid empty group", meta: groupMeta("0"), want: codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, store := exportEnv(t)
			require.NoError(t, store.WriteObject("bkt", craftedMeta, []byte(tt.meta), ""))
			require.NoError(t, store.WriteObject("bkt", "q.overall_export_metadata", []byte(tt.meta), ""))
			require.NoError(t, store.WriteObject("bkt", "z/Job/output-0", nil, ""))

			url := "gs://bkt/" + craftedMeta
			if tt.url != "" {
				url = tt.url
			}

			_, err := e.admin.ImportEntities(
				t.Context(),
				&adminpb.ImportEntitiesRequest{ProjectId: project, InputUrl: url},
			)
			assert.Equal(t, tt.want, status.Code(err))

			if tt.name == "newer format" || tt.name == "older format" {
				assert.Contains(t, err.Error(), "not supported")
			}
		})
	}
}

func TestExportOutputURLRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		url  string
		want codes.Code
	}{
		{url: "gs://bkt/a/b", want: codes.OK},
		{url: "gs://bkt", want: codes.OK},
		{url: "gs://bkt/../x", want: codes.InvalidArgument},
		{url: "gs://bkt/a/../../x", want: codes.InvalidArgument},
		{url: "gs://bkt/a//b", want: codes.InvalidArgument},
		{url: "gs://bkt/./a", want: codes.InvalidArgument},
		{url: "gs://bkt/a\x00b", want: codes.InvalidArgument},
		{url: "gs://UPPER/a", want: codes.InvalidArgument},
		{url: "gs://ab/a", want: codes.InvalidArgument},
		{url: "gs:///a", want: codes.InvalidArgument},
		{url: "gs://bkt/" + strings.Repeat("a", 1100), want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.url, "/", "_"), func(t *testing.T) {
			t.Parallel()

			e, store := exportEnv(t)

			_, err := e.admin.ExportEntities(t.Context(), &adminpb.ExportEntitiesRequest{
				ProjectId: project, OutputUrlPrefix: tt.url,
			})
			assert.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				assert.Empty(t, store.objs)
			}
		})
	}
}

func TestImportFilterAppliesToEntityKeysNotGroupLabels(t *testing.T) {
	t.Parallel()

	e, store := exportEnv(t)
	e.wipe(t)

	meta := strings.Replace(groupMeta("1"), `"kind":"Job"`, `"kind":"Other"`, 1)

	err := e.importCrafted(
		t,
		store,
		meta,
		frame(t, jobEntity("sneaky")),
		&adminpb.EntityFilter{Kinds: []string{"Other"}},
	)
	require.NoError(t, err)
	assert.Zero(t, e.count(t, "Job", ""))
}

type reentrantStore struct {
	*memObjects
	hook func()
}

func (r reentrantStore) WriteObject(bucket, name string, data []byte, ct string) error {
	r.hook()

	return r.memObjects.WriteObject(bucket, name, data, ct)
}

func (r reentrantStore) ReadObject(bucket, name string) ([]byte, error) {
	r.hook()

	return r.memObjects.ReadObject(bucket, name)
}

func TestExportImportCallStorageOutsideLock(t *testing.T) {
	t.Parallel()

	e, store := exportEnv(t)
	url := e.exportURL(t, nil)

	calls := 0
	e.handler.Backend.SetObjectStore(reentrantStore{memObjects: store, hook: func() {
		calls++

		e.seed(t, "Probe", seedTasks())
	}})

	_ = e.exportURL(t, nil)

	_, err := e.admin.ImportEntities(t.Context(), &adminpb.ImportEntitiesRequest{ProjectId: project, InputUrl: url})
	require.NoError(t, err)
	assert.Positive(t, calls)
}
