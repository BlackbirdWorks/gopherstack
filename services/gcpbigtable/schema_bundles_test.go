package gcpbigtable_test

import (
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func descriptorSet(t *testing.T, fields ...*descriptorpb.FieldDescriptorProto) []byte {
	t.Helper()

	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: new("a.proto"), Package: new("p"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("M"), Field: fields}},
	}}})
	require.NoError(t, err)

	return raw
}

func field(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: new(name), Number: new(num), Type: typ.Enum(),
		Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: new(name),
	}
}

func TestSchemaBundleLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_ = e.seeded(t, "t", "cf")
	ctx := t.Context()

	v1 := descriptorSet(t, field("x", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32))
	v2 := descriptorSet(t,
		field("x", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32),
		field("y", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING))
	broken := descriptorSet(t, field("x", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING))

	conf := func(raw []byte) bigtable.SchemaBundleConf {
		return bigtable.SchemaBundleConf{
			TableID: "t", SchemaBundleID: "sb", ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: raw},
		}
	}

	require.NoError(t, e.ac.CreateSchemaBundle(ctx, &bigtable.SchemaBundleConf{
		TableID: "t", SchemaBundleID: "sb", ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: v1},
	}))

	info, err := e.ac.GetSchemaBundle(ctx, "t", "sb")
	require.NoError(t, err)
	assert.Equal(t, v1, info.SchemaBundle)
	assert.NotEmpty(t, info.Etag)

	names, err := e.ac.SchemaBundles(ctx, "t")
	require.NoError(t, err)
	assert.Equal(t, []string{"sb"}, names)

	tests := []struct {
		name   string
		update bigtable.UpdateSchemaBundleConf
		want   codes.Code
	}{
		{name: "stale etag", want: codes.Aborted, update: bigtable.UpdateSchemaBundleConf{
			SchemaBundleConf: bigtable.SchemaBundleConf{
				TableID: "t", SchemaBundleID: "sb", Etag: "stale",
				ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: v2},
			},
		}},
		{
			name:   "incompatible",
			want:   codes.FailedPrecondition,
			update: bigtable.UpdateSchemaBundleConf{SchemaBundleConf: conf(broken)},
		},
		{name: "incompatible ignored", want: codes.OK, update: bigtable.UpdateSchemaBundleConf{
			SchemaBundleConf: conf(broken), IgnoreWarnings: true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newEnv(t)
			_ = env.seeded(t, "t", "cf")
			require.NoError(t, env.ac.CreateSchemaBundle(t.Context(), &bigtable.SchemaBundleConf{
				TableID: "t", SchemaBundleID: "sb", ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: v1},
			}))
			assert.Equal(t, tt.want, code(env.ac.UpdateSchemaBundle(t.Context(), tt.update)))
		})
	}

	require.NoError(t, e.ac.UpdateSchemaBundle(ctx, bigtable.UpdateSchemaBundleConf{
		SchemaBundleConf: bigtable.SchemaBundleConf{
			TableID: "t", SchemaBundleID: "sb", Etag: info.Etag,
			ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: v2},
		},
	}))

	updated, err := e.ac.GetSchemaBundle(ctx, "t", "sb")
	require.NoError(t, err)
	assert.Equal(t, v2, updated.SchemaBundle)
	assert.NotEqual(t, info.Etag, updated.Etag)

	require.NoError(t, e.ac.DeleteSchemaBundle(ctx, "t", "sb"))

	_, err = e.ac.GetSchemaBundle(ctx, "t", "sb")
	assert.Equal(t, codes.NotFound, code(err))
}

func TestSchemaBundleCreateErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  []byte
		want codes.Code
	}{
		{name: "garbage", raw: []byte("not a descriptor set"), want: codes.InvalidArgument},
		{name: "empty set", raw: []byte{0x10, 0x01}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_ = e.seeded(t, "t", "cf")
			err := e.ac.CreateSchemaBundle(t.Context(), &bigtable.SchemaBundleConf{
				TableID: "t", SchemaBundleID: "sb", ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: tt.raw},
			})
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestSchemaBundlePersistence(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	_ = src.seeded(t, "t", "cf")
	raw := descriptorSet(t, field("x", 1, descriptorpb.FieldDescriptorProto_TYPE_INT32))
	require.NoError(t, src.ac.CreateSchemaBundle(t.Context(), &bigtable.SchemaBundleConf{
		TableID: "t", SchemaBundleID: "sb", ProtoSchema: &bigtable.ProtoSchemaInfo{ProtoDescriptors: raw},
	}))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(t.Context(), src.handler.Backend.Snapshot(t.Context())))

	info, err := dst.ac.GetSchemaBundle(t.Context(), "t", "sb")
	require.NoError(t, err)
	assert.Equal(t, raw, info.SchemaBundle)
}
