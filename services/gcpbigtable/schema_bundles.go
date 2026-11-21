package gcpbigtable

import (
	"encoding/json/jsontext"
	"slices"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) bundleLocked(name string) (*instRec, *tableRec, string, error) {
	inst, tbl, id, err := parseChild(name, "schemaBundles")
	if err != nil {
		return nil, nil, "", err
	}

	rec, err := b.instanceLocked(inst)
	if err != nil {
		return nil, nil, "", err
	}

	t, ok := rec.tables[tbl]
	if !ok {
		return nil, nil, "", errTableNotFound(tableName(inst, tbl))
	}

	if _, ok = t.bundles[id]; !ok {
		return nil, nil, "", errNotFound("SchemaBundle %q not found.", name)
	}

	return rec, t, id, nil
}

func protoFiles(raw []byte) (*protoregistry.Files, error) {
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		return nil, errInvalid("proto_schema.proto_descriptors is not a valid FileDescriptorSet: %v.", err)
	}

	if len(set.GetFile()) == 0 {
		return nil, errInvalid("proto_schema.proto_descriptors must contain at least one file.")
	}

	files, err := protodesc.NewFiles(&set)
	if err != nil {
		return nil, errInvalid("proto_schema.proto_descriptors is not self-contained: %v.", err)
	}

	return files, nil
}

func validateBundle(sb *adminpb.SchemaBundle) error {
	switch ty := sb.GetType().(type) {
	case *adminpb.SchemaBundle_ProtoSchema:
		_, err := protoFiles(ty.ProtoSchema.GetProtoDescriptors())

		return err
	case *adminpb.SchemaBundle_AvroSchema:
		if len(ty.AvroSchema.GetJsonSchemas()) == 0 {
			return errInvalid("avro_schema.json_schemas must contain at least one schema.")
		}

		for i, s := range ty.AvroSchema.GetJsonSchemas() {
			if !jsontext.Value(s).IsValid() {
				return errInvalid("avro_schema.json_schemas[%d] is not valid JSON.", i)
			}
		}

		return nil
	default:
		return errInvalid("schema_bundle must set proto_schema or avro_schema.")
	}
}

type fieldShape struct {
	kind protoreflect.Kind
	list bool
}

func messageShapes(files *protoregistry.Files) map[string]map[protoreflect.FieldNumber]fieldShape {
	out := map[string]map[protoreflect.FieldNumber]fieldShape{}

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		collectShapes(fd.Messages(), out)

		return true
	})

	return out
}

func collectShapes(msgs protoreflect.MessageDescriptors, out map[string]map[protoreflect.FieldNumber]fieldShape) {
	for i := range msgs.Len() {
		md := msgs.Get(i)
		fields := make(map[protoreflect.FieldNumber]fieldShape)

		for j := range md.Fields().Len() {
			f := md.Fields().Get(j)
			fields[f.Number()] = fieldShape{kind: f.Kind(), list: f.IsList()}
		}

		out[string(md.FullName())] = fields
		collectShapes(md.Messages(), out)
	}
}

// bundleCompatible reports whether every message and field of the old proto schema survives with the same kind.
func bundleCompatible(oldSB, next *adminpb.SchemaBundle) error {
	op, np := oldSB.GetProtoSchema(), next.GetProtoSchema()
	if op == nil || np == nil {
		return nil
	}

	of, err := protoFiles(op.GetProtoDescriptors())
	if err != nil {
		return err
	}

	nf, err := protoFiles(np.GetProtoDescriptors())
	if err != nil {
		return err
	}

	newShapes := messageShapes(nf)

	for msg, fields := range messageShapes(of) {
		cur, ok := newShapes[msg]
		if !ok {
			return errPrecondition("The new schema bundle is not backwards compatible: message %q was removed.", msg)
		}

		for num, shape := range fields {
			if got, found := cur[num]; !found || got != shape {
				return errPrecondition(
					"The new schema bundle is not backwards compatible: field %d of message %q was removed or changed.",
					num, msg,
				)
			}
		}
	}

	return nil
}

// CreateSchemaBundle stores a proto or Avro schema bundle on a table.
func (b *InMemoryBackend) CreateSchemaBundle(in *adminpb.CreateSchemaBundleRequest) (*longrunningpb.Operation, error) {
	id := in.GetSchemaBundleId()
	if !tableIDRe.MatchString(id) || len(id) > maxTableIDLen {
		return nil, errInvalid("Invalid schema bundle id %q.", id)
	}

	pb := clone(in.GetSchemaBundle())
	if pb == nil {
		return nil, errInvalid("schema_bundle is required.")
	}

	if err := validateBundle(pb); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSchemaBundle")
	defer b.mu.Unlock()

	rec, t, _, err := b.tableLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	if _, exists := t.bundles[id]; exists {
		return nil, errExists("SchemaBundle %q already exists.", in.GetParent()+"/schemaBundles/"+id)
	}

	pb.Name = in.GetParent() + "/schemaBundles/" + id
	pb.Etag = b.newEtag()
	t.bundles[id] = pb

	return b.finish(rec.pb.GetName(), &adminpb.CreateSchemaBundleMetadata{
		Name: pb.GetName(), StartTime: b.ts(), EndTime: b.ts(),
	}, clone(pb))
}

// GetSchemaBundle returns one bundle.
func (b *InMemoryBackend) GetSchemaBundle(in *adminpb.GetSchemaBundleRequest) (*adminpb.SchemaBundle, error) {
	b.mu.RLock("GetSchemaBundle")
	defer b.mu.RUnlock()

	_, t, id, err := b.bundleLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	return clone(t.bundles[id]), nil
}

// ListSchemaBundles lists the bundles of a table.
func (b *InMemoryBackend) ListSchemaBundles(
	in *adminpb.ListSchemaBundlesRequest,
) (*adminpb.ListSchemaBundlesResponse, error) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListSchemaBundles")
	defer b.mu.RUnlock()

	_, t, _, err := b.tableLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	all := make([]*adminpb.SchemaBundle, 0, len(t.bundles))
	for _, id := range collections.SortedKeys(t.bundles) {
		all = append(all, clone(t.bundles[id]))
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultViewPage)

	return &adminpb.ListSchemaBundlesResponse{SchemaBundles: pg.Data, NextPageToken: pg.Next}, nil
}

var bundleMaskPaths = []string{ //nolint:gochecknoglobals // immutable catalog
	"proto_schema", "proto_schema.proto_descriptors", "avro_schema", "avro_schema.json_schemas",
}

func bundleUpdatePaths(in *adminpb.UpdateSchemaBundleRequest) ([]string, error) {
	paths := in.GetUpdateMask().GetPaths()

	if len(paths) == 0 || slices.Contains(paths, "*") {
		if in.GetSchemaBundle().GetProtoSchema() != nil {
			return []string{"proto_schema"}, nil
		}

		if in.GetSchemaBundle().GetAvroSchema() != nil {
			return []string{"avro_schema"}, nil
		}

		return nil, errInvalid("schema_bundle must set proto_schema or avro_schema.")
	}

	for _, p := range paths {
		if !slices.Contains(bundleMaskPaths, p) {
			return nil, errInvalid("SchemaBundle field %q cannot be updated.", p)
		}
	}

	return paths, nil
}

// UpdateSchemaBundle replaces the schema of a bundle; the kind of schema cannot change.
func (b *InMemoryBackend) UpdateSchemaBundle(in *adminpb.UpdateSchemaBundleRequest) (*longrunningpb.Operation, error) {
	paths, err := bundleUpdatePaths(in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateSchemaBundle")
	defer b.mu.Unlock()

	rec, t, id, err := b.bundleLocked(in.GetSchemaBundle().GetName())
	if err != nil {
		return nil, err
	}

	cur := t.bundles[id]

	if e := in.GetSchemaBundle().GetEtag(); e != "" && e != cur.GetEtag() {
		return nil, errAborted("SchemaBundle etag %q does not match the current etag.", e)
	}

	next := clone(cur)
	if err = applyMask(next, in.GetSchemaBundle(), paths); err != nil {
		return nil, err
	}

	if next.GetType() == nil || (cur.GetProtoSchema() != nil) != (next.GetProtoSchema() != nil) {
		return nil, errInvalid("The type of a schema bundle cannot change after creation.")
	}

	if err = validateBundle(next); err != nil {
		return nil, err
	}

	if !in.GetIgnoreWarnings() {
		if err = bundleCompatible(cur, next); err != nil {
			return nil, err
		}
	}

	next.Etag = b.newEtag()
	t.bundles[id] = next

	return b.finish(rec.pb.GetName(), &adminpb.UpdateSchemaBundleMetadata{
		Name: next.GetName(), StartTime: b.ts(), EndTime: b.ts(),
	}, clone(next))
}

// DeleteSchemaBundle removes a bundle.
func (b *InMemoryBackend) DeleteSchemaBundle(in *adminpb.DeleteSchemaBundleRequest) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteSchemaBundle")
	defer b.mu.Unlock()

	_, t, id, err := b.bundleLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if e := in.GetEtag(); e != "" && e != t.bundles[id].GetEtag() {
		return nil, errAborted("SchemaBundle etag %q does not match the current etag.", e)
	}

	delete(t.bundles, id)

	return &emptypb.Empty{}, nil
}
