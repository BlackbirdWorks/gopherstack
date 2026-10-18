package gcppubsub

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	deletedSchema = "_deleted-schema_"
	revisionIDLen = 8
)

type schemaRevision struct {
	Created    time.Time `json:"created"`
	ID         string    `json:"id"`
	Definition string    `json:"definition"`
}

type schemaRec struct {
	Name      string           `json:"name"`
	Revisions []schemaRevision `json:"revisions"`
	Type      int32            `json:"type"`
}

func (r *schemaRec) view(rev schemaRevision, full bool) *pubsubpb.Schema {
	out := &pubsubpb.Schema{
		Name:               r.Name,
		Type:               pubsubpb.Schema_Type(r.Type),
		RevisionId:         rev.ID,
		RevisionCreateTime: timestamppb.New(rev.Created),
	}
	if full {
		out.Definition = rev.Definition
	}

	return out
}

func (r *schemaRec) latest() schemaRevision { return r.Revisions[len(r.Revisions)-1] }

func (r *schemaRec) revision(id string) (schemaRevision, bool) {
	for _, rv := range r.Revisions {
		if rv.ID == id {
			return rv, true
		}
	}

	return schemaRevision{}, false
}

func validateDefinition(typ pubsubpb.Schema_Type, def string) error {
	if strings.TrimSpace(def) == "" {
		return errInvalid("schema definition must not be empty")
	}

	switch typ {
	case pubsubpb.Schema_AVRO:
		if _, err := parseAvroSchema(def); err != nil {
			return errInvalid("invalid Avro schema: %v", err)
		}
	case pubsubpb.Schema_PROTOCOL_BUFFER:
		return validateProtoDefinition(def)
	case pubsubpb.Schema_TYPE_UNSPECIFIED:
		fallthrough
	default:
		return errInvalid("schema type must be AVRO or PROTOCOL_BUFFER")
	}

	return nil
}

func validateProtoDefinition(def string) error {
	if strings.Count(def, "{") != strings.Count(def, "}") {
		return errInvalid("invalid protocol buffer schema: unbalanced braces")
	}

	if !strings.Contains(def, "message ") && !strings.Contains(def, "message\t") {
		return errInvalid("invalid protocol buffer schema: no message type declared")
	}

	if !strings.Contains(def, "syntax") {
		return errInvalid("invalid protocol buffer schema: missing syntax declaration")
	}

	return nil
}

// validateData checks a message payload against one schema revision.
func validateData(typ pubsubpb.Schema_Type, def string, enc pubsubpb.Encoding, data []byte) bool {
	switch typ {
	case pubsubpb.Schema_AVRO:
		t, err := parseAvroSchema(def)
		if err != nil {
			return false
		}

		if enc == pubsubpb.Encoding_BINARY {
			r := &avroReader{buf: data}

			return t.consumeBinary(r) && len(r.buf) == 0
		}

		var v any
		dec := json.NewDecoder(bytes.NewReader(data))

		return dec.Decode(&v) == nil && !dec.More() && t.validJSON(v)
	case pubsubpb.Schema_PROTOCOL_BUFFER:
		if enc == pubsubpb.Encoding_BINARY {
			return validWire(data)
		}

		var v map[string]any

		return json.Unmarshal(data, &v) == nil
	case pubsubpb.Schema_TYPE_UNSPECIFIED:
		return false
	default:
		return false
	}
}

func validWire(b []byte) bool {
	for len(b) > 0 {
		_, _, n := protowire.ConsumeField(b)
		if n < 0 {
			return false
		}

		b = b[n:]
	}

	return true
}

// resolveSchemaLocked finds a schema by name, optionally suffixed with @revision.
func (b *InMemoryBackend) resolveSchemaLocked(ref string) (*pubsubpb.Schema, error) {
	name, rev, _ := strings.Cut(ref, "@")

	rec, ok := b.schemas[name]
	if !ok {
		return nil, errNotFound("schemas", name)
	}

	revision := rec.latest()

	if rev != "" {
		var found bool
		if revision, found = rec.revision(rev); !found {
			return nil, errNotFound("schema revisions", ref)
		}
	}

	return rec.view(revision, true), nil
}

func (b *InMemoryBackend) validateAgainstSchemaLocked(t *topic, msgs []*pubsubpb.PubsubMessage) error {
	ss := t.pb.GetSchemaSettings()
	if ss == nil || ss.GetSchema() == "" || ss.GetSchema() == deletedSchema {
		return nil
	}

	rec, ok := b.schemas[ss.GetSchema()]
	if !ok {
		return errNotFound("schemas", ss.GetSchema())
	}

	revs := rec.Revisions[len(rec.Revisions)-1:]

	if ss.GetFirstRevisionId() != "" || ss.GetLastRevisionId() != "" {
		lo, hi := 0, len(rec.Revisions)-1

		if i := slices.IndexFunc(
			rec.Revisions,
			func(r schemaRevision) bool { return r.ID == ss.GetFirstRevisionId() },
		); i >= 0 {
			lo = i
		}

		if i := slices.IndexFunc(
			rec.Revisions,
			func(r schemaRevision) bool { return r.ID == ss.GetLastRevisionId() },
		); i >= 0 {
			hi = i
		}

		revs = rec.Revisions[lo : max(lo, hi)+1]
	}

	for _, m := range msgs {
		if !slices.ContainsFunc(revs, func(r schemaRevision) bool {
			return validateData(pubsubpb.Schema_Type(rec.Type), r.Definition, ss.GetEncoding(), m.GetData())
		}) {
			return errInvalid("Message does not conform to schema %s (encoding %s).", ss.GetSchema(), ss.GetEncoding())
		}
	}

	return nil
}

// CreateSchema stores a new schema with its first revision.
func (b *InMemoryBackend) CreateSchema(req *pubsubpb.CreateSchemaRequest) (*pubsubpb.Schema, error) {
	if err := checkProject(req.GetParent()); err != nil {
		return nil, err
	}

	in := req.GetSchema()
	if in == nil {
		return nil, errInvalid("schema must be provided")
	}

	id := req.GetSchemaId()
	if id == "" {
		id, _ = parseID(in.GetName(), "schemas")
	}

	if !validID(id) {
		return nil, errInvalid("Invalid schema id %q", id)
	}

	if err := validateDefinition(in.GetType(), in.GetDefinition()); err != nil {
		return nil, err
	}

	name := req.GetParent() + "/schemas/" + id

	b.mu.Lock("CreateSchema")
	defer b.mu.Unlock()

	if _, ok := b.schemas[name]; ok {
		return nil, errExists("schema", name)
	}

	rev := schemaRevision{ID: awsid.NewN("", revisionIDLen), Definition: in.GetDefinition(), Created: b.now()}
	rec := &schemaRec{Name: name, Type: int32(in.GetType()), Revisions: []schemaRevision{rev}}
	b.schemas[name] = rec

	return rec.view(rev, true), nil
}

func viewFull(v pubsubpb.SchemaView, deflt bool) bool {
	switch v {
	case pubsubpb.SchemaView_FULL:
		return true
	case pubsubpb.SchemaView_BASIC:
		return false
	case pubsubpb.SchemaView_SCHEMA_VIEW_UNSPECIFIED:
		return deflt
	default:
		return deflt
	}
}

// GetSchema returns a schema (or one revision when the name carries @revision).
func (b *InMemoryBackend) GetSchema(req *pubsubpb.GetSchemaRequest) (*pubsubpb.Schema, error) {
	b.mu.RLock("GetSchema")
	defer b.mu.RUnlock()

	s, err := b.resolveSchemaLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	if !viewFull(req.GetView(), true) {
		s.Definition = ""
	}

	return s, nil
}

// ListSchemas lists the latest revision of every schema of a project.
func (b *InMemoryBackend) ListSchemas(req *pubsubpb.ListSchemasRequest) ([]*pubsubpb.Schema, string, error) {
	if err := checkProject(req.GetParent()); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()

	prefix := req.GetParent() + "/schemas/"
	full := viewFull(req.GetView(), false)

	var all []*pubsubpb.Schema

	for name, rec := range b.schemas {
		if strings.HasPrefix(name, prefix) {
			all = append(all, rec.view(rec.latest(), full))
		}
	}

	slices.SortFunc(all, func(a, c *pubsubpb.Schema) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

// ListSchemaRevisions lists all revisions of a schema, newest first.
func (b *InMemoryBackend) ListSchemaRevisions(
	req *pubsubpb.ListSchemaRevisionsRequest,
) ([]*pubsubpb.Schema, string, error) {
	b.mu.RLock("ListSchemaRevisions")
	defer b.mu.RUnlock()

	name, _, _ := strings.Cut(req.GetName(), "@")

	rec, ok := b.schemas[name]
	if !ok {
		return nil, "", errNotFound("schemas", name)
	}

	full := viewFull(req.GetView(), false)
	all := make([]*pubsubpb.Schema, 0, len(rec.Revisions))

	for _, rv := range slices.Backward(rec.Revisions) {
		all = append(all, rec.view(rv, full))
	}

	pg, err := paginate(all, req.GetPageToken(), req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	return pg.Data, pg.Next, nil
}

func (b *InMemoryBackend) commitLocked(rec *schemaRec, def string) *pubsubpb.Schema {
	rev := schemaRevision{ID: awsid.NewN("", revisionIDLen), Definition: def, Created: b.now()}
	rec.Revisions = append(rec.Revisions, rev)

	return rec.view(rev, true)
}

// CommitSchema adds a revision to an existing schema.
func (b *InMemoryBackend) CommitSchema(req *pubsubpb.CommitSchemaRequest) (*pubsubpb.Schema, error) {
	in := req.GetSchema()
	if in == nil {
		return nil, errInvalid("schema must be provided")
	}

	b.mu.Lock("CommitSchema")
	defer b.mu.Unlock()

	rec, ok := b.schemas[req.GetName()]
	if !ok {
		return nil, errNotFound("schemas", req.GetName())
	}

	if in.GetType() != pubsubpb.Schema_TYPE_UNSPECIFIED && int32(in.GetType()) != rec.Type {
		return nil, errInvalid("schema type cannot change between revisions")
	}

	if err := validateDefinition(pubsubpb.Schema_Type(rec.Type), in.GetDefinition()); err != nil {
		return nil, err
	}

	return b.commitLocked(rec, in.GetDefinition()), nil
}

// RollbackSchema creates a new revision copying an earlier one.
func (b *InMemoryBackend) RollbackSchema(req *pubsubpb.RollbackSchemaRequest) (*pubsubpb.Schema, error) {
	b.mu.Lock("RollbackSchema")
	defer b.mu.Unlock()

	rec, ok := b.schemas[req.GetName()]
	if !ok {
		return nil, errNotFound("schemas", req.GetName())
	}

	if req.GetRevisionId() == "" {
		return nil, errInvalid("revision_id must be provided")
	}

	old, found := rec.revision(req.GetRevisionId())
	if !found {
		return nil, errNotFound("schema revisions", req.GetName()+"@"+req.GetRevisionId())
	}

	return b.commitLocked(rec, old.Definition), nil
}

// DeleteSchemaRevision removes one revision; the last remaining revision cannot be deleted.
func (b *InMemoryBackend) DeleteSchemaRevision(req *pubsubpb.DeleteSchemaRevisionRequest) (*pubsubpb.Schema, error) {
	name, rev, _ := strings.Cut(req.GetName(), "@")
	if rev == "" {
		rev = req.GetRevisionId() //nolint:staticcheck // deprecated field still sent by older clients
	}

	b.mu.Lock("DeleteSchemaRevision")
	defer b.mu.Unlock()

	rec, ok := b.schemas[name]
	if !ok {
		return nil, errNotFound("schemas", name)
	}

	idx := slices.IndexFunc(rec.Revisions, func(r schemaRevision) bool { return r.ID == rev })
	if idx < 0 {
		return nil, errNotFound("schema revisions", name+"@"+rev)
	}

	if len(rec.Revisions) == 1 {
		return nil, errInvalid("cannot delete the only revision of a schema; delete the schema instead")
	}

	rec.Revisions = slices.Delete(rec.Revisions, idx, idx+1)

	return rec.view(rec.latest(), true), nil
}

// DeleteSchema removes a schema; topics that reference it are re-pointed at the deleted-schema marker.
func (b *InMemoryBackend) DeleteSchema(name string) error {
	b.mu.Lock("DeleteSchema")
	defer b.mu.Unlock()

	if _, ok := b.schemas[name]; !ok {
		return errNotFound("schemas", name)
	}

	delete(b.schemas, name)

	for _, t := range b.topics {
		if ss := t.pb.GetSchemaSettings(); ss != nil && ss.GetSchema() == name {
			ss.Schema = deletedSchema
		}
	}

	return nil
}

// ValidateMessage checks a payload against a stored or inline schema.
func (b *InMemoryBackend) ValidateMessage(req *pubsubpb.ValidateMessageRequest) error {
	if err := checkProject(req.GetParent()); err != nil {
		return err
	}

	if req.GetEncoding() == pubsubpb.Encoding_ENCODING_UNSPECIFIED {
		return errInvalid("encoding must be JSON or BINARY")
	}

	var (
		typ pubsubpb.Schema_Type
		def string
	)

	if inline := req.GetSchema(); inline != nil {
		if err := validateDefinition(inline.GetType(), inline.GetDefinition()); err != nil {
			return err
		}

		typ, def = inline.GetType(), inline.GetDefinition()
	} else {
		b.mu.RLock("ValidateMessage")
		s, err := b.resolveSchemaLocked(req.GetName())
		b.mu.RUnlock()

		if err != nil {
			return err
		}

		typ, def = s.GetType(), s.GetDefinition()
	}

	if !validateData(typ, def, req.GetEncoding(), req.GetMessage()) {
		return errInvalid("Message does not conform to the schema (encoding %s).", req.GetEncoding())
	}

	return nil
}
