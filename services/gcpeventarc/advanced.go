package gcpeventarc

import (
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	kindMessageBuses = "messageBuses"
	kindEnrollments  = "enrollments"
	kindPipelines    = "pipelines"
	kindAPISources   = "googleApiSources"
)

// advKind describes one Eventarc Advanced collection.
type advKind struct {
	create func() proto.Message
	// noun is the resource name used in messages, e.g. "MessageBus".
	noun       string
	collection string
}

func advKinds() []advKind {
	return []advKind{
		{
			collection: kindMessageBuses,
			noun:       "MessageBus",
			create:     func() proto.Message { return &eventarcpb.MessageBus{} },
		},
		{
			collection: kindEnrollments,
			noun:       "Enrollment",
			create:     func() proto.Message { return &eventarcpb.Enrollment{} },
		},
		{collection: kindPipelines, noun: "Pipeline", create: func() proto.Message { return &eventarcpb.Pipeline{} }},
		{
			collection: kindAPISources, noun: "GoogleApiSource",
			create: func() proto.Message { return &eventarcpb.GoogleApiSource{} },
		},
	}
}

func busKind() advKind      { return advKinds()[0] }
func enrollKind() advKind   { return advKinds()[1] }
func pipelineKind() advKind { return advKinds()[2] }
func sourceKind() advKind   { return advKinds()[3] }

func immutableFields() []protoreflect.Name {
	return []protoreflect.Name{"name", "uid", fieldCreateTime, fieldUpdateTime, "etag"}
}

func fieldOf(m proto.Message, name protoreflect.Name) protoreflect.FieldDescriptor {
	return m.ProtoReflect().Descriptor().Fields().ByName(name)
}

func getString(m proto.Message, name protoreflect.Name) string {
	if fd := fieldOf(m, name); fd != nil {
		return m.ProtoReflect().Get(fd).String()
	}

	return ""
}

func setString(m proto.Message, name protoreflect.Name, v string) {
	if fd := fieldOf(m, name); fd != nil {
		m.ProtoReflect().Set(fd, protoreflect.ValueOfString(v))
	}
}

func getTime(m proto.Message, name protoreflect.Name) *timestamppb.Timestamp {
	fd := fieldOf(m, name)
	if fd == nil || !m.ProtoReflect().Has(fd) {
		return nil
	}

	ts, _ := m.ProtoReflect().Get(fd).Message().Interface().(*timestamppb.Timestamp)

	return ts
}

func setTime(m proto.Message, name protoreflect.Name, ts *timestamppb.Timestamp) {
	if fd := fieldOf(m, name); fd != nil {
		m.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(ts.ProtoReflect()))
	}
}

func advEtag(m proto.Message) string {
	c := proto.Clone(m)
	setString(c, "etag", "")

	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return base64.RawURLEncoding.EncodeToString(sum[:etagLen])
}

func (b *InMemoryBackend) advTable(k advKind) map[string]proto.Message {
	t := b.adv[k.collection]
	if t == nil {
		t = map[string]proto.Message{}
		b.adv[k.collection] = t
	}

	return t
}

// advCreate creates a resource of kind k; validate runs with the backend lock held.
func (b *InMemoryBackend) advCreate(
	k advKind, parent, id string, in proto.Message, validateOnly bool, validate func(proto.Message) error,
) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(parent)
	if err != nil {
		return nil, err
	}

	if !knownLocation(location) || location == globalLocation && k.collection != kindMessageBuses {
		return nil, errInvalid("Location %q is not supported for %s.", location, k.noun)
	}

	if !triggerIDRe.MatchString(id) {
		return nil, errInvalid("Invalid %s id %q: use 3-63 lowercase letters, digits and hyphens.", k.noun, id)
	}

	if in == nil || !in.ProtoReflect().IsValid() {
		return nil, errInvalid("%s is required.", strings.ToLower(k.noun[:1])+k.noun[1:])
	}

	res := proto.Clone(in)

	if lf := fieldOf(res, "labels"); lf != nil {
		if err = validateLabels(stringMap(res, lf)); err != nil {
			return nil, err
		}
	}

	name := childName(project, location, k.collection, id)

	b.mu.Lock("advCreate")
	defer b.mu.Unlock()

	if _, ok := b.advTable(k)[name]; ok {
		return nil, errExists("%s %q already exists.", k.noun, name)
	}

	setString(res, "name", name)
	setString(res, "uid", uuid.NewString())
	setTime(res, fieldCreateTime, b.ts())
	setTime(res, fieldUpdateTime, b.ts())

	if err = validate(res); err != nil {
		return nil, err
	}

	setString(res, "etag", advEtag(res))

	if !validateOnly {
		b.advTable(k)[name] = res
	}

	return b.finish(project, location, name, "create", proto.Clone(res))
}

func stringMap(m proto.Message, fd protoreflect.FieldDescriptor) map[string]string {
	out := map[string]string{}

	m.ProtoReflect().Get(fd).Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		out[k.String()] = v.String()

		return true
	})

	return out
}

func (b *InMemoryBackend) advGet(k advKind, name string) (proto.Message, error) {
	if _, _, _, err := parseChild(name, k.collection); err != nil {
		return nil, err
	}

	b.mu.RLock("advGet")
	defer b.mu.RUnlock()

	res, ok := b.advTable(k)[name]
	if !ok {
		return nil, errNotFound("%s %q not found.", k.noun, name)
	}

	return proto.Clone(res), nil
}

// advList lists the resources of k under parent ("-" spans locations), ordered by order_by (default name).
func (b *InMemoryBackend) advList(
	k advKind, parent, orderBy, filter string, pageSize int32, token string,
) ([]proto.Message, string, error) {
	project, location, err := parseParent(parent)
	if err != nil {
		return nil, "", err
	}

	if filter != "" {
		return nil, "", errInvalid("filter is not supported for %s.", k.collection)
	}

	if pageSize < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("advList")

	var all []proto.Message

	for name, res := range b.advTable(k) {
		p, l, _, perr := parseChild(name, k.collection)
		if perr == nil && p == project && (location == wildcardLocation || l == location) {
			all = append(all, proto.Clone(res))
		}
	}

	b.mu.RUnlock()

	cmpFn, err := orderFunc(orderBy, func(m proto.Message, field string) (string, bool) {
		switch field {
		case fieldName:
			return getString(m, "name"), m != nil
		case fieldCreateTime, fieldUpdateTime:
			if m == nil {
				return "", true
			}

			ts := getTime(m, protoreflect.Name(field))

			return ts.AsTime().Format(time.RFC3339Nano), true
		}

		return "", false
	})
	if err != nil {
		return nil, "", err
	}

	slices.SortStableFunc(all, cmpFn)

	pg := page.New(all, token, int(pageSize), defaultListPage)

	return pg.Data, pg.Next, nil
}

func applyAdvMask(next, src proto.Message, paths []string) error {
	n, s := next.ProtoReflect(), src.ProtoReflect()
	fields := n.Descriptor().Fields()

	var targets []protoreflect.FieldDescriptor

	if len(paths) == 0 || slices.Contains(paths, updateMaskAll) {
		s.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			targets = append(targets, fd)

			return true
		})
	} else {
		for _, p := range paths {
			top, _, _ := strings.Cut(p, ".")

			fd := fields.ByName(protoreflect.Name(top))
			if fd == nil {
				return errInvalid("Unsupported update_mask path %q.", p)
			}

			targets = append(targets, fd)
		}
	}

	for _, fd := range targets {
		if slices.Contains(immutableFields(), fd.Name()) {
			continue
		}

		if s.Has(fd) {
			n.Set(fd, s.Get(fd))
		} else {
			n.Clear(fd)
		}
	}

	return nil
}

// advUpdate applies the masked fields of src to the stored resource.
func (b *InMemoryBackend) advUpdate(
	k advKind, src proto.Message, mask *fieldmaskpb.FieldMask, allowMissing, validateOnly bool,
	validate func(proto.Message) error,
) (*longrunningpb.Operation, error) {
	name := getString(src, "name")

	project, location, id, err := parseChild(name, k.collection)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("advUpdate")
	defer b.mu.Unlock()

	cur, ok := b.advTable(k)[name]
	if !ok {
		if !allowMissing {
			return nil, errNotFound("%s %q not found.", k.noun, name)
		}

		b.mu.Unlock()

		op, cerr := b.advCreate(k, locationName(project, location), id, src, validateOnly, validate)

		b.mu.Lock("advUpdate")

		return op, cerr
	}

	if e := getString(src, "etag"); e != "" && e != getString(cur, "etag") {
		return nil, errAborted("The %s was modified since it was read; read it again.", strings.ToLower(k.noun))
	}

	next := proto.Clone(cur)
	if err = applyAdvMask(next, src, mask.GetPaths()); err != nil {
		return nil, err
	}

	if err = validate(next); err != nil {
		return nil, err
	}

	setTime(next, fieldUpdateTime, b.ts())
	setString(next, "etag", advEtag(next))

	if !validateOnly {
		b.advTable(k)[name] = next
	}

	return b.finish(project, location, name, "update", proto.Clone(next))
}

func (b *InMemoryBackend) advDelete(
	k advKind, name, etag string, allowMissing, validateOnly bool,
) (*longrunningpb.Operation, error) {
	project, location, _, err := parseChild(name, k.collection)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("advDelete")
	defer b.mu.Unlock()

	cur, ok := b.advTable(k)[name]
	if !ok {
		if allowMissing {
			stub := k.create()
			setString(stub, "name", name)

			return b.finish(project, location, name, "delete", stub)
		}

		return nil, errNotFound("%s %q not found.", k.noun, name)
	}

	if etag != "" && etag != getString(cur, "etag") {
		return nil, errAborted("The %s was modified since it was read; read it again.", strings.ToLower(k.noun))
	}

	if err = b.deleteBlockedLocked(k, name); err != nil {
		return nil, err
	}

	if !validateOnly {
		delete(b.advTable(k), name)
	}

	return b.finish(project, location, name, "delete", proto.Clone(cur))
}

// deleteBlockedLocked refuses to delete a resource another one still points at.
func (b *InMemoryBackend) deleteBlockedLocked(k advKind, name string) error {
	refs := map[string][]struct {
		kind  advKind
		field protoreflect.Name
	}{
		kindMessageBuses: {{enrollKind(), "message_bus"}, {sourceKind(), "destination"}},
		kindPipelines:    {{enrollKind(), "destination"}},
	}

	for _, r := range refs[k.collection] {
		for _, res := range b.advTable(r.kind) {
			if getString(res, r.field) == name {
				return errPrecondition(
					"%s %q is still used by %s %q.",
					k.noun,
					name,
					r.kind.noun,
					getString(res, "name"),
				)
			}
		}
	}

	return nil
}

// ListMessageBusEnrollments lists the names of the enrollments on a message bus.
func (b *InMemoryBackend) ListMessageBusEnrollments(
	in *eventarcpb.ListMessageBusEnrollmentsRequest,
) (*eventarcpb.ListMessageBusEnrollmentsResponse, error) {
	if _, err := b.advGet(busKind(), in.GetParent()); err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListMessageBusEnrollments")

	var names []string

	for name, res := range b.advTable(enrollKind()) {
		if getString(res, "message_bus") == in.GetParent() {
			names = append(names, name)
		}
	}

	b.mu.RUnlock()

	slices.Sort(names)

	pg := page.New(names, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &eventarcpb.ListMessageBusEnrollmentsResponse{Enrollments: pg.Data, NextPageToken: pg.Next}, nil
}
