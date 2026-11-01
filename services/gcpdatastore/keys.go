package gcpdatastore

import (
	"math"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

const (
	maxKeyPath    = 100
	maxNameBytes  = 1500
	maxKeysLookup = 1000
	defaultDBName = "(default)"
	pathSep       = 0x1e
	kindSep       = 0x1f
)

// partition is the (project, database, namespace) a key lives in.
type partition struct {
	project, database, namespace string
}

func (p partition) String() string {
	return p.project + "\x00" + p.database + "\x00" + p.namespace
}

func requestPartition(project, database string) (partition, error) {
	if project == "" {
		return partition{}, errInvalid("Missing required project ID.")
	}

	if database == defaultDBName {
		return partition{}, errInvalid(
			"database_id '(default)' is not allowed; use the empty string for the default database.",
		)
	}

	return partition{project: project, database: database}, nil
}

// queryPartition applies a request's partition_id (namespace, and optional matching project/database).
func queryPartition(req partition, id *datastorepb.PartitionId) (partition, error) {
	if id == nil {
		return req, nil
	}

	if id.GetProjectId() != "" && id.GetProjectId() != req.project {
		return partition{}, errInvalid("The project ID in partition_id (%q) does not match the request project (%q).",
			id.GetProjectId(), req.project)
	}

	if id.GetDatabaseId() != "" && id.GetDatabaseId() != req.database {
		return partition{}, errInvalid("The database ID in partition_id (%q) does not match the request database (%q).",
			id.GetDatabaseId(), req.database)
	}

	if err := validateNamespace(id.GetNamespaceId()); err != nil {
		return partition{}, err
	}

	req.namespace = id.GetNamespaceId()

	return req, nil
}

func validateNamespace(ns string) error {
	if len(ns) > maxNameBytes {
		return errInvalid("Namespace is longer than %d bytes.", maxNameBytes)
	}

	if isReserved(ns) {
		return errInvalid("Namespace %q is reserved.", ns)
	}

	return nil
}

func isReserved(s string) bool {
	return len(s) >= 4 && strings.HasPrefix(s, "__") && strings.HasSuffix(s, "__")
}

func lastElem(k *datastorepb.Key) *datastorepb.Key_PathElement {
	p := k.GetPath()
	if len(p) == 0 {
		return nil
	}

	return p[len(p)-1]
}

func elemHasID(e *datastorepb.Key_PathElement) bool {
	switch e.GetIdType().(type) {
	case *datastorepb.Key_PathElement_Id:
		return e.GetId() != 0
	case *datastorepb.Key_PathElement_Name:
		return true
	default:
		return false
	}
}

func isComplete(k *datastorepb.Key) bool {
	e := lastElem(k)

	return e != nil && elemHasID(e)
}

func keyKind(k *datastorepb.Key) string { return lastElem(k).GetKind() }

// normKey validates k against the request partition and returns a copy stamped with the partition it lives in.
func normKey(k *datastorepb.Key, req partition, allowIncomplete, forWrite bool) (*datastorepb.Key, partition, error) {
	if k == nil || len(k.GetPath()) == 0 {
		return nil, partition{}, errInvalid("Key path is empty.")
	}

	p, err := queryPartition(req, k.GetPartitionId())
	if err != nil {
		return nil, partition{}, err
	}

	if len(k.GetPath()) > maxKeyPath {
		return nil, partition{}, errInvalid("Key path is longer than %d elements.", maxKeyPath)
	}

	out := &datastorepb.Key{PartitionId: p.stored(), Path: make([]*datastorepb.Key_PathElement, 0, len(k.GetPath()))}

	for i, e := range k.GetPath() {
		if err = validateElem(e, forWrite, i == len(k.GetPath())-1, allowIncomplete); err != nil {
			return nil, partition{}, err
		}

		el, _ := proto.Clone(e).(*datastorepb.Key_PathElement)
		out.Path = append(out.Path, el)
	}

	return out, p, nil
}

func (p partition) stored() *datastorepb.PartitionId {
	return &datastorepb.PartitionId{ProjectId: p.project, DatabaseId: p.database, NamespaceId: p.namespace}
}

func validateElem(e *datastorepb.Key_PathElement, forWrite, last, allowIncomplete bool) error {
	if e.GetKind() == "" {
		return errInvalid("The key path element kind must not be empty.")
	}

	if len(e.GetKind()) > maxNameBytes {
		return errInvalid("The key path element kind is longer than %d bytes.", maxNameBytes)
	}

	if forWrite && isReserved(e.GetKind()) {
		return errInvalid("The key path element kind %q is reserved/read-only.", e.GetKind())
	}

	if err := validateElemID(e, forWrite); err != nil {
		return err
	}

	if elemHasID(e) {
		return nil
	}

	if !last {
		return errInvalid("Only the last element of a key path may be incomplete.")
	}

	if !allowIncomplete {
		return errInvalid("The key path element must have an id or name: %s.", e.GetKind())
	}

	return nil
}

func validateElemID(e *datastorepb.Key_PathElement, forWrite bool) error {
	switch id := e.GetIdType().(type) {
	case *datastorepb.Key_PathElement_Id:
		if id.Id < 0 {
			return errInvalid("The key path element id must be positive, got %d.", id.Id)
		}
	case *datastorepb.Key_PathElement_Name:
		if id.Name == "" {
			return errInvalid("The key path element name must not be empty.")
		}

		if len(id.Name) > maxNameBytes {
			return errInvalid("The key path element name is longer than %d bytes.", maxNameBytes)
		}

		if forWrite && isReserved(id.Name) {
			return errInvalid("The key path element name %q is reserved/read-only.", id.Name)
		}
	}

	return nil
}

// keyString is the canonical map key of a complete key within its partition.
func keyString(k *datastorepb.Key) string {
	var sb strings.Builder

	pid := k.GetPartitionId()
	sb.WriteString(pid.GetProjectId())
	sb.WriteByte(0)
	sb.WriteString(pid.GetDatabaseId())
	sb.WriteByte(0)
	sb.WriteString(pid.GetNamespaceId())

	for _, e := range k.GetPath() {
		sb.WriteByte(pathSep)
		sb.WriteString(e.GetKind())
		sb.WriteByte(kindSep)

		if n, ok := e.GetIdType().(*datastorepb.Key_PathElement_Name); ok {
			sb.WriteByte('n')
			sb.WriteString(n.Name)
		} else {
			sb.WriteByte('i')
			sb.WriteString(strconv.FormatInt(e.GetId(), 10))
		}
	}

	return sb.String()
}

func keyPartition(k *datastorepb.Key) partition {
	pid := k.GetPartitionId()

	return partition{project: pid.GetProjectId(), database: pid.GetDatabaseId(), namespace: pid.GetNamespaceId()}
}

func compareElems(a, b *datastorepb.Key_PathElement) int {
	if c := strings.Compare(a.GetKind(), b.GetKind()); c != 0 {
		return c
	}

	an, aName := a.GetIdType().(*datastorepb.Key_PathElement_Name)
	bn, bName := b.GetIdType().(*datastorepb.Key_PathElement_Name)

	switch {
	case aName && bName:
		return strings.Compare(an.Name, bn.Name)
	case aName:
		return 1
	case bName:
		return -1
	case a.GetId() < b.GetId():
		return -1
	case a.GetId() > b.GetId():
		return 1
	default:
		return 0
	}
}

// compareKeys orders keys by partition, then path element by element (numeric ids before names).
func compareKeys(a, b *datastorepb.Key) int {
	if c := strings.Compare(keyPartition(a).String(), keyPartition(b).String()); c != 0 {
		return c
	}

	pa, pb := a.GetPath(), b.GetPath()
	for i := 0; i < len(pa) && i < len(pb); i++ {
		if c := compareElems(pa[i], pb[i]); c != 0 {
			return c
		}
	}

	switch {
	case len(pa) < len(pb):
		return -1
	case len(pa) > len(pb):
		return 1
	default:
		return 0
	}
}

// isDescendantOrSelf reports whether key's path starts with ancestor's path in the same partition.
func isDescendantOrSelf(key, ancestor *datastorepb.Key) bool {
	if keyPartition(key) != keyPartition(ancestor) || len(key.GetPath()) < len(ancestor.GetPath()) {
		return false
	}

	for i, e := range ancestor.GetPath() {
		if compareElems(key.GetPath()[i], e) != 0 {
			return false
		}
	}

	return true
}

func keyDescription(k *datastorepb.Key) string {
	parts := make([]string, 0, len(k.GetPath()))

	for _, e := range k.GetPath() {
		if n, ok := e.GetIdType().(*datastorepb.Key_PathElement_Name); ok {
			parts = append(parts, e.GetKind()+", "+strconv.Quote(n.Name))
		} else {
			parts = append(parts, e.GetKind()+", "+strconv.FormatInt(e.GetId(), 10))
		}
	}

	return "Key(" + strings.Join(parts, ", ") + ")"
}

func clampInt32(n int) int32 {
	return int32(min(n, math.MaxInt32)) //nolint:gosec // clamped to the int32 range
}

// stampKey returns a copy of k whose missing project and database come from p; a missing namespace comes
// from p only when nsDefault is set (query filters), because stored keys carry their own namespace.
func stampKey(k *datastorepb.Key, p partition, nsDefault bool) *datastorepb.Key {
	out, _ := proto.Clone(k).(*datastorepb.Key)

	pid := out.GetPartitionId()
	if pid == nil {
		pid = &datastorepb.PartitionId{}
		out.PartitionId = pid
	}

	if pid.GetProjectId() == "" {
		pid.ProjectId = p.project
	}

	if pid.GetDatabaseId() == "" {
		pid.DatabaseId = p.database
	}

	if nsDefault && pid.GetNamespaceId() == "" {
		pid.NamespaceId = p.namespace
	}

	return out
}

// stampValueKeys rewrites every key value inside v in place.
func stampValueKeys(v *datastorepb.Value, fn func(*datastorepb.Key) *datastorepb.Key) {
	switch t := v.GetValueType().(type) {
	case *datastorepb.Value_KeyValue:
		t.KeyValue = fn(t.KeyValue)
	case *datastorepb.Value_ArrayValue:
		for _, el := range t.ArrayValue.GetValues() {
			stampValueKeys(el, fn)
		}
	case *datastorepb.Value_EntityValue:
		for _, pv := range t.EntityValue.GetProperties() {
			stampValueKeys(pv, fn)
		}
	}
}
