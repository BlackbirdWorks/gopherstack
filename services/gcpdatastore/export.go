package gcpdatastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const (
	overallSuffix     = ".overall_export_metadata"
	dataFileName      = "output-0"
	exportFormat      = "gopherstack-datastore-export-v1"
	exportFormatBase  = "gopherstack-datastore-export-"
	maxBucketName     = 222
	minBucketName     = 3
	maxObjectName     = 1024
	maxExportEntities = 200_000
	maxExportBytes    = 256 << 20
	maxExportEntity   = 1 << 20
	exportContentType = "application/octet-stream"
	defaultNsDir      = "default_namespace"
)

// ObjectStore is the Cloud Storage surface export and import use.
type ObjectStore interface {
	ListObjects(bucket, prefix string) ([]string, error)
	ReadObject(bucket, name string) ([]byte, error)
	WriteObject(bucket, name string, data []byte, contentType string) error
}

type exportGroup struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	DataFile  string `json:"dataFile"`
	Entities  int    `json:"entities"`
}

type exportMeta struct {
	Format     string        `json:"format"`
	Kinds      []string      `json:"kinds"`
	Namespaces []string      `json:"namespaces"`
	Groups     []exportGroup `json:"groups"`
}

// SetObjectStore wires the Cloud Storage emulator that export and import read and write.
func (b *InMemoryBackend) SetObjectStore(s ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	b.objects = s
}

func (b *InMemoryBackend) objectStore() ObjectStore {
	b.mu.RLock("objectStore")
	defer b.mu.RUnlock()

	return b.objects
}

func parseGSURL(raw string) (string, string, error) {
	rest, ok := strings.CutPrefix(raw, "gs://")
	if !ok || rest == "" {
		return "", "", errInvalid("The URL %q must be of the form gs://BUCKET_NAME[/NAMESPACE_PATH].", raw)
	}

	bucket, path, _ := strings.Cut(rest, "/")
	if !validBucketName(bucket) {
		return "", "", errInvalid("The URL %q has no valid bucket name.", raw)
	}

	path = strings.Trim(path, "/")
	if path != "" && !validObjectPath(path) {
		return "", "", errInvalid("The URL %q has an empty, relative or control-character path segment.", raw)
	}

	return bucket, path, nil
}

func validBucketName(b string) bool {
	if len(b) < minBucketName || len(b) > maxBucketName || strings.ContainsFunc(b, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.'
	}) {
		return false
	}

	return b[0] != '-' && b[0] != '_' && b[0] != '.' && b[len(b)-1] != '-' && b[len(b)-1] != '_' && b[len(b)-1] != '.'
}

func validObjectPath(p string) bool {
	if len(p) > maxObjectName || strings.ContainsFunc(p, func(r rune) bool { return r < ' ' || r == 0x7f }) {
		return false
	}

	return !slices.ContainsFunc(strings.Split(p, "/"), func(seg string) bool {
		return seg == "" || seg == "." || seg == ".."
	})
}

func validateFilter(f *adminpb.EntityFilter) error {
	seen := map[string]struct{}{}

	for _, ns := range f.GetNamespaceIds() {
		if _, dup := seen[ns]; dup {
			return errInvalid("Namespace %q is listed more than once in the entity filter.", ns)
		}

		seen[ns] = struct{}{}

		if err := validateNamespace(ns); err != nil {
			return err
		}
	}

	for _, k := range f.GetKinds() {
		if k == "" || len(k) > maxNameBytes {
			return errInvalid("Entity filter kinds must be 1 to %d bytes.", maxNameBytes)
		}
	}

	return nil
}

func filterMatches(kinds, nss []string, k *datastorepb.Key) bool {
	if len(kinds) > 0 && !slices.Contains(kinds, keyKind(k)) {
		return false
	}

	return len(nss) == 0 || slices.Contains(nss, k.GetPartitionId().GetNamespaceId())
}

func nsDir(ns string) string {
	if ns == "" {
		return defaultNsDir
	}

	return "namespace_" + url.PathEscape(ns)
}

func encodeEntities(ents []*datastorepb.Entity, budget int64) ([]byte, error) {
	var buf bytes.Buffer

	for _, e := range ents {
		raw, err := proto.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("marshal entity: %w", err)
		}

		if int64(buf.Len()+len(raw)) > budget {
			return nil, errPrecondition("The export would be larger than %d bytes.", maxExportBytes)
		}

		buf.Write(binary.AppendUvarint(nil, uint64(len(raw))))
		buf.Write(raw)
	}

	return buf.Bytes(), nil
}

func decodeEntities(data []byte, limit int) ([]*datastorepb.Entity, error) {
	var out []*datastorepb.Entity

	for len(data) > 0 {
		n, w := binary.Uvarint(data)
		if w <= 0 || n > maxExportEntity {
			return nil, errPrecondition("The export data file is corrupt.")
		}

		size := int(n)
		if size > len(data)-w {
			return nil, errPrecondition("The export data file is corrupt.")
		}

		if len(out) >= limit {
			return nil, errPrecondition("The export holds more than %d entities.", limit)
		}

		e := &datastorepb.Entity{}
		if err := proto.Unmarshal(data[w:w+size], e); err != nil {
			return nil, errPrecondition("The export data file is corrupt.")
		}

		out = append(out, e)
		data = data[w+size:]
	}

	return out, nil
}

// snapshotEntities copies the current entities of project that match the filter, ordered by key.
func (b *InMemoryBackend) snapshotEntities(project string, f *adminpb.EntityFilter) ([]*datastorepb.Entity, error) {
	b.mu.RLock("ExportEntities")
	defer b.mu.RUnlock()

	var out []*datastorepb.Entity

	for _, rec := range b.ents {
		ver := rec.cur()
		if ver == nil || keyPartition(rec.key).project != project || keyPartition(rec.key).database != "" ||
			!filterMatches(f.GetKinds(), f.GetNamespaceIds(), rec.key) {
			continue
		}

		if len(out) >= maxExportEntities {
			return nil, errPrecondition("The export would hold more than %d entities.", maxExportEntities)
		}

		cp, ok := proto.Clone(ver.ent).(*datastorepb.Entity)
		if !ok {
			continue
		}

		out = append(out, cp)
	}

	slices.SortFunc(out, func(x, y *datastorepb.Entity) int { return compareKeys(x.GetKey(), y.GetKey()) })

	return out, nil
}

func (b *InMemoryBackend) nextExportStamp() string {
	b.mu.Lock("exportStamp")
	defer b.mu.Unlock()

	b.exportSeq++

	return b.wallLocked().Format("2006-01-02T15:04:05") + "_" + strconv.FormatInt(10000+b.exportSeq, 10)
}

// ExportEntities writes the matching entities to the Cloud Storage emulator and returns a finished operation.
func (b *InMemoryBackend) ExportEntities(
	_ context.Context, in *adminpb.ExportEntitiesRequest,
) (*longrunningpb.Operation, error) {
	if in.GetProjectId() == "" {
		return nil, errInvalid("Missing required project ID.")
	}

	if err := validateFilter(in.GetEntityFilter()); err != nil {
		return nil, err
	}

	bucket, path, err := parseGSURL(in.GetOutputUrlPrefix())
	if err != nil {
		return nil, err
	}

	store := b.objectStore()
	if store == nil {
		return nil, errPrecondition("Cloud Storage is not available to export to.")
	}

	started := timestamppb.New(b.wallStamp())

	ents, err := b.snapshotEntities(in.GetProjectId(), in.GetEntityFilter())
	if err != nil {
		return nil, err
	}

	stamp := b.nextExportStamp()
	base := stamp + "/"

	if path != "" {
		base = path + "/" + base
	}

	meta, total, err := writeGroups(store, bucket, base, in.GetEntityFilter(), ents)
	if err != nil {
		return nil, err
	}

	raw, err := json.Marshal(meta)
	if err != nil {
		return nil, errInvalid("encode export metadata: %v", err)
	}

	if err = store.WriteObject(bucket, base+stamp+overallSuffix, raw, exportContentType); err != nil {
		return nil, errPrecondition("Could not write the export to gs://%s.", bucket)
	}

	return b.exportOp(in, started, "gs://"+bucket+"/"+base+stamp+overallSuffix, len(ents), total)
}

func (b *InMemoryBackend) wallStamp() time.Time {
	b.mu.RLock("wall")
	defer b.mu.RUnlock()

	return b.wallLocked()
}

func writeGroups(
	store ObjectStore, bucket, base string, f *adminpb.EntityFilter, ents []*datastorepb.Entity,
) (*exportMeta, int64, error) {
	type gk struct{ kind, ns string }

	groups := map[gk][]*datastorepb.Entity{}

	var order []gk

	for _, e := range ents {
		g := gk{kind: keyKind(e.GetKey()), ns: e.GetKey().GetPartitionId().GetNamespaceId()}
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}

		groups[g] = append(groups[g], e)
	}

	meta := &exportMeta{
		Format: exportFormat, Kinds: slices.Clone(f.GetKinds()), Namespaces: slices.Clone(f.GetNamespaceIds()),
	}

	var total int64

	for _, g := range order {
		data, err := encodeEntities(groups[g], maxExportBytes-total)
		if err != nil {
			return nil, 0, err
		}

		total += int64(len(data))

		file := base + nsDir(g.ns) + "/kind_" + url.PathEscape(g.kind) + "/" + dataFileName
		if err = store.WriteObject(bucket, file, data, exportContentType); err != nil {
			return nil, 0, errPrecondition("Could not write the export to gs://%s.", bucket)
		}

		meta.Groups = append(meta.Groups, exportGroup{
			Kind: g.kind, Namespace: g.ns, DataFile: file, Entities: len(groups[g]),
		})
	}

	return meta, total, nil
}

func (b *InMemoryBackend) exportOp(
	in *adminpb.ExportEntitiesRequest, started *timestamppb.Timestamp, outURL string, n int, size int64,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("exportOp")
	defer b.mu.Unlock()

	meta := &adminpb.ExportEntitiesMetadata{
		Common: &adminpb.CommonMetadata{
			StartTime: started, EndTime: timestamppb.New(b.wallLocked()),
			OperationType: adminpb.OperationType_EXPORT_ENTITIES, State: adminpb.CommonMetadata_SUCCESSFUL,
			Labels: in.GetLabels(),
		},
		ProgressEntities: &adminpb.Progress{WorkCompleted: int64(n), WorkEstimated: int64(n)},
		ProgressBytes:    &adminpb.Progress{WorkCompleted: size, WorkEstimated: size},
		EntityFilter:     in.GetEntityFilter(), OutputUrlPrefix: in.GetOutputUrlPrefix(),
	}

	return b.putOpLocked(in.GetProjectId(), meta, &adminpb.ExportEntitiesResponse{OutputUrl: outURL})
}

func (b *InMemoryBackend) putOpLocked(project string, meta, resp proto.Message) (*longrunningpb.Operation, error) {
	b.exportSeq++

	name := "projects/" + project + "/operations/" + strconv.FormatInt(b.wallLocked().UnixNano(), 36) +
		strconv.FormatInt(b.exportSeq, 36)

	o, err := gcplro.Done(name, meta, resp)
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	return b.ops.Put(o), nil
}
