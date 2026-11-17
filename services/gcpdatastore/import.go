package gcpdatastore

import (
	"context"
	"encoding/json/v2"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxMetaBytes = 4 << 20

func subsetOf(want, have []string) bool {
	if len(have) == 0 {
		return true
	}

	return !slices.ContainsFunc(want, func(s string) bool { return !slices.Contains(have, s) })
}

func (m *exportMeta) validate(in *adminpb.ImportEntitiesRequest, base string) error {
	if m.Format != exportFormat {
		if strings.HasPrefix(m.Format, exportFormatBase) {
			return errPrecondition(
				"The export format %q is not supported; this emulator reads %q.",
				m.Format,
				exportFormat,
			)
		}

		return errPrecondition("The input URL does not name an overall export metadata file written by ExportEntities.")
	}

	f := in.GetEntityFilter()
	if !subsetOf(f.GetKinds(), m.Kinds) || !subsetOf(f.GetNamespaceIds(), m.Namespaces) {
		return errPrecondition("The entity filter must be a subset of the filter used to create the export.")
	}

	for _, g := range m.Groups {
		if base == "" || !strings.HasPrefix(g.DataFile, base) || !validObjectPath(g.DataFile) {
			return errPrecondition("The export metadata refers to a file outside the export.")
		}
	}

	return nil
}

func (b *InMemoryBackend) loadExport(
	store ObjectStore, in *adminpb.ImportEntitiesRequest,
) ([]*datastorepb.Entity, int64, error) {
	bucket, path, err := parseGSURL(in.GetInputUrl())
	if err != nil {
		return nil, 0, err
	}

	if !strings.HasSuffix(path, overallSuffix) {
		return nil, 0, errInvalid("The input URL must name the %s file written by ExportEntities.", overallSuffix)
	}

	raw, err := store.ReadObject(bucket, path)
	if err != nil {
		return nil, 0, errNotFound("The export metadata file gs://%s/%s was not found.", bucket, path)
	}

	if len(raw) > maxMetaBytes {
		return nil, 0, errPrecondition("The export metadata file is larger than %d bytes.", maxMetaBytes)
	}

	meta := &exportMeta{}
	if err = json.Unmarshal(raw, meta); err != nil {
		return nil, 0, errPrecondition("The export metadata file is not valid.")
	}

	base := path[:strings.LastIndex(path, "/")+1]
	if err = meta.validate(in, base); err != nil {
		return nil, 0, err
	}

	var (
		out   []*datastorepb.Entity
		total int64
	)

	for _, g := range meta.Groups {
		if len(in.GetEntityFilter().GetKinds()) > 0 && !slices.Contains(in.GetEntityFilter().GetKinds(), g.Kind) {
			continue
		}

		data, rerr := store.ReadObject(bucket, g.DataFile)
		if rerr != nil {
			return nil, 0, errNotFound("The export data file gs://%s/%s was not found.", bucket, g.DataFile)
		}

		if total += int64(len(data)); total > maxExportBytes {
			return nil, 0, errPrecondition("The export is larger than %d bytes.", maxExportBytes)
		}

		ents, derr := decodeEntities(data, maxExportEntities-len(out))
		if derr != nil {
			return nil, 0, derr
		}

		if g.Entities != len(ents) {
			return nil, 0, errPrecondition("The export data file gs://%s/%s is corrupt.", bucket, g.DataFile)
		}

		if slices.ContainsFunc(ents, func(e *datastorepb.Entity) bool { return !isComplete(e.GetKey()) }) {
			return nil, 0, errPrecondition(
				"The export data file gs://%s/%s holds an entity with an incomplete key.",
				bucket,
				g.DataFile,
			)
		}

		out = append(out, ents...)
	}

	return out, total, nil
}

func retarget(k *datastorepb.Key, req partition) *datastorepb.Key {
	if k == nil {
		return k
	}

	out := stampKey(k, req, false)
	out.PartitionId.ProjectId, out.PartitionId.DatabaseId = req.project, req.database

	return out
}

// ImportEntities loads a previous export into the project in one atomic commit.
func (b *InMemoryBackend) ImportEntities(
	_ context.Context, in *adminpb.ImportEntitiesRequest,
) (*longrunningpb.Operation, error) {
	if in.GetProjectId() == "" {
		return nil, errInvalid("Missing required project ID.")
	}

	if err := validateFilter(in.GetEntityFilter()); err != nil {
		return nil, err
	}

	store := b.objectStore()
	if store == nil {
		return nil, errPrecondition("Cloud Storage is not available to import from.")
	}

	started := timestamppb.New(b.wallStamp())

	ents, size, err := b.loadExport(store, in)
	if err != nil {
		return nil, err
	}

	req := partition{project: in.GetProjectId()}
	f := in.GetEntityFilter()
	muts := make([]*datastorepb.Mutation, 0, len(ents))

	for _, e := range ents {
		if !filterMatches(f.GetKinds(), f.GetNamespaceIds(), e.GetKey()) {
			continue
		}

		e.Key = retarget(e.GetKey(), req)

		for _, pv := range e.GetProperties() {
			stampValueKeys(pv, func(k *datastorepb.Key) *datastorepb.Key { return retarget(k, req) })
		}

		muts = append(muts, &datastorepb.Mutation{Operation: &datastorepb.Mutation_Upsert{Upsert: e}})
	}

	return b.commitImport(in, req, muts, started, size)
}

func (b *InMemoryBackend) commitImport(
	in *adminpb.ImportEntitiesRequest, req partition, muts []*datastorepb.Mutation, started *timestamppb.Timestamp,
	size int64,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("ImportEntities")
	defer b.mu.Unlock()

	b.sweepLocked(b.wallLocked())

	c := &commitCtx{b: b, req: req, work: map[string]*wstate{}, strict: false}
	c.at = b.wallLocked()

	floor := b.lastCommit
	if b.lastTxnRead.After(floor) {
		floor = b.lastTxnRead
	}

	if !c.at.After(floor) {
		c.at = floor.Add(time.Microsecond)
	}

	c.version = c.at.UnixMicro()

	for _, m := range muts {
		if _, err := c.apply(m); err != nil {
			return nil, errPrecondition("The export holds an entity that cannot be imported: %v", err)
		}
	}

	if c.anyDirty() {
		b.lastCommit = c.at
		c.persist()
	}

	meta := &adminpb.ImportEntitiesMetadata{
		Common: &adminpb.CommonMetadata{
			StartTime: started, EndTime: timestamppb.New(b.wallLocked()),
			OperationType: adminpb.OperationType_IMPORT_ENTITIES, State: adminpb.CommonMetadata_SUCCESSFUL,
			Labels: in.GetLabels(),
		},
		ProgressEntities: &adminpb.Progress{WorkCompleted: int64(len(muts)), WorkEstimated: int64(len(muts))},
		ProgressBytes:    &adminpb.Progress{WorkCompleted: size, WorkEstimated: size},
		EntityFilter:     in.GetEntityFilter(), InputUrl: in.GetInputUrl(),
	}

	return b.putOpLocked(in.GetProjectId(), meta, &emptypb.Empty{})
}
