package iceberg

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Snapshot summary operations.
const (
	OpAppend    = "append"
	OpDelete    = "delete"
	OpOverwrite = "overwrite"
	OpReplace   = "replace"
)

// Change is one snapshot's worth of file changes.
type Change struct {
	RemoveData    map[string]bool
	RemoveDeletes map[string]bool
	Operation     string
	AddData       []DataFile
	AddDeletes    []DataFile
}

// ManifestReader loads the entries of the manifest stored at path.
type ManifestReader func(path string) ([]ManifestEntry, error)

type commitTally struct {
	addedFiles, addedRows, addedSize       int64
	addedDeleteFiles, addedDeletes         int64
	removedFiles, removedRows, removedSize int64
	removedDeleteFiles, removedDeletes     int64
}

// PlanCommit prepares a snapshot applying ch to the table's current manifests
// prev. Manifests holding removed files are rewritten (removed entries become
// DELETED, the rest EXISTING); read is only needed when ch removes files.
func (m *TableMetadata) PlanCommit(
	currentLocation string, prev []ManifestFile, read ManifestReader, ch Change, now time.Time,
) (*AppendPlan, error) {
	spec := m.DefaultSpec()
	snapID := NewSnapshotID()
	seq := m.LastSequenceNumber + 1
	tsMS := now.UnixMilli()

	var parent *int64

	var prevSummary map[string]string

	if cur, ok := m.CurrentSnapshot(); ok {
		id := cur.SnapshotID
		parent, prevSummary = &id, cur.Summary
		tsMS = max(tsMS, cur.TimestampMS)
	}

	base := strings.TrimSuffix(m.Location, "/") + "/metadata/"
	pc := &planCtx{m: m, base: base, snapID: snapID, seq: seq, read: read, ch: ch}

	var list []ManifestFile

	if len(ch.AddData) > 0 {
		mf, body, err := pc.addedManifest(spec, ContentData, ch.AddData)
		if err != nil {
			return nil, err
		}

		list = append(list, mf)
		pc.objects = append(pc.objects, Object{Location: mf.Path, Body: body})
	}

	if len(ch.AddDeletes) > 0 {
		mf, body, err := pc.addedManifest(spec, ContentPositionDeletes, ch.AddDeletes)
		if err != nil {
			return nil, err
		}

		list = append(list, mf)
		pc.objects = append(pc.objects, Object{Location: mf.Path, Body: body})
	}

	for _, pm := range prev {
		kept, err := pc.carry(pm)
		if err != nil {
			return nil, err
		}

		list = append(list, kept...)
	}

	listBody, err := EncodeManifestList(snapID, parent, seq, list)
	if err != nil {
		return nil, err
	}

	listLoc := fmt.Sprintf("%ssnap-%d-1-%s.avro", base, snapID, uuid.NewString())
	schemaID := m.CurrentSchemaID
	snap := Snapshot{
		SnapshotID: snapID, ParentSnapshotID: parent, SequenceNumber: seq, TimestampMS: tsMS,
		ManifestList: listLoc, SchemaID: &schemaID, Summary: pc.summary(prevSummary),
	}

	next := m.AppendSnapshot(snap, currentLocation, now)
	metaLoc := next.NextMetadataLocation()

	body, err := marshalMetadata(next)
	if err != nil {
		return nil, err
	}

	pc.objects = append(pc.objects, Object{Location: listLoc, Body: listBody}, Object{Location: metaLoc, Body: body})

	return &AppendPlan{Metadata: next, MetadataLocation: metaLoc, Objects: pc.objects}, nil
}

type planCtx struct {
	read    ManifestReader
	m       *TableMetadata
	ch      Change
	base    string
	objects []Object
	tally   commitTally
	snapID  int64
	seq     int64
}

func (pc *planCtx) addedManifest(spec PartitionSpec, content int, files []DataFile) (ManifestFile, []byte, error) {
	entries := make([]ManifestEntry, 0, len(files))

	var rows int64

	for _, f := range files {
		f.Content = content
		entries = append(entries, ManifestEntry{
			Status: StatusAdded, SnapshotID: &pc.snapID, SequenceNumber: &pc.seq, DataFile: f,
		})

		rows += f.RecordCount

		if content == ContentData {
			pc.tally.addedFiles++
			pc.tally.addedRows += f.RecordCount
			pc.tally.addedSize += f.FileSize
		} else {
			pc.tally.addedDeleteFiles++
			pc.tally.addedDeletes += f.RecordCount
			pc.tally.addedSize += f.FileSize
		}
	}

	body, err := EncodeManifest(pc.m, spec, content, entries)
	if err != nil {
		return ManifestFile{}, nil, err
	}

	return ManifestFile{
		Path: pc.base + uuid.NewString() + "-m0.avro", Length: int64(len(body)), SpecID: spec.SpecID,
		Content: content, SequenceNumber: pc.seq, MinSequenceNumber: pc.seq, AddedSnapshotID: pc.snapID,
		AddedFiles: len(files), AddedRows: rows,
	}, body, nil
}

func (pc *planCtx) removals(content int) map[string]bool {
	if content == ContentData {
		return pc.ch.RemoveData
	}

	return pc.ch.RemoveDeletes
}

// carry keeps a manifest of the previous snapshot, rewriting it when it lists a removed file.
func (pc *planCtx) carry(pm ManifestFile) ([]ManifestFile, error) {
	rm := pc.removals(pm.Content)
	if len(rm) == 0 || pc.read == nil {
		return []ManifestFile{pm}, nil
	}

	entries, err := pc.read(pm.Path)
	if err != nil {
		return nil, err
	}

	hit := false

	for _, e := range entries {
		if e.Status != StatusDeleted && rm[e.DataFile.Path] {
			hit = true
		}
	}

	if !hit {
		return []ManifestFile{pm}, nil
	}

	spec, ok := pc.m.SpecByID(pm.SpecID)
	if !ok {
		return nil, fmt.Errorf("%w: manifest %s uses unknown partition spec %d", ErrInvalidManifest, pm.Path, pm.SpecID)
	}

	out, mf := make([]ManifestEntry, 0, len(entries)), pm
	mf.Path, mf.SequenceNumber, mf.AddedSnapshotID = pc.base+uuid.NewString()+"-m0.avro", pc.seq, pc.snapID
	mf.AddedFiles, mf.AddedRows, mf.ExistingFiles, mf.ExistingRows, mf.DeletedFiles, mf.DeletedRows = 0, 0, 0, 0, 0, 0
	mf.MinSequenceNumber = pc.seq

	for _, e := range entries {
		if e.Status == StatusDeleted {
			continue
		}

		if e.SequenceNumber == nil {
			s := pm.SequenceNumber
			e.SequenceNumber = &s
		}

		if e.SnapshotID == nil {
			s := pm.AddedSnapshotID
			e.SnapshotID = &s
		}

		if rm[e.DataFile.Path] {
			e.Status, e.SnapshotID = StatusDeleted, &pc.snapID
			mf.DeletedFiles++
			mf.DeletedRows += e.DataFile.RecordCount
			pc.tallyRemoved(e.DataFile)
		} else {
			e.Status = StatusExisting
			mf.ExistingFiles++
			mf.ExistingRows += e.DataFile.RecordCount
			mf.MinSequenceNumber = min(mf.MinSequenceNumber, *e.SequenceNumber)
		}

		out = append(out, e)
	}

	body, err := EncodeManifest(pc.m, spec, pm.Content, out)
	if err != nil {
		return nil, err
	}

	mf.Length = int64(len(body))
	pc.objects = append(pc.objects, Object{Location: mf.Path, Body: body})

	return []ManifestFile{mf}, nil
}

func (pc *planCtx) tallyRemoved(f DataFile) {
	if f.Content == ContentData {
		pc.tally.removedFiles++
		pc.tally.removedRows += f.RecordCount
	} else {
		pc.tally.removedDeleteFiles++
		pc.tally.removedDeletes += f.RecordCount
	}

	pc.tally.removedSize += f.FileSize
}

func (pc *planCtx) summary(prev map[string]string) map[string]string {
	t := pc.tally
	itoa := func(n int64) string { return strconv.FormatInt(n, 10) }
	s := map[string]string{summaryOp: pc.ch.Operation}

	set := func(k string, n int64) {
		if n > 0 {
			s[k] = itoa(n)
		}
	}

	set(summaryAdded, t.addedFiles)
	set(summaryRecords, t.addedRows)
	set("added-delete-files", t.addedDeleteFiles)
	set("added-position-delete-files", t.addedDeleteFiles)
	set("added-position-deletes", t.addedDeletes)
	set("deleted-data-files", t.removedFiles)
	set("deleted-records", t.removedRows)
	set("removed-delete-files", t.removedDeleteFiles)
	set("removed-position-delete-files", t.removedDeleteFiles)
	set("removed-position-deletes", t.removedDeletes)
	set(summarySize, t.addedSize)

	s[totalRecords] = itoa(summaryInt(prev, totalRecords) + t.addedRows - t.removedRows)
	s[totalFiles] = itoa(summaryInt(prev, totalFiles) + t.addedFiles - t.removedFiles)
	s[totalSize] = itoa(max(0, summaryInt(prev, totalSize)+t.addedSize-t.removedSize))
	s[totalDeletes] = itoa(summaryInt(prev, totalDeletes) + t.addedDeleteFiles - t.removedDeleteFiles)
	s[totalPosDeletes] = itoa(summaryInt(prev, totalPosDeletes) + t.addedDeletes - t.removedDeletes)
	s[totalEqDeletes] = itoa(summaryInt(prev, totalEqDeletes))

	return s
}

// clone copies the mutable members so a metadata update never aliases the source.
func (m *TableMetadata) clone() *TableMetadata {
	next := *m
	next.Properties = maps.Clone(m.Properties)
	next.Refs = maps.Clone(m.Refs)
	next.Schemas = slices.Clone(m.Schemas)
	next.PartitionSpecs = slices.Clone(m.PartitionSpecs)
	next.Snapshots = slices.Clone(m.Snapshots)
	next.SnapshotLog = slices.Clone(m.SnapshotLog)
	next.MetadataLog = slices.Clone(m.MetadataLog)

	return &next
}

// PlanMetadataUpdate prepares a commit that changes only the metadata
// (schema, properties, retained snapshots): mutate edits a copy of m.
func (m *TableMetadata) PlanMetadataUpdate(
	currentLocation string, mutate func(next *TableMetadata) error, now time.Time,
) (*AppendPlan, error) {
	next := m.clone()
	if err := mutate(next); err != nil {
		return nil, err
	}

	if currentLocation != "" {
		next.MetadataLog = append(next.MetadataLog,
			MetadataLogEntry{MetadataFile: currentLocation, TimestampMS: m.LastUpdatedMS})
	}

	next.LastUpdatedMS = max(now.UnixMilli(), m.LastUpdatedMS)
	loc := next.NextMetadataLocation()

	body, err := marshalMetadata(next)
	if err != nil {
		return nil, err
	}

	return &AppendPlan{Metadata: next, MetadataLocation: loc, Objects: []Object{{Location: loc, Body: body}}}, nil
}
