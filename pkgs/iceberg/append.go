package iceberg

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Object is a file to store alongside a table commit.
type Object struct {
	Location string
	Body     []byte
}

// AppendPlan is a prepared append commit: the metadata, manifest and manifest
// list objects to store, and the metadata location to commit to the catalog.
type AppendPlan struct {
	Metadata         *TableMetadata
	MetadataLocation string
	Objects          []Object
}

const (
	maxInt63        = 0x7fffffffffffffff
	summaryOp       = "operation"
	summaryAdded    = "added-data-files"
	summaryRecords  = "added-records"
	summarySize     = "added-files-size"
	totalRecords    = "total-records"
	totalFiles      = "total-data-files"
	totalSize       = "total-files-size"
	totalDeletes    = "total-delete-files"
	totalPosDeletes = "total-position-deletes"
	totalEqDeletes  = "total-equality-deletes"
)

func summaryInt(s map[string]string, k string) int64 {
	n, _ := strconv.ParseInt(s[k], 10, 64)

	return n
}

// NewSnapshotID returns a random positive snapshot id.
func NewSnapshotID() int64 {
	u := uuid.New()

	return int64(binary.BigEndian.Uint64(u[:8]) & maxInt63)
}

// PlanAppend prepares a commit appending files to the table. currentLocation
// is the metadata file m was read from; prev are the current snapshot's manifests.
func (m *TableMetadata) PlanAppend(
	currentLocation string, prev []ManifestFile, files []DataFile, now time.Time,
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

	entries := make([]ManifestEntry, 0, len(files))

	var rows, size int64

	for _, f := range files {
		f.Content = ContentData
		entries = append(entries, ManifestEntry{
			Status: StatusAdded, SnapshotID: &snapID, SequenceNumber: &seq, DataFile: f,
		})
		rows += f.RecordCount
		size += f.FileSize
	}

	manifestBody, err := EncodeManifest(m, spec, ContentData, entries)
	if err != nil {
		return nil, err
	}

	manifestLoc := base + uuid.NewString() + "-m0.avro"
	list := make([]ManifestFile, 0, 1+len(prev))
	list = append(list, ManifestFile{
		Path: manifestLoc, Length: int64(len(manifestBody)), SpecID: spec.SpecID, Content: ContentData,
		SequenceNumber: seq, MinSequenceNumber: seq, AddedSnapshotID: snapID,
		AddedFiles: len(files), AddedRows: rows,
	})
	list = append(list, prev...)

	listBody, err := EncodeManifestList(snapID, parent, seq, list)
	if err != nil {
		return nil, err
	}

	listLoc := fmt.Sprintf("%ssnap-%d-1-%s.avro", base, snapID, uuid.NewString())
	schemaID := m.CurrentSchemaID
	snap := Snapshot{
		SnapshotID: snapID, ParentSnapshotID: parent, SequenceNumber: seq, TimestampMS: tsMS,
		ManifestList: listLoc, SchemaID: &schemaID,
		Summary: map[string]string{
			summaryOp: "append", summaryAdded: strconv.Itoa(len(files)),
			summaryRecords: strconv.FormatInt(rows, 10), summarySize: strconv.FormatInt(size, 10),
			totalRecords:    strconv.FormatInt(summaryInt(prevSummary, totalRecords)+rows, 10),
			totalFiles:      strconv.FormatInt(summaryInt(prevSummary, totalFiles)+int64(len(files)), 10),
			totalSize:       strconv.FormatInt(summaryInt(prevSummary, totalSize)+size, 10),
			totalDeletes:    strconv.FormatInt(summaryInt(prevSummary, totalDeletes), 10),
			totalPosDeletes: strconv.FormatInt(summaryInt(prevSummary, totalPosDeletes), 10),
			totalEqDeletes:  strconv.FormatInt(summaryInt(prevSummary, totalEqDeletes), 10),
		},
	}

	next := m.AppendSnapshot(snap, currentLocation, now)
	metaLoc := next.NextMetadataLocation()

	body, err := marshalMetadata(next)
	if err != nil {
		return nil, err
	}

	return &AppendPlan{
		Metadata: next, MetadataLocation: metaLoc,
		Objects: []Object{
			{Location: manifestLoc, Body: manifestBody},
			{Location: listLoc, Body: listBody},
			{Location: metaLoc, Body: body},
		},
	}, nil
}

func marshalMetadata(m *TableMetadata) ([]byte, error) {
	return m.MarshalJSON()
}

// EncodeMetadata renders the metadata.json document.
func EncodeMetadata(m *TableMetadata) ([]byte, error) { return marshalMetadata(m) }
