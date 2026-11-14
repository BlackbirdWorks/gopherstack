package iceberg

import (
	"encoding/binary"
	"strconv"
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
	return m.PlanCommit(currentLocation, prev, nil, Change{Operation: OpAppend, AddData: files}, now)
}

func marshalMetadata(m *TableMetadata) ([]byte, error) {
	return m.MarshalJSON()
}

// EncodeMetadata renders the metadata.json document.
func EncodeMetadata(m *TableMetadata) ([]byte, error) { return marshalMetadata(m) }
