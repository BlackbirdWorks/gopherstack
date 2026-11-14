package azuresearch

import (
	"errors"
	"strings"
	"time"
)

// ErrSourceUnavailable is returned when no in-process data source emulator is wired.
var ErrSourceUnavailable = errors.New("azuresearch: data source emulator is not available")

// BlobEntry describes one blob of the in-process Blob Storage emulator.
type BlobEntry struct {
	LastModified time.Time
	Metadata     map[string]string
	Name         string
	ContentType  string
	ETag         string
	MD5          string
	Size         int64
}

// BlobSource reads blobs from the in-process Blob Storage emulator.
type BlobSource interface {
	ListBlobs(container string) ([]BlobEntry, error)
	ReadBlob(container, name string) ([]byte, error)
}

// CosmosDoc is one document of the in-process Cosmos DB emulator.
type CosmosDoc struct {
	Timestamp time.Time
	Body      map[string]any
	ID        string
	ETag      string
}

// CosmosSource reads documents from the in-process Cosmos DB emulator.
type CosmosSource interface {
	ListDocuments(database, container string) ([]CosmosDoc, error)
}

// TableEntity is one entity of the in-process Table Storage emulator.
type TableEntity struct {
	Timestamp    time.Time
	Properties   map[string]any
	PartitionKey string
	RowKey       string
	ETag         string
}

// TableSource reads entities from the in-process Table Storage emulator; filter is an OData $filter or empty.
type TableSource interface {
	ListEntities(table, filter string) ([]TableEntity, error)
}

type noTables struct{}

func (noTables) ListEntities(string, string) ([]TableEntity, error) { return nil, ErrSourceUnavailable }

type noBlobs struct{}

func (noBlobs) ListBlobs(string) ([]BlobEntry, error)   { return nil, ErrSourceUnavailable }
func (noBlobs) ReadBlob(string, string) ([]byte, error) { return nil, ErrSourceUnavailable }

type noCosmos struct{}

func (noCosmos) ListDocuments(string, string) ([]CosmosDoc, error) { return nil, ErrSourceUnavailable }

// connValue extracts key from a "k=v;k=v" connection string.
func connValue(conn, key string) string {
	for part := range strings.SplitSeq(conn, ";") {
		k, v, ok := strings.Cut(part, "=")
		if ok && strings.EqualFold(strings.TrimSpace(k), key) {
			return strings.TrimSpace(v)
		}
	}

	return ""
}
