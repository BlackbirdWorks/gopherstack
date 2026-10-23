package gcplogging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ErrObjectNotFound is returned by an ObjectStore when a bucket or object does not exist.
var ErrObjectNotFound = errors.New("object not found")

var (
	errNoPublisher = errors.New("no Pub/Sub emulator wired")
	errNoObjects   = errors.New("no Cloud Storage emulator wired")
	errNoTables    = errors.New("no BigQuery emulator wired")
	errBadDest     = errors.New("unsupported or malformed destination")
)

// Publisher publishes one message to a Pub/Sub topic (projects/P/topics/T).
type Publisher interface {
	Publish(topic string, data []byte, attrs map[string]string) error
}

// ObjectStore is the slice of Cloud Storage that sink export needs.
type ObjectStore interface {
	// ReadObject returns the object content or an error wrapping ErrObjectNotFound.
	ReadObject(bucket, name string) ([]byte, error)
	WriteObject(bucket, name string, data []byte, contentType string) error
}

// TableStore is the slice of BigQuery that sink export needs.
type TableStore interface {
	// EnsureTable creates the table with the schema when it does not exist.
	EnsureTable(project, dataset, table string, schema []map[string]any) error
	InsertRows(project, dataset, table string, rows []map[string]any) error
}

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]+`)

// export snapshots the wired destinations so deliveries never touch the lock.
type exporters struct {
	pub    Publisher
	objs   ObjectStore
	tables TableStore
}

func (b *InMemoryBackend) exporters() exporters {
	b.mu.RLock("exporters")
	defer b.mu.RUnlock()

	return exporters{pub: b.pub, objs: b.objs, tables: b.tables}
}

type exportBatch struct {
	objects map[objectRef][]byte
	rows    map[tableRef][]map[string]any
}

type objectRef struct{ bucket, name string }

type tableRef struct{ project, dataset, table string }

func (b *InMemoryBackend) deliver(ctx context.Context, ds []delivery) {
	if len(ds) == 0 {
		return
	}

	ex := b.exporters()
	log := logger.Load(ctx)
	batch := exportBatch{objects: map[objectRef][]byte{}, rows: map[tableRef][]map[string]any{}}

	for _, d := range ds {
		if err := ex.route(d, &batch); err != nil {
			log.WarnContext(ctx, "gcplogging: sink export failed", "sink", d.sink.GetName(), "error", err)
		}
	}

	b.flush(ctx, ex, &batch)
}

func (x exporters) route(d delivery, batch *exportBatch) error {
	dest := d.sink.GetDestination()

	switch {
	case strings.HasPrefix(dest, destPubSub):
		return x.publish(strings.TrimPrefix(dest, destPubSub), d.entry)
	case strings.HasPrefix(dest, destStorage):
		return x.queueObject(strings.TrimPrefix(dest, destStorage), d.entry, batch)
	case strings.HasPrefix(dest, destBigQuery):
		return x.queueRow(strings.TrimPrefix(dest, destBigQuery), d, batch)
	}

	return fmt.Errorf("%w: %q", errBadDest, dest)
}

func (x exporters) publish(topic string, e *loggingpb.LogEntry) error {
	if x.pub == nil {
		return errNoPublisher
	}

	data, err := protojson.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	attrs := map[string]string{
		"logging.googleapis.com/timestamp": e.GetTimestamp().AsTime().UTC().Format(time.RFC3339Nano),
	}

	return x.pub.Publish(topic, data, attrs)
}

// objectNameFor names the hourly batch file the real export writes for an entry.
func objectNameFor(e *loggingpb.LogEntry) string {
	ts := e.GetTimestamp().AsTime().UTC()

	return fmt.Sprintf(
		"%s/%s/%02d:00:00_%02d:59:59_S0.json",
		logIDOf(e.GetLogName()),
		ts.Format("2006/01/02"),
		ts.Hour(),
		ts.Hour(),
	)
}

func (x exporters) queueObject(bucket string, e *loggingpb.LogEntry, batch *exportBatch) error {
	if x.objs == nil {
		return errNoObjects
	}

	line, err := protojson.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	ref := objectRef{bucket: bucket, name: objectNameFor(e)}
	batch.objects[ref] = append(append(batch.objects[ref], line...), '\n')

	return nil
}

func (x exporters) queueRow(dest string, d delivery, batch *exportBatch) error {
	if x.tables == nil {
		return errNoTables
	}

	project, dataset, ok := parseDataset(dest)
	if !ok {
		return fmt.Errorf("%w: %q", errBadDest, dest)
	}

	row, err := bqRow(d.entry)
	if err != nil {
		return err
	}

	ref := tableRef{project: project, dataset: dataset, table: bqTableName(d.sink, d.entry)}
	batch.rows[ref] = append(batch.rows[ref], row)

	return nil
}

func parseDataset(dest string) (string, string, bool) {
	parts := strings.Split(dest, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "datasets" {
		return "", "", false
	}

	return parts[1], parts[3], parts[1] != "" && parts[3] != ""
}

// bqTableName shards by date unless the sink asked for partitioned tables.
func bqTableName(sink *loggingpb.LogSink, e *loggingpb.LogEntry) string {
	base := nonAlnum.ReplaceAllString(logIDOf(e.GetLogName()), "_")
	if sink.GetBigqueryOptions().GetUsePartitionedTables() {
		return base
	}

	return base + "_" + e.GetTimestamp().AsTime().UTC().Format("20060102")
}

func (b *InMemoryBackend) flush(ctx context.Context, x exporters, batch *exportBatch) {
	log := logger.Load(ctx)

	b.exportMu.Lock("flush")
	defer b.exportMu.Unlock()

	for ref, data := range batch.objects {
		if err := appendObject(x.objs, ref, data); err != nil {
			log.WarnContext(ctx, "gcplogging: storage export failed", "bucket", ref.bucket, "error", err)
		}
	}

	for ref, rows := range batch.rows {
		if err := x.tables.EnsureTable(ref.project, ref.dataset, ref.table, bqSchema()); err != nil {
			log.WarnContext(ctx, "gcplogging: bigquery export failed", "table", ref.table, "error", err)

			continue
		}

		if err := x.tables.InsertRows(ref.project, ref.dataset, ref.table, rows); err != nil {
			log.WarnContext(ctx, "gcplogging: bigquery export failed", "table", ref.table, "error", err)
		}
	}
}

func appendObject(store ObjectStore, ref objectRef, data []byte) error {
	existing, err := store.ReadObject(ref.bucket, ref.name)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return fmt.Errorf("read %s/%s: %w", ref.bucket, ref.name, err)
	}

	if errors.Is(err, ErrObjectNotFound) {
		existing = nil
	}

	return store.WriteObject(ref.bucket, ref.name, bytes.Join([][]byte{existing, data}, nil), "application/json")
}
