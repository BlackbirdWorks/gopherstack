package app

import (
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type logSinkPublisher struct {
	b *gcppubsubbackend.InMemoryBackend
}

var errLogSinkRowsRejected = errors.New("rejected rows")

func (p logSinkPublisher) Publish(topic string, data []byte, attrs map[string]string) error {
	_, err := p.b.Publish(&pubsubpb.PublishRequest{
		Topic:    topic,
		Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
	})
	if err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}

	return nil
}

type logSinkObjects struct{ gcs gcsObjectStore }

func (s logSinkObjects) ReadObject(bucket, name string) ([]byte, error) {
	data, err := s.gcs.ReadObject(bucket, name)
	if errors.Is(err, gcpbigquerybackend.ErrObjectNotFound) {
		return nil, fmt.Errorf("%w: %s/%s", gcploggingbackend.ErrObjectNotFound, bucket, name)
	}

	return data, err
}

func (s logSinkObjects) WriteObject(bucket, name string, data []byte, contentType string) error {
	return s.gcs.WriteObject(bucket, name, data, contentType)
}

type logSinkTables struct {
	b *gcpbigquerybackend.InMemoryBackend
}

func (t logSinkTables) EnsureTable(project, dataset, table string, schema []map[string]any) error {
	if _, err := t.b.GetTable(project, dataset, table); err == nil {
		return nil
	}

	fields := make([]any, len(schema))
	for i, f := range schema {
		fields[i] = f
	}

	_, err := t.b.CreateTable(project, dataset, map[string]any{
		"tableReference": map[string]any{"projectId": project, "datasetId": dataset, "tableId": table},
		"schema":         map[string]any{"fields": fields},
	})
	if err != nil {
		return fmt.Errorf("create table %s.%s: %w", dataset, table, err)
	}

	return nil
}

func (t logSinkTables) InsertRows(project, dataset, table string, rows []map[string]any) error {
	req := gcpbigquerybackend.InsertRequest{Rows: make([]gcpbigquerybackend.InsertRow, len(rows))}
	for i, r := range rows {
		req.Rows[i] = gcpbigquerybackend.InsertRow{JSON: r}
	}

	res, err := t.b.InsertAll(project, dataset, table, req)
	if err != nil {
		return fmt.Errorf("insert into %s.%s: %w", dataset, table, err)
	}

	if errs, bad := res["insertErrors"]; bad {
		return fmt.Errorf("insert into %s.%s: %w: %v", dataset, table, errLogSinkRowsRejected, errs)
	}

	return nil
}

// wireGCPLogging connects log sinks to the Pub/Sub, Cloud Storage and BigQuery emulators.
func wireGCPLogging(byName map[string]service.Registerable) {
	logH, ok := byName["GCPLogging"].(*gcploggingbackend.Handler)
	if !ok {
		return
	}

	if ps, isPS := byName["GCPPubSub"].(*gcppubsubbackend.Handler); isPS {
		logH.Backend.SetPublisher(logSinkPublisher{b: ps.Backend})
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		logH.Backend.SetObjectStore(logSinkObjects{gcs: gcsObjectStore{b: gcs.Backend}})
	}

	if bq, isBQ := byName["GCPBigQuery"].(*gcpbigquerybackend.Handler); isBQ {
		logH.Backend.SetTableStore(logSinkTables{b: bq.Backend})
	}

	wireGCPLogCapture(byName)
}
