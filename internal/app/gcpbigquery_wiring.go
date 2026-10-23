package app

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// gcsObjectStore lets BigQuery load and extract jobs read and write the in-process GCS emulator.
type gcsObjectStore struct {
	b *gcpstoragebackend.InMemoryBackend
}

func notFoundStatus(err error) bool {
	var ae *gcpstoragebackend.APIError

	return errors.As(err, &ae) && ae.Status == http.StatusNotFound
}

func (s gcsObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	var names []string

	opts := gcpstoragebackend.ListOptions{Prefix: prefix}

	for {
		res, err := s.b.ListObjects(bucket, opts)
		if err != nil {
			if notFoundStatus(err) {
				return nil, fmt.Errorf("list gs://%s: %w", bucket, gcpbigquerybackend.ErrObjectNotFound)
			}

			return nil, fmt.Errorf("list gs://%s: %w", bucket, err)
		}

		for _, o := range res.Items {
			names = append(names, o.Name)
		}

		if res.Next == "" {
			return names, nil
		}

		opts.PageToken = res.Next
	}
}

func (s gcsObjectStore) ReadObject(bucket, name string) ([]byte, error) {
	o, err := s.b.GetObject(bucket, name, 0, gcpstoragebackend.Preconds{})
	if err != nil {
		if notFoundStatus(err) {
			return nil, fmt.Errorf("read gs://%s/%s: %w", bucket, name, gcpbigquerybackend.ErrObjectNotFound)
		}

		return nil, fmt.Errorf("read gs://%s/%s: %w", bucket, name, err)
	}

	return o.Data, nil
}

func (s gcsObjectStore) WriteObject(bucket, name string, data []byte, contentType string) error {
	patch := gcpstoragebackend.Patch{}
	if contentType != "" {
		patch["contentType"] = []byte(fmt.Sprintf("%q", contentType))
	}

	if _, err := s.b.PutObject(bucket, name, data, patch, gcpstoragebackend.Preconds{}); err != nil {
		if notFoundStatus(err) {
			return fmt.Errorf("write gs://%s/%s: %w", bucket, name, gcpbigquerybackend.ErrObjectNotFound)
		}

		return fmt.Errorf("write gs://%s/%s: %w", bucket, name, err)
	}

	return nil
}

// wireGCPBigQuery connects BigQuery load and extract jobs to the GCS emulator.
func wireGCPBigQuery(byName map[string]service.Registerable) {
	bq, ok := byName["GCPBigQuery"].(*gcpbigquerybackend.Handler)
	if !ok {
		return
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		bq.Backend.SetObjectStore(gcsObjectStore{b: gcs.Backend})
	}
}
