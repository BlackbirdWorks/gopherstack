package gcpbigquery_test

import (
	"fmt"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery"
)

const sdkProject = "test-project"

type sdkEnv struct {
	client  *bigquery.Client
	backend *gcpbigquery.InMemoryBackend
	objects *fakeObjects
	url     string
}

func newSDK(t *testing.T) *sdkEnv {
	t.Helper()

	backend := gcpbigquery.NewInMemoryBackend()
	objects := &fakeObjects{data: map[string][]byte{}}
	backend.SetObjectStore(objects)

	e := echo.New()
	e.Any("/*", gcpbigquery.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(t.Context(), sdkProject,
		option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &sdkEnv{client: client, backend: backend, objects: objects, url: srv.URL}
}

// fakeObjects is an in-memory ObjectStore.
type fakeObjects struct {
	data map[string][]byte
}

func (f *fakeObjects) ListObjects(bucket, prefix string) ([]string, error) {
	var out []string

	for k := range f.data {
		if rest, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(rest, prefix) {
			out = append(out, rest)
		}
	}

	sort.Strings(out)

	return out, nil
}

func (f *fakeObjects) ReadObject(bucket, name string) ([]byte, error) {
	d, ok := f.data[bucket+"/"+name]
	if !ok {
		return nil, gcpbigquery.ErrObjectNotFound
	}

	return d, nil
}

func (f *fakeObjects) WriteObject(bucket, name string, data []byte, _ string) error {
	f.data[bucket+"/"+name] = data

	return nil
}

func (s *sdkEnv) dataset(t *testing.T, id string) *bigquery.Dataset {
	t.Helper()

	ds := s.client.Dataset(id)
	require.NoError(t, ds.Create(t.Context(), &bigquery.DatasetMetadata{Location: "US"}))

	return ds
}

func (s *sdkEnv) rows(t *testing.T, sql string, params ...bigquery.QueryParameter) [][]bigquery.Value {
	t.Helper()

	q := s.client.Query(sql)
	q.Parameters = params

	it, err := q.Read(t.Context())
	require.NoError(t, err)

	var out [][]bigquery.Value

	for {
		var row []bigquery.Value

		err = it.Next(&row)
		if err != nil {
			require.Equal(t, "no more items in iterator", fmt.Sprint(err))

			return out
		}

		out = append(out, row)
	}
}
