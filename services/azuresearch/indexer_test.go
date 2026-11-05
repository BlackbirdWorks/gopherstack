package azuresearch_test

import (
	"crypto/md5" //nolint:gosec // test fixture mirrors the storage Content-MD5 field
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

type fakeBlobs struct {
	files map[string]*fakeFile
	err   error
}

type fakeFile struct {
	meta map[string]string
	data string
	etag int
}

func newFakeBlobs() *fakeBlobs { return &fakeBlobs{files: map[string]*fakeFile{}} }

func (f *fakeBlobs) put(name, data string) {
	cur := f.files[name]
	if cur == nil {
		cur = &fakeFile{}
		f.files[name] = cur
	}

	cur.data = data
	cur.etag++
}

func (f *fakeBlobs) ListBlobs(container string) ([]azuresearch.BlobEntry, error) {
	if f.err != nil {
		return nil, f.err
	}

	if container != "docs" {
		return nil, errors.New("container not found")
	}

	var out []azuresearch.BlobEntry

	for name, file := range f.files {
		sum := md5.Sum([]byte(file.data)) //nolint:gosec // fixture

		out = append(out, azuresearch.BlobEntry{
			Name: name, ContentType: "text/plain", ETag: fmt.Sprintf("etag-%d", file.etag), Size: int64(len(file.data)),
			MD5: base64.StdEncoding.EncodeToString(sum[:]), LastModified: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), Metadata: file.meta,
		})
	}

	return out, nil
}

func (f *fakeBlobs) ReadBlob(_, name string) ([]byte, error) { return []byte(f.files[name].data), nil }

type fakeCosmos struct{ docs []azuresearch.CosmosDoc }

func (f *fakeCosmos) ListDocuments(db, container string) ([]azuresearch.CosmosDoc, error) {
	if db != "db" || container != "items" {
		return nil, errors.New("not found")
	}

	return f.docs, nil
}

func docsIndex() map[string]any {
	return map[string]any{
		"name": "docs",
		"fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true},
			map[string]any{"name": "content", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "name", "type": "Edm.String", "filterable": true},
			map[string]any{"name": "size", "type": "Edm.Int64", "filterable": true},
		},
	}
}

func blobIndexer(extra map[string]any) map[string]any {
	idx := map[string]any{
		"name": "ix", "dataSourceName": "ds", "targetIndexName": "docs",
		"schedule": map[string]any{"interval": "PT5M"},
		"parameters": map[string]any{"configuration": map[string]any{"excludedFileNameExtensions": ".png"}},
		"fieldMappings": []any{
			map[string]any{"sourceFieldName": "metadata_storage_path", "targetFieldName": "id", "mappingFunction": map[string]any{"name": "base64Encode"}},
			map[string]any{"sourceFieldName": "metadata_storage_name", "targetFieldName": "name"},
			map[string]any{"sourceFieldName": "metadata_storage_size", "targetFieldName": "size"},
		},
	}
	for k, v := range extra {
		idx[k] = v
	}

	return idx
}

func newIndexerStack(t *testing.T) (*stack, *fakeBlobs) {
	t.Helper()

	s := newStack(t, "standard")
	blobs := newFakeBlobs()
	blobs.put("a.txt", "hello world")
	blobs.put("b.txt", "goodbye moon")
	blobs.put("pic.png", "binary")
	s.b.SetBlobSource(blobs)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource("ds"), http.StatusCreated)

	return s, blobs
}

func (s *stack) status(t *testing.T, name string) map[string]any {
	t.Helper()

	return s.must(t, http.MethodGet, "/indexers('"+name+"')/search.status?"+apiVer, nil, http.StatusOK).body
}

func (s *stack) searchIDs(t *testing.T, term string) []string {
	t.Helper()

	r := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search="+url.QueryEscape(term)+"&$select=name", nil)
	require.Equal(t, http.StatusOK, r.status, string(r.raw))

	return keysByField(r, "name")
}

func TestIndexerBlobOnDemand(t *testing.T) {
	t.Parallel()

	s, _ := newIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)

	before := s.status(t, "ix")
	assert.Equal(t, "unknown", before["status"], "an indexer that never ran has unknown status")
	assert.Nil(t, before["lastResult"])

	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	st := s.status(t, "ix")
	assert.Equal(t, "running", st["status"])

	last, _ := st["lastResult"].(map[string]any)
	assert.Equal(t, "success", last["status"])
	assert.EqualValues(t, 2, last["itemsProcessed"], "the .png blob is excluded")
	assert.EqualValues(t, 0, last["itemsFailed"])

	assert.Equal(t, []string{"a.txt"}, s.searchIDs(t, "hello"))
	assert.Equal(t, []string{"b.txt"}, s.searchIDs(t, "moon"))

	doc := s.must(t, http.MethodGet, "/indexes('docs')/docs/$count?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "2", string(doc.raw))

	size := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&$filter="+url.QueryEscape("size eq 11")+"&$select=name", nil)
	assert.Equal(t, []string{"a.txt"}, keysByField(size, "name"), "metadata_storage_size maps onto an Edm.Int64 field")
}

func TestIndexerKeyEncoding(t *testing.T) {
	t.Parallel()

	s, _ := newIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	r := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search=hello&$select=id", nil)
	require.Equal(t, http.StatusOK, r.status)

	path := "https://acct.blob.core.windows.net/docs/a.txt"
	enc := strings.TrimRight(base64.RawURLEncoding.EncodeToString([]byte(path)), "=")
	pad := (4 - len(enc)%4) % 4

	assert.Equal(t, []string{fmt.Sprintf("%s%d", enc, pad)}, keysByField(r, "id"), "base64Encode uses URL-token encoding with a padding digit")
}

func TestIndexerScheduleOnFakeClock(t *testing.T) {
	t.Parallel()

	s, blobs := newIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)

	assert.Equal(t, 1, s.b.RunDue(), "first tick arms the schedule and runs")
	assert.Equal(t, 0, s.b.RunDue(), "nothing due until the interval passes")

	s.clock.now = s.clock.now.Add(4 * time.Minute)
	assert.Equal(t, 0, s.b.RunDue())

	blobs.put("b.txt", "goodbye sun")
	blobs.put("c.txt", "fresh arrival")

	s.clock.now = s.clock.now.Add(time.Minute)
	assert.Equal(t, 1, s.b.RunDue(), "due at the five minute mark")

	last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
	assert.EqualValues(t, 2, last["itemsProcessed"], "only the changed and the new blob are reprocessed")

	assert.Equal(t, []string{"b.txt"}, s.searchIDs(t, "sun"))
	assert.Empty(t, s.searchIDs(t, "moon"))
	assert.Equal(t, []string{"c.txt"}, s.searchIDs(t, "arrival"))

	hist, _ := s.status(t, "ix")["executionHistory"].([]any)
	require.Len(t, hist, 2)

	newest, _ := hist[0].(map[string]any)
	assert.EqualValues(t, 2, newest["itemsProcessed"], "history lists the newest run first")

	s.must(t, http.MethodPost, "/indexers('ix')/search.reset?"+apiVer, nil, http.StatusNoContent)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	last, _ = s.status(t, "ix")["lastResult"].(map[string]any)
	assert.EqualValues(t, 3, last["itemsProcessed"], "reset clears change tracking")
}

func TestIndexerDisabledAndUnscheduled(t *testing.T) {
	t.Parallel()

	s, _ := newIndexerStack(t)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(map[string]any{"disabled": true}), http.StatusCreated)
	assert.Equal(t, 0, s.b.RunDue(), "disabled indexers do not run on schedule")

	unscheduled := blobIndexer(map[string]any{"name": "manual"})
	delete(unscheduled, "schedule")
	s.must(t, http.MethodPost, "/indexers?"+apiVer, unscheduled, http.StatusCreated)
	assert.Equal(t, 0, s.b.RunDue(), "indexers without a schedule only run on demand")
}

func TestIndexerFailures(t *testing.T) {
	t.Parallel()

	t.Run("unreadable source", func(t *testing.T) {
		t.Parallel()

		s, blobs := newIndexerStack(t)
		blobs.err = errors.New("storage offline")
		s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)
		s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

		last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
		assert.Equal(t, "transientFailure", last["status"])
		assert.Contains(t, last["errorMessage"], "storage offline")
	})

	t.Run("invalid key characters", func(t *testing.T) {
		t.Parallel()

		s, _ := newIndexerStack(t)
		mapping := []any{map[string]any{"sourceFieldName": "metadata_storage_name", "targetFieldName": "id"}}
		s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(map[string]any{"fieldMappings": mapping}), http.StatusCreated)
		s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

		last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
		assert.Equal(t, "transientFailure", last["status"], "file names with dots are not valid document keys")
	})

	t.Run("missing key tolerated", func(t *testing.T) {
		t.Parallel()

		s, _ := newIndexerStack(t)
		ix := blobIndexer(map[string]any{
			"fieldMappings": []any{map[string]any{"sourceFieldName": "metadata_storage_name", "targetFieldName": "name"}},
			"parameters": map[string]any{
				"maxFailedItems": -1, "configuration": map[string]any{"excludedFileNameExtensions": ".png"},
			},
		})
		s.must(t, http.MethodPost, "/indexers?"+apiVer, ix, http.StatusCreated)
		s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

		last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
		assert.Equal(t, "success", last["status"])
		assert.EqualValues(t, 2, last["itemsFailed"])
		assert.Len(t, last["errors"], 2)
	})

	t.Run("max failed items zero", func(t *testing.T) {
		t.Parallel()

		s, _ := newIndexerStack(t)
		ix := blobIndexer(map[string]any{
			"fieldMappings": []any{map[string]any{"sourceFieldName": "metadata_storage_name", "targetFieldName": "name"}},
		})
		s.must(t, http.MethodPost, "/indexers?"+apiVer, ix, http.StatusCreated)
		s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

		last, _ := s.status(t, "ix")["lastResult"].(map[string]any)
		assert.Equal(t, "transientFailure", last["status"])
		assert.EqualValues(t, 1, last["itemsFailed"], "the run stops once maxFailedItems is exceeded")
	})
}

func TestIndexerParsingModes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		files map[string]string
		conf  map[string]any
		name  string
		term  string
		want  int
	}{
		{map[string]string{"a.json": `{"content":"jsonbody","name":"j"}`}, map[string]any{"parsingMode": "json"}, "json", "jsonbody", 1},
		{map[string]string{"a.json": `[{"content":"one"},{"content":"two"}]`}, map[string]any{"parsingMode": "jsonArray"}, "jsonArray", "two", 1},
		{map[string]string{"a.jsonl": "{\"content\":\"l1\"}\n{\"content\":\"l2\"}\n"}, map[string]any{"parsingMode": "jsonLines"}, "jsonLines", "l2", 1},
		{map[string]string{"a.csv": "content,name\nrow1,n1\nrow2,n2\n"}, map[string]any{"parsingMode": "delimitedText"}, "csv", "row2", 1},
		{map[string]string{"a.txt": "plain text"}, map[string]any{"dataToExtract": "storageMetadata"}, "metadata only", "plain", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			blobs := newFakeBlobs()

			for n, d := range tc.files {
				blobs.put(n, d)
			}

			s.b.SetBlobSource(blobs)
			s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
			s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource("ds"), http.StatusCreated)

			ix := blobIndexer(map[string]any{
				"parameters": map[string]any{"configuration": tc.conf, "maxFailedItems": -1},
				"fieldMappings": []any{
					map[string]any{"sourceFieldName": "metadata_storage_path", "targetFieldName": "id", "mappingFunction": map[string]any{"name": "base64Encode"}},
				},
			})
			delete(ix, "schedule")
			s.must(t, http.MethodPost, "/indexers?"+apiVer, ix, http.StatusCreated)
			s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

			r := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search="+tc.term+"&$select=id", nil)
			require.Equal(t, http.StatusOK, r.status)
			assert.Len(t, r.body["value"], tc.want)
		})
	}
}

func TestIndexerSoftDeleteDetection(t *testing.T) {
	t.Parallel()

	s, blobs := newIndexerStack(t)
	blobs.files["a.txt"].meta = map[string]string{"IsDeleted": "false"}
	blobs.files["b.txt"].meta = map[string]string{"IsDeleted": "false"}

	ds := blobSource("ds")
	ds["dataDeletionDetectionPolicy"] = map[string]any{
		"@odata.type":          "#Microsoft.Azure.Search.SoftDeleteColumnDeletionDetectionPolicy",
		"softDeleteColumnName": "IsDeleted", "softDeleteMarkerValue": "true",
	}
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, ds, http.StatusNoContent)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, blobIndexer(nil), http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)
	require.Equal(t, []string{"a.txt"}, s.searchIDs(t, "hello"))

	blobs.files["a.txt"].meta = map[string]string{"IsDeleted": "true"}
	blobs.files["a.txt"].etag++

	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)
	assert.Empty(t, s.searchIDs(t, "hello"), "a document flagged by the soft-delete column leaves the index")
	assert.Equal(t, []string{"b.txt"}, s.searchIDs(t, "moon"))
}

func TestIndexerCosmosSource(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	cosmos := &fakeCosmos{docs: []azuresearch.CosmosDoc{
		{ID: "1", ETag: "e1", Body: map[string]any{"id": "1", "content": "alpha record", "name": "first"}},
		{ID: "2", ETag: "e2", Body: map[string]any{"id": "2", "content": "beta record", "name": "second"}},
	}}
	s.b.SetCosmosSource(cosmos)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, docsIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('cs')?"+apiVer, map[string]any{
		"name": "cs", "type": "cosmosdb",
		"credentials": map[string]any{"connectionString": "AccountEndpoint=https://x.documents.azure.com;AccountKey=eA==;Database=db"},
		"container":   map[string]any{"name": "items"},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{"name": "cx", "dataSourceName": "cs", "targetIndexName": "docs"}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('cx')/search.run?"+apiVer, nil, http.StatusAccepted)

	assert.Equal(t, []string{"first"}, s.searchIDs(t, "alpha"))
	assert.Equal(t, []string{"first", "second"}, s.searchIDs(t, "record"))

	cosmos.docs[0].ETag = "e1b"
	cosmos.docs[0].Body["content"] = "gamma record"

	s.must(t, http.MethodPost, "/indexers('cx')/search.run?"+apiVer, nil, http.StatusAccepted)

	last, _ := s.status(t, "cx")["lastResult"].(map[string]any)
	assert.EqualValues(t, 1, last["itemsProcessed"], "unchanged ETags are skipped")
	assert.Equal(t, []string{"first"}, s.searchIDs(t, "gamma"))
	assert.Empty(t, s.searchIDs(t, "alpha"))
}
