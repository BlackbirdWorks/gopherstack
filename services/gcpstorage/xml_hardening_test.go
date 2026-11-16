package gcpstorage_test

import (
	"encoding/xml"
	"net/http"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestXMLMalformedPreconditionHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		hdr    string
	}{
		{name: "get generation", method: http.MethodGet, hdr: "X-Goog-If-Generation-Match"},
		{name: "put generation", method: http.MethodPut, hdr: "X-Goog-If-Generation-Match"},
		{name: "put metageneration", method: http.MethodPut, hdr: "X-Goog-If-Metageneration-Match"},
		{name: "delete generation", method: http.MethodDelete, hdr: "X-Goog-If-Generation-Match"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			b := e.bucket(t, "pre", nil)
			write(t, b.Object("o"), []byte("keep"), 0, nil)

			r := xmlDo(t, e, tt.method, "/pre/o", "", []byte("new"), map[string]string{tt.hdr: "not-a-number"})
			assert.Equal(t, http.StatusBadRequest, r.status, r.body)
			assert.Equal(t, "keep", string(readAll(t, b.Object("o"))))
		})
	}
}

var multipartETagRe = regexp.MustCompile(`^"[0-9a-f]{32}-2"$`)

func TestXMLMultipartETagFormat(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	e.bucket(t, "etag", nil)

	id := startMPU(t, e, "/etag/obj", nil)
	e1 := putPart(t, e, "/etag/obj", id, 1, make([]byte, 5<<20))
	e2 := putPart(t, e, "/etag/obj", id, 2, []byte("tail"))

	r := xmlDo(t, e, http.MethodPost, "/etag/obj?uploadId="+id, "",
		completeBody([2]string{"1", e1}, [2]string{"2", e2}), nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	var out struct {
		ETag string `xml:"ETag"`
	}

	require.NoError(t, xml.Unmarshal([]byte(r.body), &out))
	assert.Regexp(t, multipartETagRe, out.ETag)

	head := xmlDo(t, e, http.MethodHead, "/etag/obj", "", nil, nil)
	assert.Equal(t, out.ETag, head.header.Get("ETag"))
}

func TestXMLMultipartStateBounds(t *testing.T) {
	t.Parallel()

	t.Run("staged bytes", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		e.bucket(t, "cap", nil)
		e.backend.SetStagedLimit(10)

		id := startMPU(t, e, "/cap/obj", nil)
		r := xmlDo(t, e, http.MethodPut, "/cap/obj?partNumber=1&uploadId="+id, "", []byte("0123456789a"), nil)
		assert.Equal(t, http.StatusServiceUnavailable, r.status, r.body)
		assert.Equal(t, "SlowDown", xmlErrCode(t, r))
	})

	t.Run("old uploads stay open", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		e.bucket(t, "old", nil)

		start := time.Now()
		e.backend.SetClock(func() time.Time { return start })

		id := startMPU(t, e, "/old/obj", nil)

		e.backend.SetClock(func() time.Time { return start.Add(30 * 24 * time.Hour) })
		startMPU(t, e, "/old/other", nil)

		r := xmlDo(t, e, http.MethodPut, "/old/obj?partNumber=1&uploadId="+id, "", []byte("x"), nil)
		assert.Equal(t, http.StatusOK, r.status, r.body)
	})

	t.Run("bucket delete drops uploads", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		b := e.bucket(t, "gone", nil)
		id := startMPU(t, e, "/gone/obj", nil)

		require.NoError(t, b.Delete(t.Context()))
		e.bucket(t, "gone", nil)

		r := xmlDo(t, e, http.MethodPut, "/gone/obj?partNumber=1&uploadId="+id, "", []byte("x"), nil)
		assert.Equal(t, http.StatusNotFound, r.status, r.body)
		assert.Equal(t, "NoSuchUpload", xmlErrCode(t, r))
	})
}

func TestXMLCopyResultIsDestination(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	b := e.bucket(t, "cpt", nil)

	t0 := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	e.backend.SetClock(func() time.Time { return t0 })
	write(t, b.Object("src"), []byte("d"), 0, nil)

	t1 := t0.Add(2 * time.Hour)
	e.backend.SetClock(func() time.Time { return t1 })

	r := xmlDo(t, e, http.MethodPut, "/cpt/dst", "", nil, map[string]string{"X-Goog-Copy-Source": "/cpt/src"})
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Contains(t, r.body, "2030-01-02T05:04:05")
}

func TestXMLListMaxKeysClamp(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	e.bucket(t, "many", nil)

	for i := range 1001 {
		name := "k" + strconv.Itoa(1000+i)
		_, err := e.backend.PutObject("many", name, []byte("x"), gcpstorage.Patch{}, gcpstorage.Preconds{})
		require.NoError(t, err)
	}

	l := parseList(t, xmlDo(t, e, http.MethodGet, "/many?max-keys=5000", "", nil, nil))
	assert.Len(t, l.Keys, 1000)
	assert.True(t, l.IsTruncated)

	next := parseList(t, xmlDo(t, e, http.MethodGet, "/many?marker="+l.Keys[999], "", nil, nil))
	assert.Equal(t, []string{"k2000"}, next.Keys)
	assert.False(t, next.IsTruncated)

	v2 := parseList(t, xmlDo(t, e, http.MethodGet, "/many?list-type=2", "", nil, nil))
	assert.True(t, v2.IsTruncated)
	assert.NotEmpty(t, v2.NextToken)
}

func TestMultipartETagSurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	e.bucket(t, "snap", nil)

	id := startMPU(t, e, "/snap/obj", nil)
	e1 := putPart(t, e, "/snap/obj", id, 1, []byte("only"))
	r := xmlDo(t, e, http.MethodPost, "/snap/obj?uploadId="+id, "", completeBody([2]string{"1", e1}), nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	want := xmlDo(t, e, http.MethodHead, "/snap/obj", "", nil, nil).header.Get("ETag")
	require.Regexp(t, `-1"$`, want)

	dst := newSDK(t)
	require.NoError(t, dst.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	got := xmlDo(t, dst, http.MethodHead, "/snap/obj", "", nil, nil)
	assert.Equal(t, want, got.header.Get("ETag"))
}
