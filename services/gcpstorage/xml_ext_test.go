package gcpstorage_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

type xmlReply struct {
	header http.Header
	body   string
	status int
}

func xmlDo(t *testing.T, e *sdkEnv, method, path, host string, body []byte, hdr map[string]string) xmlReply {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.url+path, bytes.NewReader(body))
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := httpclient.New(30 * time.Second).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return xmlReply{status: resp.StatusCode, header: resp.Header, body: string(raw)}
}

type listed struct {
	Name        string   `xml:"Name"`
	NextMarker  string   `xml:"NextMarker"`
	NextToken   string   `xml:"NextContinuationToken"`
	Keys        []string `xml:"Contents>Key"`
	Prefixes    []string `xml:"CommonPrefixes>Prefix"`
	KeyCount    int      `xml:"KeyCount"`
	IsTruncated bool     `xml:"IsTruncated"`
}

func parseList(t *testing.T, r xmlReply) listed {
	t.Helper()
	require.Equal(t, http.StatusOK, r.status, r.body)

	var l listed
	require.NoError(t, xml.Unmarshal([]byte(r.body), &l))

	return l
}

func xmlErrCode(t *testing.T, r xmlReply) string {
	t.Helper()

	var e struct {
		Code string `xml:"Code"`
	}
	require.NoError(t, xml.Unmarshal([]byte(r.body), &e), r.body)

	return e.Code
}

func TestXMLListObjects(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	b := e.bucket(t, "xml-list", nil)

	for _, n := range []string{"a/1", "a/2", "b", "c", "d d"} {
		write(t, b.Object(n), []byte("x"), 0, nil)
	}

	t.Run("plain", func(t *testing.T) {
		t.Parallel()

		l := parseList(t, xmlDo(t, e, http.MethodGet, "/xml-list", "", nil, nil))
		assert.Equal(t, "xml-list", l.Name)
		assert.Equal(t, []string{"a/1", "a/2", "b", "c", "d d"}, l.Keys)
		assert.False(t, l.IsTruncated)
	})

	t.Run("delimiter and prefix", func(t *testing.T) {
		t.Parallel()

		l := parseList(t, xmlDo(t, e, http.MethodGet, "/xml-list?delimiter=/", "", nil, nil))
		assert.Equal(t, []string{"b", "c", "d d"}, l.Keys)
		assert.Equal(t, []string{"a/"}, l.Prefixes)

		l = parseList(t, xmlDo(t, e, http.MethodGet, "/xml-list?prefix=a/", "", nil, nil))
		assert.Equal(t, []string{"a/1", "a/2"}, l.Keys)
	})

	t.Run("marker paging", func(t *testing.T) {
		t.Parallel()

		var got []string

		marker := ""

		for range 10 {
			l := parseList(
				t,
				xmlDo(t, e, http.MethodGet, "/xml-list?max-keys=2&marker="+url.QueryEscape(marker), "", nil, nil),
			)
			got = append(got, l.Keys...)

			if !l.IsTruncated {
				break
			}

			require.NotEmpty(t, l.Keys)
			marker = l.Keys[len(l.Keys)-1]
		}

		assert.Equal(t, []string{"a/1", "a/2", "b", "c", "d d"}, got)
	})

	t.Run("marker paging with delimiter", func(t *testing.T) {
		t.Parallel()

		var keys, prefixes []string

		marker := ""

		for range 10 {
			l := parseList(
				t,
				xmlDo(
					t,
					e,
					http.MethodGet,
					"/xml-list?delimiter=/&max-keys=2&marker="+url.QueryEscape(marker),
					"",
					nil,
					nil,
				),
			)
			keys, prefixes = append(keys, l.Keys...), append(prefixes, l.Prefixes...)

			if !l.IsTruncated {
				break
			}

			require.NotEmpty(t, l.NextMarker)
			marker = l.NextMarker
		}

		assert.Equal(t, []string{"b", "c", "d d"}, keys)
		assert.Equal(t, []string{"a/"}, prefixes)
	})

	t.Run("continuation token paging", func(t *testing.T) {
		t.Parallel()

		var got []string

		token := ""

		for range 10 {
			l := parseList(t, xmlDo(t, e, http.MethodGet,
				"/xml-list?list-type=2&max-keys=2&continuation-token="+url.QueryEscape(token), "", nil, nil))
			got = append(got, l.Keys...)
			assert.Equal(t, len(l.Keys)+len(l.Prefixes), l.KeyCount)

			if !l.IsTruncated {
				break
			}

			token = l.NextToken
			require.NotEmpty(t, token)
		}

		assert.Equal(t, []string{"a/1", "a/2", "b", "c", "d d"}, got)
	})

	t.Run("start after and url encoding", func(t *testing.T) {
		t.Parallel()

		l := parseList(
			t,
			xmlDo(t, e, http.MethodGet, "/xml-list?list-type=2&start-after=b&encoding-type=url", "", nil, nil),
		)
		assert.Equal(t, []string{"c", "d+d"}, l.Keys)
	})

	t.Run("rejections", func(t *testing.T) {
		t.Parallel()

		for path, want := range map[string]int{
			"/xml-list?continuation-token=x": http.StatusBadRequest,
			"/xml-list?list-type=2&marker=a": http.StatusBadRequest,
			"/xml-list?max-keys=-1":          http.StatusBadRequest,
			"/xml-list?versions":             http.StatusNotImplemented,
			"/missing-bucket":                http.StatusNotFound,
		} {
			assert.Equal(t, want, xmlDo(t, e, http.MethodGet, path, "", nil, nil).status, path)
		}
	})
}

func TestXMLVirtualHostedStyle(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	b := e.bucket(t, "vhost-bkt", nil)
	write(t, b.Object("dir/obj"), []byte("hello"), 0, nil)

	got := xmlDo(t, e, http.MethodGet, "/dir/obj", "vhost-bkt.storage.googleapis.com", nil, nil)
	assert.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, "hello", got.body)

	l := parseList(t, xmlDo(t, e, http.MethodGet, "/", "vhost-bkt.storage.googleapis.com:443", nil, nil))
	assert.Equal(t, []string{"dir/obj"}, l.Keys)

	put := xmlDo(t, e, http.MethodPut, "/new", "vhost-bkt.storage.googleapis.com", []byte("n"), nil)
	assert.Equal(t, http.StatusOK, put.status)
	assert.Equal(t, "n", string(readAll(t, b.Object("new"))))

	other := xmlDo(t, e, http.MethodGet, "/dir/obj", "storage.googleapis.com", nil, nil)
	assert.Equal(t, http.StatusNotFound, other.status, "a bare endpoint host is not a bucket")
}

type uploadInit struct {
	UploadID string `xml:"UploadId"`
}

func startMPU(t *testing.T, e *sdkEnv, path string, hdr map[string]string) string {
	t.Helper()

	r := xmlDo(t, e, http.MethodPost, path+"?uploads", "", nil, hdr)
	require.Equal(t, http.StatusOK, r.status, r.body)

	var out uploadInit
	require.NoError(t, xml.Unmarshal([]byte(r.body), &out))
	require.NotEmpty(t, out.UploadID)

	return out.UploadID
}

func putPart(t *testing.T, e *sdkEnv, path, id string, n int, data []byte) string {
	t.Helper()

	r := xmlDo(t, e, http.MethodPut, path+"?partNumber="+strconv.Itoa(n)+"&uploadId="+id, "", data, nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	return r.header.Get("ETag")
}

func completeBody(parts ...[2]string) []byte {
	var sb strings.Builder

	sb.WriteString("<CompleteMultipartUpload>")

	for _, p := range parts {
		sb.WriteString("<Part><PartNumber>" + p[0] + "</PartNumber><ETag>" + p[1] + "</ETag></Part>")
	}

	sb.WriteString("</CompleteMultipartUpload>")

	return []byte(sb.String())
}

func TestXMLMultipartUpload(t *testing.T) {
	t.Parallel()

	big := bytes.Repeat([]byte("a"), 5<<20)

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()

		e := newSDK(t)
		b := e.bucket(t, "mpu", nil)
		id := startMPU(t, e, "/mpu/obj", map[string]string{"Content-Type": "text/plain", "X-Goog-Meta-K": "v"})

		e2 := putPart(t, e, "/mpu/obj", id, 2, []byte("tail"))
		e1 := putPart(t, e, "/mpu/obj", id, 1, big)

		r := xmlDo(
			t,
			e,
			http.MethodPost,
			"/mpu/obj?uploadId="+id,
			"",
			completeBody([2]string{"1", e1}, [2]string{"2", e2}),
			nil,
		)
		require.Equal(t, http.StatusOK, r.status, r.body)
		assert.Contains(t, r.body, "<CompleteMultipartUploadResult")
		assert.Contains(t, r.body, "<Key>obj</Key>")

		attrs, err := b.Object("obj").Attrs(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(len(big)+4), attrs.Size)
		assert.Equal(t, "text/plain", attrs.ContentType)
		assert.Equal(t, "v", attrs.Metadata["k"])
		assert.Empty(t, attrs.MD5, "a multipart object has no MD5")
		assert.NotZero(t, attrs.CRC32C)

		again := xmlDo(t, e, http.MethodPost, "/mpu/obj?uploadId="+id, "", completeBody([2]string{"1", e1}), nil)
		assert.Equal(t, http.StatusNotFound, again.status)
		assert.Equal(t, "NoSuchUpload", xmlErrCode(t, again), "an upload completes once")
	})

	tests := []struct {
		run  func(t *testing.T, e *sdkEnv, id string, e1, e2 string) xmlReply
		name string
		code string
		want int
	}{
		{
			name: "wrong etag", want: http.StatusBadRequest, code: "InvalidPart",
			run: func(t *testing.T, e *sdkEnv, id, e1, _ string) xmlReply {
				t.Helper()

				return xmlDo(
					t,
					e,
					http.MethodPost,
					"/mpb/obj?uploadId="+id,
					"",
					completeBody([2]string{"1", e1}, [2]string{"2", `"deadbeef"`}),
					nil,
				)
			},
		},
		{
			name: "unknown part", want: http.StatusBadRequest, code: "InvalidPart",
			run: func(t *testing.T, e *sdkEnv, id, e1, _ string) xmlReply {
				t.Helper()

				return xmlDo(
					t,
					e,
					http.MethodPost,
					"/mpb/obj?uploadId="+id,
					"",
					completeBody([2]string{"1", e1}, [2]string{"9", e1}),
					nil,
				)
			},
		},
		{
			name: "descending order", want: http.StatusBadRequest, code: "InvalidPartOrder",
			run: func(t *testing.T, e *sdkEnv, id, e1, e2 string) xmlReply {
				t.Helper()

				return xmlDo(
					t,
					e,
					http.MethodPost,
					"/mpb/obj?uploadId="+id,
					"",
					completeBody([2]string{"2", e2}, [2]string{"1", e1}),
					nil,
				)
			},
		},
		{
			name: "small non-final part", want: http.StatusBadRequest, code: "InvalidArgument",
			run: func(t *testing.T, e *sdkEnv, id, e1, e2 string) xmlReply {
				t.Helper()

				return xmlDo(
					t,
					e,
					http.MethodPost,
					"/mpb/obj?uploadId="+id,
					"",
					completeBody([2]string{"2", e2}, [2]string{"3", e1}),
					nil,
				)
			},
		},
		{
			name: "part number out of range", want: http.StatusBadRequest, code: "InvalidArgument",
			run: func(t *testing.T, e *sdkEnv, id, _, _ string) xmlReply {
				t.Helper()

				return xmlDo(t, e, http.MethodPut, "/mpb/obj?partNumber=10001&uploadId="+id, "", []byte("x"), nil)
			},
		},
		{
			name: "abort then part", want: http.StatusNotFound, code: "NoSuchUpload",
			run: func(t *testing.T, e *sdkEnv, id, _, _ string) xmlReply {
				t.Helper()

				ab := xmlDo(t, e, http.MethodDelete, "/mpb/obj?uploadId="+id, "", nil, nil)
				require.Equal(t, http.StatusNoContent, ab.status)

				return xmlDo(t, e, http.MethodPut, "/mpb/obj?partNumber=1&uploadId="+id, "", []byte("x"), nil)
			},
		},
		{
			name: "other object name", want: http.StatusNotFound, code: "NoSuchUpload",
			run: func(t *testing.T, e *sdkEnv, id, _, _ string) xmlReply {
				t.Helper()

				return xmlDo(t, e, http.MethodPut, "/mpb/elsewhere?partNumber=1&uploadId="+id, "", []byte("x"), nil)
			},
		},
		{
			name: "unknown upload id", want: http.StatusNotFound, code: "NoSuchUpload",
			run: func(t *testing.T, e *sdkEnv, _, _, _ string) xmlReply {
				t.Helper()

				return xmlDo(t, e, http.MethodDelete, "/mpb/obj?uploadId=nope", "", nil, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			e.bucket(t, "mpb", nil)
			id := startMPU(t, e, "/mpb/obj", nil)
			e1 := putPart(t, e, "/mpb/obj", id, 1, big)
			e2 := putPart(t, e, "/mpb/obj", id, 2, []byte("tail"))

			r := tt.run(t, e, id, e1, e2)
			assert.Equal(t, tt.want, r.status, r.body)
			assert.Equal(t, tt.code, xmlErrCode(t, r))
		})
	}
}

func TestXMLMultipartCustomerKey(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	e.bucket(t, "mpukey", nil)

	key := csekKey(3)
	sum := sha256.Sum256(key)
	hdr := map[string]string{
		"X-Goog-Encryption-Algorithm":  "AES256",
		"X-Goog-Encryption-Key":        base64.StdEncoding.EncodeToString(key),
		"X-Goog-Encryption-Key-Sha256": base64.StdEncoding.EncodeToString(sum[:]),
	}
	id := startMPU(t, e, "/mpukey/o", hdr)

	r := xmlDo(t, e, http.MethodPut, "/mpukey/o?partNumber=1&uploadId="+id, "", []byte("x"), nil)
	assert.Equal(t, http.StatusBadRequest, r.status, "parts must repeat the initiation key")

	r = xmlDo(t, e, http.MethodPut, "/mpukey/o?partNumber=1&uploadId="+id, "", []byte("x"), hdr)
	assert.Equal(t, http.StatusOK, r.status, r.body)
}

func TestAbortIncompleteMultipartUploadRule(t *testing.T) {
	t.Parallel()

	e, clk := newClockedSDK(t)
	b := e.bucket(t, "mpurule", nil)

	_, err := b.Update(
		t.Context(),
		storage.BucketAttrsToUpdate{Lifecycle: &storage.Lifecycle{Rules: []storage.LifecycleRule{{
			Action:    storage.LifecycleAction{Type: storage.AbortIncompleteMPUAction},
			Condition: storage.LifecycleCondition{AgeInDays: 2, MatchesPrefix: []string{"tmp/"}},
		}}}},
	)
	require.NoError(t, err)

	tmp := startMPU(t, e, "/mpurule/tmp/a", nil)
	keep := startMPU(t, e, "/mpurule/keep/a", nil)

	clk.advance(24 * time.Hour)
	e.backend.RunLifecycle()
	assert.Equal(
		t,
		http.StatusOK,
		xmlDo(t, e, http.MethodPut, "/mpurule/tmp/a?partNumber=1&uploadId="+tmp, "", []byte("x"), nil).status,
	)

	clk.advance(25 * time.Hour)
	e.backend.RunLifecycle()

	assert.Equal(
		t,
		http.StatusNotFound,
		xmlDo(t, e, http.MethodPut, "/mpurule/tmp/a?partNumber=1&uploadId="+tmp, "", []byte("x"), nil).status,
		"an upload matching the rule is aborted once old enough",
	)
	assert.Equal(
		t,
		http.StatusOK,
		xmlDo(t, e, http.MethodPut, "/mpurule/keep/a?partNumber=1&uploadId="+keep, "", []byte("x"), nil).status,
		"an upload outside the prefix stays",
	)
}

func TestAbortRuleRejectsOtherConditions(t *testing.T) {
	t.Parallel()

	e := newSDK(t)
	b := e.bucket(t, "badrule", nil)

	_, err := b.Update(
		t.Context(),
		storage.BucketAttrsToUpdate{Lifecycle: &storage.Lifecycle{Rules: []storage.LifecycleRule{{
			Action:    storage.LifecycleAction{Type: storage.AbortIncompleteMPUAction},
			Condition: storage.LifecycleCondition{AgeInDays: 2, MatchesStorageClasses: []string{"STANDARD"}},
		}}}},
	)
	assert.Equal(t, "invalid", apiReason(t, err, http.StatusBadRequest))
}

func TestXMLCopyObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr    map[string]string
		check  func(t *testing.T, b *storage.BucketHandle)
		name   string
		dst    string
		status int
	}{
		{
			name: "copy keeps source metadata", dst: "/cpy/dst", status: http.StatusOK,
			hdr: map[string]string{"X-Goog-Copy-Source": "/cpy/src"},
			check: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()

				a, err := b.Object("dst").Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "text/plain", a.ContentType)
				assert.Equal(t, "v", a.Metadata["k"])
				assert.Equal(t, "data", string(readAll(t, b.Object("dst"))))
			},
		},
		{
			name: "replace takes request metadata", dst: "/cpy/dst", status: http.StatusOK,
			hdr: map[string]string{
				"X-Goog-Copy-Source": "/cpy/src", "X-Goog-Metadata-Directive": "REPLACE", "Content-Type": "image/png",
				"X-Goog-Meta-Other": "o",
			},
			check: func(t *testing.T, b *storage.BucketHandle) {
				t.Helper()

				a, err := b.Object("dst").Attrs(t.Context())
				require.NoError(t, err)
				assert.Equal(t, "image/png", a.ContentType)
				assert.Equal(t, map[string]string{"other": "o"}, a.Metadata)
			},
		},
		{
			name:   "self copy needs replace",
			dst:    "/cpy/src",
			status: http.StatusBadRequest,
			hdr:    map[string]string{"X-Goog-Copy-Source": "/cpy/src"},
		},
		{
			name:   "self copy with replace",
			dst:    "/cpy/src",
			status: http.StatusOK,
			hdr: map[string]string{
				"X-Goog-Copy-Source":        "/cpy/src",
				"X-Goog-Metadata-Directive": "REPLACE",
				"Content-Type":              "text/csv",
			},
		},
		{
			name:   "missing source",
			dst:    "/cpy/dst",
			status: http.StatusNotFound,
			hdr:    map[string]string{"X-Goog-Copy-Source": "/cpy/nope"},
		},
		{
			name: "source generation mismatch", dst: "/cpy/dst", status: http.StatusPreconditionFailed,
			hdr: map[string]string{"X-Goog-Copy-Source": "/cpy/src", "X-Goog-Copy-Source-If-Generation-Match": "1"},
		},
		{
			name: "source etag mismatch", dst: "/cpy/dst", status: http.StatusPreconditionFailed,
			hdr: map[string]string{"X-Goog-Copy-Source": "/cpy/src", "X-Goog-Copy-Source-If-Match": `"nope"`},
		},
		{
			name:   "bad directive",
			dst:    "/cpy/dst",
			status: http.StatusBadRequest,
			hdr:    map[string]string{"X-Goog-Copy-Source": "/cpy/src", "X-Goog-Metadata-Directive": "MOVE"},
		},
		{
			name:   "malformed source",
			dst:    "/cpy/dst",
			status: http.StatusBadRequest,
			hdr:    map[string]string{"X-Goog-Copy-Source": "nobucketpath"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			b := e.bucket(t, "cpy", nil)
			write(t, b.Object("src"), []byte("data"), 0, func(w *storage.Writer) {
				w.ContentType = "text/plain"
				w.Metadata = map[string]string{"k": "v"}
			})

			r := xmlDo(t, e, http.MethodPut, tt.dst, "", nil, tt.hdr)
			assert.Equal(t, tt.status, r.status, r.body)

			if tt.status == http.StatusOK {
				assert.Contains(t, r.body, "<CopyObjectResult>")
			}

			if tt.check != nil {
				tt.check(t, b)
			}
		})
	}
}
