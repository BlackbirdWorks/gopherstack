package gcpstorage_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"maps"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func csekKey(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestSDK_CustomerSuppliedKeys(t *testing.T) {
	t.Parallel()

	const chunk = 256 * 1024

	tests := []struct {
		name  string
		chunk int
		size  int
	}{
		{name: "multipart", chunk: 0, size: 10},
		{name: "resumable", chunk: chunk, size: 3 * chunk},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSDK(t)
			b := s.bucket(t, "csek-"+tt.name, nil)
			k1, k2 := csekKey(1), csekKey(2)
			data := bytes.Repeat([]byte("x"), tt.size)

			obj := b.Object("o").Key(k1)
			attrs := write(t, obj, data, tt.chunk, nil)

			sum := sha256.Sum256(k1)
			assert.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), attrs.CustomerKeySHA256)
			assert.Equal(t, data, readAll(t, obj))

			_, err := b.Object("o").NewReader(t.Context())
			assert.Equal(t, http.StatusBadRequest, apiCode(err))

			_, err = b.Object("o").Key(k2).NewReader(t.Context())
			assert.Equal(t, http.StatusBadRequest, apiCode(err))

			plain := write(t, b.Object("plain"), []byte("p"), 0, nil)
			assert.Empty(t, plain.CustomerKeySHA256)

			_, err = b.Object("plain").Key(k1).NewReader(t.Context())
			assert.Equal(t, http.StatusBadRequest, apiCode(err))
		})
	}
}

func TestSDK_CustomerSuppliedKeysMetadata(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "csek-meta", nil)
	k1, k2 := csekKey(1), csekKey(2)

	write(t, b.Object("o").Key(k1), []byte("hello"), 0, nil)

	got, err := b.Object("o").Attrs(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, got.CustomerKeySHA256)
	assert.Zero(t, got.CRC32C)
	assert.Empty(t, got.MD5)

	got, err = b.Object("o").Key(k1).Attrs(t.Context())
	require.NoError(t, err)
	assert.NotZero(t, got.CRC32C)

	_, err = b.Object("o").Key(k2).Attrs(t.Context())
	assert.Equal(t, http.StatusBadRequest, apiCode(err))

	upd, err := b.Object("o").Update(t.Context(), storage.ObjectAttrsToUpdate{ContentLanguage: "en"})
	require.NoError(t, err)
	assert.Equal(t, "en", upd.ContentLanguage)

	require.NoError(t, b.Object("o").Delete(t.Context()))
}

func TestSDK_CustomerSuppliedKeysCopyCompose(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	b := s.bucket(t, "csek-copy", nil)
	k1, k2 := csekKey(1), csekKey(2)

	write(t, b.Object("a").Key(k1), []byte("aa"), 0, nil)
	write(t, b.Object("b").Key(k1), []byte("bb"), 0, nil)

	_, err := b.Object("noKey").CopierFrom(b.Object("a")).Run(t.Context())
	assert.Equal(t, http.StatusBadRequest, apiCode(err))

	_, err = b.Object("badKey").CopierFrom(b.Object("a").Key(k2)).Run(t.Context())
	assert.Equal(t, http.StatusBadRequest, apiCode(err))

	plain, err := b.Object("plain").CopierFrom(b.Object("a").Key(k1)).Run(t.Context())
	require.NoError(t, err)
	assert.Empty(t, plain.CustomerKeySHA256)
	assert.Equal(t, []byte("aa"), readAll(t, b.Object("plain")))

	enc, err := b.Object("enc").Key(k2).CopierFrom(b.Object("a").Key(k1)).Run(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, enc.CustomerKeySHA256)
	assert.Equal(t, []byte("aa"), readAll(t, b.Object("enc").Key(k2)))

	comp, err := b.Object("c").Key(k1).ComposerFrom(b.Object("a"), b.Object("b")).Run(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, comp.CustomerKeySHA256)
	assert.Equal(t, []byte("aabb"), readAll(t, b.Object("c").Key(k1)))

	_, err = b.Object("c2").ComposerFrom(b.Object("a"), b.Object("b")).Run(t.Context())
	assert.Equal(t, http.StatusBadRequest, apiCode(err))
}

func TestXML_CustomerSuppliedKeyHeaders(t *testing.T) {
	t.Parallel()

	s := newSDK(t)
	s.bucket(t, "csek-xml", nil)

	key := csekKey(7)
	sum := sha256.Sum256(key)
	good := map[string]string{
		"X-Goog-Encryption-Algorithm":  "AES256",
		"X-Goog-Encryption-Key":        base64.StdEncoding.EncodeToString(key),
		"X-Goog-Encryption-Key-Sha256": base64.StdEncoding.EncodeToString(sum[:]),
	}
	badHash := maps.Clone(good)
	badHash["X-Goog-Encryption-Key-Sha256"] = base64.StdEncoding.EncodeToString(key)

	client := testclient.New(t)

	do := func(t *testing.T, method, name string, hdr map[string]string) *http.Response {
		t.Helper()

		req, err := http.NewRequestWithContext(t.Context(), method, s.url+"/csek-xml/"+name, strings.NewReader("d"))
		require.NoError(t, err)

		for k, v := range hdr {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		return resp
	}

	badAlg := map[string]string{"X-Goog-Encryption-Algorithm": "AES128"}
	partial := map[string]string{"X-Goog-Encryption-Key": good["X-Goog-Encryption-Key"]}

	tests := []struct {
		hdr  map[string]string
		name string
		want int
	}{
		{name: "bad algorithm", hdr: badAlg, want: http.StatusBadRequest},
		{name: "partial", hdr: partial, want: http.StatusBadRequest},
		{name: "bad hash", hdr: badHash, want: http.StatusBadRequest},
		{name: "good", hdr: good, want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name := "o-" + strings.ReplaceAll(tt.name, " ", "-")
			assert.Equal(t, tt.want, do(t, http.MethodPut, name, tt.hdr).StatusCode)
		})
	}

	t.Run("read", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, http.StatusOK, do(t, http.MethodPut, "read", good).StatusCode)
		assert.Equal(t, http.StatusBadRequest, do(t, http.MethodGet, "read", nil).StatusCode)
		assert.Equal(t, http.StatusOK, do(t, http.MethodHead, "read", nil).StatusCode)

		resp := do(t, http.MethodGet, "read", good)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "AES256", resp.Header.Get("X-Goog-Encryption-Algorithm"))
		assert.Equal(t, good["X-Goog-Encryption-Key-Sha256"], resp.Header.Get("X-Goog-Encryption-Key-Sha256"))
	})
}
