package ociobjectstorage_test

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sum(alg string, data []byte) []byte {
	switch alg {
	case "CRC32C":
		v := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	case "SHA256":
		s := sha256.Sum256(data)

		return s[:]
	default:
		s := sha512.Sum384(data)

		return s[:]
	}
}

func md5Sum(s string) []byte {
	h := md5.Sum([]byte(s))

	return h[:]
}

func sum64(alg string, data []byte) string { return base64.StdEncoding.EncodeToString(sum(alg, data)) }

func putChecksum(t *testing.T, e *env, alg, header, body string,
) (objectstorage.PutObjectResponse, error) {
	t.Helper()

	r := objectstorage.PutObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("ck"), ObjectName: new("obj-" + alg),
		ContentLength: new(int64(len(body))), PutObjectBody: io.NopCloser(strings.NewReader(body)),
		OpcChecksumAlgorithm: objectstorage.PutObjectOpcChecksumAlgorithmEnum(alg),
	}

	switch alg {
	case "CRC32C":
		r.OpcContentCrc32c = optStr(header)
	case "SHA256":
		r.OpcContentSha256 = optStr(header)
	case "SHA384":
		r.OpcContentSha384 = optStr(header)
	}

	return e.client.PutObject(t.Context(), r)
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func pick(alg string, c, s256, s384 *string) string {
	switch alg {
	case "CRC32C":
		return deref(c)
	case "SHA256":
		return deref(s256)
	default:
		return deref(s384)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

func TestObjectChecksums(t *testing.T) {
	t.Parallel()

	body := "checksum me"

	tests := []struct {
		name    string
		alg     string
		corrupt bool
		omit    bool
	}{
		{name: "crc32c supplied", alg: "CRC32C"},
		{name: "sha256 supplied", alg: "SHA256"},
		{name: "sha384 supplied", alg: "SHA384"},
		{name: "sha256 computed", alg: "SHA256", omit: true},
		{name: "crc32c mismatch", alg: "CRC32C", corrupt: true},
		{name: "sha384 mismatch", alg: "SHA384", corrupt: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkBucket(t, "ck", nil)

			want := sum64(tt.alg, []byte(body))
			header := want

			switch {
			case tt.omit:
				header = ""
			case tt.corrupt:
				header = sum64(tt.alg, []byte("other"))
			}

			put, err := putChecksum(t, e, tt.alg, header, body)
			if tt.corrupt {
				assert.Equal(t, "InvalidDigest", svcErr(t, err).GetCode())
				assert.Equal(t, 400, svcErr(t, err).GetHTTPStatusCode())

				_, gerr := e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
					NamespaceName: new(e.ns), BucketName: new("ck"), ObjectName: new("obj-" + tt.alg),
				})
				assert.Equal(t, 404, svcErr(t, gerr).GetHTTPStatusCode(), "rejected object must not be stored")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, want, pick(tt.alg, put.OpcContentCrc32c, put.OpcContentSha256, put.OpcContentSha384))

			head, err := e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
				NamespaceName: new(e.ns), BucketName: new("ck"), ObjectName: new("obj-" + tt.alg),
			})
			require.NoError(t, err)
			assert.Equal(t, want, pick(tt.alg, head.OpcContentCrc32c, head.OpcContentSha256, head.OpcContentSha384))

			got, err := e.client.GetObject(t.Context(), objectstorage.GetObjectRequest{
				NamespaceName: new(e.ns), BucketName: new("ck"), ObjectName: new("obj-" + tt.alg),
			})
			require.NoError(t, err)

			defer got.Content.Close()

			assert.Equal(t, want, pick(tt.alg, got.OpcContentCrc32c, got.OpcContentSha256, got.OpcContentSha384))
		})
	}
}

func TestPutWithoutAlgorithmHasNoChecksumHeaders(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "ck", nil)
	e.put(t, "ck", "plain", "x")

	head, err := e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("ck"), ObjectName: new("plain"),
	})
	require.NoError(t, err)
	assert.Nil(t, head.OpcContentCrc32c)
	assert.Nil(t, head.OpcContentSha256)
}

func startUpload(t *testing.T, e *env, object string, alg objectstorage.CreateMultipartUploadOpcChecksumAlgorithmEnum,
) string {
	t.Helper()

	up, err := e.client.CreateMultipartUpload(t.Context(), objectstorage.CreateMultipartUploadRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), OpcChecksumAlgorithm: alg,
		Object: new(object),
	})
	require.NoError(t, err)

	return *up.UploadId
}

func uploadPart(t *testing.T, e *env, object, id string, num int, body string) (*string, error) {
	t.Helper()

	r, err := e.client.UploadPart(t.Context(), objectstorage.UploadPartRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new(object), UploadId: new(id),
		UploadPartNum: new(num), ContentLength: new(int64(len(body))),
		UploadPartBody: io.NopCloser(strings.NewReader(body)),
	})
	if err != nil {
		return nil, err
	}

	return r.ETag, nil
}

func commitParts(t *testing.T, e *env, object, id string, etags []string,
) (objectstorage.CommitMultipartUploadResponse, error) {
	t.Helper()

	parts := make([]objectstorage.CommitMultipartUploadPartDetails, 0, len(etags))
	for i, et := range etags {
		parts = append(parts, objectstorage.CommitMultipartUploadPartDetails{PartNum: new(i + 1), Etag: new(et)})
	}

	return e.client.CommitMultipartUpload(t.Context(), objectstorage.CommitMultipartUploadRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new(object), UploadId: new(id),
		PartsToCommit: parts,
	})
}

func TestMultipartRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetMinPartSize(4)
	e.mkBucket(t, "mp", nil)

	id := startUpload(t, e, "rules", "")
	small, err := uploadPart(t, e, "rules", id, 1, "AAA")
	require.NoError(t, err)

	last, err := uploadPart(t, e, "rules", id, 2, "B")
	require.NoError(t, err)

	_, err = commitParts(t, e, "rules", id, []string{*small, *last})
	require.Error(t, err)
	assert.Equal(t, "InvalidUploadPart", svcErr(t, err).GetCode(), "non-last part below the minimum")

	full, err := uploadPart(t, e, "rules", id, 1, "AAAA")
	require.NoError(t, err)

	_, err = commitParts(t, e, "rules", id, []string{*small, *last})
	assert.Equal(t, "InvalidUploadPart", svcErr(t, err).GetCode(), "overwritten part's old etag is stale")

	done, err := commitParts(t, e, "rules", id, []string{*full, *last})
	require.NoError(t, err)

	raw1, raw2 := md5Sum("AAAA"), md5Sum("B")
	want := base64.StdEncoding.EncodeToString(md5Sum(string(raw1)+string(raw2))) + "-2"
	assert.Equal(t, want, *done.OpcMultipartMd5)
	assert.Equal(t, "AAAAB", e.get(t, "mp", "rules"))

	single := startUpload(t, e, "one", "")
	only, err := uploadPart(t, e, "one", single, 1, "x")
	require.NoError(t, err)

	_, err = commitParts(t, e, "one", single, []string{*only})
	require.NoError(t, err, "a lone part is the last part")
}

func TestMultipartCompositeChecksums(t *testing.T) {
	t.Parallel()

	parts := []string{"first part!", "second", "3rd"}
	whole := strings.Join(parts, "")

	tests := []struct {
		name string
		alg  string
	}{
		{name: "crc32c whole object", alg: "CRC32C"},
		{name: "sha256 composite", alg: "SHA256"},
		{name: "sha384 composite", alg: "SHA384"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkBucket(t, "mp", nil)

			id := startUpload(t, e, "obj", objectstorage.CreateMultipartUploadOpcChecksumAlgorithmEnum(tt.alg))
			etags := make([]string, len(parts))

			concat := make([]byte, 0, 64)

			for i, p := range parts {
				et, err := uploadPart(t, e, "obj", id, i+1, p)
				require.NoError(t, err)

				etags[i] = *et
				concat = append(concat, sum(tt.alg, []byte(p))...)
			}

			done, err := commitParts(t, e, "obj", id, etags)
			require.NoError(t, err)

			head, err := e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
				NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("obj"),
			})
			require.NoError(t, err)

			switch tt.alg {
			case "CRC32C":
				assert.Equal(t, sum64(tt.alg, []byte(whole)), deref(done.OpcContentCrc32c))
				assert.Equal(t, sum64(tt.alg, []byte(whole)), deref(head.OpcContentCrc32c))
			case "SHA256":
				want := sum64(tt.alg, concat) + "-" + strconv.Itoa(len(parts))
				assert.Equal(t, want, deref(done.OpcMultipartSha256))
				assert.Equal(t, want, deref(head.OpcMultipartSha256))
				assert.Nil(t, head.OpcContentSha256, "no whole-object SHA for multipart")
			default:
				want := sum64(tt.alg, concat) + "-" + strconv.Itoa(len(parts))
				assert.Equal(t, want, deref(done.OpcMultipartSha384))
				assert.Equal(t, want, deref(head.OpcMultipartSha384))
			}
		})
	}
}

func TestMultipartPartChecksumMismatchAndMixedAlgorithms(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "mp", nil)

	id := startUpload(t, e, "obj", objectstorage.CreateMultipartUploadOpcChecksumAlgorithmSha256)

	_, err := e.client.UploadPart(t.Context(), objectstorage.UploadPartRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("obj"), UploadId: new(id),
		UploadPartNum: new(1), ContentLength: new(int64(1)), UploadPartBody: io.NopCloser(strings.NewReader("a")),
		OpcChecksumAlgorithm: objectstorage.UploadPartOpcChecksumAlgorithmSha256,
		OpcContentSha256:     new(sum64("SHA256", []byte("zzz"))),
	})
	assert.Equal(t, "InvalidDigest", svcErr(t, err).GetCode())

	first, err := uploadPart(t, e, "obj", id, 1, "a")
	require.NoError(t, err)

	r, err := e.client.UploadPart(t.Context(), objectstorage.UploadPartRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("obj"), UploadId: new(id),
		UploadPartNum: new(2), ContentLength: new(int64(1)), UploadPartBody: io.NopCloser(strings.NewReader("b")),
		OpcChecksumAlgorithm: objectstorage.UploadPartOpcChecksumAlgorithmCrc32c,
	})
	require.NoError(t, err)
	assert.Equal(t, sum64("CRC32C", []byte("b")), deref(r.OpcContentCrc32c))

	_, err = commitParts(t, e, "obj", id, []string{*first, *r.ETag})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}
