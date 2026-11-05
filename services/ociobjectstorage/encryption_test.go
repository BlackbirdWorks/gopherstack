package ociobjectstorage_test

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
)

type cust struct{ key, sha string }

func newCust(seed byte) cust {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = seed
	}

	h := sha256.Sum256(raw)

	return cust{key: base64.StdEncoding.EncodeToString(raw), sha: base64.StdEncoding.EncodeToString(h[:])}
}

func (c cust) details() *objectstorage.SseCustomerKeyDetails {
	return &objectstorage.SseCustomerKeyDetails{
		Algorithm: objectstorage.SseCustomerKeyDetailsAlgorithmAes256, Key: new(c.key), KeySha256: new(c.sha),
	}
}

func putSSE(t *testing.T, e *env, name, body string, c cust) error {
	t.Helper()

	_, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new(name),
		ContentLength: new(int64(len(body))), PutObjectBody: io.NopCloser(strings.NewReader(body)),
		OpcSseCustomerAlgorithm: new("AES256"), OpcSseCustomerKey: new(c.key), OpcSseCustomerKeySha256: new(c.sha),
	})

	return err
}

func getSSE(t *testing.T, e *env, name string, c *cust) (string, error) {
	t.Helper()

	r := objectstorage.GetObjectRequest{NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new(name)}
	if c != nil {
		r.OpcSseCustomerAlgorithm, r.OpcSseCustomerKey, r.OpcSseCustomerKeySha256 =
			new("AES256"), new(c.key), new(c.sha)
	}

	resp, err := e.client.GetObject(t.Context(), r)
	if err != nil {
		return "", err
	}

	defer resp.Content.Close()

	b, err := io.ReadAll(resp.Content)

	return string(b), err
}

func TestSSECustomerKey(t *testing.T) {
	t.Parallel()

	right, wrong := newCust(1), newCust(2)
	badHash := cust{key: right.key, sha: wrong.sha}

	tests := []struct {
		key      *cust
		name     string
		wantCode string
		wantBody string
	}{
		{name: "right key", key: &right, wantBody: "secret"},
		{name: "wrong key", key: &wrong, wantCode: "InvalidSseCustomerKey"},
		{name: "no key", wantCode: "InvalidSseCustomerKey"},
		{name: "key and hash disagree", key: &badHash, wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkBucket(t, "sse", nil)
			require.NoError(t, putSSE(t, e, "o", "secret", right))

			body, err := getSSE(t, e, "o", tt.key)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
				assert.Equal(t, 400, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantBody, body)
		})
	}
}

func TestSSECustomerKeyHeadCopyAndInvalidPut(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "sse", nil)

	c := newCust(7)
	require.NoError(t, putSSE(t, e, "o", "secret", c))

	_, err := e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new("o"),
	})
	assert.Equal(t, 400, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.HeadObject(t.Context(), objectstorage.HeadObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new("o"),
		OpcSseCustomerAlgorithm: new("AES256"), OpcSseCustomerKey: new(c.key), OpcSseCustomerKeySha256: new(c.sha),
	})
	require.NoError(t, err)

	copyReq := func(src *cust, dest *cust, name string) error {
		r := objectstorage.CopyObjectRequest{
			NamespaceName: new(e.ns), BucketName: new("sse"),
			SourceObjectName: new("o"), DestinationRegion: new("us-ashburn-1"), DestinationNamespace: new(e.ns),
			DestinationBucket: new("sse"), DestinationObjectName: new(name),
		}
		if src != nil {
			r.OpcSourceSseCustomerAlgorithm, r.OpcSourceSseCustomerKey, r.OpcSourceSseCustomerKeySha256 =
				new("AES256"), new(src.key), new(src.sha)
		}

		if dest != nil {
			r.OpcSseCustomerAlgorithm, r.OpcSseCustomerKey, r.OpcSseCustomerKeySha256 =
				new("AES256"), new(dest.key), new(dest.sha)
		}

		_, cerr := e.client.CopyObject(t.Context(), r)

		return cerr
	}

	assert.Equal(t, 400, svcErr(t, copyReq(nil, nil, "no-src-key")).GetHTTPStatusCode())

	d := newCust(9)
	require.NoError(t, copyReq(&c, &d, "reencrypted-copy"))

	body, err := getSSE(t, e, "reencrypted-copy", &d)
	require.NoError(t, err)
	assert.Equal(t, "secret", body)

	_, err = getSSE(t, e, "reencrypted-copy", &c)
	assert.Equal(t, "InvalidSseCustomerKey", svcErr(t, err).GetCode())

	require.NoError(t, copyReq(&c, nil, "plain-copy"))
	assert.Equal(t, "secret", e.get(t, "sse", "plain-copy"))

	bad := objectstorage.PutObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new("x"),
		ContentLength: new(int64(1)), PutObjectBody: io.NopCloser(strings.NewReader("x")),
		OpcSseCustomerAlgorithm: new("AES128"), OpcSseCustomerKey: new(c.key), OpcSseCustomerKeySha256: new(c.sha),
	}
	_, err = e.client.PutObject(t.Context(), bad)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestSSECustomerKeyMultipart(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "mp", nil)
	e.mkBucket(t, "sse", nil)

	c, other := newCust(3), newCust(4)
	up, err := e.client.CreateMultipartUpload(t.Context(), objectstorage.CreateMultipartUploadRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"),
		OpcSseCustomerAlgorithm: new("AES256"), OpcSseCustomerKey: new(c.key), OpcSseCustomerKeySha256: new(c.sha),
		Object: new("big"),
	})
	require.NoError(t, err)

	part := func(k cust) (*string, error) {
		r, perr := e.client.UploadPart(t.Context(), objectstorage.UploadPartRequest{
			NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("big"), UploadId: up.UploadId,
			UploadPartNum: new(1), ContentLength: new(int64(3)), UploadPartBody: io.NopCloser(strings.NewReader("abc")),
			OpcSseCustomerAlgorithm: new("AES256"), OpcSseCustomerKey: new(k.key), OpcSseCustomerKeySha256: new(k.sha),
		})
		if perr != nil {
			return nil, perr
		}

		return r.ETag, nil
	}

	_, err = part(other)
	assert.Equal(t, "InvalidSseCustomerKey", svcErr(t, err).GetCode())

	et, err := part(c)
	require.NoError(t, err)

	_, err = e.client.CommitMultipartUpload(t.Context(), objectstorage.CommitMultipartUploadRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("big"), UploadId: up.UploadId,
		PartsToCommit: []objectstorage.CommitMultipartUploadPartDetails{{PartNum: new(1), Etag: et}},
	})
	require.NoError(t, err)

	_, err = e.client.GetObject(t.Context(), objectstorage.GetObjectRequest{
		NamespaceName: new(e.ns), BucketName: new("mp"), ObjectName: new("big"),
	})
	assert.Equal(t, "InvalidSseCustomerKey", svcErr(t, err).GetCode())
}

func TestKMSKeyValidationAndReencrypt(t *testing.T) {
	t.Parallel()

	const (
		enabled  = "ocid1.key.oc1..enabled"
		disabled = "ocid1.key.oc1..disabled"
		second   = "ocid1.key.oc1..second"
	)

	e := newEnv(t, envOpts{})
	e.backend.SetKeyChecker(func(id string) (string, bool) {
		return map[string]string{enabled: "ENABLED", second: "ENABLED", disabled: "DISABLED"}[id],
			id == enabled || id == disabled || id == second
	})
	e.mkBucket(t, "sse", func(d *objectstorage.CreateBucketDetails) { d.KmsKeyId = new(enabled) })

	put := func(name, key string) error {
		_, err := e.client.PutObject(t.Context(), objectstorage.PutObjectRequest{
			NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new(name),
			ContentLength: new(int64(1)), PutObjectBody: io.NopCloser(strings.NewReader("x")),
			OpcSseKmsKeyId: optStr(key),
		})

		return err
	}

	assert.Equal(t, 404, svcErr(t, put("a", "ocid1.key.oc1..missing")).GetHTTPStatusCode())
	assert.Equal(t, "InvalidParameter", svcErr(t, put("a", disabled)).GetCode())
	require.NoError(t, put("a", second))
	require.NoError(t, put("b", ""))

	keyOf := func(name string) string {
		o, err := e.backend.GetObject(e.ns, "sse", name, "", oci.Preconds{}, false)
		require.NoError(t, err)

		return o.KmsKeyID
	}

	assert.Equal(t, second, keyOf("a"))
	assert.Equal(t, enabled, keyOf("b"), "bucket key applies when none is given")

	reencrypt := func(name string, d objectstorage.ReencryptObjectDetails) error {
		_, err := e.client.ReencryptObject(t.Context(), objectstorage.ReencryptObjectRequest{
			NamespaceName: new(e.ns), BucketName: new("sse"), ObjectName: new(name), ReencryptObjectDetails: d,
		})

		return err
	}

	require.NoError(t, reencrypt("b", objectstorage.ReencryptObjectDetails{KmsKeyId: new(second)}))
	assert.Equal(t, second, keyOf("b"))
	assert.Equal(
		t,
		404,
		svcErr(t, reencrypt("b", objectstorage.ReencryptObjectDetails{KmsKeyId: new("nope")})).GetHTTPStatusCode(),
	)
	assert.Equal(t, 404, svcErr(t, reencrypt("zz", objectstorage.ReencryptObjectDetails{})).GetHTTPStatusCode())

	require.NoError(t, reencrypt("a", objectstorage.ReencryptObjectDetails{}))
	assert.Equal(t, enabled, keyOf("a"), "empty payload falls back to the bucket key")

	c := newCust(5)
	require.NoError(t, reencrypt("a", objectstorage.ReencryptObjectDetails{SseCustomerKey: c.details()}))

	_, err := getSSE(t, e, "a", nil)
	assert.Equal(t, "InvalidSseCustomerKey", svcErr(t, err).GetCode())

	body, err := getSSE(t, e, "a", &c)
	require.NoError(t, err)
	assert.Equal(t, "x", body)

	err = reencrypt("a", objectstorage.ReencryptObjectDetails{KmsKeyId: new(second)})
	assert.Equal(t, "InvalidSseCustomerKey", svcErr(t, err).GetCode(), "source key required to leave SSE-C")

	require.NoError(t, reencrypt("a", objectstorage.ReencryptObjectDetails{
		KmsKeyId: new(second), SourceSseCustomerKey: c.details(),
	}))
	assert.Equal(t, second, keyOf("a"))
	assert.Equal(t, "x", e.get(t, "sse", "a"))
}

func TestReencryptBucket(t *testing.T) {
	t.Parallel()

	const key = "ocid1.key.oc1..bucketkey"

	e := newEnv(t, envOpts{})
	e.mkBucket(t, "sse", nil)
	e.put(t, "sse", "o", "x")

	c := newCust(6)
	require.NoError(t, putSSE(t, e, "ssec", "y", c))

	_, err := e.client.UpdateBucket(t.Context(), objectstorage.UpdateBucketRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"),
		KmsKeyId: new(key),
	})
	require.NoError(t, err)

	r, err := e.client.ReencryptBucket(t.Context(), objectstorage.ReencryptBucketRequest{
		NamespaceName: new(e.ns), BucketName: new("sse"),
	})
	require.NoError(t, err)

	wr, err := e.client.GetWorkRequest(
		t.Context(),
		objectstorage.GetWorkRequestRequest{WorkRequestId: r.OpcWorkRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, objectstorage.WorkRequestOperationTypeReencrypt, wr.OperationType)
	assert.Equal(t, objectstorage.WorkRequestStatusCompleted, wr.Status)

	o, err := e.backend.GetObject(e.ns, "sse", "o", "", oci.Preconds{}, false)
	require.NoError(t, err)
	assert.Equal(t, key, o.KmsKeyID)

	sc, err := e.backend.GetObjectWith(e.ns, "sse", "ssec", "", oci.Preconds{}, false, oci.SSEKey{
		Algorithm: "AES256", Key: c.key, KeySha256: c.sha,
	})
	require.NoError(t, err)
	assert.Empty(t, sc.KmsKeyID, "SSE-C objects keep their customer key")

	_, err = e.client.ReencryptBucket(t.Context(), objectstorage.ReencryptBucketRequest{
		NamespaceName: new(e.ns), BucketName: new("missing"),
	})
	assert.Equal(t, 404, svcErr(t, err).GetHTTPStatusCode())
}
