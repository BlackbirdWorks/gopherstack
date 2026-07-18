package s3_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutBucketReplication_RequiresRoleAndRules verifies that
// PutBucketReplication rejects configurations missing a Role ARN or with no
// rules. Real AWS returns 400 MalformedXML for both cases.
func TestPutBucketReplication_RequiresRoleAndRules(t *testing.T) {
	t.Parallel()

	const validCfg = `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::000000000000:role/Repl</Role>` +
		`<Rule>` +
		`<Status>Enabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	const noRoleCfg = `<ReplicationConfiguration>` +
		`<Rule>` +
		`<Status>Enabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::dst</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	const noRulesCfg = `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::000000000000:role/Repl</Role>` +
		`</ReplicationConfiguration>`

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "missing_role_rejected",
			body:     noRoleCfg,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_rules_rejected",
			body:     noRulesCfg,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid_config_accepted",
			body:     validCfg,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "src-bucket")
			// Real S3 requires versioning enabled before a replication config
			// can be applied.
			enableVersioning(t, handler, "src-bucket")

			req := httptest.NewRequest(
				http.MethodPut,
				"/src-bucket?replication",
				strings.NewReader(tt.body),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code,
				"PutBucketReplication status for case %q", tt.name)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "MalformedXML",
					"expected MalformedXML error code")
			}
		})
	}
}

// TestPutBucketReplication_ExistingConfigIsOverwritten verifies that a second
// PutBucketReplication call replaces the previously stored configuration.
func TestPutBucketReplication_ExistingConfigIsOverwritten(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "rep-bucket")
	// Real S3 requires versioning enabled before a replication config can be
	// applied.
	enableVersioning(t, handler, "rep-bucket")

	cfg1 := `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::000000000000:role/R1</Role>` +
		`<Rule>` +
		`<Status>Enabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::dst1</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	cfg2 := `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::000000000000:role/R2</Role>` +
		`<Rule>` +
		`<Status>Enabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::dst2</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	req1 := httptest.NewRequest(http.MethodPut, "/rep-bucket?replication", strings.NewReader(cfg1))
	rec1 := httptest.NewRecorder()
	serveS3Handler(handler, rec1, req1)
	require.Equal(t, http.StatusOK, rec1.Code)

	req2 := httptest.NewRequest(http.MethodPut, "/rep-bucket?replication", strings.NewReader(cfg2))
	rec2 := httptest.NewRecorder()
	serveS3Handler(handler, rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/rep-bucket?replication", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "R2", "second config should overwrite first")
	assert.NotContains(t, getRec.Body.String(), "R1", "first config should be gone")
}

// TestS3BucketReplication_PutObjectReplicates verifies that PutObject on a
// bucket with an active replication rule asynchronously copies the object to
// the destination bucket.
func TestS3BucketReplication_PutObjectReplicates(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-src-bucket"
	dst := "repl-dst-bucket"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "create bucket %s", b)
	}

	// Real S3 requires versioning enabled on the source before replication.
	enableVersioning(t, handler, src)

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>r1</ID>
  <Status>Enabled</Status>
  <Prefix></Prefix>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// PUT an object into the source bucket.
	body := []byte("hello-replication")
	req = httptest.NewRequest(http.MethodPut, "/"+src+"/test.txt", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Allow the async goroutine to run.
	testKey := "test.txt"
	require.Eventually(t, func() bool {
		_, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: &dst,
			Key:    &testKey,
		})

		return err == nil
	}, 3*time.Second, 50*time.Millisecond, "replicated object should appear in destination bucket")
}

// TestS3BucketReplication_PrefixFilter verifies that only keys matching the
// replication rule's prefix filter are replicated.
func TestS3BucketReplication_PrefixFilter(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-prefix-src"
	dst := "repl-prefix-dst"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Real S3 requires versioning enabled on the source before replication.
	enableVersioning(t, handler, src)

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>prefix-rule</ID>
  <Status>Enabled</Status>
  <Prefix>images/</Prefix>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		key      string
		wantRepl bool
	}{
		{"images/photo.jpg", true},
		{"documents/report.pdf", false},
	}

	for _, tt := range tests {
		req = httptest.NewRequest(http.MethodPut, "/"+src+"/"+tt.key, strings.NewReader("data"))
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Wait for the replicated key to appear, then verify the non-replicated one is absent.
	imgKey := "images/photo.jpg"
	require.Eventually(t, func() bool {
		_, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{Bucket: &dst, Key: &imgKey})

		return err == nil
	}, 3*time.Second, 50*time.Millisecond, "images/photo.jpg should be replicated to destination")

	docKey := "documents/report.pdf"
	_, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{Bucket: &dst, Key: &docKey})
	assert.Error(t, err, "documents/report.pdf should NOT be replicated (prefix filter)")
}

// TestS3BucketReplication_DeleteMarker verifies that deleting an object from
// a versioned, replication-configured source bucket propagates a delete
// marker to the destination bucket when DeleteMarkerReplication is enabled.
func TestS3BucketReplication_DeleteMarker(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-del-src"
	dst := "repl-del-dst"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Enable versioning on both buckets so delete creates a delete marker.
	for _, b := range []string{src, dst} {
		vxml := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
		req := httptest.NewRequest(http.MethodPut, "/"+b+"?versioning", strings.NewReader(vxml))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Put the object into both buckets BEFORE enabling replication so that the
	// replication goroutines spawned by these PUTs see an empty config and
	// return immediately. DrainReplicationGoroutines ensures those goroutines
	// finish before we set the config, preventing a race where a stale
	// goroutine replicates the live object on top of a later delete marker.
	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b+"/note.txt", strings.NewReader("content"))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}
	bk.DrainReplicationGoroutines()

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>dmr-rule</ID>
  <Status>Enabled</Status>
  <Prefix></Prefix>
  <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete from source — should trigger delete-marker replication to dest.
	req = httptest.NewRequest(http.MethodDelete, "/"+src+"/note.txt", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	noteKey := "note.txt"
	require.Eventually(t, func() bool {
		out, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: &dst,
			Key:    &noteKey,
		})
		if err != nil {
			return true // key was deleted
		}
		_ = out.Body.Close()

		return false
	}, 3*time.Second, 50*time.Millisecond, "delete marker should propagate to destination")
}

func TestS3BucketReplicationCRUD(t *testing.T) {
	t.Parallel()

	replicationXML := `<ReplicationConfiguration>` +
		`<Role>arn:aws:iam::123456789012:role/replication-role</Role>` +
		`<Rule>` +
		`<ID>rule1</ID>` +
		`<Status>Enabled</Status>` +
		`<Prefix></Prefix>` +
		`<Destination><Bucket>arn:aws:s3:::dest-bucket</Bucket></Destination>` +
		`</Rule>` +
		`</ReplicationConfiguration>`

	tests := []struct {
		name       string
		configXML  string
		wantBody   string
		wantPut    int
		wantGet    int
		wantDelete int
	}{
		{
			name:       "valid-replication",
			configXML:  replicationXML,
			wantPut:    http.StatusOK,
			wantGet:    http.StatusOK,
			wantDelete: http.StatusNoContent,
			wantBody:   "dest-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "replication-test-" + tt.name

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			// Real S3 requires versioning enabled before replication config.
			enableVersioning(t, handler, bucket)

			// GetBucketReplication before put → 404
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			assert.Contains(t, rec.Body.String(), "ReplicationConfigurationNotFoundError")

			// PutBucketReplication
			req = httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?replication",
				strings.NewReader(tt.configXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketReplication
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			// DeleteBucketReplication
			req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantDelete, rec.Code)

			// GetBucketReplication after delete → 404
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?replication", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestS3BucketReplication_MalformedXML verifies that PutBucketReplication rejects invalid XML.

func TestS3BucketReplication_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "replication-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?replication",
		strings.NewReader("not-valid-xml"),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3NewOperations_GetSupportedOperations verifies that all new operations are listed.
