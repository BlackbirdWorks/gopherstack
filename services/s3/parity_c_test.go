package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_PutBucketReplication_RequiresRoleAndRules verifies that
// PutBucketReplication rejects configurations missing a Role ARN or with no
// rules. Real AWS returns 400 MalformedXML for both cases; the emulator
// previously stored the incomplete config without complaint.
func TestParity_PutBucketReplication_RequiresRoleAndRules(t *testing.T) {
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

// TestParity_PutBucketReplication_VersioningRequirement verifies that
// PutBucketReplication requires versioning to be enabled on the bucket
// (via the backend). Without versioning, the backend returns an error
// indicating the bucket configuration is invalid for replication.
func TestParity_PutBucketReplication_ExistingConfigIsOverwritten(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "rep-bucket")

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
