package s3control_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestRefinement1_StorageBackend_Interface(t *testing.T) {
	t.Parallel()

	// Compile-time assertion: var _ StorageBackend = (*InMemoryBackend)(nil) in interfaces.go
	// This test confirms the assertion file exists and compiles.
	var _ s3control.StorageBackend = s3control.NewInMemoryBackend()
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	assert.Equal(t, 100, s3control.HandlerOpsLen(h))
}

func TestRefinement1_Provider_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &s3control.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, s3control.ErrNilAppContext)
}

func TestRefinement1_Backend_Reset(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc1", BlockPublicAcls: true})
	b.AddBatchJobInternal("acc1", "arn:aws:iam::acc1:role/R", 5)

	require.Equal(t, 1, s3control.AccessBlockCount(b))
	require.Equal(t, 1, s3control.BatchJobCount(b))

	b.Reset()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
	assert.Equal(t, 0, s3control.BatchJobCount(b))
}

func TestRefinement1_Handler_Reset(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "x", BlockPublicAcls: true})

	require.Equal(t, 1, s3control.AccessBlockCount(b))

	h.Reset()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantID    string
	}{
		{name: "custom", accountID: "111122223333", wantID: "111122223333"},
		{name: "default", accountID: "000000000000", wantID: "000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig(tt.accountID, "us-east-1")
			assert.Equal(t, tt.wantID, b.AccountID())
		})
	}
}

func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestRefinement1_CreateJob_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{name: "empty_role_arn", roleArn: "", wantErr: true},
		{name: "valid_role_arn", roleArn: "arn:aws:iam::123:role/R", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			job, err := b.CreateJob("acc1", tt.roleArn, 5)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, s3control.ErrValidation)
				assert.Nil(t, job)
			} else {
				require.NoError(t, err)
				require.NotNil(t, job)
				assert.NotEmpty(t, job.JobID)
			}
		})
	}
}

func TestRefinement1_CreateAccessGrant_RequiresPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		permission string
		wantErr    bool
	}{
		{name: "empty_permission", permission: "", wantErr: true},
		{name: "read_permission", permission: "READ", wantErr: false},
		{name: "write_permission", permission: "WRITE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			grant, err := b.CreateAccessGrant("acc1", "loc1", "DIRECTORY_USER", "user@example.com", tt.permission, "")

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, s3control.ErrValidation)
				assert.Nil(t, grant)
			} else {
				require.NoError(t, err)
				require.NotNil(t, grant)
				assert.Equal(t, tt.permission, grant.Permission)
			}
		})
	}
}

func TestRefinement1_CreateAccessPoint_ShortAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
	}{
		{name: "short_id", accountID: "abc"},
		{name: "empty_id", accountID: ""},
		{name: "long_id", accountID: "123456789012"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			// Should not panic regardless of accountID length
			ap := b.CreateAccessPoint(tt.accountID, "my-ap", "my-bucket")
			assert.NotNil(t, ap)
			assert.NotEmpty(t, ap.Alias)
		})
	}
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	block := &s3control.PublicAccessBlock{AccountID: "a1", BlockPublicAcls: true}
	b.AddPublicAccessBlockInternal("a1", block)
	assert.Equal(t, 1, s3control.AccessBlockCount(b))

	inst := b.AddAccessGrantsInstanceInternal("a1", "arn:aws:sso:::instance/ins-1")
	require.NotNil(t, inst)
	assert.Equal(t, 1, s3control.AccessGrantsInstanceCount(b))

	grant := b.AddAccessGrantInternal("a1", "loc1", "DIRECTORY_USER", "u@x.com", "READ")
	require.NotNil(t, grant)
	assert.Equal(t, 1, s3control.AccessGrantCount(b))

	ap := b.AddAccessPointInternal("a1", "my-ap", "my-bucket")
	require.NotNil(t, ap)
	assert.Equal(t, 1, s3control.AccessPointCount(b))

	job := b.AddBatchJobInternal("a1", "arn:aws:iam::a1:role/R", 10)
	require.NotNil(t, job)
	assert.Equal(t, 1, s3control.BatchJobCount(b))
}

func TestRefinement1_ARNsUseConfiguredRegion(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackendWithConfig("123456789012", "ap-southeast-1")
	inst := b.CreateAccessGrantsInstance("123456789012", "")
	assert.Contains(t, inst.AccessGrantsInstanceArn, "ap-southeast-1")

	ap := b.CreateAccessPoint("123456789012", "my-ap", "my-bucket")
	assert.Contains(t, ap.AccessPointArn, "ap-southeast-1")
}

func TestRefinement1_SnapshotRestoreDeepCopy(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc1", BlockPublicAcls: true})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Mutate original after snapshot
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc2", BlockPublicAcls: false})
	assert.Equal(t, 2, s3control.AccessBlockCount(b))

	// Restore from snapshot
	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, s3control.AccessBlockCount(b2))
}

func TestRefinement1_Handler_CreateJob_BadRoleArn(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(t)
	rec := doRefinementRequest(
		t, h, http.MethodPost, "/v20180820/jobs",
		"123456789012",
		`<CreateJobRequest><Priority>5</Priority></CreateJobRequest>`,
	)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_Handler_CreateAccessGrant_EmptyPermission(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(t)
	body := `<CreateAccessGrantRequest>
<AccessGrantsLocationId>loc-1</AccessGrantsLocationId>
<Grantee><GranteeType>DIRECTORY_USER</GranteeType><GranteeIdentifier>user@example.com</GranteeIdentifier></Grantee>
</CreateAccessGrantRequest>`

	rec := doRefinementRequest(t, h, http.MethodPost, "/v20180820/accessgrantsinstance/grant", "123456789012", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
	assert.Equal(t, 0, s3control.AccessGrantsInstanceCount(b))
	assert.Equal(t, 0, s3control.AccessGrantCount(b))
	assert.Equal(t, 0, s3control.AccessGrantsLocationCount(b))
	assert.Equal(t, 0, s3control.AccessPointCount(b))
	assert.Equal(t, 0, s3control.ObjectLambdaAccessPointCount(b))
	assert.Equal(t, 0, s3control.OutpostsBucketCount(b))
	assert.Equal(t, 0, s3control.BatchJobCount(b))
	assert.Equal(t, 0, s3control.MRAPRequestCount(b))
	assert.Equal(t, 0, s3control.StorageLensGroupCount(b))

	b.CreateAccessGrantsInstance("a1", "")
	assert.Equal(t, 1, s3control.AccessGrantsInstanceCount(b))

	loc := b.CreateAccessGrantsLocation("a1", "s3://bucket", "arn:aws:iam::a1:role/R")
	require.NotNil(t, loc)
	assert.Equal(t, 1, s3control.AccessGrantsLocationCount(b))

	b.CreateAccessPoint("a1", "ap1", "bucket1")
	assert.Equal(t, 1, s3control.AccessPointCount(b))

	b.CreateAccessPointForObjectLambda("a1", "olap1")
	assert.Equal(t, 1, s3control.ObjectLambdaAccessPointCount(b))

	b.CreateBucket("a1", "outpost-bucket")
	assert.Equal(t, 1, s3control.OutpostsBucketCount(b))

	b.CreateMultiRegionAccessPoint("a1", "mrap1", "token1")
	assert.Equal(t, 1, s3control.MRAPRequestCount(b))

	b.CreateStorageLensGroup("a1", "group1")
	assert.Equal(t, 1, s3control.StorageLensGroupCount(b))
}

// --- helpers ---

func newRefinementHandler(t *testing.T) *s3control.Handler {
	t.Helper()

	return s3control.NewHandler(s3control.NewInMemoryBackend())
}

func doRefinementRequest(
	t *testing.T,
	h *s3control.Handler,
	method, path, accountID, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	return doS3ControlNewOpRequest(t, h, method, path, accountID, body)
}
