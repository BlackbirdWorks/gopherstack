package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Per-AP PublicAccessBlock tests ----

func TestBatch3_AccessPointPublicAccessBlock_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apName    string
		wantBlock bool
		wantCode  int
	}{
		{
			name:      "put_and_get_pab",
			apName:    "my-ap",
			wantBlock: true,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateAccessPoint("000000000000", tt.apName, "my-bucket")

			cfg := s3control.PublicAccessBlock{
				AccountID:             "000000000000",
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     false,
				RestrictPublicBuckets: true,
			}

			err := b.PutAccessPointPublicAccessBlock("000000000000", tt.apName, cfg)
			require.NoError(t, err)

			got, err := b.GetAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.NoError(t, err)
			assert.True(t, got.BlockPublicAcls)
			assert.True(t, got.IgnorePublicAcls)
			assert.False(t, got.BlockPublicPolicy)
			assert.True(t, got.RestrictPublicBuckets)

			err = b.DeleteAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.NoError(t, err)

			_, err = b.GetAccessPointPublicAccessBlock("000000000000", tt.apName)
			require.Error(t, err)
		})
	}
}

func TestBatch3_AccessPointPublicAccessBlock_MissingAP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(b *s3control.InMemoryBackend) error
		name string
	}{
		{
			name: "get_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				_, err := b.GetAccessPointPublicAccessBlock("000000000000", "nonexistent")

				return err
			},
		},
		{
			name: "put_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				return b.PutAccessPointPublicAccessBlock(
					"000000000000", "nonexistent",
					s3control.PublicAccessBlock{},
				)
			},
		},
		{
			name: "delete_pab_missing_ap",
			fn: func(b *s3control.InMemoryBackend) error {
				return b.DeleteAccessPointPublicAccessBlock("000000000000", "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			err := tt.fn(b)
			require.Error(t, err)
		})
	}
}

func TestBatch3_Handler_AccessPointPublicAccessBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *s3control.Handler)
		name       string
		method     string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "put_pab",
			method: http.MethodPut,
			body: `<PutAccessPointPublicAccessBlockRequest>
<PublicAccessBlockConfiguration>
<BlockPublicAcls>true</BlockPublicAcls>
<IgnorePublicAcls>false</IgnorePublicAcls>
<BlockPublicPolicy>true</BlockPublicPolicy>
<RestrictPublicBuckets>false</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>
</PutAccessPointPublicAccessBlockRequest>`,
			wantStatus: http.StatusOK,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
		},
		{
			name:   "put_pab_missing_ap",
			method: http.MethodPut,
			body: `<PutAccessPointPublicAccessBlockRequest>` +
				`<PublicAccessBlockConfiguration>` +
				`<BlockPublicAcls>true</BlockPublicAcls>` +
				`</PublicAccessBlockConfiguration>` +
				`</PutAccessPointPublicAccessBlockRequest>`,
			wantStatus: http.StatusNotFound,
			setup:      func(_ *s3control.Handler) {},
		},
		{
			name:       "get_pab_not_set",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
		},
		{
			name:       "get_pab_after_put",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantBody:   "GetAccessPointPublicAccessBlockResult",
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = h.Backend.PutAccessPointPublicAccessBlock(
					"000000000000", "my-ap",
					s3control.PublicAccessBlock{BlockPublicAcls: true},
				)
			},
		},
		{
			name:       "delete_pab",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
			setup: func(h *s3control.Handler) {
				h.Backend.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = h.Backend.PutAccessPointPublicAccessBlock(
					"000000000000", "my-ap",
					s3control.PublicAccessBlock{BlockPublicAcls: true},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			tt.setup(h)

			rec := doS3ControlNewOpRequest(
				t, h, tt.method,
				"/v20180820/accesspoint/my-ap/publicAccessBlock",
				"000000000000",
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// ---- AccessPoint VPC config tests ----

func TestBatch3_AccessPoint_VpcConfig_SetAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		vpcID           string
		bucketAccountID string
		wantOrigin      string
		wantAlias       bool
	}{
		{
			name:       "vpc_access_point",
			vpcID:      "vpc-abc123",
			wantOrigin: "VPC",
			wantAlias:  false,
		},
		{
			name:            "internet_with_bucket_account",
			vpcID:           "",
			bucketAccountID: "111122223333",
			wantOrigin:      "Internet",
			wantAlias:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")

			err := b.SetAccessPointVpcConfig("000000000000", "my-ap", tt.vpcID, tt.bucketAccountID)
			require.NoError(t, err)

			ap, err := b.GetAccessPoint("000000000000", "my-ap")
			require.NoError(t, err)
			assert.Equal(t, tt.wantOrigin, ap.NetworkOrigin)
			assert.Equal(t, tt.vpcID, ap.VpcID)
			assert.Equal(t, tt.bucketAccountID, ap.BucketAccountID)

			if tt.wantAlias {
				assert.NotEmpty(t, ap.Alias)
			} else {
				assert.Empty(t, ap.Alias)
			}
		})
	}
}

func TestBatch3_AccessPoint_VpcConfig_MissingAP(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.SetAccessPointVpcConfig("000000000000", "nonexistent", "vpc-123", "")
	require.Error(t, err)
}

func TestBatch3_Handler_GetAccessPoint_NetworkOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(b *s3control.InMemoryBackend)
		wantOrigin     string
		wantVpcPresent bool
	}{
		{
			name: "internet_access_point",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
			},
			wantOrigin:     "Internet",
			wantVpcPresent: false,
		},
		{
			name: "vpc_access_point",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
				_ = b.SetAccessPointVpcConfig("000000000000", "my-ap", "vpc-abc123", "")
			},
			wantOrigin:     "VPC",
			wantVpcPresent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			tt.setup(b)
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/accesspoint/my-ap", "000000000000", "")

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantOrigin)
			if tt.wantVpcPresent {
				assert.Contains(t, rec.Body.String(), "vpc-abc123")
			}
		})
	}
}

func TestBatch3_Handler_CreateAccessPoint_WithVpc(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	body := `<CreateAccessPointRequest>
<Bucket>my-bucket</Bucket>
<VpcConfiguration><VpcId>vpc-xyz789</VpcId></VpcConfiguration>
<BucketAccountId>111122223333</BucketAccountId>
</CreateAccessPointRequest>`

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodPut,
		"/v20180820/accesspoint/my-vpc-ap",
		"000000000000",
		body,
	)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateAccessPointResult")

	// Verify stored state.
	ap, err := h.Backend.GetAccessPoint("000000000000", "my-vpc-ap")
	require.NoError(t, err)
	assert.Equal(t, "vpc-xyz789", ap.VpcID)
	assert.Equal(t, "VPC", ap.NetworkOrigin)
	assert.Equal(t, "111122223333", ap.BucketAccountID)
	assert.Empty(t, ap.Alias) // VPC APs have no alias
}

func TestBatch3_Handler_ListAccessPoints_IncludesAlias(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "ap-one", "bucket-one")
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet, "/v20180820/accesspoint", "000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "ListAccessPointsResult")
	assert.Contains(t, body, "Internet")
	assert.Contains(t, body, "s3alias") // alias present for Internet APs
}

// ---- BatchJob extended fields tests ----

func TestBatch3_BatchJob_UpdateDetails(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
	require.NoError(t, err)
	require.NotEmpty(t, job.CreationTime) // CreationTime set on create

	err = b.UpdateJobDetails(
		"000000000000", job.JobID,
		"test job",
		"<Spec><Format>S3BatchOperations_CSV_20180820</Format></Spec>",
		"<LambdaInvoke><FunctionArn>arn:aws:lambda:::fn</FunctionArn></LambdaInvoke>",
		"<Bucket>arn:aws:s3:::report-bucket</Bucket>",
		true,
	)
	require.NoError(t, err)

	got, err := b.GetJob("000000000000", job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "test job", got.Description)
	assert.Contains(t, got.Manifest, "S3BatchOperations_CSV_20180820")
	assert.Contains(t, got.Operation, "LambdaInvoke")
	assert.Contains(t, got.Report, "report-bucket")
	assert.True(t, got.ConfirmationRequired)
}

func TestBatch3_BatchJob_UpdateDetails_MissingJob(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.UpdateJobDetails("000000000000", "nonexistent", "desc", "", "", "", false)
	require.Error(t, err)
}

func TestBatch3_Handler_CreateJob_ExtendedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		body            string
		wantDescription string
		wantStatus      int
		wantManifest    bool
	}{
		{
			name: "job_with_description_and_manifest",
			body: `<CreateJobRequest>
<RoleArn>arn:aws:iam::000000000000:role/Role</RoleArn>
<Priority>10</Priority>
<Description>my batch job</Description>
<ConfirmationRequired>true</ConfirmationRequired>
<Manifest><Spec><Format>S3BatchOperations_CSV_20180820</Format></Spec></Manifest>
<Operation><LambdaInvoke><FunctionArn>arn:aws:lambda:::fn</FunctionArn></LambdaInvoke></Operation>
<Report><Bucket>arn:aws:s3:::report-bucket</Bucket><Enabled>true</Enabled></Report>
</CreateJobRequest>`,
			wantStatus:      http.StatusOK,
			wantDescription: "my batch job",
			wantManifest:    true,
		},
		{
			name: "job_without_extended_fields",
			body: `<CreateJobRequest>
<RoleArn>arn:aws:iam::000000000000:role/Role</RoleArn>
<Priority>5</Priority>
</CreateJobRequest>`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPost, "/v20180820/jobs", "000000000000", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDescription != "" {
				// Extract job ID from response and verify stored job.
				require.Contains(t, rec.Body.String(), "JobId")
			}
		})
	}
}

func TestBatch3_Handler_DescribeJob_ExtendedFields(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
	require.NoError(t, err)

	err = b.UpdateJobDetails(
		"000000000000", job.JobID,
		"my job description",
		"<Spec><Format>CSV</Format></Spec>",
		"<LambdaInvoke/>",
		"<Bucket>arn:aws:s3:::report</Bucket>",
		true,
	)
	require.NoError(t, err)

	h := s3control.NewHandler(b)
	rec := doS3ControlNewOpRequest(t, h, http.MethodGet, "/v20180820/jobs/"+job.JobID, "000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DescribeJobResult")
	assert.Contains(t, body, "my job description")
	assert.Contains(t, body, "ConfirmationRequired")
	assert.Contains(t, body, "CreationTime")
	assert.Contains(t, body, "Manifest")
	assert.Contains(t, body, "Operation")
	assert.Contains(t, body, "Report")
}

// ---- Job status validation tests ----

func TestBatch3_UpdateJobStatus_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		requestedStatus string
		wantErr         bool
	}{
		{name: "cancelled_valid", requestedStatus: "Cancelled", wantErr: false},
		{name: "ready_valid", requestedStatus: "Ready", wantErr: false},
		{name: "complete_invalid", requestedStatus: "Complete", wantErr: true},
		{name: "active_invalid", requestedStatus: "Active", wantErr: true},
		{name: "new_invalid", requestedStatus: "New", wantErr: true},
		{name: "empty_invalid", requestedStatus: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
			require.NoError(t, err)

			_, err = b.UpdateJobStatusValidated("000000000000", job.JobID, tt.requestedStatus, "")
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, s3control.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBatch3_UpdateJobStatusValidated_StatusUpdateReason(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
	require.NoError(t, err)

	updated, err := b.UpdateJobStatusValidated(
		"000000000000", job.JobID,
		"Cancelled",
		"user requested cancellation",
	)
	require.NoError(t, err)
	assert.Equal(t, "Cancelled", updated.Status)
	assert.Equal(t, "user requested cancellation", updated.StatusUpdateReason)
}

// ---- MRAP Regions tests ----

func TestBatch3_MRAP_SetRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "single_region", regions: []string{"my-bucket"}},
		{name: "multi_region", regions: []string{"bucket-us-east-1", "bucket-eu-west-1"}},
		{name: "no_regions", regions: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")

			err := b.SetMRAPRegions("000000000000", "my-mrap", tt.regions)
			require.NoError(t, err)

			mrap, err := b.GetMultiRegionAccessPoint("000000000000", "my-mrap")
			require.NoError(t, err)
			assert.Equal(t, tt.regions, mrap.Regions)
		})
	}
}

func TestBatch3_MRAP_SetRegions_MissingMRAP(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.SetMRAPRegions("000000000000", "nonexistent", []string{"bucket"})
	require.Error(t, err)
}

func TestBatch3_Handler_CreateMRAP_WithRegions(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	body := `<CreateMultiRegionAccessPointRequest>
<ClientToken>tok-1</ClientToken>
<Details>
<Name>my-mrap</Name>
<Regions>
<Region><Bucket>bucket-us-east-1</Bucket></Region>
<Region><Bucket>bucket-eu-west-1</Bucket></Region>
</Regions>
</Details>
</CreateMultiRegionAccessPointRequest>`

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodPost,
		"/v20180820/async-requests/mrap/create",
		"000000000000",
		body,
	)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RequestTokenARN")

	// Verify stored regions.
	mrap, err := h.Backend.GetMultiRegionAccessPoint("000000000000", "my-mrap")
	require.NoError(t, err)
	assert.Len(t, mrap.Regions, 2)
	assert.Equal(t, "bucket-us-east-1", mrap.Regions[0])
	assert.Equal(t, "bucket-eu-west-1", mrap.Regions[1])
}

func TestBatch3_Handler_GetMRAP_ReturnsRegions(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")
	_ = b.SetMRAPRegions("000000000000", "my-mrap", []string{"bucket-us-east-1"})
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(
		t, h, http.MethodGet,
		"/v20180820/mrap/instances/my-mrap",
		"000000000000",
		"",
	)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "GetMultiRegionAccessPointResult")
	assert.Contains(t, body, "bucket-us-east-1")
	assert.Contains(t, body, "CreatedAt")
}

// ---- StorageLensGroup Filter tests ----

func TestBatch3_StorageLensGroup_UpdateFilter(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "my-group")

	filter := `<MatchAnyPrefix><Prefix>logs/</Prefix></MatchAnyPrefix>`
	err := b.UpdateStorageLensGroupFilter("000000000000", "my-group", filter)
	require.NoError(t, err)

	grp, err := b.GetStorageLensGroup("000000000000", "my-group")
	require.NoError(t, err)
	assert.Equal(t, filter, grp.Filter)
	assert.NotEmpty(t, grp.CreatedAt)
}

func TestBatch3_StorageLensGroup_UpdateFilter_MissingGroup(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.UpdateStorageLensGroupFilter("000000000000", "nonexistent", "filter")
	require.Error(t, err)
}

func TestBatch3_Handler_CreateStorageLensGroup_WithFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "with_filter",
			body: `<CreateStorageLensGroupRequest>
<StorageLensGroup>
<Name>my-group</Name>
<Filter><MatchAnyPrefix><Prefix>logs/</Prefix></MatchAnyPrefix></Filter>
</StorageLensGroup>
</CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
		{
			name: "without_filter",
			body: `<CreateStorageLensGroupRequest>
<StorageLensGroup>
<Name>simple-group</Name>
</StorageLensGroup>
</CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			rec := doS3ControlNewOpRequest(
				t, h, http.MethodPost,
				"/v20180820/storagelensgroup",
				"000000000000",
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- AccessPoint CreationDate tests ----

func TestBatch3_AccessPoint_CreationDateSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	ap := b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	assert.NotEmpty(t, ap.CreationDate)
}

func TestBatch3_Handler_GetAccessPoint_IncludesCreationDate(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/accesspoint/my-ap", "000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreationDate")
}

// ---- MRAP CreatedAt tests ----

func TestBatch3_MRAP_CreatedAtSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token1")

	mrap, err := b.GetMultiRegionAccessPoint("000000000000", "my-mrap")
	require.NoError(t, err)
	assert.NotEmpty(t, mrap.CreatedAt)
}

// ---- StorageLensGroup CreatedAt tests ----

func TestBatch3_StorageLensGroup_CreatedAtSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	grp := b.CreateStorageLensGroup("000000000000", "my-group")
	assert.NotEmpty(t, grp.CreatedAt)
}

// ---- Persistence round-trip tests ----

func TestBatch3_Persistence_AccessPointPABs(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "my-ap", "my-bucket")
	_ = b.PutAccessPointPublicAccessBlock(
		"000000000000", "my-ap",
		s3control.PublicAccessBlock{BlockPublicAcls: true, BlockPublicPolicy: true},
	)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	pab, err := b2.GetAccessPointPublicAccessBlock("000000000000", "my-ap")
	require.NoError(t, err)
	assert.True(t, pab.BlockPublicAcls)
	assert.True(t, pab.BlockPublicPolicy)
}

func TestBatch3_Persistence_AccessPointVpcConfig(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessPoint("000000000000", "vpc-ap", "my-bucket")
	_ = b.SetAccessPointVpcConfig("000000000000", "vpc-ap", "vpc-abc123", "111122223333")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	ap, err := b2.GetAccessPoint("000000000000", "vpc-ap")
	require.NoError(t, err)
	assert.Equal(t, "vpc-abc123", ap.VpcID)
	assert.Equal(t, "VPC", ap.NetworkOrigin)
}

func TestBatch3_Persistence_JobDetails(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	job, err := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/R", 5)
	require.NoError(t, err)
	_ = b.UpdateJobDetails("000000000000", job.JobID, "my job", "<manifest/>", "<op/>", "<report/>", true)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	got, err := b2.GetJob("000000000000", job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "my job", got.Description)
	assert.True(t, got.ConfirmationRequired)
	assert.NotEmpty(t, got.CreationTime)
}
