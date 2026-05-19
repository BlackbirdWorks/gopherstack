package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Job Tagging ----

func TestBatch1_JobTagging(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()

	_, setupErr := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/test", 1)
	require.NoError(t, setupErr)

	t.Run("put and get tags", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		j, _ := b2.CreateJob("000000000000", "arn:aws:iam::000000000000:role/test", 1)

		require.NoError(
			t,
			b2.PutJobTagging(
				"000000000000",
				j.JobID,
				s3control.TagSet{"env": "test", "project": "s3"},
			),
		)
		tags, err := b2.GetJobTagging("000000000000", j.JobID)
		require.NoError(t, err)
		assert.Equal(t, "test", tags["env"])
		assert.Equal(t, "s3", tags["project"])
	})

	t.Run("delete tags", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		j, _ := b2.CreateJob("000000000000", "arn:aws:iam::000000000000:role/test", 1)
		_ = b2.PutJobTagging("000000000000", j.JobID, s3control.TagSet{"k": "v"})

		require.NoError(t, b2.DeleteJobTagging("000000000000", j.JobID))
		tags, err := b2.GetJobTagging("000000000000", j.JobID)
		require.NoError(t, err)
		assert.Empty(t, tags)
	})

	t.Run("unknown job returns error", func(t *testing.T) {
		t.Parallel()
		_, err := b.GetJobTagging("000000000000", "job-missing")
		require.Error(t, err)
	})
}

// ---- Access Grants Instance ----

func TestBatch1_AccessGrantsInstance(t *testing.T) {
	t.Parallel()

	t.Run("get after create", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateAccessGrantsInstance("000000000000", "")

		inst, err := b.GetAccessGrantsInstance("000000000000")
		require.NoError(t, err)
		assert.NotEmpty(t, inst.AccessGrantsInstanceArn)
	})

	t.Run("delete instance", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateAccessGrantsInstance("000000000000", "")
		require.NoError(t, b.DeleteAccessGrantsInstance("000000000000"))
		_, err := b.GetAccessGrantsInstance("000000000000")
		require.Error(t, err)
	})

	t.Run("resource policy CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.PutAccessGrantsInstanceResourcePolicy("000000000000", `{"Version":"2012-10-17"}`)
		policy, err := b.GetAccessGrantsInstanceResourcePolicy("000000000000")
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
		b.DeleteAccessGrantsInstanceResourcePolicy("000000000000")
		policy2, _ := b.GetAccessGrantsInstanceResourcePolicy("000000000000")
		assert.Empty(t, policy2)
	})

	t.Run("dissociate identity center", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateAccessGrantsInstance("000000000000", "arn:aws:sso:::instance/ssoins-test")
		b.DissociateAccessGrantsIdentityCenter("000000000000")
		inst, _ := b.GetAccessGrantsInstance("000000000000")
		assert.Empty(t, inst.IdentityCenterArn)
	})

	t.Run("get for prefix after create", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateAccessGrantsInstance("000000000000", "")
		inst, err := b.GetAccessGrantsInstanceForPrefix("000000000000", "s3://my-bucket/prefix/")
		require.NoError(t, err)
		assert.NotEmpty(t, inst.AccessGrantsInstanceArn)
	})
}

// ---- Access Grants CRUD ----

func TestBatch1_AccessGrantsCRUD(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	b.CreateAccessGrantsInstance("000000000000", "")
	loc := b.CreateAccessGrantsLocation(
		"000000000000",
		"s3://bucket/",
		"arn:aws:iam::000000000000:role/test",
	)
	grant, setupErr := b.CreateAccessGrant(
		"000000000000",
		loc.AccessGrantsLocationID,
		"IAMUser",
		"arn:aws:iam::000000000000:user/test",
		"READ",
		"",
	)
	require.NoError(t, setupErr)

	t.Run("get grant", func(t *testing.T) {
		t.Parallel()
		g, err := b.GetAccessGrant("000000000000", grant.AccessGrantID)
		require.NoError(t, err)
		assert.Equal(t, "READ", g.Permission)
	})

	t.Run("list grants", func(t *testing.T) {
		t.Parallel()
		grants := b.ListAccessGrants("000000000000", "")
		assert.NotEmpty(t, grants)
	})

	t.Run("list caller grants", func(t *testing.T) {
		t.Parallel()
		grants := b.ListCallerAccessGrants("000000000000")
		assert.NotEmpty(t, grants)
	})

	t.Run("delete grant", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		b2.CreateAccessGrantsInstance("000000000000", "")
		l := b2.CreateAccessGrantsLocation(
			"000000000000",
			"s3://bucket/",
			"arn:aws:iam::000000000000:role/r",
		)
		g, _ := b2.CreateAccessGrant(
			"000000000000",
			l.AccessGrantsLocationID,
			"IAMUser",
			"arn:test",
			"READ",
			"",
		)
		require.NoError(t, b2.DeleteAccessGrant("000000000000", g.AccessGrantID))
	})
}

func TestBatch1_AccessGrantsLocation(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	loc := b.CreateAccessGrantsLocation(
		"000000000000",
		"s3://bucket/",
		"arn:aws:iam::000000000000:role/test",
	)

	t.Run("get location", func(t *testing.T) {
		t.Parallel()
		l, err := b.GetAccessGrantsLocation("000000000000", loc.AccessGrantsLocationID)
		require.NoError(t, err)
		assert.Equal(t, "s3://bucket/", l.LocationScope)
	})

	t.Run("update location", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		l := b2.CreateAccessGrantsLocation("000000000000", "s3://b/", "arn:old")
		updated, err := b2.UpdateAccessGrantsLocation(
			"000000000000",
			l.AccessGrantsLocationID,
			"arn:new",
		)
		require.NoError(t, err)
		assert.Equal(t, "arn:new", updated.IAMRoleArn)
	})

	t.Run("list locations", func(t *testing.T) {
		t.Parallel()
		locs := b.ListAccessGrantsLocations("000000000000")
		assert.NotEmpty(t, locs)
	})

	t.Run("delete location", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		l := b2.CreateAccessGrantsLocation("000000000000", "s3://b/", "arn:test")
		require.NoError(t, b2.DeleteAccessGrantsLocation("000000000000", l.AccessGrantsLocationID))
	})
}

func TestBatch1_GetDataAccess(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	b.CreateAccessGrantsInstance("000000000000", "")

	url, err := b.GetDataAccess("000000000000", "s3://bucket/prefix/", "READ")
	require.NoError(t, err)
	assert.NotEmpty(t, url)
}

// ---- Outposts Bucket ----

func TestBatch1_OutpostsBucket(t *testing.T) {
	t.Parallel()

	t.Run("get bucket", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "my-bucket")
		bucket, err := b.GetBucket("000000000000", "my-bucket")
		require.NoError(t, err)
		assert.Equal(t, "my-bucket", bucket.Name)
	})

	t.Run("delete bucket", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "to-delete")
		require.NoError(t, b.DeleteBucket("000000000000", "to-delete"))
		_, err := b.GetBucket("000000000000", "to-delete")
		require.Error(t, err)
	})

	t.Run("bucket policy CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "policy-bucket")
		require.NoError(
			t,
			b.PutBucketPolicy("000000000000", "policy-bucket", `{"Version":"2012-10-17"}`),
		)
		policy, err := b.GetBucketPolicy("000000000000", "policy-bucket")
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
		require.NoError(t, b.DeleteBucketPolicy("000000000000", "policy-bucket"))
		policy2, _ := b.GetBucketPolicy("000000000000", "policy-bucket")
		assert.Empty(t, policy2)
	})

	t.Run("bucket tagging CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "tag-bucket")
		require.NoError(
			t,
			b.PutBucketTagging("000000000000", "tag-bucket", s3control.TagSet{"env": "prod"}),
		)
		tags, err := b.GetBucketTagging("000000000000", "tag-bucket")
		require.NoError(t, err)
		assert.Equal(t, "prod", tags["env"])
		require.NoError(t, b.DeleteBucketTagging("000000000000", "tag-bucket"))
	})

	t.Run("bucket versioning", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "ver-bucket")
		status, _ := b.GetBucketVersioning("000000000000", "ver-bucket")
		assert.Equal(t, "Suspended", status)
		require.NoError(t, b.PutBucketVersioning("000000000000", "ver-bucket", "Enabled"))
		status2, _ := b.GetBucketVersioning("000000000000", "ver-bucket")
		assert.Equal(t, "Enabled", status2)
	})

	t.Run("bucket lifecycle CRUD", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "lc-bucket")
		require.NoError(
			t,
			b.PutBucketLifecycleConfiguration(
				"000000000000",
				"lc-bucket",
				"<LifecycleConfiguration/>",
			),
		)
		config, err := b.GetBucketLifecycleConfiguration("000000000000", "lc-bucket")
		require.NoError(t, err)
		assert.Contains(t, config, "Lifecycle")
		require.NoError(t, b.DeleteBucketLifecycleConfiguration("000000000000", "lc-bucket"))
	})

	t.Run("list regional buckets", func(t *testing.T) {
		t.Parallel()
		b := s3control.NewInMemoryBackend()
		b.CreateBucket("000000000000", "b1")
		b.CreateBucket("000000000000", "b2")
		buckets := b.ListRegionalBuckets("000000000000")
		require.Len(t, buckets, 2)
	})
}

// ---- Object Lambda AP ----

func TestBatch1_ObjectLambdaAP(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	ap := b.CreateAccessPointForObjectLambda("000000000000", "my-olap")

	t.Run("get OLAP", func(t *testing.T) {
		t.Parallel()
		got, err := b.GetAccessPointForObjectLambda("000000000000", ap.Name)
		require.NoError(t, err)
		assert.Equal(t, "my-olap", got.Name)
	})

	t.Run("list OLAPs", func(t *testing.T) {
		t.Parallel()
		aps := b.ListAccessPointsForObjectLambda("000000000000")
		assert.NotEmpty(t, aps)
	})

	t.Run("policy CRUD", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "test-olap")
		require.NoError(
			t,
			b2.PutAccessPointPolicyForObjectLambda(
				"000000000000",
				a.Name,
				`{"Version":"2012-10-17"}`,
			),
		)
		policy, err := b2.GetAccessPointPolicyForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
		isPublic, _ := b2.GetAccessPointPolicyStatusForObjectLambda("000000000000", a.Name)
		assert.True(t, isPublic)
		require.NoError(t, b2.DeleteAccessPointPolicyForObjectLambda("000000000000", a.Name))
	})

	t.Run("config CRUD", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "config-olap")
		require.NoError(
			t,
			b2.PutAccessPointConfigurationForObjectLambda("000000000000", a.Name, "<Config/>"),
		)
		cfg, err := b2.GetAccessPointConfigurationForObjectLambda("000000000000", a.Name)
		require.NoError(t, err)
		assert.Contains(t, cfg, "Config")
	})

	t.Run("delete OLAP", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		a := b2.CreateAccessPointForObjectLambda("000000000000", "del-olap")
		require.NoError(t, b2.DeleteAccessPointForObjectLambda("000000000000", a.Name))
	})
}

// ---- MRAP ----

func TestBatch1_MRAP(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	req := b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "token-1")

	t.Run("describe MRAP operation", func(t *testing.T) {
		t.Parallel()
		op, err := b.DescribeMultiRegionAccessPointOperation("000000000000", req.RequestTokenARN)
		require.NoError(t, err)
		assert.Equal(t, req.RequestTokenARN, op.RequestTokenARN)
	})

	t.Run("get MRAP policy", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		r := b2.CreateMultiRegionAccessPoint("000000000000", "pol-mrap", "tok-pol")
		_ = r
		b2.PutMultiRegionAccessPointPolicy("000000000000", "pol-mrap", `{"Version":"2012-10-17"}`)
		policy, err := b2.GetMultiRegionAccessPointPolicy("000000000000", "pol-mrap")
		require.NoError(t, err)
		assert.Contains(t, policy, "Version")
	})

	t.Run("get MRAP policy status", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		b2.CreateMultiRegionAccessPoint("000000000000", "st-mrap", "tok-st")
		b2.PutMultiRegionAccessPointPolicy("000000000000", "st-mrap", `{"Version":"2012-10-17"}`)
		isPublic, err := b2.GetMultiRegionAccessPointPolicyStatus("000000000000", "st-mrap")
		require.NoError(t, err)
		assert.True(t, isPublic)
	})

	t.Run("get MRAP routes empty", func(t *testing.T) {
		t.Parallel()
		b2 := s3control.NewInMemoryBackend()
		b2.CreateMultiRegionAccessPoint("000000000000", "rt-mrap", "tok-rt")
		routes, err := b2.GetMultiRegionAccessPointRoutes("000000000000", "rt-mrap")
		require.NoError(t, err)
		assert.Empty(t, routes)
	})
}

// ---- HTTP dispatch tests ----

func TestBatch1_HTTP_JobTagging(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	job, _ := b.CreateJob("000000000000", "arn:aws:iam::000000000000:role/r", 1)

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/jobs/"+job.JobID+"/tagging",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestBatch1_HTTP_GetBucket(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("000000000000", "test-bucket")

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/bucket/test-bucket",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestBatch1_HTTP_ListRegionalBuckets(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateBucket("000000000000", "b1")

	resp := doS3ControlNewOpRequest(t, h, http.MethodGet, "/v20180820/bucket", "000000000000", "")
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestBatch1_HTTP_GetAccessGrantsInstance(t *testing.T) {
	t.Parallel()
	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateAccessGrantsInstance("000000000000", "")

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/accessgrantsinstance",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestBatch1_HTTP_ListAccessPointsForObjectLambda(t *testing.T) {
	t.Parallel()
	h := s3control.NewHandler(s3control.NewInMemoryBackend())

	resp := doS3ControlNewOpRequest(
		t,
		h,
		http.MethodGet,
		"/v20180820/accesspointforobjectlambda",
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusOK, resp.Code)
}
