package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Bucket Replication ----

func TestBatch2_BucketReplication(t *testing.T) {
	t.Parallel()

	const accountID = "acct1"
	const bucketName = "mybucket"
	const replicationPath = "/v20180820/bucket/" + bucketName + "/replication"

	t.Run("put and get replication", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)

		putRec := doS3Request(t, h, http.MethodPut, replicationPath,
			`<ReplicationConfiguration><Rules>my-rule</Rules></ReplicationConfiguration>`)
		require.Equal(t, http.StatusOK, putRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), "my-rule")
	})

	t.Run("get missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)

		rec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete replication", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateBucket(accountID, bucketName)
		_ = doS3Request(t, h, http.MethodPut, replicationPath,
			`<ReplicationConfiguration><Rules>r</Rules></ReplicationConfiguration>`)

		delRec := doS3Request(t, h, http.MethodDelete, replicationPath, "")
		require.Equal(t, http.StatusNoContent, delRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, replicationPath, "")
		assert.Equal(t, http.StatusNotFound, getRec.Code)
	})

}

// ---- MRAP Routes ----

func TestBatch2_SubmitMRAPRoutes(t *testing.T) {
	t.Parallel()

	const accountID = "acct1"
	const mrapName = "my-mrap"
	const routesPath = "/v20180820/mrap/instances/" + mrapName + "/routes"

	t.Run("submit routes returns 200", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		h.Backend.CreateMultiRegionAccessPoint(accountID, mrapName, "token")

		rec := doS3Request(
			t,
			h,
			http.MethodPatch,
			routesPath,
			`<SubmitMultiRegionAccessPointRoutesRequest><Routes>r1</Routes></SubmitMultiRegionAccessPointRoutesRequest>`,
		)
		require.Equal(t, http.StatusOK, rec.Code)
	})

}

// ---- Storage Lens Configuration ----

func TestBatch2_StorageLensConfiguration(t *testing.T) {
	t.Parallel()

	const configName = "my-config"
	const configPath = "/v20180820/storagelens/" + configName
	const listPath = "/v20180820/storagelens"

	t.Run("put and get configuration", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		putRec := doS3Request(t, h, http.MethodPut, configPath,
			`<PutStorageLensConfigurationRequest><Config>cfg-data</Config></PutStorageLensConfigurationRequest>`)
		require.Equal(t, http.StatusOK, putRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, configPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), configName)
	})

	t.Run("get missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodGet, configPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete configuration", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPut, configPath, `<PutStorageLensConfigurationRequest/>`)

		delRec := doS3Request(t, h, http.MethodDelete, configPath, "")
		require.Equal(t, http.StatusNoContent, delRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, configPath, "")
		assert.Equal(t, http.StatusNotFound, getRec.Code)
	})

	t.Run("list configurations", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPut, configPath, `<PutStorageLensConfigurationRequest/>`)
		_ = doS3Request(t, h, http.MethodPut, "/v20180820/storagelens/other-config",
			`<PutStorageLensConfigurationRequest/>`)

		listRec := doS3Request(t, h, http.MethodGet, listPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		body := listRec.Body.String()
		assert.Contains(t, body, configName)
		assert.Contains(t, body, "other-config")
	})
}

// ---- Storage Lens Configuration Tagging ----

func TestBatch2_StorageLensConfigurationTagging(t *testing.T) {
	t.Parallel()

	const configName = "my-config"
	const configPath = "/v20180820/storagelens/" + configName
	const taggingPath = "/v20180820/storagelens/" + configName + "/tagging"

	setupConfig := func(t *testing.T) *s3control.Handler {
		t.Helper()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPut, configPath, `<PutStorageLensConfigurationRequest/>`)

		return h
	}

	t.Run("put and get tagging", func(t *testing.T) {
		t.Parallel()

		h := setupConfig(t)

		putTagBody := `<PutStorageLensConfigurationTaggingRequest>` +
			`<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>` +
			`</PutStorageLensConfigurationTaggingRequest>`
		putRec := doS3Request(t, h, http.MethodPut, taggingPath, putTagBody)
		require.Equal(t, http.StatusOK, putRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, taggingPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), "env")
		assert.Contains(t, getRec.Body.String(), "prod")
	})

	t.Run("get tagging on missing config returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodGet, taggingPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete tagging", func(t *testing.T) {
		t.Parallel()

		h := setupConfig(t)

		delTagBody := `<PutStorageLensConfigurationTaggingRequest>` +
			`<Tags><Tag><Key>k</Key><Value>v</Value></Tag></Tags>` +
			`</PutStorageLensConfigurationTaggingRequest>`
		_ = doS3Request(t, h, http.MethodPut, taggingPath, delTagBody)

		delRec := doS3Request(t, h, http.MethodDelete, taggingPath, "")
		require.Equal(t, http.StatusNoContent, delRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, taggingPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		// Tags removed; response should not contain our tag
		assert.NotContains(t, getRec.Body.String(), ">k<")
	})

}

// ---- Storage Lens Groups ----

func TestBatch2_StorageLensGroups(t *testing.T) {
	t.Parallel()

	const groupName = "my-group"
	const groupPath = "/v20180820/storagelensgroup/" + groupName
	const listPath = "/v20180820/storagelensgroup"
	// createGroupBody is reused across sub-tests.
	createGroupBody := `<CreateStorageLensGroupRequest>` +
		`<StorageLensGroup><Name>` + groupName + `</Name></StorageLensGroup>` +
		`</CreateStorageLensGroupRequest>`

	t.Run("create and get group", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPost, listPath, createGroupBody)

		getRec := doS3Request(t, h, http.MethodGet, groupPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Contains(t, getRec.Body.String(), groupName)
	})

	t.Run("get missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodGet, groupPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("update group returns 200", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPost, listPath, createGroupBody)

		rec := doS3Request(t, h, http.MethodPut, groupPath, `<StorageLensGroup/>`)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("update missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodPut, groupPath, `<StorageLensGroup/>`)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("delete group", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPost, listPath, createGroupBody)

		delRec := doS3Request(t, h, http.MethodDelete, groupPath, "")
		require.Equal(t, http.StatusNoContent, delRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, groupPath, "")
		assert.Equal(t, http.StatusNotFound, getRec.Code)
	})

	t.Run("delete missing returns 404", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodDelete, groupPath, "")
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("list groups", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		bodyA := `<CreateStorageLensGroupRequest>` +
			`<StorageLensGroup><Name>group-a</Name></StorageLensGroup>` +
			`</CreateStorageLensGroupRequest>`
		bodyB := `<CreateStorageLensGroupRequest>` +
			`<StorageLensGroup><Name>group-b</Name></StorageLensGroup>` +
			`</CreateStorageLensGroupRequest>`
		_ = doS3Request(t, h, http.MethodPost, listPath, bodyA)
		_ = doS3Request(t, h, http.MethodPost, listPath, bodyB)

		listRec := doS3Request(t, h, http.MethodGet, listPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		body := listRec.Body.String()
		assert.Contains(t, body, "group-a")
		assert.Contains(t, body, "group-b")
	})
}

// ---- Resource Tags ----

func TestBatch2_ResourceTags(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:s3:us-east-1:acct1:accesspoint/my-ap"
	const tagsPath = "/v20180820/tags/" + arn

	t.Run("tag and list tags", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		tagRec := doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags></TagResourceRequest>`)
		require.Equal(t, http.StatusOK, tagRec.Code)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		assert.Contains(t, listRec.Body.String(), "env")
		assert.Contains(t, listRec.Body.String(), "prod")
	})

	t.Run("list empty tags returns 200 with empty list", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("untag removes specific keys", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		twoTagsBody := `<TagResourceRequest><Tags>` +
			`<Tag><Key>a</Key><Value>1</Value></Tag>` +
			`<Tag><Key>b</Key><Value>2</Value></Tag>` +
			`</Tags></TagResourceRequest>`
		_ = doS3Request(t, h, http.MethodPost, tagsPath, twoTagsBody)

		untagRec := doS3Request(t, h, http.MethodDelete, tagsPath,
			`<UntagResourceRequest><TagKeys><TagKey>a</TagKey></TagKeys></UntagResourceRequest>`)
		require.Equal(t, http.StatusNoContent, untagRec.Code)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		body := listRec.Body.String()
		assert.NotContains(t, body, ">a<")
		assert.Contains(t, body, ">b<")
	})

	t.Run("tag overwrites existing keys", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>dev</Value></Tag></Tags></TagResourceRequest>`)
		_ = doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags></TagResourceRequest>`)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		assert.Contains(t, listRec.Body.String(), "prod")
		assert.NotContains(t, listRec.Body.String(), "dev")
	})
}

// ---- Backend unit tests ----

func TestBatch2_BackendBucketReplication(t *testing.T) {
	t.Parallel()

	t.Run("get missing returns error", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		b.CreateBucket("acct1", "bkt")

		_, err := b.GetBucketReplication("acct1", "bkt")
		require.Error(t, err)
	})

	t.Run("delete missing is idempotent", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		err := b.DeleteBucketReplication("acct1", "bkt")
		require.NoError(t, err)
	})
}

func TestBatch2_BackendStorageLensConfigTagging(t *testing.T) {
	t.Parallel()

	t.Run("get tagging on missing config returns error", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		_, err := b.GetStorageLensConfigurationTagging("acct1", "missing")
		require.Error(t, err)
	})

	t.Run("put and delete tagging are idempotent without config", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		err := b.PutStorageLensConfigurationTagging("acct1", "missing", s3control.TagSet{"k": "v"})
		require.NoError(t, err)
		err = b.DeleteStorageLensConfigurationTagging("acct1", "missing")
		require.NoError(t, err)
	})
}

func TestBatch2_BackendResourceTags(t *testing.T) {
	t.Parallel()

	t.Run("untag on unknown ARN is a no-op", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		// Should not panic
		b.UntagResource("arn:aws:s3:::nonexistent", []string{"key"})
	})

	t.Run("list on unknown ARN returns empty map", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		tags := b.ListTagsForResource("arn:aws:s3:::nonexistent")
		assert.Empty(t, tags)
	})
}

func TestBatch2_SubmitMRAPRoutes_Backend(t *testing.T) {
	t.Parallel()

	t.Run("submit on missing MRAP is idempotent", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		err := b.SubmitMultiRegionAccessPointRoutes("acct1", "missing", "routes")
		require.NoError(t, err)
	})

	t.Run("submit and retrieve routes", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		b.CreateMultiRegionAccessPoint("acct1", "my-mrap", "token")
		err := b.SubmitMultiRegionAccessPointRoutes("acct1", "my-mrap", "route-data")
		require.NoError(t, err)

		routes, err := b.GetMultiRegionAccessPointRoutes("acct1", "my-mrap")
		require.NoError(t, err)
		assert.Equal(t, "route-data", routes)
	})
}
