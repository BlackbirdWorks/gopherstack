package s3control_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Bucket Replication ----

func TestBatch4_BucketReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		rules    string
		wantGet  string
		wantCode int
	}{
		{
			name:     "put_and_get_rules",
			bucket:   "my-bucket",
			rules:    "<Rule><ID>rule1</ID><Status>Enabled</Status></Rule>",
			wantGet:  "rule1",
			wantCode: http.StatusOK,
		},
		{
			name:     "overwrite_rules",
			bucket:   "bucket2",
			rules:    "<Rule><ID>rule2</ID></Rule>",
			wantGet:  "rule2",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			body := `<ReplicationConfiguration>` + tt.rules + `</ReplicationConfiguration>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", "")
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantGet)
		})
	}
}

func TestBatch4_BucketReplication_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		preload  bool
		wantCode int
	}{
		{
			name:     "delete_existing",
			bucket:   "my-bucket",
			preload:  true,
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete_nonexistent_still_204",
			bucket:   "missing-bucket",
			preload:  false,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.preload {
				b.PutBucketReplication("000000000000", tt.bucket, "<Rule/>")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/bucket/"+tt.bucket+"/replication", "000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch4_BucketReplication_GetMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/bucket/no-such-bucket/replication", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Storage Lens Configuration ----

func TestBatch4_StorageLensConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		config     string
		wantCode   int
	}{
		{
			name:       "put_and_get",
			configName: "my-config",
			config:     "<IsEnabled>true</IsEnabled>",
			wantCode:   http.StatusOK,
		},
		{
			name:       "put_minimal",
			configName: "minimal",
			config:     "",
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			body := `<StorageLensConfiguration><Config>` + tt.config + `</Config></StorageLensConfiguration>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelens/"+tt.configName, "000000000000", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens/"+tt.configName, "000000000000", "")
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.configName)
		})
	}
}

func TestBatch4_StorageLensConfiguration_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		preload    bool
		wantCode   int
	}{
		{
			name:       "delete_existing",
			configName: "existing-config",
			preload:    true,
			wantCode:   http.StatusNoContent,
		},
		{
			name:       "delete_missing",
			configName: "no-config",
			preload:    false,
			wantCode:   http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.preload {
				b.PutStorageLensConfiguration("000000000000", tt.configName, "<IsEnabled>true</IsEnabled>")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/storagelens/"+tt.configName, "000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch4_StorageLensConfiguration_GetMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelens/no-such-config", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch4_ListStorageLensConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		configs []string
		wantIDs []string
	}{
		{
			name:    "empty_list",
			configs: nil,
			wantIDs: nil,
		},
		{
			name:    "single_config",
			configs: []string{"cfg-1"},
			wantIDs: []string{"cfg-1"},
		},
		{
			name:    "multiple_configs",
			configs: []string{"cfg-a", "cfg-b", "cfg-c"},
			wantIDs: []string{"cfg-a", "cfg-b", "cfg-c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			for _, c := range tt.configs {
				b.PutStorageLensConfiguration("000000000000", c, "")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for _, id := range tt.wantIDs {
				assert.Contains(t, body, id)
			}
			assert.Equal(t, len(tt.configs), s3control.StorageLensConfigCount(b))
		})
	}
}

// ---- Storage Lens Configuration Tagging ----

func TestBatch4_StorageLensConfigurationTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		tags       map[string]string
		wantKeys   []string
	}{
		{
			name:       "put_and_get_tags",
			configName: "cfg-tagged",
			tags:       map[string]string{"Env": "prod", "Team": "storage"},
			wantKeys:   []string{"Env", "Team"},
		},
		{
			name:       "single_tag",
			configName: "cfg-single",
			tags:       map[string]string{"Owner": "alice"},
			wantKeys:   []string{"Owner"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			b.PutStorageLensConfiguration("000000000000", tt.configName, "")
			b.PutStorageLensConfigurationTagging("000000000000", tt.configName, tt.tags)

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens/"+tt.configName+"/tagging", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for _, k := range tt.wantKeys {
				assert.Contains(t, body, k)
			}
		})
	}
}

func TestBatch4_StorageLensConfigTagging_PutViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		body       string
		wantCode   int
	}{
		{
			name:       "valid_tags",
			configName: "my-cfg",
			body: `<PutStorageLensConfigurationTaggingRequest>` +
				`<Tags><Tag><Key>k1</Key><Value>v1</Value></Tag></Tags>` +
				`</PutStorageLensConfigurationTaggingRequest>`,
			wantCode: http.StatusOK,
		},
		{
			name:       "empty_tags",
			configName: "my-cfg2",
			body:       `<PutStorageLensConfigurationTaggingRequest><Tags></Tags></PutStorageLensConfigurationTaggingRequest>`,
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelens/"+tt.configName+"/tagging", "000000000000", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch4_StorageLensConfigTagging_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		preload    bool
		wantCode   int
	}{
		{
			name:       "delete_existing_tags",
			configName: "tagged-cfg",
			preload:    true,
			wantCode:   http.StatusNoContent,
		},
		{
			name:       "delete_missing_tags",
			configName: "untagged-cfg",
			preload:    false,
			wantCode:   http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.preload {
				b.PutStorageLensConfigurationTagging("000000000000", tt.configName, map[string]string{"k": "v"})
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/storagelens/"+tt.configName+"/tagging", "000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- MRAP Routes (SubmitMultiRegionAccessPointRoutes) ----

func TestBatch4_SubmitMRAPRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mrap     string
		routes   string
		body     string
		wantCode int
	}{
		{
			name:     "submit_routes",
			mrap:     "my-mrap",
			routes:   "<Route><Bucket>b1</Bucket><TrafficDialPercentage>100</TrafficDialPercentage></Route>",
			wantCode: http.StatusOK,
		},
		{
			name:     "submit_empty_routes",
			mrap:     "my-mrap2",
			routes:   "",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			body := `<SubmitMultiRegionAccessPointRoutesRequest>` +
				`<Routes>` + tt.routes + `</Routes>` +
				`</SubmitMultiRegionAccessPointRoutesRequest>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPatch,
				"/v20180820/mrap/instances/"+tt.mrap+"/routes", "000000000000", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Resource Tags (ListTagsForResource / TagResource / UntagResource) ----

func TestBatch4_ResourceTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		arn     string
		wantLen int
	}{
		{
			name:    "tag_and_list",
			arn:     "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			tags:    map[string]string{"Purpose": "analytics", "CostCenter": "123"},
			wantLen: 2,
		},
		{
			name:    "no_tags",
			arn:     "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			tags:    nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if len(tt.tags) > 0 {
				b.TagResource(tt.arn, tt.tags)
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/tags/"+tt.arn, "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for k, v := range tt.tags {
				assert.Contains(t, body, k)
				assert.Contains(t, body, v)
			}
		})
	}
}

func TestBatch4_TagResource_PutViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		body     string
		wantKeys []string
		wantCode int
	}{
		{
			name: "tag_resource",
			arn:  "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			body: `<TagResourceRequest><Tags>` +
				`<Tag><Key>Env</Key><Value>prod</Value></Tag>` +
				`<Tag><Key>Owner</Key><Value>team-a</Value></Tag>` +
				`</Tags></TagResourceRequest>`,
			wantCode: http.StatusOK,
			wantKeys: []string{"Env", "Owner"},
		},
		{
			name:     "tag_resource_empty_tags",
			arn:      "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			body:     `<TagResourceRequest><Tags></Tags></TagResourceRequest>`,
			wantCode: http.StatusOK,
			wantKeys: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPost,
				"/v20180820/tags/"+tt.arn, "000000000000", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			// Verify via list
			rec2 := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/tags/"+tt.arn, "000000000000", "")
			listBody := rec2.Body.String()
			for _, k := range tt.wantKeys {
				assert.Contains(t, listBody, k)
			}
		})
	}
}

func TestBatch4_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags map[string]string
		name        string
		arn         string
		removeKeys  []string
		wantGone    []string
		wantStay    []string
		wantCode    int
	}{
		{
			name:        "remove_one_tag",
			arn:         "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			initialTags: map[string]string{"Env": "prod", "Owner": "alice"},
			removeKeys:  []string{"Env"},
			wantCode:    http.StatusNoContent,
			wantGone:    []string{"Env"},
			wantStay:    []string{"Owner"},
		},
		{
			name:        "remove_all_tags",
			arn:         "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			initialTags: map[string]string{"k1": "v1", "k2": "v2"},
			removeKeys:  []string{"k1", "k2"},
			wantCode:    http.StatusNoContent,
			wantGone:    []string{"k1", "k2"},
			wantStay:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			b.TagResource(tt.arn, tt.initialTags)

			// Build untag body
			var sb strings.Builder
			sb.WriteString(`<UntagResourceRequest><TagKeys>`)
			for _, k := range tt.removeKeys {
				sb.WriteString(`<TagKey>` + k + `</TagKey>`)
			}
			sb.WriteString(`</TagKeys></UntagResourceRequest>`)

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/tags/"+tt.arn, "000000000000", sb.String())
			assert.Equal(t, tt.wantCode, rec.Code)

			remaining := b.ListTagsForResource(tt.arn)
			for _, k := range tt.wantGone {
				_, ok := remaining[k]
				assert.False(t, ok, "tag %q should be gone", k)
			}
			for _, k := range tt.wantStay {
				_, ok := remaining[k]
				assert.True(t, ok, "tag %q should remain", k)
			}
		})
	}
}

// ---- Storage Lens Config round-trip via persistence ----

func TestBatch4_StorageLensConfig_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		config     string
	}{
		{
			name:       "round_trip_with_data",
			configName: "snap-cfg",
			config:     "<IsEnabled>true</IsEnabled>",
		},
		{
			name:       "round_trip_empty_config",
			configName: "empty-cfg",
			config:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.PutStorageLensConfiguration("acc1", tt.configName, tt.config)
			require.Equal(t, 1, s3control.StorageLensConfigCount(b))

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := s3control.NewInMemoryBackend()
			require.NoError(t, b2.Restore(snap))
			assert.Equal(t, 1, s3control.StorageLensConfigCount(b2))

			cfg, err := b2.GetStorageLensConfiguration("acc1", tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.config, cfg)
		})
	}
}

// ---- Storage Lens Groups ----

func TestBatch4_StorageLensGroup_CreateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		filter    string
		wantCode  int
	}{
		{
			name:      "get_existing_group",
			groupName: "my-group",
			filter:    "<MatchAnyPrefix><Prefix>logs/</Prefix></MatchAnyPrefix>",
			wantCode:  http.StatusOK,
		},
		{
			name:      "get_group_no_filter",
			groupName: "plain-group",
			filter:    "",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			grp := b.CreateStorageLensGroup("000000000000", tt.groupName)
			require.NotNil(t, grp)

			if tt.filter != "" {
				err := b.UpdateStorageLensGroupFilter("000000000000", tt.groupName, tt.filter)
				require.NoError(t, err)
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelensgroup/"+tt.groupName, "000000000000", "")
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.groupName)

			if tt.filter != "" {
				assert.Contains(t, rec.Body.String(), "MatchAnyPrefix")
			}
		})
	}
}

func TestBatch4_StorageLensGroup_ListShowsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		groups  []string
		wantLen int
	}{
		{
			name:    "list_empty",
			groups:  nil,
			wantLen: 0,
		},
		{
			name:    "list_one",
			groups:  []string{"grp1"},
			wantLen: 1,
		},
		{
			name:    "list_three",
			groups:  []string{"grp-a", "grp-b", "grp-c"},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			for _, g := range tt.groups {
				b.CreateStorageLensGroup("000000000000", g)
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelensgroup", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for _, g := range tt.groups {
				assert.Contains(t, body, g)
			}
			assert.Equal(t, tt.wantLen, s3control.StorageLensGroupCount(b))
		})
	}
}

func TestBatch4_StorageLensGroup_UpdateFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		body      string
		wantCode  int
	}{
		{
			name:      "update_with_filter",
			groupName: "my-grp",
			body: `<StorageLensGroup><Name>my-grp</Name>` +
				`<Filter><MatchAnyPrefix><Prefix>data/</Prefix></MatchAnyPrefix></Filter>` +
				`</StorageLensGroup>`,
			wantCode: http.StatusOK,
		},
		{
			name:      "update_no_filter",
			groupName: "my-grp2",
			body:      `<StorageLensGroup><Name>my-grp2</Name></StorageLensGroup>`,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			b.CreateStorageLensGroup("000000000000", tt.groupName)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelensgroup/"+tt.groupName, "000000000000", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch4_StorageLensGroup_UpdateMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
		"/v20180820/storagelensgroup/no-such-group", "000000000000",
		`<StorageLensGroup><Name>no-such-group</Name></StorageLensGroup>`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch4_StorageLensGroup_DeleteMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
		"/v20180820/storagelensgroup/no-group", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- StorageLensGroup ARN contains region ----

func TestBatch4_StorageLensGroup_ArnContainsRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		groupName  string
		wantRegion string
	}{
		{
			name:       "us_east_1",
			region:     "us-east-1",
			groupName:  "grp-east",
			wantRegion: "us-east-1",
		},
		{
			name:       "eu_west_1",
			region:     "eu-west-1",
			groupName:  "grp-west",
			wantRegion: "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig("000000000000", tt.region)
			grp := b.CreateStorageLensGroup("000000000000", tt.groupName)
			require.NotNil(t, grp)
			assert.Contains(t, grp.StorageLensGroupArn, tt.wantRegion)
		})
	}
}
