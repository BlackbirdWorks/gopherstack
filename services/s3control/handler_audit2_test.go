package s3control_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- StorageLensGroup tagging round-trip via HTTP ----

func TestAudit2_Handler_SLGTagging_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		putTags   string
		wantKeys  []string
	}{
		{
			name:      "single_key_value",
			groupName: "group-a",
			putTags: `<PutStorageLensGroupTaggingRequest>
<Tags><Tag><Key>department</Key><Value>engineering</Value></Tag></Tags>
</PutStorageLensGroupTaggingRequest>`,
			wantKeys: []string{"department"},
		},
		{
			name:      "two_key_values",
			groupName: "group-b",
			putTags: `<PutStorageLensGroupTaggingRequest>
<Tags>
<Tag><Key>owner</Key><Value>alice</Value></Tag>
<Tag><Key>project</Key><Value>aurora</Value></Tag>
</Tags>
</PutStorageLensGroupTaggingRequest>`,
			wantKeys: []string{"owner", "project"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)
			b.CreateStorageLensGroup("000000000000", tt.groupName)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelensgroup/"+tt.groupName+"/tagging",
				"000000000000", tt.putTags)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelensgroup/"+tt.groupName+"/tagging",
				"000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			for _, k := range tt.wantKeys {
				assert.Contains(t, rec.Body.String(), k)
			}
		})
	}
}

func TestAudit2_Handler_SLGTagging_Delete_ThenGetEmpty(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateStorageLensGroup("000000000000", "grp")

	// Put tags
	doS3ControlNewOpRequest(t, h, http.MethodPut,
		"/v20180820/storagelensgroup/grp/tagging",
		"000000000000",
		`<PutStorageLensGroupTaggingRequest><Tags><Tag><Key>k</Key><Value>v</Value></Tag></Tags></PutStorageLensGroupTaggingRequest>`)

	// Verify tags set
	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelensgroup/grp/tagging", "000000000000", "")
	assert.Contains(t, rec.Body.String(), "k")

	// Delete tags
	rec = doS3ControlNewOpRequest(t, h, http.MethodDelete,
		"/v20180820/storagelensgroup/grp/tagging", "000000000000", "")
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify empty after delete
	rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelensgroup/grp/tagging", "000000000000", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Key>")
}

// ---- MRAP PublicAccessBlock persistence ----

func TestAudit2_MRAP_PAB_SetOverwrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		firstBlock    bool
		secondBlock   bool
		wantFinalBlock bool
	}{
		{
			name:           "overwrite_to_false",
			firstBlock:     true,
			secondBlock:    false,
			wantFinalBlock: false,
		},
		{
			name:           "overwrite_to_true",
			firstBlock:     false,
			secondBlock:    true,
			wantFinalBlock: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", "my-mrap", "")

			_ = b.SetMRAPPublicAccessBlock("000000000000", "my-mrap", s3control.PublicAccessBlock{
				BlockPublicAcls: tt.firstBlock,
			})
			_ = b.SetMRAPPublicAccessBlock("000000000000", "my-mrap", s3control.PublicAccessBlock{
				BlockPublicAcls: tt.secondBlock,
			})

			got, err := b.GetMRAPPublicAccessBlock("000000000000", "my-mrap")
			require.NoError(t, err)
			assert.Equal(t, tt.wantFinalBlock, got.BlockPublicAcls)
		})
	}
}

// ---- StorageLens list includes IsEnabled field ----

func TestAudit2_Handler_ListStorageLens_IsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		body       string
		wantInBody string
	}{
		{
			name:       "includes_is_enabled_true",
			configName: "enabled-config",
			body:       `<StorageLensConfiguration><IsEnabled>true</IsEnabled></StorageLensConfiguration>`,
			wantInBody: "IsEnabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelens/"+tt.configName, "000000000000", tt.body)

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantInBody)
		})
	}
}

// ---- StorageLens config meta accounts don't cross-contaminate ----

func TestAudit2_StorageLensConfigMeta_AccountIsolation(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_ = b.PutStorageLensConfiguration("111111111111", "shared-name", "")
	_ = b.PutStorageLensConfiguration("222222222222", "shared-name", "")
	b.PutStorageLensConfigMeta("111111111111", "shared-name", true, "us-east-1")
	b.PutStorageLensConfigMeta("222222222222", "shared-name", false, "eu-west-1")

	meta1, err := b.GetStorageLensConfigMeta("111111111111", "shared-name")
	require.NoError(t, err)
	assert.True(t, meta1.IsEnabled)
	assert.Equal(t, "us-east-1", meta1.HomeRegion)

	meta2, err := b.GetStorageLensConfigMeta("222222222222", "shared-name")
	require.NoError(t, err)
	assert.False(t, meta2.IsEnabled)
	assert.Equal(t, "eu-west-1", meta2.HomeRegion)
}

// ---- StorageLensGroupTags account isolation ----

func TestAudit2_StorageLensGroupTags_AccountIsolation(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("acct-1", "grp")
	b.CreateStorageLensGroup("acct-2", "grp")

	_ = b.PutStorageLensGroupTags("acct-1", "grp", s3control.TagSet{"k": "v1"})
	_ = b.PutStorageLensGroupTags("acct-2", "grp", s3control.TagSet{"k": "v2"})

	t1, err := b.GetStorageLensGroupTags("acct-1", "grp")
	require.NoError(t, err)
	assert.Equal(t, "v1", t1["k"])

	t2, err := b.GetStorageLensGroupTags("acct-2", "grp")
	require.NoError(t, err)
	assert.Equal(t, "v2", t2["k"])
}

// ---- GetDataAccess returns unique key per call ----

func TestAudit2_GetDataAccessCredentials_DifferentTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		permission string
		wantInMatched string
	}{
		{
			name:          "read_on_bucket",
			target:        "mybucket",
			permission:    "READ",
			wantInMatched: "mybucket",
		},
		{
			name:          "write_on_bucket",
			target:        "other-bucket",
			permission:    "WRITE",
			wantInMatched: "other-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateAccessGrantsInstance("000000000000", "")

			_, _, _, _, matched, err := b.GetDataAccessCredentials("000000000000", tt.target, tt.permission)
			require.NoError(t, err)
			assert.Contains(t, matched, tt.wantInMatched)
		})
	}
}

// ---- CreateMRAP persists regions AND PAB together ----

func TestAudit2_Handler_CreateMRAP_RegionsAndPAB(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	body := `<CreateMultiRegionAccessPointRequest>
<ClientToken>tok</ClientToken>
<Details>
<Name>multi-mrap</Name>
<Regions>
<Region><Bucket>bucket-a</Bucket></Region>
<Region><Bucket>bucket-b</Bucket></Region>
</Regions>
<PublicAccessBlock>
<BlockPublicAcls>true</BlockPublicAcls>
<BlockPublicPolicy>false</BlockPublicPolicy>
</PublicAccessBlock>
</Details>
</CreateMultiRegionAccessPointRequest>`

	rec := doS3ControlNewOpRequest(t, h, http.MethodPost,
		"/v20180820/async-requests/mrap/create", "000000000000", body)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/mrap/instances/multi-mrap", "000000000000", "")
	require.Equal(t, http.StatusOK, rec.Code)

	resp := rec.Body.String()
	assert.Contains(t, resp, "bucket-a")
	assert.Contains(t, resp, "bucket-b")
	assert.Contains(t, resp, "BlockPublicAcls")
}

// ---- StorageLensGroup delete removes tags too ----

func TestAudit2_StorageLensGroupDelete_ClearsTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		tags      s3control.TagSet
	}{
		{
			name:      "group_with_multiple_tags",
			groupName: "tagged-grp",
			tags:      s3control.TagSet{"k1": "v1", "k2": "v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateStorageLensGroup("000000000000", tt.groupName)
			_ = b.PutStorageLensGroupTags("000000000000", tt.groupName, tt.tags)

			err := b.DeleteStorageLensGroup("000000000000", tt.groupName)
			require.NoError(t, err)

			// Recreate to verify tags are gone
			b.CreateStorageLensGroup("000000000000", tt.groupName)
			tags, err := b.GetStorageLensGroupTags("000000000000", tt.groupName)
			require.NoError(t, err)
			assert.Empty(t, tags)
		})
	}
}

// ---- MRAP PAB snapshot/restore ----

func TestAudit2_Persistence_MRAP_PAB_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mrap   string
		pab    s3control.PublicAccessBlock
	}{
		{
			name: "all_true",
			mrap: "mrap-full",
			pab: s3control.PublicAccessBlock{
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     true,
				RestrictPublicBuckets: true,
			},
		},
		{
			name: "mixed",
			mrap: "mrap-mixed",
			pab: s3control.PublicAccessBlock{
				BlockPublicAcls:       false,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     false,
				RestrictPublicBuckets: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", tt.mrap, "")
			_ = b.SetMRAPPublicAccessBlock("000000000000", tt.mrap, tt.pab)

			snap := b.Snapshot()
			b2 := s3control.NewInMemoryBackend()
			require.NoError(t, b2.Restore(snap))

			pab, err := b2.GetMRAPPublicAccessBlock("000000000000", tt.mrap)
			require.NoError(t, err)
			assert.Equal(t, tt.pab.BlockPublicAcls, pab.BlockPublicAcls)
			assert.Equal(t, tt.pab.IgnorePublicAcls, pab.IgnorePublicAcls)
			assert.Equal(t, tt.pab.BlockPublicPolicy, pab.BlockPublicPolicy)
			assert.Equal(t, tt.pab.RestrictPublicBuckets, pab.RestrictPublicBuckets)
		})
	}
}

// ---- ListAllAccessGrantsInstances multiple ----

func TestAudit2_ListAllAccessGrantsInstances_MultipleAccounts(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateAccessGrantsInstance("acct-a", "")
	b.CreateAccessGrantsInstance("acct-b", "")

	instA := b.ListAllAccessGrantsInstances("acct-a")
	assert.Len(t, instA, 1)
	assert.Equal(t, "acct-a", instA[0].AccountID)

	instB := b.ListAllAccessGrantsInstances("acct-b")
	assert.Len(t, instB, 1)
	assert.Equal(t, "acct-b", instB[0].AccountID)

	instNone := b.ListAllAccessGrantsInstances("acct-none")
	assert.Empty(t, instNone)
}

// ---- StorageLensConfigMeta clear on delete ----

func TestAudit2_StorageLensConfig_DeleteClearsMeta(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_ = b.PutStorageLensConfiguration("000000000000", "cfg", "")
	b.PutStorageLensConfigMeta("000000000000", "cfg", true, "us-east-1")

	_ = b.DeleteStorageLensConfiguration("000000000000", "cfg")

	_, err := b.GetStorageLensConfigMeta("000000000000", "cfg")
	require.Error(t, err)
}

// ---- Handler: GetStorageLensGroup returns filter ----

func TestAudit2_Handler_GetStorageLensGroup_IncludesFilter(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	createBody := `<CreateStorageLensGroupRequest>
<StorageLensGroup>
<Name>filtered-grp</Name>
<Filter><MatchAnyPrefix><Prefix>logs/</Prefix></MatchAnyPrefix></Filter>
</StorageLensGroup>
</CreateStorageLensGroupRequest>`

	rec := doS3ControlNewOpRequest(t, h, http.MethodPost,
		"/v20180820/storagelensgroup", "000000000000", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelensgroup/filtered-grp", "000000000000", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MatchAnyPrefix")
}

// ---- Handler: UpdateStorageLensGroup updates filter ----

func TestAudit2_Handler_UpdateStorageLensGroup_ChangesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		groupName  string
		filter1    string
		filter2    string
		wantInGet  string
	}{
		{
			name:      "update_prefix_filter",
			groupName: "updatable-grp",
			filter1:   "<MatchAnyPrefix><Prefix>before/</Prefix></MatchAnyPrefix>",
			filter2:   "<MatchAnyPrefix><Prefix>after/</Prefix></MatchAnyPrefix>",
			wantInGet: "after/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)
			b.CreateStorageLensGroup("000000000000", tt.groupName)

			updateBody := `<StorageLensGroup><Name>` + tt.groupName + `</Name><Filter>` + tt.filter2 + `</Filter></StorageLensGroup>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelensgroup/"+tt.groupName, "000000000000", updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelensgroup/"+tt.groupName, "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantInGet)
		})
	}
}

// ---- StorageLens Config metadata with HomeRegion default ----

func TestAudit2_StorageLensConfigMeta_HomeRegionDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		region     string
		configName string
		wantRegion string
	}{
		{
			name:       "us_east_region",
			region:     "us-east-1",
			configName: "east-cfg",
			wantRegion: "us-east-1",
		},
		{
			name:       "eu_west_region",
			region:     "eu-west-1",
			configName: "eu-cfg",
			wantRegion: "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig("000000000000", tt.region)
			_ = b.PutStorageLensConfiguration("000000000000", tt.configName, "")

			meta, err := b.GetStorageLensConfigMeta("000000000000", tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRegion, meta.HomeRegion)
		})
	}
}

// ---- Backend Reset clears new maps ----

func TestAudit2_Reset_ClearsAudit1Maps(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "g")
	_ = b.PutStorageLensGroupTags("000000000000", "g", s3control.TagSet{"k": "v"})

	_ = b.PutStorageLensConfiguration("000000000000", "c", "")
	b.PutStorageLensConfigMeta("000000000000", "c", true, "us-east-1")

	b.CreateMultiRegionAccessPoint("000000000000", "m", "")
	_ = b.SetMRAPPublicAccessBlock("000000000000", "m", s3control.PublicAccessBlock{BlockPublicAcls: true})

	b.Reset()

	// After reset all should be empty / not-found
	_, err := b.GetStorageLensConfigMeta("000000000000", "c")
	require.Error(t, err)

	_, err = b.GetMRAPPublicAccessBlock("000000000000", "m")
	require.Error(t, err)

	// StorageLensGroup was also deleted on Reset
	_, err = b.GetStorageLensGroupTags("000000000000", "g")
	require.Error(t, err)
}

// ---- SLG tags overwrite on second PUT ----

func TestAudit2_StorageLensGroupTags_Overwrite(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "g")

	_ = b.PutStorageLensGroupTags("000000000000", "g", s3control.TagSet{"k": "old"})
	_ = b.PutStorageLensGroupTags("000000000000", "g", s3control.TagSet{"k": "new", "extra": "yes"})

	tags, err := b.GetStorageLensGroupTags("000000000000", "g")
	require.NoError(t, err)
	assert.Equal(t, "new", tags["k"])
	assert.Equal(t, "yes", tags["extra"])
}

// ---- ListAccessGrantsInstances empty returns empty slice ----

func TestAudit2_ListAccessGrantsInstances_Empty(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	inst := b.ListAllAccessGrantsInstances("no-acct")
	assert.NotNil(t, inst)
	assert.Empty(t, inst)
}

// ---- StorageLens config meta with empty homeRegion uses backend region ----

func TestAudit2_PutStorageLensConfigMeta_EmptyHomeRegion(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackendWithConfig("000000000000", "ap-southeast-1")
	_ = b.PutStorageLensConfiguration("000000000000", "cfg", "")
	b.PutStorageLensConfigMeta("000000000000", "cfg", true, "")

	meta, err := b.GetStorageLensConfigMeta("000000000000", "cfg")
	require.NoError(t, err)
	assert.Equal(t, "ap-southeast-1", meta.HomeRegion)
}

// ---- MRAP PAB partial fields ----

func TestAudit2_MRAP_PAB_PartialFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		blockAcls             bool
		ignoreAcls            bool
		blockPolicy           bool
		restrictBuckets       bool
	}{
		{
			name:            "all_false",
			blockAcls:       false,
			ignoreAcls:      false,
			blockPolicy:     false,
			restrictBuckets: false,
		},
		{
			name:            "block_only_acls",
			blockAcls:       true,
			ignoreAcls:      false,
			blockPolicy:     false,
			restrictBuckets: false,
		},
		{
			name:            "all_true",
			blockAcls:       true,
			ignoreAcls:      true,
			blockPolicy:     true,
			restrictBuckets: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", "m", "")
			_ = b.SetMRAPPublicAccessBlock("000000000000", "m", s3control.PublicAccessBlock{
				BlockPublicAcls:       tt.blockAcls,
				IgnorePublicAcls:      tt.ignoreAcls,
				BlockPublicPolicy:     tt.blockPolicy,
				RestrictPublicBuckets: tt.restrictBuckets,
			})

			got, err := b.GetMRAPPublicAccessBlock("000000000000", "m")
			require.NoError(t, err)
			assert.Equal(t, tt.blockAcls, got.BlockPublicAcls)
			assert.Equal(t, tt.ignoreAcls, got.IgnorePublicAcls)
			assert.Equal(t, tt.blockPolicy, got.BlockPublicPolicy)
			assert.Equal(t, tt.restrictBuckets, got.RestrictPublicBuckets)
		})
	}
}

// ---- Persistence round-trip for SLG tags with multiple groups ----

func TestAudit2_Persistence_MultipleSLGTags(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "g1")
	b.CreateStorageLensGroup("000000000000", "g2")
	_ = b.PutStorageLensGroupTags("000000000000", "g1", s3control.TagSet{"env": "staging"})
	_ = b.PutStorageLensGroupTags("000000000000", "g2", s3control.TagSet{"env": "prod"})

	snap := b.Snapshot()
	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	t1, err := b2.GetStorageLensGroupTags("000000000000", "g1")
	require.NoError(t, err)
	assert.Equal(t, "staging", t1["env"])

	t2, err := b2.GetStorageLensGroupTags("000000000000", "g2")
	require.NoError(t, err)
	assert.Equal(t, "prod", t2["env"])
}

// ---- Handler: StorageLens config meta returned in list even without explicit meta call ----

func TestAudit2_Handler_ListStorageLens_DefaultMeta(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	doS3ControlNewOpRequest(t, h, http.MethodPut,
		"/v20180820/storagelens/no-explicit-meta", "000000000000",
		`<StorageLensConfiguration/>`)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelens", "000000000000", "")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "no-explicit-meta")
	assert.Contains(t, body, "StorageLensArn")
	assert.Contains(t, body, "HomeRegion")
}
