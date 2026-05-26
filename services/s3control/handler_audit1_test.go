package s3control_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- StorageLensGroup Tagging tests ----

func TestAudit1_StorageLensGroupTags_PutGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		groupName  string
		tags       s3control.TagSet
		wantKey    string
		wantValue  string
	}{
		{
			name:      "single_tag",
			groupName: "group-one",
			tags:      s3control.TagSet{"env": "prod"},
			wantKey:   "env",
			wantValue: "prod",
		},
		{
			name:      "multiple_tags",
			groupName: "group-two",
			tags:      s3control.TagSet{"team": "data", "cost": "123"},
			wantKey:   "team",
			wantValue: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateStorageLensGroup("000000000000", tt.groupName)

			err := b.PutStorageLensGroupTags("000000000000", tt.groupName, tt.tags)
			require.NoError(t, err)

			got, err := b.GetStorageLensGroupTags("000000000000", tt.groupName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, got[tt.wantKey])

			err = b.DeleteStorageLensGroupTags("000000000000", tt.groupName)
			require.NoError(t, err)

			empty, err := b.GetStorageLensGroupTags("000000000000", tt.groupName)
			require.NoError(t, err)
			assert.Empty(t, empty)
		})
	}
}

func TestAudit1_StorageLensGroupTags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
	}{
		{name: "get_tags_missing_group", op: "get"},
		{name: "put_tags_missing_group", op: "put"},
		{name: "delete_tags_missing_group", op: "delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()

			switch tt.op {
			case "get":
				_, err := b.GetStorageLensGroupTags("000000000000", "no-group")
				require.Error(t, err)
			case "put":
				err := b.PutStorageLensGroupTags("000000000000", "no-group", s3control.TagSet{"k": "v"})
				require.Error(t, err)
			case "delete":
				err := b.DeleteStorageLensGroupTags("000000000000", "no-group")
				require.Error(t, err)
			}
		})
	}
}

func TestAudit1_Handler_StorageLensGroupTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		groupName  string
		putBody    string
		wantGet    string
		wantPutCode int
		wantGetCode int
	}{
		{
			name:      "put_and_get_tags",
			groupName: "my-group",
			putBody: `<PutStorageLensGroupTaggingRequest>
<Tags><Tag><Key>env</Key><Value>production</Value></Tag></Tags>
</PutStorageLensGroupTaggingRequest>`,
			wantGet:     "env",
			wantPutCode: http.StatusOK,
			wantGetCode: http.StatusOK,
		},
		{
			name:      "put_multiple_tags",
			groupName: "group-b",
			putBody: `<PutStorageLensGroupTaggingRequest>
<Tags>
<Tag><Key>team</Key><Value>data</Value></Tag>
<Tag><Key>cost</Key><Value>456</Value></Tag>
</Tags>
</PutStorageLensGroupTaggingRequest>`,
			wantGet:     "team",
			wantPutCode: http.StatusOK,
			wantGetCode: http.StatusOK,
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
				"000000000000", tt.putBody)
			assert.Equal(t, tt.wantPutCode, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelensgroup/"+tt.groupName+"/tagging",
				"000000000000", "")
			assert.Equal(t, tt.wantGetCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantGet)
		})
	}
}

func TestAudit1_Handler_StorageLensGroupTagging_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		preload   bool
		wantCode  int
	}{
		{
			name:      "delete_existing_tags",
			groupName: "tagged-group",
			preload:   true,
			wantCode:  http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)
			b.CreateStorageLensGroup("000000000000", tt.groupName)

			if tt.preload {
				_ = b.PutStorageLensGroupTags("000000000000", tt.groupName, s3control.TagSet{"k": "v"})
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/storagelensgroup/"+tt.groupName+"/tagging",
				"000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- MRAP PublicAccessBlock tests ----

func TestAudit1_MRAP_PublicAccessBlock_SetGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mrapName      string
		blockAcls     bool
		blockPolicy   bool
		wantBlockAcls bool
	}{
		{
			name:          "block_acls_set",
			mrapName:      "my-mrap",
			blockAcls:     true,
			blockPolicy:   false,
			wantBlockAcls: true,
		},
		{
			name:          "all_false",
			mrapName:      "mrap-open",
			blockAcls:     false,
			blockPolicy:   false,
			wantBlockAcls: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			b.CreateMultiRegionAccessPoint("000000000000", tt.mrapName, "")

			pab := s3control.PublicAccessBlock{
				BlockPublicAcls:   tt.blockAcls,
				BlockPublicPolicy: tt.blockPolicy,
			}
			err := b.SetMRAPPublicAccessBlock("000000000000", tt.mrapName, pab)
			require.NoError(t, err)

			got, err := b.GetMRAPPublicAccessBlock("000000000000", tt.mrapName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBlockAcls, got.BlockPublicAcls)
		})
	}
}

func TestAudit1_MRAP_PublicAccessBlock_NotFound(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	err := b.SetMRAPPublicAccessBlock("000000000000", "no-mrap", s3control.PublicAccessBlock{})
	require.Error(t, err)

	_, err = b.GetMRAPPublicAccessBlock("000000000000", "no-mrap")
	require.Error(t, err)
}

func TestAudit1_MRAP_PublicAccessBlock_NoConfig(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "clean-mrap", "")

	_, err := b.GetMRAPPublicAccessBlock("000000000000", "clean-mrap")
	require.ErrorIs(t, err, s3control.ErrNotFound)
}

func TestAudit1_Handler_CreateMRAP_WithPublicAccessBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mrapName    string
		body        string
		wantPAB     bool
		wantGetCode int
	}{
		{
			name:     "create_with_pab",
			mrapName: "secure-mrap",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>tok1</ClientToken>
<Details>
<Name>secure-mrap</Name>
<PublicAccessBlock>
<BlockPublicAcls>true</BlockPublicAcls>
<IgnorePublicAcls>true</IgnorePublicAcls>
<BlockPublicPolicy>true</BlockPublicPolicy>
<RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlock>
</Details>
</CreateMultiRegionAccessPointRequest>`,
			wantPAB:     true,
			wantGetCode: http.StatusOK,
		},
		{
			name:     "create_without_pab",
			mrapName: "open-mrap",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>tok2</ClientToken>
<Details><Name>open-mrap</Name></Details>
</CreateMultiRegionAccessPointRequest>`,
			wantPAB:     false,
			wantGetCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())

			rec := doS3ControlNewOpRequest(t, h, http.MethodPost,
				"/v20180820/async-requests/mrap/create", "000000000000", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/mrap/instances/"+tt.mrapName, "000000000000", "")
			assert.Equal(t, tt.wantGetCode, rec.Code)

			if tt.wantPAB {
				assert.Contains(t, rec.Body.String(), "BlockPublicAcls")
			}
		})
	}
}

// ---- StorageLensConfigMeta tests ----

func TestAudit1_StorageLensConfigMeta_PutGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		isEnabled  bool
		homeRegion string
		wantRegion string
	}{
		{
			name:       "enabled_config_with_home_region",
			configName: "my-config",
			isEnabled:  true,
			homeRegion: "us-west-2",
			wantRegion: "us-west-2",
		},
		{
			name:       "disabled_config",
			configName: "disabled-config",
			isEnabled:  false,
			homeRegion: "eu-west-1",
			wantRegion: "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			_ = b.PutStorageLensConfiguration("000000000000", tt.configName, "<Config/>")
			b.PutStorageLensConfigMeta("000000000000", tt.configName, tt.isEnabled, tt.homeRegion)

			meta, err := b.GetStorageLensConfigMeta("000000000000", tt.configName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantRegion, meta.HomeRegion)
			assert.Equal(t, tt.isEnabled, meta.IsEnabled)
			assert.NotEmpty(t, meta.StorageLensArn)
		})
	}
}

func TestAudit1_StorageLensConfigMeta_DefaultValues(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_ = b.PutStorageLensConfiguration("000000000000", "no-meta-config", "<Config/>")

	meta, err := b.GetStorageLensConfigMeta("000000000000", "no-meta-config")
	require.NoError(t, err)
	assert.Equal(t, "no-meta-config", meta.ID)
	assert.True(t, meta.IsEnabled)
	assert.NotEmpty(t, meta.HomeRegion)
	assert.Contains(t, meta.StorageLensArn, "storage-lens")
}

func TestAudit1_StorageLensConfigMeta_NotFound(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_, err := b.GetStorageLensConfigMeta("000000000000", "nonexistent")
	require.Error(t, err)
}

func TestAudit1_StorageLensConfigMeta_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configs     []string
		wantCount   int
	}{
		{
			name:      "list_multiple",
			configs:   []string{"alpha", "beta", "gamma"},
			wantCount: 3,
		},
		{
			name:      "list_empty",
			configs:   nil,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			for _, name := range tt.configs {
				_ = b.PutStorageLensConfiguration("000000000000", name, "<Config/>")
			}

			metas := b.ListStorageLensConfigMeta("000000000000")
			assert.Len(t, metas, tt.wantCount)
		})
	}
}

func TestAudit1_Handler_ListStorageLensConfigurations_IncludesMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		isEnabled  bool
		wantArn    bool
		wantRegion bool
	}{
		{
			name:       "config_with_metadata",
			configName: "full-config",
			isEnabled:  true,
			wantArn:    true,
			wantRegion: true,
		},
		{
			name:       "config_default_metadata",
			configName: "minimal-config",
			isEnabled:  false,
			wantArn:    true,
			wantRegion: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelens/"+tt.configName, "000000000000",
				`<StorageLensConfiguration><Config/></StorageLensConfiguration>`)
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			if tt.wantArn {
				assert.Contains(t, body, "StorageLensArn")
			}
			if tt.wantRegion {
				assert.Contains(t, body, "HomeRegion")
			}
			assert.Contains(t, body, tt.configName)
		})
	}
}

// ---- ListAllAccessGrantsInstances tests ----

func TestAudit1_ListAllAccessGrantsInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		instances []string
		wantCount int
	}{
		{
			name:      "single_instance",
			instances: []string{"arn:aws:sso:::instance/sso-1"},
			wantCount: 1,
		},
		{
			name:      "no_instances",
			instances: nil,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			for _, arn := range tt.instances {
				b.CreateAccessGrantsInstance("000000000000", arn)
			}

			all := b.ListAllAccessGrantsInstances("000000000000")
			assert.Len(t, all, tt.wantCount)
		})
	}
}

func TestAudit1_Handler_ListAccessGrantsInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preCreate bool
		wantCode  int
	}{
		{
			name:      "list_with_instance",
			preCreate: true,
			wantCode:  http.StatusOK,
		},
		{
			// GetAccessGrantsInstance returns 404 when none exists (one instance per account).
			name:      "list_empty_returns_not_found",
			preCreate: false,
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.preCreate {
				b.CreateAccessGrantsInstance("000000000000", "arn:aws:sso:::instance/sso-x")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/accessgrantsinstance", "000000000000", "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- GetDataAccess credentials tests ----

func TestAudit1_GetDataAccessCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		hasInstance   bool
		target        string
		permission    string
		wantErr       bool
		wantKeyPrefix string
	}{
		{
			name:          "valid_with_instance",
			hasInstance:   true,
			target:        "my-bucket",
			permission:    "READ",
			wantErr:       false,
			wantKeyPrefix: "ASIA",
		},
		{
			name:        "no_instance_returns_error",
			hasInstance: false,
			target:      "my-bucket",
			permission:  "READ",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			if tt.hasInstance {
				b.CreateAccessGrantsInstance("000000000000", "")
			}

			keyID, secretKey, sessionToken, expiration, matched, err :=
				b.GetDataAccessCredentials("000000000000", tt.target, tt.permission)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.True(t, strings.HasPrefix(keyID, tt.wantKeyPrefix),
					"expected key to start with %s, got %s", tt.wantKeyPrefix, keyID)
				assert.NotEmpty(t, secretKey)
				assert.NotEmpty(t, sessionToken)
				assert.NotEmpty(t, expiration)
				assert.NotEmpty(t, matched)
			}
		})
	}
}

func TestAudit1_Handler_GetDataAccess_WithInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hasAGI     bool
		wantCode   int
		wantInBody string
	}{
		{
			name:       "access_with_instance_returns_credentials",
			hasAGI:     true,
			wantCode:   http.StatusOK,
			wantInBody: "AccessKeyId",
		},
		{
			name:     "access_without_instance_returns_error",
			hasAGI:   false,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if tt.hasAGI {
				b.CreateAccessGrantsInstance("000000000000", "")
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/accessgrantsinstance/dataaccess?target=my-bucket&permission=READ",
				"000000000000", "")

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

// ---- Persistence tests for new audit1 maps ----

func TestAudit1_Persistence_StorageLensGroupTags(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "my-group")
	_ = b.PutStorageLensGroupTags("000000000000", "my-group", s3control.TagSet{"k": "v"})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	tags, err := b2.GetStorageLensGroupTags("000000000000", "my-group")
	require.NoError(t, err)
	assert.Equal(t, "v", tags["k"])
}

func TestAudit1_Persistence_StorageLensConfigMetas(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	_ = b.PutStorageLensConfiguration("000000000000", "my-cfg", "<Config/>")
	b.PutStorageLensConfigMeta("000000000000", "my-cfg", true, "us-east-1")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	meta, err := b2.GetStorageLensConfigMeta("000000000000", "my-cfg")
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", meta.HomeRegion)
	assert.True(t, meta.IsEnabled)
}

func TestAudit1_Persistence_MRAPPublicAccessBlock(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateMultiRegionAccessPoint("000000000000", "secure-mrap", "")
	_ = b.SetMRAPPublicAccessBlock("000000000000", "secure-mrap", s3control.PublicAccessBlock{
		BlockPublicAcls:       true,
		RestrictPublicBuckets: true,
	})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	pab, err := b2.GetMRAPPublicAccessBlock("000000000000", "secure-mrap")
	require.NoError(t, err)
	assert.True(t, pab.BlockPublicAcls)
	assert.True(t, pab.RestrictPublicBuckets)
}

func TestAudit1_Persistence_EmptyMaps_Restore(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := s3control.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))
	// Should not panic or error on empty maps
}

// ---- StorageLensConfig with IsEnabled in list ----

func TestAudit1_StorageLensConfigMeta_IsEnabledField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		isEnabled  bool
	}{
		{name: "enabled", configName: "cfg-enabled", isEnabled: true},
		{name: "disabled", configName: "cfg-disabled", isEnabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			_ = b.PutStorageLensConfiguration("000000000000", tt.configName, "")
			b.PutStorageLensConfigMeta("000000000000", tt.configName, tt.isEnabled, "")

			metas := b.ListStorageLensConfigMeta("000000000000")
			require.Len(t, metas, 1)
			assert.Equal(t, tt.isEnabled, metas[0].IsEnabled)
		})
	}
}

// ---- StorageLensGroup tagging dispatch operation name ----

func TestAudit1_ExtractOp_StorageLensGroupTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "get_tagging",
			path:   "/v20180820/storagelensgroup/my-group/tagging",
			method: http.MethodGet,
			wantOp: "GetStorageLensGroupTagging",
		},
		{
			name:   "put_tagging",
			path:   "/v20180820/storagelensgroup/my-group/tagging",
			method: http.MethodPut,
			wantOp: "PutStorageLensGroupTagging",
		},
		{
			name:   "delete_tagging",
			path:   "/v20180820/storagelensgroup/my-group/tagging",
			method: http.MethodDelete,
			wantOp: "DeleteStorageLensGroupTagging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3control.NewHandler(s3control.NewInMemoryBackend())
			op := s3control.ExtractOp(h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

// ---- GetDataAccess with SessionToken in response body ----

func TestAudit1_Handler_GetDataAccess_ResponseContainsSessionToken(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.CreateAccessGrantsInstance("000000000000", "arn:aws:sso:::instance/sso-1")

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/accessgrantsinstance/dataaccess?target=s3://mybucket&permission=READ",
		"000000000000", "")

	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "SessionToken")
	assert.Contains(t, body, "SecretAccessKey")
	assert.Contains(t, body, "Expiration")
	assert.Contains(t, body, "MatchedGrantTarget")
}

// ---- MRAP Get response includes PAB when set ----

func TestAudit1_Handler_GetMRAP_ReturnsPublicAccessBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setPAB   bool
		wantPAB  bool
	}{
		{
			name:    "mrap_with_pab",
			setPAB:  true,
			wantPAB: true,
		},
		{
			name:    "mrap_without_pab",
			setPAB:  false,
			wantPAB: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)
			b.CreateMultiRegionAccessPoint("000000000000", "test-mrap", "")

			if tt.setPAB {
				_ = b.SetMRAPPublicAccessBlock("000000000000", "test-mrap", s3control.PublicAccessBlock{
					BlockPublicAcls:       true,
					RestrictPublicBuckets: true,
				})
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/mrap/instances/test-mrap", "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			if tt.wantPAB {
				assert.Contains(t, body, "PublicAccessBlock")
				assert.Contains(t, body, "BlockPublicAcls")
			} else {
				assert.NotContains(t, body, "PublicAccessBlock")
			}
		})
	}
}

// ---- StorageLensGroup tagging clears on group delete ----

func TestAudit1_StorageLensGroupTags_DeleteGroup_TagsGone(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.CreateStorageLensGroup("000000000000", "g1")
	_ = b.PutStorageLensGroupTags("000000000000", "g1", s3control.TagSet{"k": "v"})

	_ = b.DeleteStorageLensGroup("000000000000", "g1")

	// Recreate and check tags are gone
	b.CreateStorageLensGroup("000000000000", "g1")
	tags, err := b.GetStorageLensGroupTags("000000000000", "g1")
	require.NoError(t, err)
	assert.Empty(t, tags, "tags should be empty after group was deleted and recreated")
}

// ---- SLG tagging in supported ops ----

func TestAudit1_HandlerOpsLen_Increased(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	ops := s3control.HandlerOpsLen(h)

	assert.GreaterOrEqual(t, ops, 103, "should have at least 103 supported operations")
}

// ---- ListStorageLensConfigMeta sorted output ----

func TestAudit1_ListStorageLensConfigMeta_SortedByID(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	for _, name := range []string{"zebra", "alpha", "mango"} {
		_ = b.PutStorageLensConfiguration("000000000000", name, "")
	}

	metas := b.ListStorageLensConfigMeta("000000000000")
	require.Len(t, metas, 3)
	assert.Equal(t, "alpha", metas[0].ID)
	assert.Equal(t, "mango", metas[1].ID)
	assert.Equal(t, "zebra", metas[2].ID)
}

// ---- StorageLensConfigMeta ARN format ----

func TestAudit1_StorageLensConfigMeta_ARNFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		configName string
		region     string
		wantARNParts []string
	}{
		{
			name:         "standard_arn",
			accountID:    "123456789012",
			configName:   "my-lens",
			region:       "us-east-1",
			wantARNParts: []string{"arn:aws:s3:us-east-1:123456789012:storage-lens/my-lens"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig(tt.accountID, tt.region)
			_ = b.PutStorageLensConfiguration(tt.accountID, tt.configName, "")

			meta, err := b.GetStorageLensConfigMeta(tt.accountID, tt.configName)
			require.NoError(t, err)

			for _, part := range tt.wantARNParts {
				assert.Equal(t, part, meta.StorageLensArn)
			}
		})
	}
}
