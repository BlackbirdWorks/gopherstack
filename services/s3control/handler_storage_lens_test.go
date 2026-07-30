package s3control_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Storage Lens Configuration ----

func TestStorageLensConfiguration(t *testing.T) {
	t.Parallel()

	const configName = "my-config"
	const configPath = "/v20180820/storagelens/" + configName
	const listPath = "/v20180820/storagelens"

	t.Run("put and get configuration", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		// This is the real aws-sdk-go-v2 wire shape: the full
		// configuration nests directly under
		// "<StorageLensConfiguration>" in the request body (confirmed via
		// awsRestxml_serializeOpDocumentPutStorageLensConfigurationInput),
		// NOT under a "<Config>" child -- see storageLensConfigurationXML's
		// doc comment in handler_storage_lens.go for the previous bug this
		// locks in a fix for.
		putRec := doS3Request(t, h, http.MethodPut, configPath,
			`<PutStorageLensConfigurationRequest>`+
				`<StorageLensConfiguration><IsEnabled>true</IsEnabled></StorageLensConfiguration>`+
				`</PutStorageLensConfigurationRequest>`)
		require.Equal(t, http.StatusOK, putRec.Code)

		getRec := doS3Request(t, h, http.MethodGet, configPath, "")
		require.Equal(t, http.StatusOK, getRec.Code)
		body := getRec.Body.String()
		assert.Contains(t, body, configName)
		// The real IsEnabled field a client sent must round-trip as a
		// direct child of <StorageLensConfiguration>, not be dropped.
		assert.Contains(
			t, body,
			"<StorageLensConfiguration><Id>my-config</Id><IsEnabled>true</IsEnabled></StorageLensConfiguration>",
		)
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

func TestStorageLensConfigurationTagging(t *testing.T) {
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

func TestStorageLensGroups(t *testing.T) {
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

func TestBackendStorageLensConfigTagging(t *testing.T) {
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

// ---- StorageLensGroup Filter tests ----

func TestStorageLensGroup_UpdateFilter(t *testing.T) {
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

func TestStorageLensGroup_UpdateFilter_MissingGroup(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.UpdateStorageLensGroupFilter("000000000000", "nonexistent", "filter")
	require.Error(t, err)
}

func TestHandler_CreateStorageLensGroup_WithFilter(t *testing.T) {
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

// ---- StorageLensGroup CreatedAt tests ----

func TestStorageLensGroup_CreatedAtSet(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	grp := b.CreateStorageLensGroup("000000000000", "my-group")
	assert.NotEmpty(t, grp.CreatedAt)
}

func TestCreateStorageLensGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		body       string
		wantStatus int
	}{
		{
			name:      "creates_storage_lens_group",
			accountID: "123456789012",
			body: `<CreateStorageLensGroupRequest>
<StorageLensGroup>
<Name>my-lens-group</Name>
</StorageLensGroup>
</CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "creates_storage_lens_group_empty_name",
			accountID:  "000000000000",
			body:       `<CreateStorageLensGroupRequest><StorageLensGroup></StorageLensGroup></CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(t, h, http.MethodPost, "/v20180820/storagelensgroup", tt.accountID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- Storage Lens Configuration (table-driven variants) ----

func TestStorageLensConfiguration_Table(t *testing.T) {
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

			// Real aws-sdk-go-v2 wire shape: the configuration nests
			// directly under "<StorageLensConfiguration>" inside
			// "<PutStorageLensConfigurationRequest>", with no "<Config>"
			// wrapper (see storageLensConfigurationXML's doc comment).
			body := `<PutStorageLensConfigurationRequest>` +
				`<StorageLensConfiguration>` + tt.config + `</StorageLensConfiguration>` +
				`</PutStorageLensConfigurationRequest>`
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
				"/v20180820/storagelens/"+tt.configName, "000000000000", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/storagelens/"+tt.configName, "000000000000", "")
			require.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.configName)
			if tt.config != "" {
				assert.Contains(t, rec.Body.String(), tt.config)
			}
		})
	}
}

func TestStorageLensConfiguration_Delete(t *testing.T) {
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

func TestStorageLensConfiguration_GetMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
		"/v20180820/storagelens/no-such-config", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListStorageLensConfigurations(t *testing.T) {
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

			// ListStorageLensConfigurationsOutput's list is FLATTENED in
			// the real SDK -- repeated "<StorageLensConfiguration>"
			// elements directly under the result, no wrapping
			// "<StorageLensConfigurationList>" element (see
			// awsRestxml_deserializeDocumentStorageLensConfigurationListUnwrapped).
			// Assert the literal nested envelope, not a substring: a
			// wrapper-based decode target would silently see zero items
			// against the real (flattened) response, so round-tripping
			// into a flattened decode target is the only shape that
			// proves the fix.
			assert.NotContains(t, body, "StorageLensConfigurationList")
			var out struct {
				XMLName xml.Name `xml:"ListStorageLensConfigurationsResult"`
				Configs []struct {
					ID string `xml:"Id"`
				} `xml:"StorageLensConfiguration"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.Configs, len(tt.wantIDs))
			gotIDs := make([]string, len(out.Configs))
			for i, c := range out.Configs {
				gotIDs[i] = c.ID
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestStorageLensConfigurationTagging_Table(t *testing.T) {
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

func TestStorageLensConfigTagging_PutViaHTTP(t *testing.T) {
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

func TestStorageLensConfigTagging_Delete(t *testing.T) {
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

// ---- Storage Lens Groups (table-driven variants) ----

func TestStorageLensGroup_CreateAndGet(t *testing.T) {
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

func TestStorageLensGroup_ListShowsFilter(t *testing.T) {
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

			// ListStorageLensGroupsOutput's list is FLATTENED in the real
			// SDK (repeated "<StorageLensGroup>" elements directly under
			// the result, no wrapping "<StorageLensGroupList>" element --
			// see awsRestxml_deserializeDocumentStorageLensGroupListUnwrapped)
			// and its entries (ListStorageLensGroupEntry) carry no
			// CreatedAt or Filter field. Assert the literal nested
			// envelope and the absence of fabricated fields, not
			// substrings.
			assert.NotContains(t, body, "StorageLensGroupList")
			assert.NotContains(t, body, "CreatedAt")
			var out struct {
				XMLName xml.Name `xml:"ListStorageLensGroupsResult"`
				Groups  []struct {
					Name                string `xml:"Name"`
					StorageLensGroupArn string `xml:"StorageLensGroupArn"`
				} `xml:"StorageLensGroup"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.Groups, tt.wantLen)
			gotNames := make([]string, len(out.Groups))
			for i, g := range out.Groups {
				gotNames[i] = g.Name
				assert.NotEmpty(t, g.StorageLensGroupArn)
			}
			assert.ElementsMatch(t, tt.groups, gotNames)
		})
	}
}

func TestStorageLensGroup_UpdateFilter_Table(t *testing.T) {
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

func TestStorageLensGroup_UpdateMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodPut,
		"/v20180820/storagelensgroup/no-such-group", "000000000000",
		`<StorageLensGroup><Name>no-such-group</Name></StorageLensGroup>`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStorageLensGroup_DeleteMissing(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)

	rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
		"/v20180820/storagelensgroup/no-group", "000000000000", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestStorageLensGroup_DeleteCascadeCleansTags locks in the ghost-map-row
// fix: DeleteStorageLensGroup previously left generic resource tags behind
// forever after the group row itself was removed.
func TestStorageLensGroup_DeleteCascadeCleansTags(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	grp := b.CreateStorageLensGroup("000000000000", "cascade-slg")
	b.TagResource(grp.StorageLensGroupArn, map[string]string{"env": "test"})

	require.NoError(t, b.DeleteStorageLensGroup("000000000000", "cascade-slg"))

	assert.Empty(t, b.ListTagsForResource(grp.StorageLensGroupArn), "tags must not survive delete")
}

// ---- StorageLensGroup ARN contains region ----

func TestStorageLensGroup_ArnContainsRegion(t *testing.T) {
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
