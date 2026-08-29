package mq_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestConfigurationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		engineType string
	}{
		{
			name:       "activemq_config",
			configName: "my-activemq-config",
			engineType: "ACTIVEMQ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create configuration.
			rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
				"name":          tt.configName,
				"engineType":    tt.engineType,
				"engineVersion": "5.15.14",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			createResp := parseResponse(t, rec)
			assert.NotEmpty(t, createResp["id"])
			assert.NotEmpty(t, createResp["arn"])

			configID := createResp["id"].(string)

			// Describe configuration.
			rec = doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			descResp := parseResponse(t, rec)
			assert.Equal(t, tt.configName, descResp["name"])
			assert.Equal(t, tt.engineType, descResp["engineType"])

			// List configurations.
			rec = doRequest(t, h, http.MethodGet, "/v1/configurations", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			configs, ok := parseResponse(t, rec)["configurations"].([]any)
			require.True(t, ok)
			assert.Len(t, configs, 1)
		})
	}
}

func TestUpdateConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "updateable-config", mq.EngineTypeActiveMQ)

	updateBody, err := json.Marshal(map[string]any{
		"description": "updated description",
		"data":        "<broker>...</broker>",
	})
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPut, "/v1/configurations/"+configID, bytes.NewReader(updateBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mq/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	updateResp := parseResponse(t, rec)
	assert.Equal(t, configID, updateResp["id"])
	assert.NotNil(t, updateResp["latestRevision"])
}

func TestUpdateConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/v1/configurations/nonexistent-id",
		map[string]any{"description": "updated"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateConfiguration_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateConfiguration_MissingEngineType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name": "my-config",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDescribeConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/configurations/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateConfiguration_InvalidEngineType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "empty", engineType: ""},
		{name: "invalid", engineType: "KAFKA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.CreateConfiguration("my-cfg", "", tt.engineType, "", "", nil)
			require.ErrorIs(t, err, mq.ErrValidation)
		})
	}
}

func TestCreateConfiguration_InvalidEngineType_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "bad-config",
		"engineType": "BAD",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateConfiguration_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
		wantErr    bool
	}{
		{name: "empty rejected", configName: "", wantErr: true},
		{name: "single char accepted", configName: "a", wantErr: false},
		{name: "exactly 150 chars accepted", configName: strings.Repeat("a", 150), wantErr: false},
		{name: "151 chars rejected", configName: strings.Repeat("a", 151), wantErr: true},
		{name: "hyphen allowed", configName: "my-config", wantErr: false},
		{name: "period allowed", configName: "my.config", wantErr: false},
		{name: "underscore allowed", configName: "my_config", wantErr: false},
		{name: "tilde allowed", configName: "my~config", wantErr: false},
		{name: "space rejected", configName: "my config", wantErr: true},
		{name: "at-sign rejected", configName: "my@config", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			_, err := b.CreateConfiguration(tt.configName, "", mq.EngineTypeActiveMQ, "", "", nil)

			if tt.wantErr {
				require.ErrorIs(t, err, mq.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateConfiguration_ValidEngineTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engineType string
	}{
		{name: "activemq", engineType: mq.EngineTypeActiveMQ},
		{name: "rabbitmq", engineType: mq.EngineTypeRabbitMQ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			cfg, err := b.CreateConfiguration("cfg-"+tt.name, "", tt.engineType, "", "", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.engineType, cfg.EngineType)
		})
	}
}

func TestDeleteConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		preCreate  bool
	}{
		{
			name:       "delete_existing",
			preCreate:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_nonexistent",
			preCreate:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			configID := "nonexistent-config-id"

			if tt.preCreate {
				configID = createTestConfig(t, h, "delete-me-config", mq.EngineTypeActiveMQ)
			}

			rec := doRequest(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.preCreate {
				// Verify it's gone.
				rec = doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			}
		})
	}
}

func TestDeleteConfiguration_InUseByBroker(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "referenced-config", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName":    "config-user-broker",
		"engineType":    mq.EngineTypeActiveMQ,
		"configuration": map[string]any{"id": configID, "revision": 1},
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateBroker failed: %s", rec.Body.String())

	rec = doRequest(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code, "delete of in-use configuration must fail: %s", rec.Body.String())
	assert.Equal(t, "ConflictException", parseResponse(t, rec)["__type"])

	// still describable -- delete must not have partially applied.
	rec = doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestListConfigurations_PaginationOpaqueToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		configNames   []string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:          "single_page_no_token",
			configNames:   []string{"cfg-a"},
			maxResults:    10,
			wantCount:     1,
			wantNextToken: false,
		},
		{
			name:          "two_pages_opaque_token",
			configNames:   []string{"cfg-1", "cfg-2", "cfg-3"},
			maxResults:    1,
			wantCount:     1,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range tt.configNames {
				createTestConfig(t, h, name, mq.EngineTypeActiveMQ)
			}

			path := "/v1/configurations?maxResults=" + strconv.Itoa(tt.maxResults)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var cfgs []any
			require.NoError(t, json.Unmarshal(out["configurations"], &cfgs))
			assert.Len(t, cfgs, tt.wantCount)

			rawToken, hasToken := out["nextToken"]
			if tt.wantNextToken {
				require.True(t, hasToken)

				var tok string
				require.NoError(t, json.Unmarshal(rawToken, &tok))

				offset := decodeOpaqueToken(t, tok)
				assert.Equal(t, tt.wantCount, offset)
			} else if hasToken {
				var tok string
				require.NoError(t, json.Unmarshal(rawToken, &tok))
				assert.Empty(t, tok)
			}
		})
	}
}

func TestCreateConfiguration_ResponseIncludesEngineType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-engine-type",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, mq.EngineTypeActiveMQ, parseResponse(t, rec)["engineType"],
		"CreateConfiguration response must include engineType")
}

func TestCreateConfiguration_ResponseIncludesEngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":          "cfg-engine-ver",
		"engineType":    mq.EngineTypeActiveMQ,
		"engineVersion": "5.18.3",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.Equal(t, "5.18.3", parseResponse(t, rec)["engineVersion"],
		"CreateConfiguration response must include engineVersion")
}

func TestCreateConfiguration_ResponseDefaultEngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-default-ver",
		"engineType": mq.EngineTypeRabbitMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assert.NotEmpty(t, parseResponse(t, rec)["engineVersion"],
		"CreateConfiguration response engineVersion must not be empty when not specified")
}

func TestCreateConfiguration_ResponseFieldsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "cfg-fields",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)

	requiredFields := []string{"id", "arn", "name", "engineType", "engineVersion", "latestRevision"}
	for _, field := range requiredFields {
		assert.Contains(t, out, field, "CreateConfiguration response must contain %q field", field)
	}
}

func TestCreateConfiguration_RabbitMQ_EngineTypeRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "rmq-cfg",
		"engineType": mq.EngineTypeRabbitMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	assert.Equal(t, mq.EngineTypeRabbitMQ, out["engineType"])

	// Verify DescribeConfiguration also returns engineType.
	configID := out["id"].(string)
	desc := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc.Code)

	assert.Equal(t, mq.EngineTypeRabbitMQ, parseResponse(t, desc)["engineType"])
}

func TestListConfigurations_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createTestConfig(t, h, "cfg-paginate-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doRequest(t, h, http.MethodGet, "/v1/configurations?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	cfgs := out["configurations"].([]any)
	assert.Len(t, cfgs, 3, "maxResults=3 must return 3 configurations")
	assert.Contains(t, out, "nextToken")
}

func TestListConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	engineTypes := []string{mq.EngineTypeActiveMQ, mq.EngineTypeRabbitMQ, mq.EngineTypeActiveMQ, mq.EngineTypeRabbitMQ}

	for i := range 4 {
		createTestConfig(t, h, "cfg-page2-"+string(rune('a'+i)), engineTypes[i])
	}

	// Page 1.
	rec1 := doRequest(t, h, http.MethodGet, "/v1/configurations?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	page1 := parseResponse(t, rec1)
	cfgs1 := page1["configurations"].([]any)
	require.Len(t, cfgs1, 2)
	nextToken := page1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/configurations?maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	page2 := parseResponse(t, rec2)
	cfgs2 := page2["configurations"].([]any)
	require.Len(t, cfgs2, 2)
	_, hasMore := page2["nextToken"]
	assert.False(t, hasMore, "last page must not have nextToken")
}

func TestListConfigurations_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/v1/configurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	cfgs, hasCfgs := resp["configurations"]
	assert.True(t, hasCfgs, "ListConfigurations must include 'configurations' key")
	assert.IsType(t, []any{}, cfgs, "'configurations' must be an array")
	assert.Empty(t, cfgs, "'configurations' must be [] when none exist")
}

func TestDescribeConfiguration_TagsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "notag-cfg", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	tags, hasTagsKey := resp["tags"]
	assert.True(t, hasTagsKey, "DescribeConfiguration must include 'tags' key even when empty")
	assert.IsType(t, map[string]any{}, tags, "'tags' must be an object, not null")
	assert.Empty(t, tags, "'tags' must be empty {} not populated")
}

func TestDeleteConfiguration_Returns200(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "del-cfg-test", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDeleteConfiguration_NotFoundAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "del-cfg-gone", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodDelete, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	assert.Equal(t, http.StatusNotFound, desc.Code)
}

func TestCreateConfiguration_DuplicateName_Returns409(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestConfig(t, h, "dup-cfg", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "dup-cfg",
		"engineType": mq.EngineTypeActiveMQ,
	})
	assert.Equal(t, http.StatusConflict, rec.Code,
		"duplicate configuration name must return 409")
}

func TestConfiguration_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":       "arn-cfg",
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	configArn := out["arn"].(string)
	assert.True(t, strings.HasPrefix(configArn, "arn:aws:mq:"),
		"configuration ARN must start with arn:aws:mq:, got %s", configArn)
	assert.Contains(t, configArn, "123456789012")
	assert.Contains(t, configArn, "us-east-1")
}

// TestUpdateConfiguration_DataSizeLimit verifies the 256 KiB configuration
// data cap: rejected just over the limit, accepted exactly at the limit.
func TestUpdateConfiguration_DataSizeLimit(t *testing.T) {
	t.Parallel()

	t.Run("over_limit_rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cfg, err := b.CreateConfiguration("cfg-size", "desc", mq.EngineTypeActiveMQ, "", "", nil)
		require.NoError(t, err)

		oversizedData := strings.Repeat("a", 256*1024+1)
		_, err = b.UpdateConfiguration(cfg.ID, "too big", oversizedData)
		require.Error(t, err)
		require.ErrorIs(t, err, mq.ErrValidation, "oversized configuration data must return ErrValidation")
	})

	t.Run("at_limit_accepted", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)
		cfg, err := b.CreateConfiguration("limit-cfg", "", mq.EngineTypeActiveMQ, "", "", nil)
		require.NoError(t, err)

		atLimit := strings.Repeat("a", 256*1024)
		updated, err := b.UpdateConfiguration(cfg.ID, "at limit", atLimit)
		require.NoError(t, err, "exactly 256KiB must be accepted")
		assert.Equal(t, int32(2), updated.LatestRevision.Revision)
	})
}
