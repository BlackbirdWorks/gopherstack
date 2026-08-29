package mq_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestDescribeConfigurationRevision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		revision   string
		wantStatus int
	}{
		{
			name:       "valid_revision_1",
			revision:   "1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_revision",
			revision:   "99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_revision_format",
			revision:   "not-a-number",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			configID := createTestConfig(t, h, "revision-test-config", mq.EngineTypeActiveMQ)

			rec := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID+"/revisions/"+tt.revision, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assert.Equal(t, configID, resp["configurationId"])
				assert.NotNil(t, resp["revision"])
			}
		})
	}
}

func TestDescribeConfigurationRevision_NotFoundConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/configurations/nonexistent-id/revisions/1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListConfigurationRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configID    string
		numUpdates  int
		wantRevs    int
		wantStatus  int
		usePrebuilt bool
	}{
		{
			name:       "initial_revision",
			numUpdates: 0,
			wantRevs:   1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "after_two_updates",
			numUpdates: 2,
			wantRevs:   3,
			wantStatus: http.StatusOK,
		},
		{
			name:        "config_not_found",
			wantStatus:  http.StatusNotFound,
			configID:    "nonexistent-config",
			usePrebuilt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			configID := tt.configID
			if !tt.usePrebuilt {
				configID = createTestConfig(t, h, "revisions-test-config", mq.EngineTypeActiveMQ)

				for range tt.numUpdates {
					rec := doRequest(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
						"description": "updated",
						"data":        "<broker></broker>",
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID+"/revisions", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				revs, ok := resp["revisions"].([]any)
				require.True(t, ok)
				assert.Len(t, revs, tt.wantRevs)
			}
		})
	}
}

func TestUpdateConfiguration_RevisionIncrements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "rev-test-cfg", mq.EngineTypeActiveMQ)

	// Initial revision is 1.
	desc := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc.Code)
	latestRev := parseResponse(t, desc)["latestRevision"].(map[string]any)
	assert.Equal(t, 1, int(latestRev["revision"].(float64)))

	// Update bumps to revision 2.
	rec := doRequest(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
		"data":        "updated-config-data",
		"description": "second revision",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc2 := doRequest(t, h, http.MethodGet, "/v1/configurations/"+configID, nil)
	require.Equal(t, http.StatusOK, desc2.Code)
	latestRev2 := parseResponse(t, desc2)["latestRevision"].(map[string]any)
	assert.Equal(t, 2, int(latestRev2["revision"].(float64)))
}

func TestDescribeConfigurationRevision_DataRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "data-rt-cfg", mq.EngineTypeActiveMQ)

	testData := "<activemq-config>test</activemq-config>"
	rec := doRequest(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
		"data":        testData,
		"description": "test revision",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	revRec := doRequest(t, h, http.MethodGet,
		"/v1/configurations/"+configID+"/revisions/2", nil)
	require.Equal(t, http.StatusOK, revRec.Code)

	out := parseResponse(t, revRec)
	assert.Equal(t, testData, out["data"],
		"configuration data must round-trip through update and revision fetch")
	assert.Equal(t, 2, int(out["revision"].(float64)))
	assert.Equal(t, "test revision", out["description"])
}

func TestListConfigurationRevisions_ReturnsAllRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	configID := createTestConfig(t, h, "multi-rev-cfg", mq.EngineTypeActiveMQ)

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPut, "/v1/configurations/"+configID, map[string]any{
			"data":        "revision-" + string(rune('a'+i)),
			"description": "rev " + string(rune('a'+i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doRequest(t, h, http.MethodGet,
		"/v1/configurations/"+configID+"/revisions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	out := parseResponse(t, listRec)
	revisions, ok := out["revisions"].([]any)
	require.True(t, ok, "response must contain revisions array")
	assert.Len(t, revisions, 4, "should have 4 revisions (initial + 3 updates)")
}

func TestConfigRevision_Cap50_OldestPruned(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	cfg, err := b.CreateConfiguration("rev-cap-cfg", "init", mq.EngineTypeActiveMQ, "", "", nil)
	require.NoError(t, err)

	for i := range 55 {
		_, err = b.UpdateConfiguration(cfg.ID, "desc", strings.Repeat("x", i+1))
		require.NoError(t, err)
	}

	revisions, err := b.ListConfigurationRevisions(cfg.ID)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(revisions), 50,
		"configuration must not keep more than 50 revisions")

	firstRev := revisions[0].Revision
	assert.Greater(t, firstRev, int32(1),
		"oldest revisions must have been pruned; first surviving revision must be > 1")
}

func TestConfigRevision_OldestRevisionDataPruned(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	cfg, err := b.CreateConfiguration("rev-prune-cfg", "init", mq.EngineTypeActiveMQ, "", "", nil)
	require.NoError(t, err)

	for range 55 {
		_, err = b.UpdateConfiguration(cfg.ID, "", "data")
		require.NoError(t, err)
	}

	_, _, err = b.DescribeConfigurationRevision(cfg.ID, 1)
	require.Error(t, err, "revision 1 must be pruned after 55 updates")
	require.ErrorIs(t, err, mq.ErrNotFound)
}
