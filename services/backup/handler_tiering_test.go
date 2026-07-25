package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resourceSelectionBody builds a wire-shaped ResourceSelection request body
// entry shared by Create/Update tiering-configuration tests.
func resourceSelectionBody() []map[string]any {
	return []map[string]any{{
		"ResourceType":              "S3",
		"Resources":                 []string{"*"},
		"TieringDownSettingsInDays": 90,
	}}
}

func TestCreateTieringConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("create and get round-trips real shape", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()

		createResp := doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": "tc1",
				"BackupVaultName":          "*",
				"ResourceSelection":        resourceSelectionBody(),
			},
		})
		require.Equal(t, http.StatusOK, createResp.Code)
		created := parseResp(t, createResp)
		assert.Equal(t, "tc1", created["TieringConfigurationName"])
		assert.NotEmpty(t, created["TieringConfigurationArn"])
		assert.NotEmpty(t, created["CreationTime"])

		getResp := doREST(t, h, http.MethodGet, "/tiering-configurations/tc1", nil)
		require.Equal(t, http.StatusOK, getResp.Code)
		got := parseResp(t, getResp)
		tc, ok := got["TieringConfiguration"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "tc1", tc["TieringConfigurationName"])
		assert.Equal(t, "*", tc["BackupVaultName"])
		sel, ok := tc["ResourceSelection"].([]any)
		require.True(t, ok)
		require.Len(t, sel, 1)
		entry, ok := sel[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "S3", entry["ResourceType"])
	})

	t.Run("duplicate name is AlreadyExistsException at 400", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		body := map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": "dup",
				"BackupVaultName":          "*",
				"ResourceSelection":        resourceSelectionBody(),
			},
		}
		require.Equal(t, http.StatusOK, doREST(t, h, http.MethodPut, "/tiering-configurations", body).Code)

		rec := doREST(t, h, http.MethodPut, "/tiering-configurations", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "AlreadyExistsException")
	})

	t.Run("missing TieringConfigurationName is MissingParameterValueException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"BackupVaultName":   "*",
				"ResourceSelection": resourceSelectionBody(),
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "MissingParameterValueException")
	})

	t.Run("out-of-range TieringDownSettingsInDays is InvalidParameterValueException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": "badrange",
				"BackupVaultName":          "*",
				"ResourceSelection": []map[string]any{{
					"ResourceType":              "S3",
					"Resources":                 []string{"*"},
					"TieringDownSettingsInDays": 1,
				}},
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")
	})
}

func TestGetTieringConfiguration_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerAndBackend()

	rec := doREST(t, h, http.MethodGet, "/tiering-configurations/no-such-config", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
}

func TestListTieringConfigurations(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerAndBackend()

	for _, name := range []string{"tc_a", "tc_b"} {
		rec := doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": name,
				"BackupVaultName":          "*",
				"ResourceSelection":        resourceSelectionBody(),
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doREST(t, h, http.MethodGet, "/tiering-configurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	items, ok := resp["TieringConfigurations"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
}

func TestUpdateTieringConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("replaces vault and selection", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": "upd",
				"BackupVaultName":          "*",
				"ResourceSelection":        resourceSelectionBody(),
			},
		})

		rec := doREST(t, h, http.MethodPut, "/tiering-configurations/upd", map[string]any{
			"TieringConfiguration": map[string]any{
				"BackupVaultName": "specific-vault",
				"ResourceSelection": []map[string]any{{
					"ResourceType":              "S3",
					"Resources":                 []string{"arn:aws:s3:::b"},
					"TieringDownSettingsInDays": 120,
				}},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseResp(t, rec)
		assert.Equal(t, "upd", resp["TieringConfigurationName"])
		assert.NotEmpty(t, resp["LastUpdatedTime"])

		getResp := doREST(t, h, http.MethodGet, "/tiering-configurations/upd", nil)
		got := parseResp(t, getResp)
		tc := got["TieringConfiguration"].(map[string]any)
		assert.Equal(t, "specific-vault", tc["BackupVaultName"])
	})

	t.Run("unknown name is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodPut, "/tiering-configurations/nope", map[string]any{
			"TieringConfiguration": map[string]any{
				"BackupVaultName":   "*",
				"ResourceSelection": resourceSelectionBody(),
			},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	})
}

func TestDeleteTieringConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("removes configuration", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		doREST(t, h, http.MethodPut, "/tiering-configurations", map[string]any{
			"TieringConfiguration": map[string]any{
				"TieringConfigurationName": "del",
				"BackupVaultName":          "*",
				"ResourceSelection":        resourceSelectionBody(),
			},
		})

		rec := doREST(t, h, http.MethodDelete, "/tiering-configurations/del", nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		getResp := doREST(t, h, http.MethodGet, "/tiering-configurations/del", nil)
		assert.Equal(t, http.StatusBadRequest, getResp.Code)
	})

	t.Run("unknown name is ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandlerAndBackend()
		rec := doREST(t, h, http.MethodDelete, "/tiering-configurations/nope", nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	})
}
