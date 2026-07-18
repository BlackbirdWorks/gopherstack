package fis_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFISHandler_CreateTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		roleArn    string
		desc       string
		wantStatus int
	}{
		{
			name:       "create_with_description",
			roleArn:    "arn:aws:iam::111111111111:role/FISRole",
			desc:       "target account for testing",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "create_without_description",
			roleArn:    "arn:aws:iam::222222222222:role/FISRole",
			desc:       "",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)
			accountID := "111111111111"
			path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountID)

			body := map[string]any{
				"roleArn":     tt.roleArn,
				"description": tt.desc,
			}

			rec := doRequest(t, h, http.MethodPost, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				TargetAccountConfiguration struct {
					AccountID   string `json:"accountId"`
					RoleArn     string `json:"roleArn"`
					Description string `json:"description"`
				} `json:"targetAccountConfiguration"`
			}

			mustJSON(t, rec, &resp)
			assert.Equal(t, accountID, resp.TargetAccountConfiguration.AccountID)
			assert.Equal(t, tt.roleArn, resp.TargetAccountConfiguration.RoleArn)
			assert.Equal(t, tt.desc, resp.TargetAccountConfiguration.Description)
		})
	}
}

func TestFISHandler_CreateTargetAccountConfiguration_TemplateNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	path := "/experimentTemplates/EXTnonexistent0000000000/targetAccountConfigurations/111111111111"

	body := map[string]any{
		"roleArn": "arn:aws:iam::111111111111:role/FISRole",
	}

	rec := doRequest(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFISHandler_GetTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		wantStatus int
	}{
		{
			name:       "existing_config",
			accountID:  "111111111111",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_config",
			accountID:  "999999999999",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)

			// Create a config for accountID 111111111111.
			createPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/111111111111", templateID)
			rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
				"roleArn": "arn:aws:iam::111111111111:role/FISRole",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Get.
			getPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, tt.accountID)
			rec2 := doRequest(t, h, http.MethodGet, getPath, nil)
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestFISHandler_UpdateTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updateBody  map[string]any
		wantRoleArn string
		wantDesc    string
		wantStatus  int
	}{
		{
			name:        "update_role_arn",
			updateBody:  map[string]any{"roleArn": "arn:aws:iam::111111111111:role/NewRole"},
			wantRoleArn: "arn:aws:iam::111111111111:role/NewRole",
			wantDesc:    "initial description",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "update_description",
			updateBody:  map[string]any{"description": "updated description"},
			wantRoleArn: "arn:aws:iam::111111111111:role/FISRole",
			wantDesc:    "updated description",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)
			accountID := "111111111111"

			// Create initial config.
			createPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountID)
			rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
				"roleArn":     "arn:aws:iam::111111111111:role/FISRole",
				"description": "initial description",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Update.
			updatePath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountID)
			rec2 := doRequest(t, h, http.MethodPatch, updatePath, tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			var resp struct {
				TargetAccountConfiguration struct {
					AccountID   string `json:"accountId"`
					RoleArn     string `json:"roleArn"`
					Description string `json:"description"`
				} `json:"targetAccountConfiguration"`
			}

			mustJSON(t, rec2, &resp)
			assert.Equal(t, tt.wantRoleArn, resp.TargetAccountConfiguration.RoleArn)
			assert.Equal(t, tt.wantDesc, resp.TargetAccountConfiguration.Description)
		})
	}
}

func TestFISHandler_DeleteTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		wantStatus int
	}{
		{
			name:       "delete_existing",
			accountID:  "111111111111",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_nonexistent",
			accountID:  "999999999999",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)

			// Create a config for accountID 111111111111.
			createPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/111111111111", templateID)
			rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
				"roleArn": "arn:aws:iam::111111111111:role/FISRole",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Delete.
			deletePath := fmt.Sprintf(
				"/experimentTemplates/%s/targetAccountConfigurations/%s",
				templateID,
				tt.accountID,
			)
			rec2 := doRequest(t, h, http.MethodDelete, deletePath, nil)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantStatus == http.StatusOK {
				// Verify it's gone.
				rec3 := doRequest(t, h, http.MethodGet, deletePath, nil)
				assert.Equal(t, http.StatusNotFound, rec3.Code)
			}
		})
	}
}

func TestFISHandler_ListTargetAccountConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accountIDs   []string
		wantCount    int
		wantStatus   int
		unknownTplID bool
	}{
		{
			name:       "empty_list",
			accountIDs: nil,
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "single_config",
			accountIDs: []string{"111111111111"},
			wantCount:  1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple_configs",
			accountIDs: []string{"111111111111", "222222222222", "333333333333"},
			wantCount:  3,
			wantStatus: http.StatusOK,
		},
		{
			name:         "template_not_found",
			unknownTplID: true,
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)

			for _, accountID := range tt.accountIDs {
				path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountID)
				rec := doRequest(t, h, http.MethodPost, path, map[string]any{
					"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			listTplID := templateID
			if tt.unknownTplID {
				listTplID = "EXTnonexistent0000000000"
			}

			listPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations", listTplID)
			rec := doRequest(t, h, http.MethodGet, listPath, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp struct {
					TargetAccountConfigurations []struct {
						AccountID string `json:"accountId"`
					} `json:"targetAccountConfigurations"`
				}

				mustJSON(t, rec, &resp)
				assert.Len(t, resp.TargetAccountConfigurations, tt.wantCount)
			}
		})
	}
}

func TestFISHandler_TargetAccountConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	accountIDs := []string{"111111111111", "222222222222"}

	// Create configs.
	for _, accountID := range accountIDs {
		path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountID)
		rec := doRequest(t, h, http.MethodPost, path, map[string]any{
			"roleArn":     "arn:aws:iam::" + accountID + ":role/FISRole",
			"description": "account " + accountID,
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// List verifies both exist.
	listPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations", templateID)
	rec := doRequest(t, h, http.MethodGet, listPath, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		TargetAccountConfigurations []struct {
			AccountID string `json:"accountId"`
		} `json:"targetAccountConfigurations"`
	}

	mustJSON(t, rec, &listResp)
	assert.Len(t, listResp.TargetAccountConfigurations, 2)

	// Delete one.
	deletePath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", templateID, accountIDs[0])
	rec2 := doRequest(t, h, http.MethodDelete, deletePath, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	// List verifies one remains.
	rec3 := doRequest(t, h, http.MethodGet, listPath, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	mustJSON(t, rec3, &listResp)
	assert.Len(t, listResp.TargetAccountConfigurations, 1)
	assert.Equal(t, accountIDs[1], listResp.TargetAccountConfigurations[0].AccountID)
}

func TestFISHandler_TargetAccountConfiguration_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/999999999999", templateID)
	rec := doRequest(t, h, http.MethodPatch, path, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestTargetAccountConfig_Description_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	path := "/experimentTemplates/" + tplID + "/targetAccountConfigurations/111111111111"
	rec := doRequest(t, h, http.MethodPost, path, map[string]any{
		"roleArn":     "arn:aws:iam::111111111111:role/FISRole",
		"description": "prod account for chaos testing",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			AccountID   string `json:"accountId"`
			RoleArn     string `json:"roleArn"`
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, "111111111111", resp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, "prod account for chaos testing", resp.TargetAccountConfiguration.Description)

	// GET must return same description.
	rec2 := doRequest(t, h, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, "prod account for chaos testing", getResp.TargetAccountConfiguration.Description)
}

// ----------------------------------------
// ListTargetAccountConfigurations: empty list not null
// ----------------------------------------

func TestListTargetAccountConfigs_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+tplID+"/targetAccountConfigurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetAccountConfigurations []any `json:"targetAccountConfigurations"`
	}

	mustJSON(t, rec, &resp)
	assert.NotNil(t, resp.TargetAccountConfigurations, "empty list must not be null")
	assert.Empty(t, resp.TargetAccountConfigurations)
}

// ----------------------------------------
// GetTargetAccountConfiguration: not found → 404
// ----------------------------------------

func TestGetTargetAccountConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/999999999999", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &resp)
	// FIS's API model defines a single ResourceNotFoundException shape service-wide.
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

// ----------------------------------------
// DeleteTargetAccountConfiguration: returns deleted config body
// ----------------------------------------

func TestDeleteTargetAccountConfig_ReturnsBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	accountID := "222222222222"
	path := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/%s", tplID, accountID)

	rec := doRequest(t, h, http.MethodPost, path, map[string]any{
		"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Delete returns the deleted config (200 with body, not 204).
	rec2 := doRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			AccountID string `json:"accountId"`
			RoleArn   string `json:"roleArn"`
		} `json:"targetAccountConfiguration"`
	}

	mustJSON(t, rec2, &resp)
	assert.Equal(t, accountID, resp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, "arn:aws:iam::"+accountID+":role/FISRole", resp.TargetAccountConfiguration.RoleArn)

	// Confirm it's gone.
	rec3 := doRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

// ----------------------------------------
// GetAction: response shape (id, arn, description, parameters, targets)
// ----------------------------------------

func TestCreateTargetAccountConfig_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-role1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-role1",
	})

	_, err := b.CreateTargetAccountConfiguration("EXT-role1", "123456789012", "", "desc")
	require.Error(t, err)
	assert.ErrorIs(t, err, fis.ErrValidation)
}

// ----------------------------------------
// Cascade delete
// ----------------------------------------

func TestSortedListTargetAccountConfigurations(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-sortcfg",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-sortcfg",
	})

	for _, acct := range []string{"999999999999", "111111111111", "555555555555"} {
		b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
			ExperimentTemplateID: "EXT-sortcfg",
			AccountID:            acct,
			RoleArn:              "arn:aws:iam::" + acct + ":role/FISRole",
		})
	}

	list, err := b.ListTargetAccountConfigurations("EXT-sortcfg")
	require.NoError(t, err)
	require.Len(t, list, 3)

	assert.Equal(t, "111111111111", list[0].AccountID)
	assert.Equal(t, "555555555555", list[1].AccountID)
	assert.Equal(t, "999999999999", list[2].AccountID)
}

func TestTargetAccountConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	accountID := "555555555555"
	roleArn := "arn:aws:iam::555555555555:role/FISCrossAccount"
	description := "cross-account config"

	// Create
	rec := doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{"roleArn": roleArn, "description": description},
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		TargetAccountConfiguration struct {
			AccountID   string `json:"accountId"`
			RoleArn     string `json:"roleArn"`
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec, &createResp)
	assert.Equal(t, accountID, createResp.TargetAccountConfiguration.AccountID)
	assert.Equal(t, roleArn, createResp.TargetAccountConfiguration.RoleArn)

	// Get
	rec2 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec2, &getResp)
	assert.Equal(t, description, getResp.TargetAccountConfiguration.Description)

	// List
	rec3 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp struct {
		TargetAccountConfigurations []struct {
			AccountID string `json:"accountId"`
		} `json:"targetAccountConfigurations"`
	}
	mustJSON(t, rec3, &listResp)
	require.Len(t, listResp.TargetAccountConfigurations, 1)
	assert.Equal(t, accountID, listResp.TargetAccountConfigurations[0].AccountID)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	// Verify deleted
	rec5 := doRequest(t, h, http.MethodGet,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestUpdateTargetAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)
	accountID := "666666666666"

	doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{
			"roleArn":     "arn:aws:iam::666666666666:role/OldRole",
			"description": "old desc",
		},
	)

	rec := doRequest(
		t, h, http.MethodPatch,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/"+accountID,
		map[string]any{"description": "new desc"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		TargetAccountConfiguration struct {
			Description string `json:"description"`
			RoleArn     string `json:"roleArn"`
		} `json:"targetAccountConfiguration"`
	}
	mustJSON(t, rec, &resp)
	assert.Equal(t, "new desc", resp.TargetAccountConfiguration.Description)
	assert.Equal(t, "arn:aws:iam::666666666666:role/OldRole", resp.TargetAccountConfiguration.RoleArn)
}

// ----------------------------------------
// Experiment target account config operations
// ----------------------------------------
