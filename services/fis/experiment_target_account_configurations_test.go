package fis_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

func TestFISHandler_GetExperimentTargetAccountConfiguration(t *testing.T) {
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

			// Create a target account config on the template.
			createPath := fmt.Sprintf("/experimentTemplates/%s/targetAccountConfigurations/111111111111", templateID)
			rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
				"roleArn":     "arn:aws:iam::111111111111:role/FISRole",
				"description": "multi-account target",
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			// Start an experiment from the template.
			startBody := map[string]any{"experimentTemplateId": templateID}
			expRec := doRequest(t, h, http.MethodPost, "/experiments", startBody)
			require.Equal(t, http.StatusCreated, expRec.Code)

			var expResp struct {
				Experiment struct {
					ID string `json:"id"`
				} `json:"experiment"`
			}

			mustJSON(t, expRec, &expResp)
			experimentID := expResp.Experiment.ID

			// Get experiment target account config.
			getPath := fmt.Sprintf("/experiments/%s/targetAccountConfigurations/%s", experimentID, tt.accountID)
			rec2 := doRequest(t, h, http.MethodGet, getPath, nil)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantStatus == http.StatusOK {
				var resp struct {
					TargetAccountConfiguration struct {
						AccountID   string `json:"accountId"`
						RoleArn     string `json:"roleArn"`
						Description string `json:"description"`
					} `json:"targetAccountConfiguration"`
				}

				mustJSON(t, rec2, &resp)
				assert.Equal(t, "111111111111", resp.TargetAccountConfiguration.AccountID)
				assert.Equal(t, "arn:aws:iam::111111111111:role/FISRole", resp.TargetAccountConfiguration.RoleArn)
				assert.Equal(t, "multi-account target", resp.TargetAccountConfiguration.Description)
			}
		})
	}
}

func TestFISHandler_ListExperimentTargetAccountConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accountIDs   []string
		wantCount    int
		wantStatus   int
		unknownExpID bool
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
			accountIDs: []string{"111111111111", "222222222222"},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
		{
			name:         "experiment_not_found",
			unknownExpID: true,
			wantStatus:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			templateID := createTestTemplate(t, h)

			for _, accountID := range tt.accountIDs {
				createPath := fmt.Sprintf(
					"/experimentTemplates/%s/targetAccountConfigurations/%s",
					templateID,
					accountID,
				)
				rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
					"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			// Start an experiment.
			startBody := map[string]any{"experimentTemplateId": templateID}
			expRec := doRequest(t, h, http.MethodPost, "/experiments", startBody)
			require.Equal(t, http.StatusCreated, expRec.Code)

			var expResp struct {
				Experiment struct {
					ID string `json:"id"`
				} `json:"experiment"`
			}

			mustJSON(t, expRec, &expResp)
			experimentID := expResp.Experiment.ID

			if tt.unknownExpID {
				experimentID = "EXPnonexistent0000000000"
			}

			listPath := fmt.Sprintf("/experiments/%s/targetAccountConfigurations", experimentID)
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

// TestListExperimentTargetAccountConfigurations_Pagination verifies
// ListExperimentTargetAccountConfigurations honors maxResults/nextToken like its sibling
// list operations, instead of always returning the full unpaginated list.
func TestListExperimentTargetAccountConfigurations_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateID := createTestTemplate(t, h)

	for _, accountID := range []string{"111111111111", "222222222222", "333333333333", "444444444444", "555555555555"} {
		createPath := fmt.Sprintf(
			"/experimentTemplates/%s/targetAccountConfigurations/%s",
			templateID,
			accountID,
		)
		rec := doRequest(t, h, http.MethodPost, createPath, map[string]any{
			"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	startBody := map[string]any{"experimentTemplateId": templateID}
	expRec := doRequest(t, h, http.MethodPost, "/experiments", startBody)
	require.Equal(t, http.StatusCreated, expRec.Code)

	var expResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, expRec, &expResp)

	listPath := fmt.Sprintf(
		"/experiments/%s/targetAccountConfigurations?maxResults=3",
		expResp.Experiment.ID,
	)
	rec := doRequest(t, h, http.MethodGet, listPath, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken                   string `json:"nextToken,omitempty"`
		TargetAccountConfigurations []struct {
			AccountID string `json:"accountId"`
		} `json:"targetAccountConfigurations"`
	}

	mustJSON(t, rec, &resp)
	assert.Len(t, resp.TargetAccountConfigurations, 3)
	assert.NotEmpty(t, resp.NextToken)
}

func TestFISHandler_GetSupportedOperations_TargetAccountConfigOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, expected := range []string{
		"CreateTargetAccountConfiguration",
		"DeleteTargetAccountConfiguration",
		"GetExperimentTargetAccountConfiguration",
		"GetTargetAccountConfiguration",
		"ListExperimentTargetAccountConfigurations",
		"ListTargetAccountConfigurations",
		"UpdateTargetAccountConfiguration",
	} {
		assert.Contains(t, ops, expected)
	}
}

func TestExperiment_TargetAccountConfigurationsCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Add two target account configurations.
	for _, accountID := range []string{"111111111111", "222222222222"} {
		path := "/experimentTemplates/" + tplID + "/targetAccountConfigurations/" + accountID
		rec := doRequest(t, h, http.MethodPost, path, map[string]any{
			"roleArn": "arn:aws:iam::" + accountID + ":role/FISRole",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		Experiment struct {
			TargetAccountConfigurationsCount int `json:"targetAccountConfigurationsCount"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &resp)
	assert.Equal(t, 2, resp.Experiment.TargetAccountConfigurationsCount)
}

// ----------------------------------------
// Issue #18 — tag quota enforcement
// ----------------------------------------

func TestDeleteTemplate_CascadesTargetAccountConfigs(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()
	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:  "EXT-cascade1",
		Arn: "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-cascade1",
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-cascade1",
		AccountID:            "333333333333",
		RoleArn:              "arn:aws:iam::333333333333:role/FISRole",
	})
	require.Equal(t, 1, b.TargetAccountConfigCount())

	err := b.DeleteExperimentTemplate("EXT-cascade1")
	require.NoError(t, err)

	assert.Equal(t, 0, b.TemplateCount())
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

// ----------------------------------------
// Sorted list outputs
// ----------------------------------------

func TestExperimentTargetAccountConfig_GetAndList(t *testing.T) {
	t.Parallel()

	b := fis.NewTestBackend()

	b.AddTemplateInternal(&fis.ExperimentTemplate{
		ID:             "EXT-expacct1",
		Arn:            "arn:aws:fis:us-east-1:000000000000:experiment-template/EXT-expacct1",
		RoleArn:        "arn:aws:iam::000000000000:role/FIS",
		Actions:        map[string]fis.ExperimentTemplateAction{},
		Targets:        map[string]fis.ExperimentTemplateTarget{},
		StopConditions: []fis.ExperimentTemplateStopCondition{{Source: "none"}},
	})
	b.AddTargetAccountConfigInternal(&fis.TargetAccountConfiguration{
		ExperimentTemplateID: "EXT-expacct1",
		AccountID:            "777777777777",
		RoleArn:              "arn:aws:iam::777777777777:role/FISRole",
		Description:          "exp-account-cfg",
	})
	b.InjectExperiment(&fis.Experiment{
		ID:                   "EXP-acct1",
		Arn:                  "arn:aws:fis:us-east-1:000000000000:experiment/EXP-acct1",
		ExperimentTemplateID: "EXT-expacct1",
		Status:               fis.ExperimentStatus{Status: "completed"},
		Tags:                 map[string]string{},
	})

	// Get
	cfg, err := b.GetExperimentTargetAccountConfiguration("EXP-acct1", "777777777777")
	require.NoError(t, err)
	assert.Equal(t, "777777777777", cfg.AccountID)
	assert.Equal(t, "exp-account-cfg", cfg.Description)
	assert.Equal(t, "EXP-acct1", cfg.ExperimentID)

	// List
	list, err := b.ListExperimentTargetAccountConfigurations("EXP-acct1")
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "777777777777", list[0].AccountID)
}

// ----------------------------------------
// ErrTooManyExperiments → 429
// ----------------------------------------

func TestDeleteTemplate_HTTP_CascadesConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	// Add a config.
	doRequest(
		t, h, http.MethodPost,
		"/experimentTemplates/"+tplID+"/targetAccountConfigurations/888888888888",
		map[string]any{"roleArn": "arn:aws:iam::888888888888:role/FISRole"},
	)

	b := h.Backend.(*fis.ExportedInMemoryBackend)
	require.Equal(t, 1, b.TargetAccountConfigCount())

	// Delete template.
	rec := doRequest(t, h, http.MethodDelete, "/experimentTemplates/"+tplID, nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Config must be gone.
	assert.Equal(t, 0, b.TargetAccountConfigCount())
}

// ----------------------------------------
// AddTargetAccountConfigInternal deep-copy
// ----------------------------------------
