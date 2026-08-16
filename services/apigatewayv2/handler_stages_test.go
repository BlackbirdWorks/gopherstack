package apigatewayv2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestHandler_CreateStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			wantStatus: http.StatusCreated,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			stageName:  "prod",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")
			}

			rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
				"stageName": tt.stageName,
			})

			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusCreated {
				var stage apigatewayv2.Stage
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &stage))
				assert.Equal(t, tt.stageName, stage.StageName)
			}
		})
	}
}

func TestHandler_GetStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stages     []string
		wantStatus int
		apiExists  bool
	}{
		{
			name:       "empty",
			stages:     nil,
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "multiple_stages",
			stages:     []string{"dev", "prod"},
			wantStatus: http.StatusOK,
			apiExists:  true,
		},
		{
			name:       "api_not_found",
			wantStatus: http.StatusNotFound,
			apiExists:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			apiID := "nonexistent"
			if tt.apiExists {
				apiID = createAPI(t, h, "test-api")

				for _, sn := range tt.stages {
					rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
						"stageName": sn,
					})
					require.Equal(t, http.StatusCreated, rr.Code)
				}
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/stages", apiID), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				type listResp struct {
					Items []apigatewayv2.Stage `json:"items"`
				}

				var resp listResp
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				assert.Len(t, resp.Items, len(tt.stages))
			}
		})
	}
}

func TestHandler_GetStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "existing",
			stageName:  "prod",
			wantStatus: http.StatusOK,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_DeleteStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			wantStatus: http.StatusNoContent,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(t, h, http.MethodDelete, fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName), nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_UpdateStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update     map[string]any
		name       string
		stageName  string
		wantStatus int
		setupStage bool
	}{
		{
			name:       "success",
			stageName:  "prod",
			update:     map[string]any{"description": "updated"},
			wantStatus: http.StatusOK,
			setupStage: true,
		},
		{
			name:       "not_found",
			stageName:  "nonexistent",
			update:     map[string]any{"description": "x"},
			wantStatus: http.StatusNotFound,
			setupStage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "test-api")

			if tt.setupStage {
				rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
					"stageName": tt.stageName,
				})
				require.Equal(t, http.StatusCreated, rr.Code)
			}

			rr := doRequest(
				t,
				h,
				http.MethodPatch,
				fmt.Sprintf("/v2/apis/%s/stages/%s", apiID, tt.stageName),
				tt.update,
			)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestGetStages_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "stage-api")

	for i := range 5 {
		rr := doRequest(t, h, http.MethodPost,
			fmt.Sprintf("/v2/apis/%s/stages", apiID),
			map[string]any{"stageName": fmt.Sprintf("stage-%02d", i)},
		)
		require.Equal(t, http.StatusCreated, rr.Code)
	}

	tests := []struct {
		name       string
		maxResults int
		wantPages  int
	}{
		{"two_per_page", 2, 3},
		{"three_per_page", 3, 2},
		{"all", 10, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			seen := map[string]int{}
			nextToken := ""
			pages := 0

			for {
				path := fmt.Sprintf("/v2/apis/%s/stages?maxResults=%d", apiID, tc.maxResults)
				if nextToken != "" {
					path += "&nextToken=" + nextToken
				}

				rr := doRequest(t, h, http.MethodGet, path, nil)
				require.Equal(t, http.StatusOK, rr.Code)

				var resp struct {
					NextToken string           `json:"nextToken"`
					Items     []map[string]any `json:"items"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
				require.LessOrEqual(t, len(resp.Items), tc.maxResults)

				for _, s := range resp.Items {
					name, _ := s["stageName"].(string)
					seen[name]++
				}

				pages++
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}

				require.Less(t, pages, 20)
			}

			assert.Equal(t, tc.wantPages, pages)
			assert.Len(t, seen, 5)
		})
	}
}
