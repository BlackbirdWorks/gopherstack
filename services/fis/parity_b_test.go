package fis_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// jsonUnmarshalFIS decodes JSON from a recorder into v.
func jsonUnmarshalFIS(t *testing.T, data []byte, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(data, v))
}

// minimalTemplate returns a request body that passes CreateExperimentTemplate validation.
func minimalTemplate(desc, roleArn string) map[string]any {
	return map[string]any{
		"description": desc,
		"roleArn":     roleArn,
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId":   "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT1S"},
			},
		},
	}
}

// TestFISPaginateOpaqueToken verifies that nextToken is a base64-encoded integer
// offset (not a raw item ID), so deleted items do not silently rewind the cursor.
func TestFISPaginateOpaqueToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		total      int
		pageSize   int
		wantPage1  int
		wantPage2  int
		wantOpaque bool
	}{
		{
			name:       "three items two per page",
			total:      3,
			pageSize:   2,
			wantPage1:  2,
			wantPage2:  1,
			wantOpaque: true,
		},
		{
			name:      "exact fit no token",
			total:     2,
			pageSize:  2,
			wantPage1: 2,
			wantPage2: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tc.total {
				rec := doRequest(
					t, h, http.MethodPost, "/experimentTemplates",
					minimalTemplate(fmt.Sprintf("tpl-%d", i), "arn:aws:iam::000000000000:role/R"),
				)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			// Page 1
			url1 := fmt.Sprintf("/experimentTemplates?maxResults=%d", tc.pageSize)
			rec1 := doRequest(t, h, http.MethodGet, url1, nil)
			require.Equal(t, http.StatusOK, rec1.Code)

			var resp1 struct { //nolint:govet // field order chosen for readability
				ExperimentTemplates []any  `json:"experimentTemplates"`
				NextToken           string `json:"nextToken"`
			}

			jsonUnmarshalFIS(t, rec1.Body.Bytes(), &resp1)
			assert.Len(t, resp1.ExperimentTemplates, tc.wantPage1)

			if !tc.wantOpaque {
				assert.Empty(t, resp1.NextToken)

				return
			}

			require.NotEmpty(t, resp1.NextToken)

			// Token must be base64-encoded integer, not a raw item ID.
			b, err := base64.StdEncoding.DecodeString(resp1.NextToken)
			require.NoError(t, err, "nextToken must be valid base64")
			offset, err := strconv.Atoi(string(b))
			require.NoError(t, err, "decoded token must be an integer offset")
			assert.Equal(t, tc.pageSize, offset)

			// Page 2
			url2 := fmt.Sprintf("/experimentTemplates?maxResults=%d&nextToken=%s", tc.pageSize, resp1.NextToken)
			rec2 := doRequest(t, h, http.MethodGet, url2, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp2 struct { //nolint:govet // field order chosen for readability
				ExperimentTemplates []any  `json:"experimentTemplates"`
				NextToken           string `json:"nextToken"`
			}

			jsonUnmarshalFIS(t, rec2.Body.Bytes(), &resp2)
			assert.Len(t, resp2.ExperimentTemplates, tc.wantPage2)
			assert.Empty(t, resp2.NextToken, "last page must have no nextToken")
		})
	}
}

// TestFISRoleArnValidation verifies that isValidRoleArn requires a 12-digit account ID.
func TestFISRoleArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		roleArn    string
		wantStatus int
	}{
		{
			name:       "valid 12-digit account",
			roleArn:    "arn:aws:iam::000000000000:role/FISRole",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "3-digit account rejects",
			roleArn:    "arn:aws:iam::000:role/FISRole",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing account rejects",
			roleArn:    "arn:aws:iam:::role/FISRole",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-digit account rejects",
			roleArn:    "arn:aws:iam::abc123456789:role/FISRole",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "13-digit account rejects",
			roleArn:    "arn:aws:iam::0000000000000:role/FISRole",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/experimentTemplates",
				minimalTemplate("rolearn-test", tc.roleArn),
			)
			assert.Equal(t, tc.wantStatus, rec.Code, "roleArn=%q", tc.roleArn)
		})
	}
}

// TestFISGetSafetyLeverAnyID verifies that GetSafetyLever returns the account's
// lever for any path segment, not just the exact stored account ID.
func TestFISGetSafetyLeverAnyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   string
	}{
		{name: "exact account ID", id: "000000000000"},
		{name: "default alias", id: "default"},
		{name: "different account ID", id: "999999999999"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/safetyLevers/"+tc.id, nil)
			assert.Equal(t, http.StatusOK, rec.Code, "id=%q", tc.id)

			var resp struct {
				SafetyLever struct {
					State struct{ Status string } `json:"state"`
				} `json:"safetyLever"`
			}

			mustJSON(t, rec, &resp)
			assert.Equal(t, "disengaged", resp.SafetyLever.State.Status)
		})
	}
}

// TestFISResolvedTargetsARNs verifies that ListExperimentResolvedTargets includes
// the actual resolvedArns in the response (not just the count).
func TestFISResolvedTargetsARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceArns []string
		wantCount    int
	}{
		{
			name:         "single ARN",
			resourceArns: []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa"},
			wantCount:    1,
		},
		{
			name: "multiple ARNs",
			resourceArns: []string{
				"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
				"arn:aws:ec2:us-east-1:000000000000:instance/i-bbb",
			},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := fis.NewTestBackend()
			h := fis.NewHandler(b)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "000000000000"

			// Inject experiment with known ResourceArns.
			injected := &fis.Experiment{
				ID:     "EXPtest000000000000000000001",
				Status: fis.ExperimentStatus{Status: "running"},
				Targets: map[string]fis.ExperimentTarget{
					"Instances": {
						ResourceType: "aws:ec2:instance",
						ResourceArns: tc.resourceArns,
					},
				},
				CreationTime: time.Now(),
			}
			b.InjectExperiment(injected)

			rec := doRequest(t, h, http.MethodGet, "/experiments/"+injected.ID+"/resolvedTargets", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResolvedTargets []struct { //nolint:govet // field order matches JSON for readability
					TargetName           string   `json:"targetName"`
					TargetResourcesCount int      `json:"targetResourcesCount"`
					ResolvedArns         []string `json:"resolvedArns"`
				} `json:"resolvedTargets"`
			}

			jsonUnmarshalFIS(t, rec.Body.Bytes(), &resp)
			require.Len(t, resp.ResolvedTargets, 1)

			got := resp.ResolvedTargets[0]
			assert.Equal(t, "Instances", got.TargetName)
			assert.Equal(t, tc.wantCount, got.TargetResourcesCount)
			assert.Equal(t, tc.resourceArns, got.ResolvedArns)
		})
	}
}
