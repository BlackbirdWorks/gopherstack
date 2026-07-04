package resourcegroups_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_GroupResources_InvalidARNReturnsFailed verifies that malformed ARNs are
// returned in the Failed list with INVALID_ARN error code, not added to the group.
// Real AWS Resource Groups rejects non-ARN strings with INVALID_ARN.
func TestParity_GroupResources_InvalidARNReturnsFailed(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name          string
		arns          []string
		wantSucceeded int
		wantFailed    int
		wantErrCode   string
	}{
		{
			name:          "valid_arns_only",
			arns:          []string{"arn:aws:s3:::my-bucket", "arn:aws:ec2:us-east-1:123456789012:instance/i-1"},
			wantSucceeded: 2,
			wantFailed:    0,
		},
		{
			name:          "not_an_arn",
			arns:          []string{"just-a-string"},
			wantSucceeded: 0,
			wantFailed:    1,
			wantErrCode:   "INVALID_ARN",
		},
		{
			name:          "arn_too_few_segments",
			arns:          []string{"arn:aws:s3"},
			wantSucceeded: 0,
			wantFailed:    1,
			wantErrCode:   "INVALID_ARN",
		},
		{
			name: "mixed_valid_and_invalid",
			arns: []string{
				"arn:aws:s3:::bucket1",
				"not-an-arn",
				"arn:aws:lambda:us-east-1:123456789012:function/fn",
				"",
			},
			wantSucceeded: 2,
			wantFailed:    2,
			wantErrCode:   "INVALID_ARN",
		},
		{
			name:          "empty_string_arn",
			arns:          []string{""},
			wantSucceeded: 0,
			wantFailed:    1,
			wantErrCode:   "INVALID_ARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "parity-grp"})

			rec := doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
				"Group":        "parity-grp",
				"ResourceArns": tt.arns,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Succeeded []string `json:"Succeeded"`
				Failed    []struct {
					ResourceArn  string `json:"ResourceArn"`
					ErrorCode    string `json:"ErrorCode"`
					ErrorMessage string `json:"ErrorMessage"`
				} `json:"Failed"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Len(t, out.Succeeded, tt.wantSucceeded, "succeeded count")
			assert.Len(t, out.Failed, tt.wantFailed, "failed count: %s", rec.Body.String())

			if tt.wantErrCode != "" && len(out.Failed) > 0 {
				assert.Equal(t, tt.wantErrCode, out.Failed[0].ErrorCode)
				assert.NotEmpty(t, out.Failed[0].ErrorMessage)
			}
		})
	}
}

// TestParity_GroupResources_InvalidARNNotAddedToGroup verifies that invalid ARNs
// rejected with INVALID_ARN are not persisted in the group resource list.
func TestParity_GroupResources_InvalidARNNotAddedToGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "isolation-grp"})

	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "isolation-grp",
		"ResourceArns": []string{"not-an-arn", "arn:aws:s3:::real-bucket"},
	})

	rec := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{"Group": "isolation-grp"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Resources []struct {
			Identifier struct {
				ResourceArn string `json:"ResourceArn"`
			} `json:"Identifier"`
		} `json:"Resources"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	require.Len(t, out.Resources, 1, "only the valid ARN should be in group")
	assert.Equal(t, "arn:aws:s3:::real-bucket", out.Resources[0].Identifier.ResourceArn)
}

// TestParity_GroupResources_OutputShape verifies the GroupResources response structure
// matches AWS: Failed entries have ResourceArn, ErrorCode, ErrorMessage fields.
func TestParity_GroupResources_OutputShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantInBody  string
		wantOutBody string
	}{
		{
			name:       "failed_has_error_code_field",
			wantInBody: "ErrorCode",
		},
		{
			name:       "failed_has_resource_arn_field",
			wantInBody: "ResourceArn",
		},
		{
			name:       "failed_has_error_message_field",
			wantInBody: "ErrorMessage",
		},
		{
			name:        "succeeded_is_list",
			wantOutBody: "Pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "shape-grp"})

			rec := doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
				"Group":        "shape-grp",
				"ResourceArns": []string{"bad-arn"},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()

			if tt.wantInBody != "" {
				assert.Contains(t, body, tt.wantInBody)
			}

			if tt.wantOutBody != "" {
				assert.NotContains(t, body, tt.wantOutBody+"\":null")
			}
		})
	}
}

// TestParity_ListGroups_NamePrefixFilter verifies the name-prefix filter on ListGroups.
func TestParity_ListGroups_NamePrefixFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prefix       string
		wantContains []string
		wantExcludes []string
	}{
		{
			name:         "prefix_alpha",
			prefix:       "alpha",
			wantContains: []string{"alpha-prod", "alpha-dev"},
			wantExcludes: []string{"beta-prod"},
		},
		{
			name:         "prefix_beta",
			prefix:       "beta",
			wantContains: []string{"beta-prod"},
			wantExcludes: []string{"alpha-prod", "alpha-dev"},
		},
		{
			name:         "no_match_prefix",
			prefix:       "gamma",
			wantExcludes: []string{"alpha-prod", "beta-prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			for _, n := range []string{"alpha-prod", "alpha-dev", "beta-prod"} {
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": n})
			}

			rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
				"Filters": []map[string]any{
					{"Name": "name-prefix", "Values": []string{tt.prefix}},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()

			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}

			for _, notWant := range tt.wantExcludes {
				assert.NotContains(t, body, notWant)
			}
		})
	}
}

// TestParity_GetGroupQuery_NilForConfigGroup verifies that GetGroupQuery returns
// a nil ResourceQuery for configuration-based groups (no query set).
func TestParity_GetGroupQuery_NilForConfigGroup(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name    string
		wantNil bool
		body    map[string]any
	}{
		{
			name: "query_group_has_query",
			body: map[string]any{
				"Name": "query-grp",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
			},
			wantNil: false,
		},
		{
			name: "config_group_no_query",
			body: map[string]any{
				"Name":          "cfg-grp",
				"Configuration": []map[string]any{{"Type": "AWS::ResourceGroups::Generic"}},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", tt.body)

			groupName, _ := tt.body["Name"].(string)
			rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"Group": groupName})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				GroupQuery struct {
					ResourceQuery *struct {
						Type  string `json:"Type"`
						Query string `json:"Query"`
					} `json:"ResourceQuery"`
				} `json:"GroupQuery"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tt.wantNil {
				assert.Nil(t, out.GroupQuery.ResourceQuery)
			} else {
				assert.NotNil(t, out.GroupQuery.ResourceQuery)
			}
		})
	}
}
