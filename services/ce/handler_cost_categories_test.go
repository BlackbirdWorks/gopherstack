package ce_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetCostCategories_MultiRuleCategory verifies cost category value lookup.
func TestGetCostCategories_MultiRuleCategory(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "DeptCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules": []map[string]any{
			{"Value": "Engineering"},
			{"Value": "Marketing"},
			{"Value": "Finance"},
		},
	})

	rec := doRequest(t, h, "GetCostCategories", map[string]any{
		"CostCategoryName": "DeptCat",
		"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostCategoryValues []string `json:"CostCategoryValues"`
		ReturnSize         int      `json:"ReturnSize"`
		TotalSize          int      `json:"TotalSize"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.CostCategoryValues, 3)
	assert.Equal(t, 3, out.ReturnSize)
	assert.Equal(t, 3, out.TotalSize)
}

// TestListCostCategoryResourceAssociations verifies the real CE wire shape
// (CostCategoryResourceAssociations, not the previously-invented
// CostCategoryReference/ResourceTagsCount fields).
func TestListCostCategoryResourceAssociations(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "ListCostCategoryResourceAssociations", map[string]any{
		"CostCategoryArn": "arn:aws:ce::000:costcategory/test",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostCategoryResourceAssociations []map[string]any `json:"CostCategoryResourceAssociations"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Empty(t, out.CostCategoryResourceAssociations)
}

// TestDescribeCostCategory_HasProcessingStatus verifies real AWS returns
// ProcessingStatus in the describe response.
func TestDescribeCostCategory_HasProcessingStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "MyCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Engineering"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	catARN := createOut["CostCategoryArn"].(string)

	descRec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
		"CostCategoryArn": catARN,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		CostCategory struct {
			ProcessingStatus []struct {
				Component string `json:"Component"`
				Status    string `json:"Status"`
			} `json:"ProcessingStatus"`
		} `json:"CostCategory"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.NotEmpty(t, descOut.CostCategory.ProcessingStatus, "ProcessingStatus must be present")
	ps := descOut.CostCategory.ProcessingStatus[0]
	assert.Equal(t, "COST_EXPLORER", ps.Component)
	assert.Equal(t, "APPLIED", ps.Status)
}

// TestListCostCategoryDefinitions_Pagination verifies MaxResults/NextToken pagination.
func TestListCostCategoryDefinitions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
			"Name":        "Cat-" + string(rune('A'+i)),
			"RuleVersion": "CostCategoryExpression.v1",
			"Rules":       []map[string]any{{"Value": "val"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["CostCategoryReferences"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")

	rec2 := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2 := out2["CostCategoryReferences"].([]any)
	assert.Len(t, page2, 2)
}

func TestHandler_CostCategoryCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *ce.Handler)
		name  string
	}{
		{
			name: "create_and_list",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "MyCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Engineering"}},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out["CostCategoryArn"])
				assert.NotEmpty(t, out["EffectiveStart"])
			},
		},
		{
			name: "list_cost_categories",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create first
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "ListCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Marketing"}},
				})
				// Then list
				rec := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				refs, ok := out["CostCategoryReferences"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, refs)
			},
		},
		{
			name: "describe_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "DescribeCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Finance"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Describe
				rec2 := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var describeOut map[string]any
				require.NoError(t, json.NewDecoder(rec2.Body).Decode(&describeOut))
				cat := describeOut["CostCategory"].(map[string]any)
				assert.Equal(t, "DescribeCategory", cat["Name"])
			},
		},
		{
			name: "update_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "UpdateCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Ops"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Update
				rec2 := doRequest(t, h, "UpdateCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
					"RuleVersion":     "CostCategoryExpression.v1",
					"Rules":           []map[string]any{{"Value": "Ops-Updated"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var updateOut map[string]any
				require.NoError(t, json.NewDecoder(rec2.Body).Decode(&updateOut))
				assert.Equal(t, arn, updateOut["CostCategoryArn"])
			},
		},
		{
			name: "delete_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "DeleteCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Temp"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Delete
				rec2 := doRequest(t, h, "DeleteCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// Describe should return ResourceNotFoundException (real AWS CE: HTTP 400).
				rec3 := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusBadRequest, rec3.Code)
			},
		},
		{
			name: "create_missing_name_returns_400",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"RuleVersion": "CostCategoryExpression.v1",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_not_found_returns_400",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": "arn:aws:ce::000000000000:costcategory/nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
		})
	}
}

func TestCEHandler_UpdateCostCategoryDefinition_DeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		initialRules      []map[string]any
		updatedRules      []map[string]any
		wantRulesAfterGet int
	}{
		{
			name:              "rules_are_deep_copied_on_update",
			initialRules:      []map[string]any{{"value": "old"}},
			updatedRules:      []map[string]any{{"value": "new"}, {"value": "extra"}},
			wantRulesAfterGet: 2,
		},
		{
			name:              "empty_rules_on_update",
			initialRules:      []map[string]any{{"value": "initial"}},
			updatedRules:      []map[string]any{},
			wantRulesAfterGet: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
				"Name":        "test-cat",
				"RuleVersion": "CostCategoryExpression.v1",
				"Rules":       tt.initialRules,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				CostCategoryArn string `json:"CostCategoryArn"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			updateRec := doRequest(t, h, "UpdateCostCategoryDefinition", map[string]any{
				"CostCategoryArn": createOut.CostCategoryArn,
				"RuleVersion":     "CostCategoryExpression.v1",
				"Rules":           tt.updatedRules,
			})
			require.Equal(t, http.StatusOK, updateRec.Code)

			describeRec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
				"CostCategoryArn": createOut.CostCategoryArn,
			})
			require.Equal(t, http.StatusOK, describeRec.Code)

			var describeOut struct {
				CostCategory struct {
					Rules []map[string]any `json:"rules"`
				} `json:"CostCategory"`
			}

			require.NoError(t, json.NewDecoder(describeRec.Body).Decode(&describeOut))
			assert.Len(t, describeOut.CostCategory.Rules, tt.wantRulesAfterGet)
		})
	}
}

func TestHandler_GetCostCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(*testing.T, *ce.Handler)
		name               string
		costCategoryName   string
		wantValuesContain  string
		wantLen            int
		wantNamesNotValues bool
	}{
		{
			name:    "returns_empty_when_no_categories",
			wantLen: 0,
		},
		{
			name:             "returns_values_for_existing_category",
			costCategoryName: "Env",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Env",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Production"}},
				})
			},
			wantValuesContain: "Production",
			wantLen:           1,
		},
		{
			// Real GetCostCategories returns CostCategoryNames (not
			// CostCategoryValues) when the request omits CostCategoryName --
			// see api_op_GetCostCategories.go: "If the CostCategoryName key
			// isn't specified in the request, the CostCategoryValues fields
			// aren't returned." A prior revision always returned
			// CostCategoryValues regardless, so a real client's typed
			// .CostCategoryNames was always empty.
			name: "returns_all_names_when_no_filter",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Env",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Production"}},
				})
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Team",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Platform"}},
				})
			},
			wantLen:            2,
			wantNamesNotValues: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			}
			if tt.costCategoryName != "" {
				body["CostCategoryName"] = tt.costCategoryName
			}

			rec := doRequest(t, h, "GetCostCategories", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CostCategoryNames  []string `json:"CostCategoryNames"`
				CostCategoryValues []string `json:"CostCategoryValues"`
				ReturnSize         int      `json:"ReturnSize"`
				TotalSize          int      `json:"TotalSize"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantLen, out.ReturnSize)
			assert.Equal(t, tt.wantLen, out.TotalSize)

			if tt.wantNamesNotValues {
				assert.Len(t, out.CostCategoryNames, tt.wantLen)
				assert.Empty(t, out.CostCategoryValues)
				assert.Contains(t, out.CostCategoryNames, "Env")
				assert.Contains(t, out.CostCategoryNames, "Team")

				return
			}

			assert.Len(t, out.CostCategoryValues, tt.wantLen)
			assert.Empty(t, out.CostCategoryNames)

			if tt.wantValuesContain != "" {
				assert.Contains(t, out.CostCategoryValues, tt.wantValuesContain)
			}
		})
	}
}

func TestHandler_DuplicateCostCategory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"Name":        "DupCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Prod"}},
	}

	rec1 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_SortedOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *ce.Handler)
		verify func(*testing.T, *ce.Handler)
		name   string
	}{
		{
			name: "list_cost_categories_sorted_by_name",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()

				for _, name := range []string{"Zebra", "Alpha", "Mango"} {
					doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
						"Name":        name,
						"RuleVersion": "CostCategoryExpression.v1",
						"Rules":       []map[string]any{{"Value": name}},
					})
				}
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					CostCategoryReferences []struct {
						Name string `json:"Name"`
					} `json:"CostCategoryReferences"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.CostCategoryReferences, 3)
				assert.Equal(t, "Alpha", out.CostCategoryReferences[0].Name)
				assert.Equal(t, "Mango", out.CostCategoryReferences[1].Name)
				assert.Equal(t, "Zebra", out.CostCategoryReferences[2].Name)
			},
		},
		{
			name: "get_cost_categories_values_sorted",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "SortCat",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules": []map[string]any{
						{"Value": "Zulu"},
						{"Value": "Alpha"},
						{"Value": "Mike"},
					},
				})
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "GetCostCategories", map[string]any{
					"CostCategoryName": "SortCat",
					"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					CostCategoryValues []string `json:"CostCategoryValues"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.CostCategoryValues, 3)
				assert.Equal(t, []string{"Alpha", "Mike", "Zulu"}, out.CostCategoryValues)
			},
		},
		{
			name: "list_tags_for_resource_sorted_by_key",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "TagSortCat",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "x"}},
					"ResourceTags": []map[string]string{
						{"Key": "Zebra", "Value": "z"},
						{"Key": "Alpha", "Value": "a"},
						{"Key": "Mike", "Value": "m"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()

				cats, _ := h.Backend.ListCostCategoryDefinitions(0, "")
				require.Len(t, cats, 1)

				rec := doRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": cats[0].ARN,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					ResourceTags []struct {
						Key string `json:"Key"`
					} `json:"ResourceTags"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.ResourceTags, 3)
				assert.Equal(t, "Alpha", out.ResourceTags[0].Key)
				assert.Equal(t, "Mike", out.ResourceTags[1].Key)
				assert.Equal(t, "Zebra", out.ResourceTags[2].Key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
			tt.verify(t, h)
		})
	}
}

// TestHandler_DuplicateCostCategory_WireStatusIs400 verifies ServiceQuotaExceededException
// is returned as HTTP 400 (real AWS CE), not 409.
func TestHandler_DuplicateCostCategory_WireStatusIs400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"Name":        "WireDupCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Prod"}},
	}

	rec1 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var out struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	assert.Equal(t, "ServiceQuotaExceededException", out.Type)
}

// TestHandler_CreateCostCategoryDefinition_RequiredFields verifies Name, RuleVersion,
// and Rules are enforced as required, matching real AWS CE's
// validateOpCreateCostCategoryDefinitionInput.
func TestHandler_CreateCostCategoryDefinition_RequiredFields(t *testing.T) {
	t.Parallel()

	full := map[string]any{
		"Name":        "ReqFieldsCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Prod"}},
	}

	tests := []struct {
		mutate         func(body map[string]any)
		name           string
		wantStatusCode int
	}{
		{
			name:           "missing_name",
			mutate:         func(b map[string]any) { delete(b, "Name") },
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_rule_version",
			mutate:         func(b map[string]any) { delete(b, "RuleVersion") },
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "missing_rules",
			mutate:         func(b map[string]any) { delete(b, "Rules") },
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "all_present_succeeds",
			mutate:         func(map[string]any) {},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := make(map[string]any, len(full))
			maps.Copy(body, full)

			tt.mutate(body)

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCostCategoryDefinition", body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

// TestHandler_UpdateCostCategoryDefinition_RequiredFields verifies RuleVersion and
// Rules are enforced as required, matching real AWS CE's
// validateOpUpdateCostCategoryDefinitionInput.
func TestHandler_UpdateCostCategoryDefinition_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update         map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "missing_rule_version",
			update: map[string]any{
				"Rules": []map[string]any{{"Value": "Prod"}},
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "missing_rules",
			update: map[string]any{
				"RuleVersion": "CostCategoryExpression.v1",
			},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
				"Name":        "UpdateReqFieldsCat",
				"RuleVersion": "CostCategoryExpression.v1",
				"Rules":       []map[string]any{{"Value": "Prod"}},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
			tt.update["CostCategoryArn"] = createOut["CostCategoryArn"]

			rec := doRequest(t, h, "UpdateCostCategoryDefinition", tt.update)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}
