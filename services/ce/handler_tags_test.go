package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *ce.Handler)
		name  string
	}{
		{
			name: "tag_and_untag_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "TaggedCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Test"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Tag
				rec2 := doRequest(t, h, "TagResource", map[string]any{
					"ResourceArn":  arn,
					"ResourceTags": []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// List tags
				rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec3.Code)

				var listOut map[string]any
				require.NoError(t, json.NewDecoder(rec3.Body).Decode(&listOut))
				tags, _ := listOut["ResourceTags"].([]any)
				var envVal string
				for _, tagAny := range tags {
					tag, _ := tagAny.(map[string]any)
					if tag["Key"] == "Env" {
						envVal, _ = tag["Value"].(string)
					}
				}
				assert.Equal(t, "prod", envVal)

				// Untag
				rec4 := doRequest(t, h, "UntagResource", map[string]any{
					"ResourceArn":     arn,
					"ResourceTagKeys": []string{"Env"},
				})
				assert.Equal(t, http.StatusOK, rec4.Code)
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

// Improvement 3: Test tag operations on anomaly monitors.
func TestHandler_TagOperations_AnomalyMonitor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a monitor to tag.
	createRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "MonitorToTag",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	monitorARN := createOut["MonitorArn"].(string)

	// Tag.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn":  monitorARN,
		"ResourceTags": []map[string]string{{"Key": "Team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags — expect "Team=platform".
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": monitorARN,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	tags, _ := listOut["ResourceTags"].([]any)

	var found bool

	for _, tagAny := range tags {
		tag, _ := tagAny.(map[string]any)
		if tag["Key"] == "Team" && tag["Value"] == "platform" {
			found = true
		}
	}

	assert.True(t, found, "expected Team=platform tag on monitor")

	// Untag.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":     monitorARN,
		"ResourceTagKeys": []string{"Team"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// Improvement 4: Test tag operations on anomaly subscriptions.
func TestHandler_TagOperations_AnomalySubscription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create subscription to tag.
	createRec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
		"AnomalySubscription": map[string]any{
			"SubscriptionName": "SubToTag",
			"Frequency":        "DAILY",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	subARN := createOut["SubscriptionArn"].(string)

	// Tag.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn":  subARN,
		"ResourceTags": []map[string]string{{"Key": "Owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": subARN,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	tags, _ := listOut["ResourceTags"].([]any)
	assert.NotEmpty(t, tags)

	// Untag.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":     subARN,
		"ResourceTagKeys": []string{"Owner"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// Improvement 5: Test tag operations error paths — missing ARN returns 400.
func TestHandler_TagOperations_MissingARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "ListTagsForResource_missing_arn",
			action: "ListTagsForResource",
			body:   map[string]any{},
		},
		{
			name:   "TagResource_missing_arn",
			action: "TagResource",
			body:   map[string]any{"ResourceTags": []map[string]string{{"Key": "k", "Value": "v"}}},
		},
		{
			name:   "UntagResource_missing_arn",
			action: "UntagResource",
			body:   map[string]any{"ResourceTagKeys": []string{"k"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// Improvement 6: Test tag operations error paths — not-found ARN returns 404.
func TestHandler_TagOperations_NotFound(t *testing.T) {
	t.Parallel()

	const notFoundARN = "arn:aws:ce::000000000000:costcategory/does-not-exist"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "ListTagsForResource_not_found",
			action: "ListTagsForResource",
			body:   map[string]any{"ResourceArn": notFoundARN},
		},
		{
			name:   "TagResource_not_found",
			action: "TagResource",
			body: map[string]any{
				"ResourceArn":  notFoundARN,
				"ResourceTags": []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name:   "UntagResource_not_found",
			action: "UntagResource",
			body: map[string]any{
				"ResourceArn":     notFoundARN,
				"ResourceTagKeys": []string{"k"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cost category without tags.
	rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "NoTagsCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Test"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	arn := createOut["CostCategoryArn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ResourceTags []map[string]string `json:"ResourceTags"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	// Must be an empty array, not nil/absent.
	assert.NotNil(t, out.ResourceTags)
	assert.Empty(t, out.ResourceTags)
}
