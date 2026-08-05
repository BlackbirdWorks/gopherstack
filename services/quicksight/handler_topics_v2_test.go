package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuickSight_TopicV2CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "tv1",
		"Topic": map[string]any{
			"Name":        "Topic1",
			"Description": "d1",
			"DataSets": []any{
				map[string]any{"DataSetArn": "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "tv1", createBody["TopicId"])
	assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:topic/tv1")

	dupRec := doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "tv1",
		"Topic":   map[string]any{"Name": "x"},
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// TopicV2Details has no UserExperienceVersion/ConfigOptions (V1-only fields).
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/tv1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	assert.Equal(t, "tv1", describeBody["TopicId"])
	topic, ok := describeBody["Topic"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Topic1", topic["Name"])
	assert.Equal(t, "d1", topic["Description"])
	dataSets, ok := topic["DataSets"].([]any)
	require.True(t, ok)
	assert.Len(t, dataSets, 1)
	assert.NotContains(t, topic, "UserExperienceVersion")
	assert.NotContains(t, describeBody, "CustomInstructions")

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// UpdateTopicV2's Topic document is a full replace: omitted fields clear.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/topicsV2/tv1"), map[string]any{
		"Topic": map[string]any{"Name": "Renamed"},
		"CustomInstructions": map[string]any{
			"CustomInstructionsString": "be concise",
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	assert.Equal(t, "tv1", parseBody(t, updateRec)["TopicId"])

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/tv1"), nil)
	afterBody := parseBody(t, describeAfterUpdate)
	afterTopic := afterBody["Topic"].(map[string]any)
	assert.Equal(t, "Renamed", afterTopic["Name"])
	assert.Empty(t, afterTopic["Description"], "full-replace: omitted Description clears the old value")
	assert.Empty(t, afterTopic["DataSets"], "full-replace: omitted DataSets clears the old value")
	ci, ok := afterBody["CustomInstructions"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "be concise", ci["CustomInstructionsString"])

	updateMissingRec := doRequest(
		t, h, http.MethodPut, accountPath("/topicsV2/notexist"),
		map[string]any{"Topic": map[string]any{"Name": "x"}},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// DeleteTopicV2Output carries Arn, unlike V1's DeleteTopic response.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/topicsV2/tv1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteBody := parseBody(t, deleteRec)
	assert.Equal(t, "tv1", deleteBody["TopicId"])
	assert.Contains(t, deleteBody["Arn"], "topic/tv1")

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/topicsV2/tv1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

func TestQuickSight_TopicV2_SharesResourceWithV1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// V1-created topic must be visible via DescribeTopicV2, V2-only fields empty.
	doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "shared1",
		"Name":    "FromV1",
	})

	describeV2 := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/shared1"), nil)
	require.Equal(t, http.StatusOK, describeV2.Code)
	v2Topic := parseBody(t, describeV2)["Topic"].(map[string]any)
	assert.Equal(t, "FromV1", v2Topic["Name"])
	assert.Empty(t, v2Topic["DataSets"])

	// V1 and V2 share one TopicId namespace, so a duplicate ID conflicts.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "shared1",
		"Topic":   map[string]any{"Name": "x"},
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// V2-created topic visible via V1 DescribeTopic, defaulted to NEW_READER_EXPERIENCE.
	doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "shared2",
		"Topic":   map[string]any{"Name": "FromV2"},
	})

	describeV1 := doRequest(t, h, http.MethodGet, accountPath("/topics/shared2"), nil)
	require.Equal(t, http.StatusOK, describeV1.Code)
	v1Topic := parseBody(t, describeV1)["Topic"].(map[string]any)
	assert.Equal(t, "FromV2", v1Topic["Name"])
	assert.Equal(t, "NEW_READER_EXPERIENCE", v1Topic["UserExperienceVersion"])

	// DeleteTopicV2 removes the V1-created shared1 topic (same store).
	delRec := doRequest(t, h, http.MethodDelete, accountPath("/topicsV2/shared1"), nil)
	require.Equal(t, http.StatusOK, delRec.Code)
	goneV1 := doRequest(t, h, http.MethodGet, accountPath("/topics/shared1"), nil)
	assert.Equal(t, http.StatusNotFound, goneV1.Code)
}

func TestQuickSight_TopicV2Permissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "ptv2",
		"Topic":   map[string]any{"Name": "P1"},
	})

	describeEmpty := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/ptv2/permissions"), nil)
	require.Equal(t, http.StatusOK, describeEmpty.Code)
	emptyPerms, ok := parseBody(t, describeEmpty)["Permissions"].([]any)
	require.True(t, ok)
	assert.Empty(t, emptyPerms)

	grantRec := doRequest(
		t, h, http.MethodPut, accountPath("/topicsV2/ptv2/permissions"),
		map[string]any{
			"GrantPermissions": []any{
				map[string]any{
					"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
					"Actions":   []any{"quicksight:DescribeTopic"},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, grantRec.Code)
	perms, ok := parseBody(t, grantRec)["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 1)

	// Visible through the V1 endpoint too -- same storedTopic.Permissions.
	describeV1Perms := doRequest(t, h, http.MethodGet, accountPath("/topics/ptv2/permissions"), nil)
	require.Equal(t, http.StatusOK, describeV1Perms.Code)
	v1Perms, ok := parseBody(t, describeV1Perms)["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, v1Perms, 1)

	permsMissing := doRequest(t, h, http.MethodGet, accountPath("/topicsV2/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, permsMissing.Code)
}

func TestQuickSight_ListTopicsV2_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(
			t, h, http.MethodPost, accountPath("/topicsV2"),
			map[string]any{"TopicId": id, "Topic": map[string]any{"Name": id}},
		)
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/topicsV2?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["TopicSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	summary, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summary, "Arn")
	assert.Contains(t, summary, "TopicId")
	assert.Contains(t, summary, "Name")
	assert.NotContains(t, summary, "UserExperienceVersion", "TopicV2Summary has no UserExperienceVersion field")

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["TopicId"].(string)] = true
	}

	page2 := doRequest(
		t, h, http.MethodGet,
		accountPath(fmt.Sprintf("/topicsV2?max-results=2&next-token=%s", next)),
		nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["TopicSummaryList"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["TopicId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// SearchTopicsV2's Filters/MaxResults/NextToken travel in the JSON body, not
// query params (unlike ListTopicsV2).
func TestQuickSight_SearchTopicsV2(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "t1",
		"Topic":   map[string]any{"Name": "Sales"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, accountPath("/topicsV2"), map[string]any{
		"TopicId": "t2",
		"Topic":   map[string]any{"Name": "Marketing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topicsV2"), map[string]any{"Filters": []any{}})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	list, ok := body["TopicSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topicsV2"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "TOPIC_NAME", "Operator": "StringEquals", "Value": "Sales"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	summary, ok := list[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Sales", summary["Name"])

	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topicsV2"), map[string]any{
		"Filters":    []any{},
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topicsV2"), map[string]any{
		"Filters":    []any{},
		"MaxResults": 1,
		"NextToken":  next,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	assert.NotEqual(t, summary["TopicId"], list[0].(map[string]any)["TopicId"])
}
