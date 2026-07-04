package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Topic CRUD round-trip, not-found, and duplicate errors ----

func TestQuickSight_TopicCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "tp1",
		"Name":    "Topic1",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "tp1", createBody["TopicId"])
	assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:topic/tp1")

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "tp1",
		"Name":    "x",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Missing TopicId/Name -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// Describe.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/topics/tp1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	topic, ok := describeBody["Topic"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Topic1", topic["Name"])

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/topics/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/topics/tp1"), map[string]any{
		"Name":        "Renamed",
		"Description": "desc",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	assert.Equal(t, "tp1", parseBody(t, updateRec)["TopicId"])

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/topics/tp1"), nil)
	afterTopic := parseBody(t, describeAfterUpdate)["Topic"].(map[string]any)
	assert.Equal(t, "Renamed", afterTopic["Name"])
	assert.Equal(t, "desc", afterTopic["Description"])

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/topics/notexist"),
		map[string]any{"Name": "x"},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/topics/tp1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "tp1", parseBody(t, deleteRec)["TopicId"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/topics/tp1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- ListTopics pagination ----

func TestQuickSight_ListTopics_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(
			t,
			h,
			http.MethodPost,
			accountPath("/topics"),
			map[string]any{"TopicId": id, "Name": id},
		)
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/topics?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["TopicsSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m, isMap := it.(map[string]any)
		require.True(t, isMap)
		seen[m["TopicId"].(string)] = true
	}

	page2 := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath(fmt.Sprintf("/topics?max-results=2&next-token=%s", next)),
		nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["TopicsSummaries"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["TopicId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- Topic permissions: grant, revoke, describe ----

func TestQuickSight_TopicPermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics"),
		map[string]any{"TopicId": "ptp", "Name": "P1"},
	)

	describeEmpty := doRequest(t, h, http.MethodGet, accountPath("/topics/ptp/permissions"), nil)
	require.Equal(t, http.StatusOK, describeEmpty.Code)
	emptyPerms, ok := parseBody(t, describeEmpty)["Permissions"].([]any)
	require.True(t, ok)
	assert.Empty(t, emptyPerms)

	grantRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/topics/ptp/permissions"),
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

	permsMissing := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/notexist/permissions"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, permsMissing.Code)
}

// ---- Topic refresh: lazily synthesized status per RefreshId ----

func TestQuickSight_TopicRefresh(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics"),
		map[string]any{"TopicId": "rtp", "Name": "R1"},
	)

	rec := doRequest(t, h, http.MethodGet, accountPath("/topics/rtp/refresh/refresh-abc"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	details, ok := body["RefreshDetails"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "refresh-abc", details["RefreshId"])
	assert.Equal(t, "COMPLETED", details["RefreshStatus"])

	// Same RefreshId again returns the same stored record.
	rec2 := doRequest(t, h, http.MethodGet, accountPath("/topics/rtp/refresh/refresh-abc"), nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Missing topic -> 404.
	missingRec := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/notexist/refresh/refresh-abc"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

// ---- Topic refresh schedule CRUD, keyed by DatasetId, and duplicate/not-found errors ----

func TestQuickSight_TopicRefreshScheduleCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics"),
		map[string]any{"TopicId": "stp", "Name": "S1"},
	)

	createRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/stp/schedules"),
		map[string]any{
			"DatasetId":   "ds1",
			"DatasetArn":  "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1",
			"IsEnabled":   true,
			"RefreshType": "INCREMENTAL_REFRESH",
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "ds1", createBody["DatasetId"])

	// Duplicate -> ResourceExistsException.
	dupRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/stp/schedules"),
		map[string]any{"DatasetId": "ds1"},
	)
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Missing DatasetId -> validation.
	invalidRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/stp/schedules"),
		map[string]any{},
	)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	// Create against missing topic -> 404.
	missingTopicRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/notexist/schedules"),
		map[string]any{"DatasetId": "ds2"},
	)
	assert.Equal(t, http.StatusNotFound, missingTopicRec.Code)

	// Describe.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/topics/stp/schedules/ds1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	sched, ok := describeBody["RefreshSchedule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, sched["IsEnabled"])
	assert.Equal(t, "INCREMENTAL_REFRESH", sched["RefreshType"])

	// Describe missing schedule -> 404.
	missingSchedRec := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/stp/schedules/notexist"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, missingSchedRec.Code)

	// List.
	listRec := doRequest(t, h, http.MethodGet, accountPath("/topics/stp/schedules"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	items, ok := parseBody(t, listRec)["RefreshSchedules"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	// Update.
	updateRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/topics/stp/schedules/ds1"),
		map[string]any{
			"IsEnabled": false,
		},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	describeAfterUpdate := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/stp/schedules/ds1"),
		nil,
	)
	afterSched := parseBody(t, describeAfterUpdate)["RefreshSchedule"].(map[string]any)
	assert.Equal(t, false, afterSched["IsEnabled"])
	// RefreshType untouched by the partial update.
	assert.Equal(t, "INCREMENTAL_REFRESH", afterSched["RefreshType"])

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/topics/stp/schedules/notexist"),
		map[string]any{},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/topics/stp/schedules/ds1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	// Delete missing -> 404.
	deleteMissingRec := doRequest(
		t,
		h,
		http.MethodDelete,
		accountPath("/topics/stp/schedules/ds1"),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- Topic reviewed answers: batch create, batch delete, list ----

func TestQuickSight_TopicReviewedAnswers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics"),
		map[string]any{"TopicId": "atp", "Name": "A1"},
	)

	createRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/atp/batch-create-reviewed-answers"),
		map[string]any{
			"Answers": []any{
				map[string]any{
					"AnswerId":   "ans1",
					"DatasetArn": "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1",
					"Question":   "How many sales?",
					"Mode":       "MULTIPLE_VISUALS",
				},
				map[string]any{
					// Missing Question -> invalid.
					"AnswerId":   "ans-bad",
					"DatasetArn": "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1",
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "atp", createBody["TopicId"])
	succeeded, ok := createBody["SucceededAnswer"].([]any)
	require.True(t, ok)
	require.Len(t, succeeded, 1)
	invalid, ok := createBody["InvalidAnswers"].([]any)
	require.True(t, ok)
	require.Len(t, invalid, 1)

	// List reviewed answers.
	listRec := doRequest(t, h, http.MethodGet, accountPath("/topics/atp/reviewed-answers"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listBody := parseBody(t, listRec)
	assert.Equal(t, "atp", listBody["TopicId"])
	answers, ok := listBody["Answers"].([]any)
	require.True(t, ok)
	require.Len(t, answers, 1)

	// Batch delete: one valid, one missing.
	deleteRec := doRequest(
		t,
		h,
		http.MethodPost,
		accountPath("/topics/atp/batch-delete-reviewed-answers"),
		map[string]any{
			"AnswerIds": []any{"ans1", "doesnotexist"},
		},
	)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteBody := parseBody(t, deleteRec)
	deletedSucceeded, ok := deleteBody["SucceededAnswer"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"ans1"}, deletedSucceeded)
	deletedInvalid, ok := deleteBody["InvalidAnswers"].([]any)
	require.True(t, ok)
	require.Len(t, deletedInvalid, 1)

	// List again -> empty.
	listAfterRec := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/atp/reviewed-answers"),
		nil,
	)
	afterAnswers := parseBody(t, listAfterRec)["Answers"].([]any)
	assert.Empty(t, afterAnswers)

	// Batch ops against a missing topic -> 404.
	missingRec := doRequest(
		t, h, http.MethodPost, accountPath("/topics/notexist/batch-create-reviewed-answers"),
		map[string]any{"Answers": []any{}},
	)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}
