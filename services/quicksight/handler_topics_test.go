package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
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

	// Delete. Real DeleteTopicOutput carries Arn (api_op_DeleteTopic.go).
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/topics/tp1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteBody := parseBody(t, deleteRec)
	assert.Equal(t, "tp1", deleteBody["TopicId"])
	assert.Contains(t, deleteBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:topic/tp1")

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
	// CreateTopicRefreshScheduleOutput carries TopicArn (api_op_CreateTopicRefreshSchedule.go)
	// -- this backend already tracks it on the topic record.
	assert.Contains(t, createBody["TopicArn"], "topic/stp")

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
	assert.Contains(t, describeBody["TopicArn"], "topic/stp")

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
	listBody := parseBody(t, listRec)
	items, ok := listBody["RefreshSchedules"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	assert.Contains(t, listBody["TopicArn"], "topic/stp")

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
	assert.Contains(t, parseBody(t, updateRec)["TopicArn"], "topic/stp")

	describeAfterUpdate := doRequest(
		t,
		h,
		http.MethodGet,
		accountPath("/topics/stp/schedules/ds1"),
		nil,
	)
	afterBody := parseBody(t, describeAfterUpdate)
	afterSched := afterBody["RefreshSchedule"].(map[string]any)
	assert.Equal(t, false, afterSched["IsEnabled"])
	// RefreshType untouched by the partial update.
	assert.Equal(t, "INCREMENTAL_REFRESH", afterSched["RefreshType"])
	assert.Contains(t, afterBody["TopicArn"], "topic/stp")

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t,
		h,
		http.MethodPut,
		accountPath("/topics/stp/schedules/notexist"),
		map[string]any{},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete. DeleteTopicRefreshScheduleOutput carries DatasetArn/TopicArn
	// (api_op_DeleteTopicRefreshSchedule.go) -- both already tracked by this
	// backend, just never surfaced on delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/topics/stp/schedules/ds1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteBody := parseBody(t, deleteRec)
	assert.Contains(t, deleteBody["TopicArn"], "topic/stp")
	assert.Equal(t, "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1", deleteBody["DatasetArn"])

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
	succeeded, ok := createBody["SucceededAnswers"].([]any)
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
	deletedSucceeded, ok := deleteBody["SucceededAnswers"].([]any)
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

// ---- Topic tests ---- //nolint:godot // existing issue.
func TestQuickSight_Topics(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create topic",
			method:     http.MethodPost,
			path:       accountPath("/topics"),
			body:       map[string]any{"TopicId": "top1", "Name": "Topic1"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "update topic",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1"),
			body:       map[string]any{"Name": "Renamed"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topics",
			method:     http.MethodGet,
			path:       accountPath("/topics"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicsSummaries",
		},
		{
			name:       "describe topic permissions",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/permissions"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "update topic permissions",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1/permissions"),
			body:       map[string]any{"GrantPermissions": []any{}, "RevokePermissions": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic refresh",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/refresh/ref1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "create topic refresh schedule",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/schedules"),
			body:       map[string]any{"DatasetId": "ds1"},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "describe topic refresh schedule",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/schedules/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topic refresh schedules",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/schedules"),
			wantStatus: http.StatusOK,
			wantKey:    "RefreshSchedules",
		},
		{
			name:       "update topic refresh schedule",
			method:     http.MethodPut,
			path:       accountPath("/topics/top1/schedules/ds1"),
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "batch create reviewed answers",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/batch-create-reviewed-answers"),
			body:       map[string]any{"Answers": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "batch delete reviewed answers",
			method:     http.MethodPost,
			path:       accountPath("/topics/top1/batch-delete-reviewed-answers"),
			body:       map[string]any{"AnswerIds": []any{}},
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "list topic reviewed answers",
			method:     http.MethodGet,
			path:       accountPath("/topics/top1/reviewed-answers"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "delete topic refresh schedule",
			method:     http.MethodDelete,
			path:       accountPath("/topics/top1/schedules/ds1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
		{
			name:       "delete topic",
			method:     http.MethodDelete,
			path:       accountPath("/topics/top1"),
			wantStatus: http.StatusOK,
			wantKey:    "TopicId",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}

// ---- SearchTopics ----

func TestQuickSight_SearchTopics(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "t1",
		"Name":    "Sales",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "t2",
		"Name":    "Marketing",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// No filters: both topics come back.
	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{"Filters": []any{}})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	list, ok := body["TopicSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	summary, ok := list[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, summary, "Arn")
	assert.Contains(t, summary, "TopicId")
	assert.Contains(t, summary, "Name")
	assert.Contains(t, summary, "UserExperienceVersion")
	assert.NotContains(t, summary, "Description")
	assert.NotContains(t, summary, "CreatedTime")

	// TOPIC_NAME StringEquals filter narrows to a single match.
	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "TOPIC_NAME", "Operator": "StringEquals", "Value": "Sales"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1)
	summary, ok = list[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Sales", summary["Name"])
}

// TestQuickSight_SearchTopics_Pagination guards against a wire-shape
// regression: SearchTopicsInput carries MaxResults/NextToken in the JSON
// body (per awsRestjson1_serializeOpDocumentSearchTopicsInput), not as
// query parameters — same as SearchTopicsV2.
func TestQuickSight_SearchTopics_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, id := range []string{"t1", "t2", "t3"} {
		rec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
			"TopicId": id,
			"Name":    id,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{
		"Filters":    []any{},
		"MaxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	list, ok := body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1, "MaxResults in the JSON body should limit the page size")
	nextToken, ok := body["NextToken"].(string)
	require.True(t, ok, "a NextToken should be returned when more results remain")
	require.NotEmpty(t, nextToken)

	rec = doRequest(t, h, http.MethodPost, accountPath("/search/topics"), map[string]any{
		"Filters":    []any{},
		"MaxResults": 1,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, list, 1, "NextToken in the JSON body should resume from the prior page")
	summary, ok := list[0].(map[string]any)
	require.True(t, ok)
	assert.NotEqual(t, "t1", summary["TopicId"], "the second page must not repeat the first page's entry")

	// A query-string MaxResults is not part of the real wire shape and must
	// be ignored: with no MaxResults in the body, all three topics come back.
	rec = doRequest(
		t, h, http.MethodPost, accountPath("/search/topics?MaxResults=1"), map[string]any{"Filters": []any{}},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	body = parseBody(t, rec)
	list, ok = body["TopicSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 3, "a query-string MaxResults must be ignored; SearchTopicsInput carries it in the JSON body")
}

// ---- PredictQAResults ----

// TestQuickSight_PredictQAResults verifies that answers are grounded in real
// account state (an existing Topic), not fabricated: a query mentioning a
// registered topic's name gets a GENERATED_ANSWER result referencing that
// real topic, while an unrelated query gets the explicit NO_ANSWER result.
func TestQuickSight_PredictQAResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "sales-topic",
		"Name":    "Sales Performance",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	tests := []struct {
		name           string
		queryText      string
		wantResultType string
	}{
		{
			name:           "matches_real_topic",
			queryText:      "What is our Sales Performance this quarter?",
			wantResultType: "GENERATED_ANSWER",
		},
		{
			name:           "no_matching_topic",
			queryText:      "What is the weather today?",
			wantResultType: "NO_ANSWER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, accountPath("/qa/predict"), map[string]any{
				"QueryText": tt.queryText,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			body := parseBody(t, rec)
			primary, ok := body["PrimaryResult"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantResultType, primary["ResultType"])

			if tt.wantResultType != "GENERATED_ANSWER" {
				assert.NotContains(t, primary, "GeneratedAnswer")

				return
			}

			answer, hasAnswer := primary["GeneratedAnswer"].(map[string]any)
			require.True(t, hasAnswer)
			assert.Equal(t, "sales-topic", answer["TopicId"])
			assert.Equal(t, "Sales Performance", answer["TopicName"])
			assert.NotEmpty(t, answer["AnswerId"])
		})
	}

	// Missing QueryText -> validation error, not a fabricated 200.
	rec := doRequest(t, h, http.MethodPost, accountPath("/qa/predict"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreateTopicReturnsNonEmptyID verifies CreateTopic (POST /accounts/{id}/topics) returns
// a non-empty TopicId. AWS CreateTopic requires both TopicId and Name in the request body.
func TestCreateTopicReturnsNonEmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	rec := doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{
		"TopicId": "topic1",
		"Name":    "Topic1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseBody(t, rec)
	topicID, _ := body["TopicId"].(string)
	assert.NotEmpty(t, topicID, "CreateTopic must return a non-empty TopicId")
}
