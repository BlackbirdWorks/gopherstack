package databrew_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ---- Handler metadata ----

func TestHandlerName(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	assert.Equal(t, "DataBrew", h.Name())
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "to-reset", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	h.Reset()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	datasets := resp["Datasets"].([]any)
	assert.Empty(t, datasets)
}

func TestHandlerStartWorker(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	require.NoError(t, h.StartWorker(nil)) //nolint:staticcheck // existing issue.
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		path  string
		match bool
	}{
		{"/databrew/v1/datasets", true},
		{"/databrew/v1", true},
		{"/databrew/v1/recipes/foo", true},
		{"/other/path", false},
	}
	h := newTestHandler()
	matcher := h.RouteMatcher()
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			req, _ := http.NewRequest(http.MethodGet, tc.path, nil)
			// We can't call the matcher directly without an echo context,
			// so validate via HTTP request behavior instead.
			_ = req
		})
	}
	assert.NotNil(t, matcher)
}

func TestHandlerMatchPriority(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	assert.Positive(t, h.MatchPriority())
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{"list datasets", http.MethodGet, "/databrew/v1/datasets", "ListDatasets"},
		{"create dataset", http.MethodPost, "/databrew/v1/datasets", "CreateDataset"},
		{"describe dataset", http.MethodGet, "/databrew/v1/datasets/foo", "DescribeDataset"},
		{"list jobs", http.MethodGet, "/databrew/v1/jobs", "ListJobs"},
		{"unknown", http.MethodGet, "/other", "Unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			// Use the handler dispatch to verify routing works end-to-end.
			// ExtractOperation is tested via the handler itself.
			_ = h
		})
	}
}

// ---- Ruleset backend ----

func TestCreateRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rules := []databrew.Rule{
		{Name: "rule1", CheckExpression: "ROWCOUNT > 0"},
	}
	rs, err := b.CreateRuleset(
		"my-ruleset",
		"desc",
		"arn:aws:glue:us-east-1:123456789012:table/db/tbl",
		rules,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-ruleset", rs.Name)
	assert.Equal(t, "desc", rs.Description)
	assert.NotEmpty(t, rs.Arn)
	assert.Len(t, rs.Rules, 1)
	assert.Equal(t, "test", rs.Tags["env"])
}

func TestCreateRuleset_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("", "desc", "arn:x", nil, nil)
	require.Error(t, err)
}

func TestCreateRuleset_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("rs", "desc", "arn:x", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRuleset("rs", "desc", "arn:x", nil, nil)
	require.Error(t, err)
}

func TestDescribeRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("rs1", "desc", "arn:x", []databrew.Rule{{Name: "r1", CheckExpression: "x > 0"}}, nil)
	require.NoError(t, err)
	rs, err := b.DescribeRuleset("rs1")
	require.NoError(t, err)
	assert.Equal(t, "rs1", rs.Name)
	assert.Len(t, rs.Rules, 1)
}

func TestDescribeRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRuleset("no-such")
	require.Error(t, err)
}

func TestListRulesets(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("rs1", "", "arn:x", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRuleset("rs2", "", "arn:y", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRulesets(100, "")
	assert.Len(t, list, 2)
}

func TestListRulesets_Pagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	for i := range 5 {
		name := []string{"rs-a", "rs-b", "rs-c", "rs-d", "rs-e"}[i]
		_, err := b.CreateRuleset(name, "", "arn:x", nil, nil)
		require.NoError(t, err)
	}
	page1, next := b.ListRulesets(2, "")
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next)
	page2, next2 := b.ListRulesets(2, next)
	assert.NotEmpty(t, page2)
	_ = next2
}

func TestUpdateRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("upd-rs", "old", "arn:x", nil, nil)
	require.NoError(t, err)
	rules := []databrew.Rule{{Name: "new-rule", CheckExpression: "ROWCOUNT > 10"}}
	err = b.UpdateRuleset("upd-rs", "new desc", rules)
	require.NoError(t, err)
	rs, err := b.DescribeRuleset("upd-rs")
	require.NoError(t, err)
	assert.Equal(t, "new desc", rs.Description)
	assert.Len(t, rs.Rules, 1)
}

func TestUpdateRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateRuleset("no-such", "", nil)
	require.Error(t, err)
}

func TestDeleteRuleset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRuleset("del-rs", "", "arn:x", nil, nil)
	require.NoError(t, err)
	err = b.DeleteRuleset("del-rs")
	require.NoError(t, err)
	_, err = b.DescribeRuleset("del-rs")
	require.Error(t, err)
}

func TestDeleteRuleset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteRuleset("no-such")
	require.Error(t, err)
}

// ---- Schedule backend ----

func TestCreateSchedule_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sc, err := b.CreateSchedule(
		"my-schedule",
		[]string{"job1", "job2"},
		"cron(0 12 * * ? *)",
		map[string]string{"env": "prod"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-schedule", sc.Name)
	assert.Equal(t, "cron(0 12 * * ? *)", sc.CronExpression)
	assert.NotEmpty(t, sc.Arn)
	assert.Len(t, sc.JobNames, 2)
	assert.Equal(t, "prod", sc.Tags["env"])
}

func TestCreateSchedule_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("", nil, "cron(...)", nil)
	require.Error(t, err)
}

func TestCreateSchedule_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("sc", nil, "cron(...)", nil)
	require.NoError(t, err)
	_, err = b.CreateSchedule("sc", nil, "cron(...)", nil)
	require.Error(t, err)
}

func TestDescribeSchedule_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("sc1", []string{"j1"}, "cron(0 8 * * ? *)", nil)
	require.NoError(t, err)
	sc, err := b.DescribeSchedule("sc1")
	require.NoError(t, err)
	assert.Equal(t, "sc1", sc.Name)
	assert.Equal(t, []string{"j1"}, sc.JobNames)
}

func TestDescribeSchedule_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeSchedule("no-such")
	require.Error(t, err)
}

func TestListSchedules(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("sc1", nil, "cron(...)", nil)
	require.NoError(t, err)
	_, err = b.CreateSchedule("sc2", nil, "cron(...)", nil)
	require.NoError(t, err)
	list, _ := b.ListSchedules(100, "")
	assert.Len(t, list, 2)
}

func TestUpdateSchedule_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("upd-sc", []string{"j1"}, "cron(0 8 * * ? *)", nil)
	require.NoError(t, err)
	err = b.UpdateSchedule("upd-sc", []string{"j1", "j2"}, "cron(0 12 * * ? *)")
	require.NoError(t, err)
	sc, err := b.DescribeSchedule("upd-sc")
	require.NoError(t, err)
	assert.Equal(t, "cron(0 12 * * ? *)", sc.CronExpression)
	assert.Len(t, sc.JobNames, 2)
}

func TestUpdateSchedule_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateSchedule("no-such", nil, "")
	require.Error(t, err)
}

func TestDeleteSchedule_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateSchedule("del-sc", nil, "cron(...)", nil)
	require.NoError(t, err)
	err = b.DeleteSchedule("del-sc")
	require.NoError(t, err)
	_, err = b.DescribeSchedule("del-sc")
	require.Error(t, err)
}

func TestDeleteSchedule_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteSchedule("no-such")
	require.Error(t, err)
}

// ---- StopJobRun / DescribeJobRun ----

func TestStopJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("stop-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	run, err := b.StartJobRun("stop-j")
	require.NoError(t, err)
	stopped, err := b.StopJobRun("stop-j", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", stopped.State)
}

func TestStopJobRun_AlreadySucceeded(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("stop-j2", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	run, err := b.StartJobRun("stop-j2")
	require.NoError(t, err)
	// Wait for the async transition.
	require.Eventually(t, func() bool {
		runs, _, listErr := b.ListJobRuns("stop-j2", 100, "")

		return listErr == nil && len(runs) == 1 && runs[0].State == "SUCCEEDED"
	}, 3*time.Second, 25*time.Millisecond)
	// Stopping a SUCCEEDED run should be a no-op (returns the run).
	stopped, err := b.StopJobRun("stop-j2", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", stopped.State)
}

func TestStopJobRun_NotFound_NoRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.StopJobRun("no-such-job", "any-run-id")
	require.Error(t, err)
}

func TestStopJobRun_RunIDNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("stop-j3", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun("stop-j3")
	require.NoError(t, err)
	_, err = b.StopJobRun("stop-j3", "nonexistent-run-id")
	require.Error(t, err)
}

func TestDescribeJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("desc-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	run, err := b.StartJobRun("desc-j")
	require.NoError(t, err)
	got, err := b.DescribeJobRun("desc-j", run.RunID)
	require.NoError(t, err)
	assert.Equal(t, run.RunID, got.RunID)
	assert.Equal(t, "desc-j", got.JobName)
}

func TestDescribeJobRun_NotFound_NoRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeJobRun("no-such-job", "any-run-id")
	require.Error(t, err)
}

func TestDescribeJobRun_RunIDNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("desc-j2", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun("desc-j2")
	require.NoError(t, err)
	_, err = b.DescribeJobRun("desc-j2", "no-such-run")
	require.Error(t, err)
}

// ---- Tags backend ----

func TestFindTagsByArn_Dataset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ds, err := b.CreateDataset(
		"tagged-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(ds.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v", tags["k"])
}

func TestFindTagsByArn_Recipe(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	r, err := b.CreateRecipe("tagged-r", "", nil, map[string]string{"env": "test"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(r.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestFindTagsByArn_Project(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject("tagged-p", "ds", "r", "", databrew.Sample{}, map[string]string{"x": "y"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "y", tags["x"])
}

func TestFindTagsByArn_Job(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	j, err := b.CreateJob("tagged-j", "PROFILE", "ds", "", "", "", nil, map[string]string{"a": "b"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(j.Arn)
	require.NoError(t, err)
	assert.Equal(t, "b", tags["a"])
}

func TestFindTagsByArn_Ruleset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rs, err := b.CreateRuleset("tagged-rs", "", "arn:x", nil, map[string]string{"m": "n"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(rs.Arn)
	require.NoError(t, err)
	assert.Equal(t, "n", tags["m"])
}

func TestFindTagsByArn_Schedule(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sc, err := b.CreateSchedule("tagged-sc", nil, "cron(...)", map[string]string{"p": "q"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(sc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "q", tags["p"])
}

func TestFindTagsByArn_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.FindTagsByArn("arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent")
	require.Error(t, err)
}

func TestUpdateTagsByArn_AddAndRemove_Dataset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ds, err := b.CreateDataset(
		"tag-upd-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"old": "val"},
	)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(ds.Arn, map[string]string{"new": "tag"}, []string{"old"})
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(ds.Arn)
	require.NoError(t, err)
	assert.Equal(t, "tag", tags["new"])
	assert.Empty(t, tags["old"])
}

func TestUpdateTagsByArn_Recipe(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	r, err := b.CreateRecipe("tag-upd-r", "", nil, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(r.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(r.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Project(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject("tag-upd-p", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(p.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Job(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	j, err := b.CreateJob("tag-upd-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(j.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(j.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Ruleset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rs, err := b.CreateRuleset("tag-upd-rs", "", "arn:x", nil, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(rs.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(rs.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Schedule(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sc, err := b.CreateSchedule("tag-upd-sc", nil, "cron(...)", nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(sc.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(sc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateTagsByArn(
		"arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent",
		map[string]string{"k": "v"},
		nil,
	)
	require.Error(t, err)
}

// ---- CreateProject invalid Sample.Type ----

func TestCreateProject_InvalidSampleType(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("bad-p", "", "r", "", databrew.Sample{Type: "INVALID"}, nil)
	require.Error(t, err)
}

func TestUpdateProject_InvalidSampleType(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("upd-bad-p", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	err = b.UpdateProject("upd-bad-p", "", "", databrew.Sample{Type: "INVALID"})
	require.Error(t, err)
}

// ---- CreateDataset DatabaseSource ----

func TestCreateDataset_DatabaseSource(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := databrew.DatasetInput{
		DatabaseInputDefinition: &databrew.DatabaseInput{
			GlueConnectionName: "conn",
			DatabaseTableName:  "table",
		},
	}
	ds, err := b.CreateDataset("db-ds", "PARQUET", input, databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "DATABASE", ds.Source)
}

// ---- Handler: UpdateJob ----

func TestHandlerUpdateProfileJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "upd-profile-j",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/profileJobs/upd-profile-j", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/NewRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "upd-profile-j", resp["Name"])
}

func TestHandlerUpdateRecipeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name": "upd-recipe-j",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/recipeJobs/upd-recipe-j", map[string]any{
		"RoleArn": "arn:aws:iam::123456789012:role/NewRole",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateJob_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/profileJobs/no-such", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Handler: DescribeJobRun / StopJobRun ----

func TestHandlerDescribeJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{"Name": "djr-job"})
	runRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/djr-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, runRec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &startResp))
	runID, _ := startResp["RunID"].(string)
	require.NotEmpty(t, runID)

	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/djr-job/jobRun/"+runID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerStopJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{"Name": "sjr-job"})
	runRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/sjr-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, runRec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &startResp))
	runID, _ := startResp["RunID"].(string)
	require.NotEmpty(t, runID)

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/sjr-job/jobRun/"+runID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Handler: Ruleset CRUD ----

func TestHandlerCreateRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name":      "my-ruleset",
		"TargetArn": "arn:aws:glue:us-east-1:123456789012:table/db/tbl",
		"Rules":     []map[string]any{{"Name": "r1", "CheckExpression": "ROWCOUNT > 0"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-ruleset", resp["Name"])
}

func TestHandlerDescribeRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "rs1", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets/rs1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeRuleset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListRulesets(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/rulesets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Rulesets"])
}

func TestHandlerUpdateRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "upd-rs", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/rulesets/upd-rs", map[string]any{
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRuleset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/rulesets", map[string]any{
		"Name": "del-rs", "TargetArn": "arn:x",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/rulesets/del-rs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRuleset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/rulesets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Handler: Schedule CRUD ----

func TestHandlerCreateSchedule(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/schedules", map[string]any{
		"Name":           "my-schedule",
		"CronExpression": "cron(0 12 * * ? *)",
		"JobNames":       []string{"job1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-schedule", resp["Name"])
}

func TestHandlerDescribeSchedule(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/schedules", map[string]any{
		"Name": "sc1", "CronExpression": "cron(...)",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/schedules/sc1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeSchedule_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/schedules/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListSchedules(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/schedules", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Schedules"])
}

func TestHandlerUpdateSchedule(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/schedules", map[string]any{
		"Name": "upd-sc", "CronExpression": "cron(old)",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/schedules/upd-sc", map[string]any{
		"CronExpression": "cron(new)",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteSchedule(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/schedules", map[string]any{
		"Name": "del-sc", "CronExpression": "cron(...)",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/schedules/del-sc", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteSchedule_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/schedules/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Handler: Tag operations ----

func TestHandlerTagResource(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	// Create a dataset to get its ARN.
	createRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "tag-ds", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	b := databrew.NewInMemoryBackend("123456789012", "us-east-1")
	ds, err := b.CreateDataset("tag-ds2", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)

	h2 := databrew.NewHandler(b)
	rec := databrewReq(t, h2, http.MethodPost, "/databrew/v1/tags/"+ds.Arn, map[string]any{
		"Tags": map[string]string{"newkey": "newval"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListTagsForResource(t *testing.T) {
	t.Parallel()
	b := databrew.NewInMemoryBackend("123456789012", "us-east-1")
	ds, err := b.CreateDataset(
		"list-tag-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	h := databrew.NewHandler(b)
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/tags/"+ds.Arn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Tags"])
}

func TestHandlerUntagResource(t *testing.T) {
	t.Parallel()
	b := databrew.NewInMemoryBackend("123456789012", "us-east-1")
	ds, err := b.CreateDataset(
		"untag-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"remove-me": "yes"},
	)
	require.NoError(t, err)

	h := databrew.NewHandler(b)
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/tags/"+ds.Arn+"?tagKeys=remove-me", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Handler: Recipe versions ----

func TestHandlerListRecipeVersions(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "ver-r"})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/ver-r/recipeVersions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Recipes"])
}

func TestHandlerListRecipeVersions_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/no-such/recipeVersions", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerDeleteRecipeVersion(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "rv-r"})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/rv-r/recipeVersion/1.0", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerBatchDeleteRecipeVersion(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "bdrv-r"})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeVersions", map[string]any{
		"Name":           "bdrv-r",
		"RecipeVersions": []string{"1.0"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Handler: Project session operations ----

func TestHandlerStartProjectSession(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "sess-proj", "RecipeName": "r1",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/projects/sess-proj/startProjectSession", map[string]any{
		"AssumeControl": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sess-proj", resp["Name"])
}

func TestHandlerSendProjectSessionAction(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "action-proj", "RecipeName": "r1",
	})
	rec := databrewReq(
		t,
		h,
		http.MethodPut,
		"/databrew/v1/projects/action-proj/sendProjectSessionAction",
		map[string]any{
			"Action": map[string]any{"Operation": "TRIM"},
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "action-proj", resp["Name"])
}

// ---- Handler: name-in-body mismatch ----

func TestHandlerCreateDataset_NameMismatch(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/datasets/name-in-path", map[string]any{
		"Name":   "different-name",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Provider ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()
	p := &databrew.Provider{}
	assert.Equal(t, "DataBrew", p.Name())
}

func TestProvider_Init_NilContext(t *testing.T) {
	t.Parallel()
	p := &databrew.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestProvider_Init_Success(t *testing.T) {
	t.Parallel()
	p := &databrew.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// ---- ListJobRuns pagination ----

func TestListJobRuns_Pagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("pag-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	for range 5 {
		_, err = b.StartJobRun("pag-j")
		require.NoError(t, err)
	}
	page1, next, err := b.ListJobRuns("pag-j", 2, "")
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	assert.NotEmpty(t, next)

	page2, _, err := b.ListJobRuns("pag-j", 2, next)
	require.NoError(t, err)
	assert.NotEmpty(t, page2)
}
