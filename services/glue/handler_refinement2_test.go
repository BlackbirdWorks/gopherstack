package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// --- Connection handler tests ---

func TestRefinement2_CreateGetDeleteConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "jdbc-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	assert.Equal(t, "jdbc-conn", created["Name"])

	recGet := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "jdbc-conn"})
	assert.Equal(t, http.StatusOK, recGet.Code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &got))
	conn := got["Connection"].(map[string]any)
	assert.Equal(t, "JDBC", conn["ConnectionType"])
	assert.NotZero(t, conn["CreationTime"])
	assert.NotZero(t, conn["LastUpdatedTime"])

	recDel := doGlueRequest(t, h, "DeleteConnection", map[string]any{"ConnectionName": "jdbc-conn"})
	assert.Equal(t, http.StatusOK, recDel.Code)

	recGet2 := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "jdbc-conn"})
	assert.Equal(t, http.StatusBadRequest, recGet2.Code)
}

func TestRefinement2_CreateConnection_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{"Name": ""},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{"Name": "dup"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
	rec3 := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{"Name": "dup"},
	})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestRefinement2_GetConnections_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 4 {
		doGlueRequest(t, h, "CreateConnection", map[string]any{
			"ConnectionInput": map[string]any{
				"Name":           fmt.Sprintf("conn-%d", i),
				"ConnectionType": "JDBC",
			},
		})
	}

	rec := doGlueRequest(t, h, "GetConnections", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	list, _ := out["ConnectionList"].([]any)
	assert.Len(t, list, 4)
}

func TestRefinement2_DeleteConnection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "DeleteConnection", map[string]any{"ConnectionName": "no-such"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_GetConnection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "no-such"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ListCrawlers ---

func TestRefinement2_ListCrawlers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
	for i := range 3 {
		doGlueRequest(t, h, "CreateCrawler", map[string]any{
			"Name":         fmt.Sprintf("lc-%d", i),
			"Role":         "arn:aws:iam::000000000000:role/r",
			"DatabaseName": "db",
		})
	}

	rec := doGlueRequest(t, h, "ListCrawlers", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	names, _ := out["CrawlerNames"].([]any)
	assert.Len(t, names, 3)
	assert.Equal(t, "lc-0", names[0])
	assert.Equal(t, "lc-2", names[2])
}

// --- Validation tests ---

func TestRefinement2_CreateDatabase_EmptyName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]string{"Name": ""},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement2_CreateJob_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "", "Role": "r"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "j", "Role": ""})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRefinement2_CreateCrawler_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})

	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "", "Role": "r", "DatabaseName": "db",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "c", "Role": "", "DatabaseName": "db",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRefinement2_CreateDataQualityRuleset_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{"Name": "", "Ruleset": "Rules=[]"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec2 := doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{"Name": "r", "Ruleset": ""})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// --- Timestamps ---

func TestRefinement2_Timestamps(t *testing.T) {
	t.Parallel()

	t.Run("job", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "j", "Role": "r"})
		rec := doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "j"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		job := out["Job"].(map[string]any)
		assert.NotZero(t, job["CreatedOn"])
		assert.NotZero(t, job["LastModifiedOn"])
	})

	t.Run("database", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db"}})
		rec := doGlueRequest(t, h, "GetDatabase", map[string]any{"Name": "ts-db"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		db := out["Database"].(map[string]any)
		assert.NotZero(t, db["CreateTime"])
	})

	t.Run("table", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db2"}})
		doGlueRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "ts-db2",
			"TableInput":   map[string]string{"Name": "ts-tbl"},
		})
		rec := doGlueRequest(t, h, "GetTable", map[string]any{"DatabaseName": "ts-db2", "Name": "ts-tbl"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		tbl := out["Table"].(map[string]any)
		assert.NotZero(t, tbl["CreateTime"])
		assert.NotZero(t, tbl["UpdateTime"])
	})

	t.Run("crawler", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "ts-db3"}})
		doGlueRequest(t, h, "CreateCrawler", map[string]any{
			"Name":         "ts-crawler",
			"Role":         "r",
			"DatabaseName": "ts-db3",
		})
		rec := doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "ts-crawler"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		c := out["Crawler"].(map[string]any)
		assert.NotZero(t, c["CreationTime"])
		assert.NotZero(t, c["LastUpdated"])
	})

	t.Run("dataquality_ruleset", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
			"Name": "ts-rs", "Ruleset": "Rules = [ RowCount > 0 ]",
		})
		rec := doGlueRequest(t, h, "GetDataQualityRuleset", map[string]any{"Name": "ts-rs"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotZero(t, out["CreatedOn"])
	})
}

// --- Cascade delete ---

func TestRefinement2_DeleteJob_CascadesRunsAndBookmarks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "cascade-job", "Role": "r"})
	doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})
	doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "cascade-job"})

	rec := doGlueRequest(t, h, "DeleteJob", map[string]any{"JobName": "cascade-job"})
	assert.Equal(t, http.StatusOK, rec.Code)

	recGetJob := doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "cascade-job"})
	assert.Equal(t, http.StatusBadRequest, recGetJob.Code)
}

// --- CrawlerRunningException / CrawlerNotRunningException ---

func TestRefinement2_CrawlerRunningException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "c", "Role": "r", "DatabaseName": "db",
	})
	doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "c"})

	rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "c"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "CrawlerRunningException", out["__type"])
}

func TestRefinement2_CrawlerNotRunningException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "c", "Role": "r", "DatabaseName": "db",
	})

	rec := doGlueRequest(t, h, "StopCrawler", map[string]any{"Name": "c"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "CrawlerNotRunningException", out["__type"])
}

// --- MaxConcurrentRuns enforcement ---

func TestRefinement2_MaxConcurrentRuns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "limited-job",
		"Role": "r",
		"ExecutionProperty": map[string]any{"MaxConcurrentRuns": 2},
	})

	rec1 := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "limited-job"})
	assert.Equal(t, http.StatusOK, rec1.Code)
	rec2 := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "limited-job"})
	assert.Equal(t, http.StatusOK, rec2.Code)
	rec3 := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "limited-job"})
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

// --- BatchStopJobRun state validation ---

func TestRefinement2_BatchStopJobRun_NonStoppable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "j", "Role": "r"})

	runRec := doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	runID := runOut["JobRunId"].(string)

	doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
		"JobName":   "j",
		"JobRunIds": []string{runID},
	})

	rec := doGlueRequest(t, h, "BatchStopJobRun", map[string]any{
		"JobName":   "j",
		"JobRunIds": []string{runID},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	errs, _ := out["Errors"].([]any)
	assert.Len(t, errs, 1)
}

// --- GetJobBookmark for nonexistent job ---

func TestRefinement2_GetJobBookmark_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "GetJobBookmark", map[string]any{"JobName": "no-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- ResetJobBookmark returns post-reset state ---

func TestRefinement2_ResetJobBookmark_Returns_PostReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "j", "Role": "r"})
	doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

	rec := doGlueRequest(t, h, "ResetJobBookmark", map[string]any{"JobName": "j"})
	assert.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	bm, _ := out["JobBookmarkEntry"].(map[string]any)
	assert.Equal(t, "j", bm["JobName"])

	rec2 := doGlueRequest(t, h, "ResetJobBookmark", map[string]any{"JobName": "no-job"})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// --- UpdateCrawlerSchedule uses CrawlerName ---

func TestRefinement2_UpdateCrawlerSchedule_UsesCrawlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "c", "Role": "r", "DatabaseName": "db",
	})

	rec := doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
		"CrawlerName": "c",
		"Schedule":    "cron(0 * * * ? *)",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	recGet := doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "c"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
	c := out["Crawler"].(map[string]any)
	sched := c["Schedule"].(map[string]any)
	assert.Equal(t, "cron(0 * * * ? *)", sched["ScheduleExpression"])
}

// --- StartCrawlerSchedule validates non-empty expression ---

func TestRefinement2_StartCrawlerSchedule_RequiresExpression(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "c", "Role": "r", "DatabaseName": "db",
	})

	rec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": "c"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
		"CrawlerName": "c",
		"Schedule":    "cron(0 12 * * ? *)",
	})
	rec2 := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": "c"})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// --- ListDataQualityRulesets uses Rulesets field ---

func TestRefinement2_ListDataQualityRulesets_Field(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 2 {
		doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
			"Name":    fmt.Sprintf("rs-%d", i),
			"Ruleset": "Rules = [ RowCount > 0 ]",
		})
	}

	rec := doGlueRequest(t, h, "ListDataQualityRulesets", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	rulesets, _ := out["Rulesets"].([]any)
	assert.Len(t, rulesets, 2)
	_, hasOldKey := out["DataQualityRulesets"]
	assert.False(t, hasOldKey, "should not use deprecated DataQualityRulesets key")
}

// --- Seed helpers ---

func TestRefinement2_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)

	b.AddJobRunInternal(&glue.JobRun{
		ID:          "jr-seed",
		JobName:     "seed-job",
		JobRunState: "SUCCEEDED",
	})
	b.AddDataQualityRulesetInternal(&glue.DataQualityRuleset{
		Name:    "seed-rs",
		Ruleset: "Rules = [ RowCount > 0 ]",
	})
	b.AddDataQualityEvalRunInternal(&glue.DataQualityEvaluationRun{
		RunID:  "seed-run-1",
		Status: "SUCCEEDED",
	})

	assert.Equal(t, 1, glue.DataQualityRulesetCount(b))
	assert.Equal(t, 1, glue.DataQualityEvalRunCount(b))
	assert.Equal(t, 1, glue.JobRunCount(b))
}

// --- JobBookmark ActiveRun tracking ---

func TestRefinement2_JobBookmark_ActiveRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateJob", map[string]any{"Name": "j", "Role": "r"})
	doGlueRequest(t, h, "StartJobRun", map[string]any{"JobName": "j"})

	rec := doGlueRequest(t, h, "GetJobBookmark", map[string]any{"JobName": "j"})
	assert.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	bm, _ := out["JobBookmarkEntry"].(map[string]any)
	assert.NotEmpty(t, bm["ActiveRun"])
}

// --- Connection persistence ---

func TestRefinement2_ConnectionPersistence(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.AddConnectionInternal(&glue.Connection{
		Name:           "persist-conn",
		ConnectionType: "JDBC",
		ConnectionProperties: map[string]string{
			"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306",
		},
	})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := glue.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b2.Restore(snap))
	assert.Equal(t, 1, glue.ConnectionCount(b2))

	conn, err := b2.GetConnection("persist-conn")
	require.NoError(t, err)
	assert.Equal(t, "JDBC", conn.ConnectionType)
	assert.Equal(t, "jdbc:mysql://localhost:3306", conn.ConnectionProperties["JDBC_CONNECTION_URL"])
}
