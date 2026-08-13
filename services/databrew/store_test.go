package databrew_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// newTestBackend returns a fresh InMemoryBackend. Shared across the
// databrew_test package's family test files.
func newTestBackend() *databrew.InMemoryBackend {
	return databrew.NewInMemoryBackend("123456789012", "us-east-1")
}

// s3Input builds a DatasetInput with an S3 source. Shared across the
// databrew_test package's family test files.
func s3Input(bucket, key string) databrew.DatasetInput {
	return databrew.DatasetInput{
		S3InputDefinition: &databrew.S3Location{Bucket: bucket, Key: key},
	}
}

// newTestHandler returns a fresh Handler backed by a fresh InMemoryBackend.
// Shared across the databrew_test package's family test files.
func newTestHandler() *databrew.Handler {
	return databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
}

// databrewReq performs a request against the databrew handler via echo.
// Shared across the databrew_test package's family test files.
func databrewReq(t *testing.T, h *databrew.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// extractOp is a helper that calls ExtractOperation via a fake echo context.
// Shared across the databrew_test package's family test files.
func extractOp(t *testing.T, h *databrew.Handler, method, path string) string {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())

	return h.ExtractOperation(c)
}

func TestReset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateProject(context.Background(), "p", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(context.Background(), "j", "PROFILE", "ds", "", "", "", nil, nil, databrew.JobExtras{})
	require.NoError(t, err)

	b.Reset()

	dsList, _ := b.ListDatasets(context.Background(), 100, "")
	assert.Empty(t, dsList)
	rList, _ := b.ListRecipes(context.Background(), 100, "", "")
	assert.Empty(t, rList)
	pList, _ := b.ListProjects(context.Background(), 100, "")
	assert.Empty(t, pList)
	jList, _ := b.ListJobs(context.Background(), 100, "", "", "")
	assert.Empty(t, jList)
}

// TestAccountID_PopulatedOnCreate verifies every entity that carries an
// AccountId field in the real SDK (aws-sdk-go-v2/service/databrew/types'
// Dataset/Job/Project/RulesetItem/Schedule all have AccountId; Recipe does
// not) has it populated from the backend's account ID -- these fields were
// previously entirely absent from gopherstack's models, so ListDatasets/
// ListJobs/ListProjects/ListRulesets/ListSchedules always echoed an empty
// AccountId.
func TestAccountID_PopulatedOnCreate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ctx := context.Background()
	const wantAccountID = "123456789012"

	ds, err := b.CreateDataset(ctx, "acct-ds", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, wantAccountID, ds.AccountID)

	p, err := b.CreateProject(ctx, "acct-p", "acct-ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	assert.Equal(t, wantAccountID, p.AccountID)

	j, err := b.CreateJob(ctx, "acct-j", "PROFILE", "acct-ds", "", "", "", nil, nil, databrew.JobExtras{})
	require.NoError(t, err)
	assert.Equal(t, wantAccountID, j.AccountID)

	rs, err := b.CreateRuleset(ctx, "acct-rs", "", "arn:x", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, wantAccountID, rs.AccountID)

	sc, err := b.CreateSchedule(ctx, "acct-sc", nil, "cron(0 12 * * ? *)", nil)
	require.NoError(t, err)
	assert.Equal(t, wantAccountID, sc.AccountID)
}

// TestHandlerDescribe_NoAccountIDLeak asserts the raw JSON body of each
// Describe{Dataset,Job,Project,Schedule} response has no "AccountId" key --
// none of DescribeDatasetOutput/DescribeJobOutput/DescribeProjectOutput/
// DescribeScheduleOutput has an AccountId member (only their List item
// counterparts do); an SDK client silently drops the unrecognized key, so
// only a raw-body assertion catches the leak.
func TestHandlerDescribe_NoAccountIDLeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createPath string
		createBody map[string]any
		describe   string
	}{
		{
			name:       "dataset",
			createPath: "/databrew/v1/datasets",
			createBody: map[string]any{
				"Name": "leak-ds", "Format": "CSV",
				"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
			},
			describe: "/databrew/v1/datasets/leak-ds",
		},
		{
			name:       "job",
			createPath: "/databrew/v1/profileJobs",
			createBody: map[string]any{"Name": "leak-j", "DatasetName": "leak-ds"},
			describe:   "/databrew/v1/jobs/leak-j",
		},
		{
			name:       "project",
			createPath: "/databrew/v1/projects",
			createBody: map[string]any{"Name": "leak-p", "RecipeName": "r1"},
			describe:   "/databrew/v1/projects/leak-p",
		},
		{
			name:       "schedule",
			createPath: "/databrew/v1/schedules",
			createBody: map[string]any{"Name": "leak-sc", "CronExpression": "cron(0 12 * * ? *)"},
			describe:   "/databrew/v1/schedules/leak-sc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.name == "job" {
				databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
					"Name": "leak-ds", "Format": "CSV",
					"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
				})
			}
			createRec := databrewReq(t, h, http.MethodPost, tt.createPath, tt.createBody)
			require.Equal(t, http.StatusOK, createRec.Code)

			rec := databrewReq(t, h, http.MethodGet, tt.describe, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, hasAccountID := resp["AccountId"]
			assert.False(t, hasAccountID, "Describe leaked AccountId; the real Describe*Output has no AccountId member")
		})
	}
}

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
