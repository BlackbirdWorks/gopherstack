package databrew_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func newTestBackend() *databrew.InMemoryBackend {
	return databrew.NewInMemoryBackend("123456789012", "us-east-1")
}

func s3Input(bucket, key string) databrew.DatasetInput {
	return databrew.DatasetInput{
		S3InputDefinition: &databrew.S3Location{Bucket: bucket, Key: key},
	}
}

// ---- Dataset ----

func TestCreateDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ds, err := b.CreateDataset(
		context.Background(),
		"my-dataset",
		"CSV",
		s3Input("my-bucket", "data/"),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "my-dataset", ds.Name)
	assert.Equal(t, "CSV", ds.Format)
	assert.Equal(t, "S3", ds.Source)
	assert.NotEmpty(t, ds.Arn)
}

func TestCreateDataset_DataCatalogSource(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := databrew.DatasetInput{
		DataCatalogInputDefinition: &databrew.DataCatalogInput{
			DatabaseName: "db",
			TableName:    "tbl",
		},
	}
	ds, err := b.CreateDataset(
		context.Background(),
		"catalog-ds",
		"PARQUET",
		input,
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "DATA_CATALOG", ds.Source)
}

func TestCreateDataset_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"",
		"CSV",
		s3Input("b", "k"),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.Error(t, err)
}

func TestCreateDataset_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"dup",
		"CSV",
		s3Input("b", "k"),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateDataset(
		context.Background(),
		"dup",
		"CSV",
		s3Input("b", "k"),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.Error(t, err)
}

func TestDescribeDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"ds1",
		"JSON",
		s3Input("bkt", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	ds, err := b.DescribeDataset(context.Background(), "ds1")
	require.NoError(t, err)
	assert.Equal(t, "ds1", ds.Name)
	assert.Equal(t, "test", ds.Tags["env"])
}

func TestDescribeDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeDataset(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListDatasets(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"a",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateDataset(
		context.Background(),
		"b",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	list, _ := b.ListDatasets(context.Background(), 100, "")
	assert.Len(t, list, 2)
}

func TestUpdateDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"upd-ds",
		"CSV",
		s3Input("bkt", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	err = b.UpdateDataset(
		context.Background(),
		"upd-ds",
		"JSON",
		s3Input("bkt2", "key"),
		databrew.DatasetFormatOptions{},
	)
	require.NoError(t, err)
	ds, err := b.DescribeDataset(context.Background(), "upd-ds")
	require.NoError(t, err)
	assert.Equal(t, "JSON", ds.Format)
}

func TestUpdateDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateDataset(
		context.Background(),
		"no-such",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
	)
	require.Error(t, err)
}

func TestDeleteDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		context.Background(),
		"del-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	err = b.DeleteDataset(context.Background(), "del-ds")
	require.NoError(t, err)
	_, err = b.DescribeDataset(context.Background(), "del-ds")
	require.Error(t, err)
}

func TestDeleteDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteDataset(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Recipe ----

func TestCreateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "TRIM"}}}
	r, err := b.CreateRecipe(
		context.Background(),
		"my-recipe",
		"trim recipe",
		steps,
		map[string]string{"team": "data"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-recipe", r.Name)
	assert.Equal(t, "0.1", r.RecipeVersion)
	assert.Len(t, r.Steps, 1)
	assert.Equal(t, "data", r.Tags["team"])
}

func TestCreateRecipe_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "", "desc", nil, nil)
	require.Error(t, err)
}

func TestCreateRecipe_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.Error(t, err)
}

func TestDescribeRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r1", "desc", nil, nil)
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "r1")
	require.NoError(t, err)
	assert.Equal(t, "r1", r.Name)
}

func TestDescribeRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRecipe(context.Background(), "nope")
	require.Error(t, err)
}

func TestListRecipes(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "r1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r2", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRecipes(context.Background(), 100, "")
	assert.Len(t, list, 2)
}

func TestPublishRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "pub-r", "initial", nil, nil)
	require.NoError(t, err)
	err = b.PublishRecipe(context.Background(), "pub-r", "published desc")
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "pub-r")
	require.NoError(t, err)
	assert.Equal(t, "1.0", r.RecipeVersion)
	assert.Equal(t, "published desc", r.Description)
}

func TestPublishRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.PublishRecipe(context.Background(), "no-such", "")
	require.Error(t, err)
}

func TestUpdateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "upd-r", "old desc", nil, nil)
	require.NoError(t, err)
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "UPPER_CASE"}}}
	err = b.UpdateRecipe(context.Background(), "upd-r", "new desc", steps)
	require.NoError(t, err)
	r, err := b.DescribeRecipe(context.Background(), "upd-r")
	require.NoError(t, err)
	assert.Equal(t, "new desc", r.Description)
	assert.Len(t, r.Steps, 1)
}

func TestUpdateRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateRecipe(context.Background(), "no-such", "", nil)
	require.Error(t, err)
}

func TestDeleteRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe(context.Background(), "del-r", "", nil, nil)
	require.NoError(t, err)
	err = b.DeleteRecipe(context.Background(), "del-r")
	require.NoError(t, err)
	_, err = b.DescribeRecipe(context.Background(), "del-r")
	require.Error(t, err)
}

func TestDeleteRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteRecipe(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Project ----

func TestCreateProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject(
		context.Background(),
		"my-project",
		"ds1",
		"r1",
		"arn:aws:iam::123456789012:role/Role",
		databrew.Sample{Type: "FIRST_N", Size: 500},
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)
	assert.Equal(t, "my-project", p.Name)
	assert.Equal(t, "READY", p.SessionStatus)
	assert.Equal(t, "v", p.Tags["k"])
	assert.NotEmpty(t, p.Arn)
}

func TestCreateProject_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(context.Background(), "", "ds", "r", "", databrew.Sample{}, nil)
	require.Error(t, err)
}

func TestCreateProject_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(context.Background(), "p", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateProject(context.Background(), "p", "", "r", "", databrew.Sample{}, nil)
	require.Error(t, err)
}

func TestDescribeProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(context.Background(), "proj1", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	p, err := b.DescribeProject(context.Background(), "proj1")
	require.NoError(t, err)
	assert.Equal(t, "proj1", p.Name)
}

func TestDescribeProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeProject(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListProjects(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(context.Background(), "p1", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateProject(context.Background(), "p2", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	list, _ := b.ListProjects(context.Background(), 100, "")
	assert.Len(t, list, 2)
}

func TestUpdateProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(
		context.Background(),
		"upd-p",
		"old-ds",
		"r",
		"old-role",
		databrew.Sample{},
		nil,
	)
	require.NoError(t, err)
	err = b.UpdateProject(
		context.Background(),
		"upd-p",
		"new-ds",
		"new-role",
		databrew.Sample{Type: "RANDOM", Size: 100},
	)
	require.NoError(t, err)
	p, err := b.DescribeProject(context.Background(), "upd-p")
	require.NoError(t, err)
	assert.Equal(t, "new-ds", p.DatasetName)
	assert.Equal(t, "new-role", p.RoleArn)
}

func TestUpdateProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateProject(context.Background(), "no-such", "", "", databrew.Sample{})
	require.Error(t, err)
}

func TestDeleteProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject(context.Background(), "del-p", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	err = b.DeleteProject(context.Background(), "del-p")
	require.NoError(t, err)
	_, err = b.DescribeProject(context.Background(), "del-p")
	require.Error(t, err)
}

func TestDeleteProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteProject(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Job ----

func TestCreateJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	outputs := []databrew.Output{
		{Location: databrew.S3Location{Bucket: "out-bkt", Key: "out/"}, Format: "CSV"},
	}
	j, err := b.CreateJob(
		context.Background(),
		"my-job",
		"RECIPE",
		"ds1",
		"",
		"r1",
		"arn:aws:iam::123456789012:role/Role",
		outputs,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "my-job", j.Name)
	assert.Equal(t, "RECIPE", j.Type)
	assert.NotEmpty(t, j.Arn)
	assert.Len(t, j.Outputs, 1)
}

func TestCreateJob_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "", "PROFILE", "ds", "", "", "", nil, nil)
	require.Error(t, err)
}

func TestCreateJob_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(context.Background(), "j", "PROFILE", "ds", "", "", "", nil, nil)
	require.Error(t, err)
}

func TestDescribeJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(
		context.Background(),
		"j1",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		map[string]string{"x": "y"},
	)
	require.NoError(t, err)
	j, err := b.DescribeJob(context.Background(), "j1")
	require.NoError(t, err)
	assert.Equal(t, "j1", j.Name)
	assert.Equal(t, "y", j.Tags["x"])
}

func TestDescribeJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeJob(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListJobs(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "j1", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(context.Background(), "j2", "RECIPE", "ds", "", "r", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListJobs(context.Background(), 100, "")
	assert.Len(t, list, 2)
}

func TestUpdateJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(
		context.Background(),
		"upd-j",
		"PROFILE",
		"ds",
		"",
		"",
		"old-role",
		nil,
		nil,
	)
	require.NoError(t, err)
	outputs := []databrew.Output{{Location: databrew.S3Location{Bucket: "b"}}}
	err = b.UpdateJob(context.Background(), "upd-j", "new-role", outputs, 5, 2, 60)
	require.NoError(t, err)
	j, err := b.DescribeJob(context.Background(), "upd-j")
	require.NoError(t, err)
	assert.Equal(t, "new-role", j.RoleArn)
	assert.Equal(t, 5, j.MaxCapacity)
	assert.Equal(t, 2, j.MaxRetries)
	assert.Equal(t, 60, j.Timeout)
}

func TestUpdateJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateJob(context.Background(), "no-such", "", nil, 0, 0, 0)
	require.Error(t, err)
}

func TestDeleteJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "del-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	err = b.DeleteJob(context.Background(), "del-j")
	require.NoError(t, err)
	_, err = b.DescribeJob(context.Background(), "del-j")
	require.Error(t, err)
}

func TestDeleteJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteJob(context.Background(), "no-such")
	require.Error(t, err)
}

// ---- Job Runs ----

func TestStartJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "run-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	run, err := b.StartJobRun(context.Background(), "run-j")
	require.NoError(t, err)
	assert.Equal(t, "run-j", run.JobName)
	assert.Equal(t, "STARTING", run.State)
	assert.NotEmpty(t, run.RunID)
}

func TestStartJobRun_TransitionsToSucceeded(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "run-j2", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "run-j2")
	require.NoError(t, err)

	// Poll for async state transition instead of fixed sleep.
	require.Eventually(t, func() bool {
		runs, _, listErr := b.ListJobRuns(context.Background(), "run-j2", 100, "")

		return listErr == nil && len(runs) == 1 && runs[0].State == "SUCCEEDED"
	}, 3*time.Second, 25*time.Millisecond)
}

func TestStartJobRun_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.StartJobRun(context.Background(), "no-such")
	require.Error(t, err)
}

func TestListJobRuns_Empty(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "empty-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns(context.Background(), "empty-j", 100, "")
	require.NoError(t, err)
	assert.Empty(t, runs)
}

func TestListJobRuns_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, _, err := b.ListJobRuns(context.Background(), "no-such", 100, "")
	require.Error(t, err)
}

func TestListJobRuns_MultipleRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob(context.Background(), "multi-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "multi-j")
	require.NoError(t, err)
	_, err = b.StartJobRun(context.Background(), "multi-j")
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns(context.Background(), "multi-j", 100, "")
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}

// ---- Reset ----

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
	)
	require.NoError(t, err)
	_, err = b.CreateRecipe(context.Background(), "r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateProject(context.Background(), "p", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateJob(context.Background(), "j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)

	b.Reset()

	dsList, _ := b.ListDatasets(context.Background(), 100, "")
	assert.Empty(t, dsList)
	rList, _ := b.ListRecipes(context.Background(), 100, "")
	assert.Empty(t, rList)
	pList, _ := b.ListProjects(context.Background(), 100, "")
	assert.Empty(t, pList)
	jList, _ := b.ListJobs(context.Background(), 100, "")
	assert.Empty(t, jList)
}
