package databrew_test

import (
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
	ds, err := b.CreateDataset("catalog-ds", "PARQUET", input, databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	assert.Equal(t, "DATA_CATALOG", ds.Source)
}

func TestCreateDataset_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset("", "CSV", s3Input("b", "k"), databrew.DatasetFormatOptions{}, nil)
	require.Error(t, err)
}

func TestCreateDataset_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset("dup", "CSV", s3Input("b", "k"), databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	_, err = b.CreateDataset("dup", "CSV", s3Input("b", "k"), databrew.DatasetFormatOptions{}, nil)
	require.Error(t, err)
}

func TestDescribeDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		"ds1",
		"JSON",
		s3Input("bkt", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	ds, err := b.DescribeDataset("ds1")
	require.NoError(t, err)
	assert.Equal(t, "ds1", ds.Name)
	assert.Equal(t, "test", ds.Tags["env"])
}

func TestDescribeDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeDataset("no-such")
	require.Error(t, err)
}

func TestListDatasets(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset("a", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	_, err = b.CreateDataset("b", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	list, _ := b.ListDatasets(100, "")
	assert.Len(t, list, 2)
}

func TestUpdateDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		"upd-ds",
		"CSV",
		s3Input("bkt", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	err = b.UpdateDataset("upd-ds", "JSON", s3Input("bkt2", "key"), databrew.DatasetFormatOptions{})
	require.NoError(t, err)
	ds, err := b.DescribeDataset("upd-ds")
	require.NoError(t, err)
	assert.Equal(t, "JSON", ds.Format)
}

func TestUpdateDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateDataset("no-such", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{})
	require.Error(t, err)
}

func TestDeleteDataset_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset(
		"del-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	err = b.DeleteDataset("del-ds")
	require.NoError(t, err)
	_, err = b.DescribeDataset("del-ds")
	require.Error(t, err)
}

func TestDeleteDataset_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteDataset("no-such")
	require.Error(t, err)
}

// ---- Recipe ----

func TestCreateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "TRIM"}}}
	r, err := b.CreateRecipe("my-recipe", "trim recipe", steps, map[string]string{"team": "data"})
	require.NoError(t, err)
	assert.Equal(t, "my-recipe", r.Name)
	assert.Equal(t, "0.1", r.RecipeVersion)
	assert.Len(t, r.Steps, 1)
	assert.Equal(t, "data", r.Tags["team"])
}

func TestCreateRecipe_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("", "desc", nil, nil)
	require.Error(t, err)
}

func TestCreateRecipe_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe("r", "", nil, nil)
	require.Error(t, err)
}

func TestDescribeRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("r1", "desc", nil, nil)
	require.NoError(t, err)
	r, err := b.DescribeRecipe("r1")
	require.NoError(t, err)
	assert.Equal(t, "r1", r.Name)
}

func TestDescribeRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeRecipe("nope")
	require.Error(t, err)
}

func TestListRecipes(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("r1", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe("r2", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListRecipes(100, "")
	assert.Len(t, list, 2)
}

func TestPublishRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("pub-r", "initial", nil, nil)
	require.NoError(t, err)
	err = b.PublishRecipe("pub-r", "published desc")
	require.NoError(t, err)
	r, err := b.DescribeRecipe("pub-r")
	require.NoError(t, err)
	assert.Equal(t, "1.0", r.RecipeVersion)
	assert.Equal(t, "published desc", r.Description)
}

func TestPublishRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.PublishRecipe("no-such", "")
	require.Error(t, err)
}

func TestUpdateRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("upd-r", "old desc", nil, nil)
	require.NoError(t, err)
	steps := []databrew.RecipeStep{{Action: map[string]any{"Operation": "UPPER_CASE"}}}
	err = b.UpdateRecipe("upd-r", "new desc", steps)
	require.NoError(t, err)
	r, err := b.DescribeRecipe("upd-r")
	require.NoError(t, err)
	assert.Equal(t, "new desc", r.Description)
	assert.Len(t, r.Steps, 1)
}

func TestUpdateRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateRecipe("no-such", "", nil)
	require.Error(t, err)
}

func TestDeleteRecipe_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateRecipe("del-r", "", nil, nil)
	require.NoError(t, err)
	err = b.DeleteRecipe("del-r")
	require.NoError(t, err)
	_, err = b.DescribeRecipe("del-r")
	require.Error(t, err)
}

func TestDeleteRecipe_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteRecipe("no-such")
	require.Error(t, err)
}

// ---- Project ----

func TestCreateProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject("my-project", "ds1", "r1", "arn:aws:iam::123456789012:role/Role",
		databrew.Sample{Type: "FIRST_N", Size: 500}, map[string]string{"k": "v"})
	require.NoError(t, err)
	assert.Equal(t, "my-project", p.Name)
	assert.Equal(t, "READY", p.SessionStatus)
	assert.Equal(t, "v", p.Tags["k"])
	assert.NotEmpty(t, p.Arn)
}

func TestCreateProject_EmptyName(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("", "ds", "r", "", databrew.Sample{}, nil)
	require.Error(t, err)
}

func TestCreateProject_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("p", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateProject("p", "", "r", "", databrew.Sample{}, nil)
	require.Error(t, err)
}

func TestDescribeProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("proj1", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	p, err := b.DescribeProject("proj1")
	require.NoError(t, err)
	assert.Equal(t, "proj1", p.Name)
}

func TestDescribeProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeProject("no-such")
	require.Error(t, err)
}

func TestListProjects(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("p1", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateProject("p2", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	list, _ := b.ListProjects(100, "")
	assert.Len(t, list, 2)
}

func TestUpdateProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("upd-p", "old-ds", "r", "old-role", databrew.Sample{}, nil)
	require.NoError(t, err)
	err = b.UpdateProject("upd-p", "new-ds", "new-role", databrew.Sample{Type: "RANDOM", Size: 100})
	require.NoError(t, err)
	p, err := b.DescribeProject("upd-p")
	require.NoError(t, err)
	assert.Equal(t, "new-ds", p.DatasetName)
	assert.Equal(t, "new-role", p.RoleArn)
}

func TestUpdateProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateProject("no-such", "", "", databrew.Sample{})
	require.Error(t, err)
}

func TestDeleteProject_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateProject("del-p", "", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	err = b.DeleteProject("del-p")
	require.NoError(t, err)
	_, err = b.DescribeProject("del-p")
	require.Error(t, err)
}

func TestDeleteProject_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteProject("no-such")
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
	_, err := b.CreateJob("", "PROFILE", "ds", "", "", "", nil, nil)
	require.Error(t, err)
}

func TestCreateJob_Duplicate(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob("j", "PROFILE", "ds", "", "", "", nil, nil)
	require.Error(t, err)
}

func TestDescribeJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("j1", "PROFILE", "ds", "", "", "", nil, map[string]string{"x": "y"})
	require.NoError(t, err)
	j, err := b.DescribeJob("j1")
	require.NoError(t, err)
	assert.Equal(t, "j1", j.Name)
	assert.Equal(t, "y", j.Tags["x"])
}

func TestDescribeJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.DescribeJob("no-such")
	require.Error(t, err)
}

func TestListJobs(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("j1", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateJob("j2", "RECIPE", "ds", "", "r", "", nil, nil)
	require.NoError(t, err)
	list, _ := b.ListJobs(100, "")
	assert.Len(t, list, 2)
}

func TestUpdateJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("upd-j", "PROFILE", "ds", "", "", "old-role", nil, nil)
	require.NoError(t, err)
	outputs := []databrew.Output{{Location: databrew.S3Location{Bucket: "b"}}}
	err = b.UpdateJob("upd-j", "new-role", outputs, 5, 2, 60)
	require.NoError(t, err)
	j, err := b.DescribeJob("upd-j")
	require.NoError(t, err)
	assert.Equal(t, "new-role", j.RoleArn)
	assert.Equal(t, 5, j.MaxCapacity)
	assert.Equal(t, 2, j.MaxRetries)
	assert.Equal(t, 60, j.Timeout)
}

func TestUpdateJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateJob("no-such", "", nil, 0, 0, 0)
	require.Error(t, err)
}

func TestDeleteJob_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("del-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	err = b.DeleteJob("del-j")
	require.NoError(t, err)
	_, err = b.DescribeJob("del-j")
	require.Error(t, err)
}

func TestDeleteJob_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.DeleteJob("no-such")
	require.Error(t, err)
}

// ---- Job Runs ----

func TestStartJobRun_Success(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("run-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	run, err := b.StartJobRun("run-j")
	require.NoError(t, err)
	assert.Equal(t, "run-j", run.JobName)
	assert.Equal(t, "STARTING", run.State)
	assert.NotEmpty(t, run.RunID)
}

func TestStartJobRun_TransitionsToSucceeded(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("run-j2", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun("run-j2")
	require.NoError(t, err)

	// Poll for async state transition instead of fixed sleep.
	require.Eventually(t, func() bool {
		runs, _, listErr := b.ListJobRuns("run-j2", 100, "")

		return listErr == nil && len(runs) == 1 && runs[0].State == "SUCCEEDED"
	}, 3*time.Second, 25*time.Millisecond)
}

func TestStartJobRun_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.StartJobRun("no-such")
	require.Error(t, err)
}

func TestListJobRuns_Empty(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("empty-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns("empty-j", 100, "")
	require.NoError(t, err)
	assert.Empty(t, runs)
}

func TestListJobRuns_JobNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, _, err := b.ListJobRuns("no-such", 100, "")
	require.Error(t, err)
}

func TestListJobRuns_MultipleRuns(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateJob("multi-j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)
	_, err = b.StartJobRun("multi-j")
	require.NoError(t, err)
	_, err = b.StartJobRun("multi-j")
	require.NoError(t, err)
	runs, _, err := b.ListJobRuns("multi-j", 100, "")
	require.NoError(t, err)
	assert.Len(t, runs, 2)
}

// ---- Reset ----

func TestReset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.CreateDataset("ds", "CSV", s3Input("b", ""), databrew.DatasetFormatOptions{}, nil)
	require.NoError(t, err)
	_, err = b.CreateRecipe("r", "", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateProject("p", "ds", "r", "", databrew.Sample{}, nil)
	require.NoError(t, err)
	_, err = b.CreateJob("j", "PROFILE", "ds", "", "", "", nil, nil)
	require.NoError(t, err)

	b.Reset()

	dsList, _ := b.ListDatasets(100, "")
	assert.Empty(t, dsList)
	rList, _ := b.ListRecipes(100, "")
	assert.Empty(t, rList)
	pList, _ := b.ListProjects(100, "")
	assert.Empty(t, pList)
	jList, _ := b.ListJobs(100, "")
	assert.Empty(t, jList)
}
