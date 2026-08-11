package databrew_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// ---- Tags backend ----

func TestFindTagsByArn_Dataset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ds, err := b.CreateDataset(
		context.Background(),
		"tagged-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"k": "v"},
		nil,
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), ds.Arn)
	require.NoError(t, err)
	assert.Equal(t, "v", tags["k"])
}

func TestFindTagsByArn_Recipe(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	r, err := b.CreateRecipe(
		context.Background(),
		"tagged-r",
		"",
		nil,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), r.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestFindTagsByArn_Project(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject(
		context.Background(),
		"tagged-p",
		"ds",
		"r",
		"",
		databrew.Sample{},
		map[string]string{"x": "y"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "y", tags["x"])
}

func TestFindTagsByArn_Job(t *testing.T) {
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
	j, err := b.CreateJob(
		context.Background(),
		"tagged-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		map[string]string{"a": "b"},
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), j.Arn)
	require.NoError(t, err)
	assert.Equal(t, "b", tags["a"])
}

func TestFindTagsByArn_Ruleset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rs, err := b.CreateRuleset(
		context.Background(),
		"tagged-rs",
		"",
		"arn:x",
		nil,
		map[string]string{"m": "n"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), rs.Arn)
	require.NoError(t, err)
	assert.Equal(t, "n", tags["m"])
}

func TestFindTagsByArn_Schedule(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sc, err := b.CreateSchedule(
		context.Background(),
		"tagged-sc",
		nil,
		"cron(...)",
		map[string]string{"p": "q"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), sc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "q", tags["p"])
}

func TestFindTagsByArn_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	_, err := b.FindTagsByArn(
		context.Background(),
		"arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent",
	)
	require.Error(t, err)
}

func TestUpdateTagsByArn_AddAndRemove_Dataset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	ds, err := b.CreateDataset(
		context.Background(),
		"tag-upd-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"old": "val"},
		nil,
	)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(
		context.Background(),
		ds.Arn,
		map[string]string{"new": "tag"},
		[]string{"old"},
	)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), ds.Arn)
	require.NoError(t, err)
	assert.Equal(t, "tag", tags["new"])
	assert.Empty(t, tags["old"])
}

func TestUpdateTagsByArn_Recipe(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	r, err := b.CreateRecipe(context.Background(), "tag-upd-r", "", nil, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(context.Background(), r.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), r.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Project(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	p, err := b.CreateProject(
		context.Background(),
		"tag-upd-p",
		"ds",
		"r",
		"",
		databrew.Sample{},
		nil,
	)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(context.Background(), p.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), p.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Job(t *testing.T) {
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
	j, err := b.CreateJob(
		context.Background(),
		"tag-upd-j",
		"PROFILE",
		"ds",
		"",
		"",
		"",
		nil,
		nil,
		databrew.JobExtras{},
	)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(context.Background(), j.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), j.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Ruleset(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	rs, err := b.CreateRuleset(context.Background(), "tag-upd-rs", "", "arn:x", nil, nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(context.Background(), rs.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), rs.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_Schedule(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	sc, err := b.CreateSchedule(context.Background(), "tag-upd-sc", nil, "cron(...)", nil)
	require.NoError(t, err)
	err = b.UpdateTagsByArn(context.Background(), sc.Arn, map[string]string{"key": "val"}, nil)
	require.NoError(t, err)
	tags, err := b.FindTagsByArn(context.Background(), sc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

func TestUpdateTagsByArn_NotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	err := b.UpdateTagsByArn(
		context.Background(),
		"arn:aws:databrew:us-east-1:123456789012:dataset/nonexistent",
		map[string]string{"k": "v"},
		nil,
	)
	require.Error(t, err)
}

// ---- Tags handler ----

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
	ds, err := b.CreateDataset(
		context.Background(),
		"tag-ds2",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		nil,
		nil,
	)
	require.NoError(t, err)

	h2 := databrew.NewHandler(b)
	rec := databrewReq(t, h2, http.MethodPost, "/databrew/v1/tags/"+ds.Arn, map[string]any{
		"Tags": map[string]string{"newkey": "newval"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	tags, err := b.FindTagsByArn(context.Background(), ds.Arn)
	require.NoError(t, err)
	assert.Equal(t, "newval", tags["newkey"])
}

func TestHandlerListTagsForResource(t *testing.T) {
	t.Parallel()
	b := databrew.NewInMemoryBackend("123456789012", "us-east-1")
	ds, err := b.CreateDataset(
		context.Background(),
		"list-tag-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"k": "v"},
		nil,
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
		context.Background(),
		"untag-ds",
		"CSV",
		s3Input("b", ""),
		databrew.DatasetFormatOptions{},
		map[string]string{"remove-me": "yes"},
		nil,
	)
	require.NoError(t, err)

	h := databrew.NewHandler(b)
	rec := databrewReq(
		t,
		h,
		http.MethodDelete,
		"/databrew/v1/tags/"+ds.Arn+"?tagKeys=remove-me",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// TagKeys travels as a repeated "tagKeys" query param on the real DELETE
	// /tags/{ResourceArn} wire request (there is normally no request body), so
	// this must actually remove the tag, not just return 200.
	tags, err := b.FindTagsByArn(context.Background(), ds.Arn)
	require.NoError(t, err)
	assert.NotContains(t, tags, "remove-me")
}

// TestTagsOps verifies tag CRUD on the /databrew/v1/tags/{arn} path.
func TestTagsOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "tagged-ds",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	resourceArn := "arn:aws:databrew:us-east-1:123456789012:dataset/tagged-ds"
	prefix := "/databrew/v1/tags/" + resourceArn

	tagRec := databrewReq(t, h, http.MethodPost, prefix,
		map[string]any{"Tags": map[string]any{"env": "prod"}})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := databrewReq(t, h, http.MethodGet, prefix, nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])

	untagRec := databrewReq(t, h, http.MethodDelete, prefix,
		map[string]any{"tagKeys": []string{"env"}})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}
