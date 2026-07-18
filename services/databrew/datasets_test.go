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

// ---- Dataset backend ----

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

func TestCreateDataset_DatabaseSource(t *testing.T) {
	t.Parallel()
	b := newTestBackend()
	input := databrew.DatasetInput{
		DatabaseInputDefinition: &databrew.DatabaseInput{
			GlueConnectionName: "conn",
			DatabaseTableName:  "table",
		},
	}
	ds, err := b.CreateDataset(
		context.Background(),
		"db-ds",
		"PARQUET",
		input,
		databrew.DatasetFormatOptions{},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "DATABASE", ds.Source)
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

// ---- Dataset handler ----

func TestHandlerCreateDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":   "my-ds",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "my-bucket"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-ds", resp["Name"])
}

func TestHandlerCreateDataset_Duplicate(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	body := map[string]any{
		"Name":   "dup-ds",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	}
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", body)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec2 := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandlerDescribeDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "ds1", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/ds1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var ds map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ds))
	assert.Equal(t, "ds1", ds["Name"])
}

func TestHandlerDescribeDataset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListDatasets(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "a", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Datasets"])
}

func TestHandlerUpdateDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "upd-ds", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/datasets/upd-ds", map[string]any{
		"Format": "JSON",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b2"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "del-ds", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/datasets/del-ds", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec2 := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/del-ds", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}
