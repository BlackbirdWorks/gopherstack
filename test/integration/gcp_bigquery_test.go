package integration_test

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type bqPerson struct {
	Name string
	Age  int64
}

func newBigQueryClient(t *testing.T) *bigquery.Client {
	t.Helper()

	client, err := bigquery.NewClient(t.Context(), gcpProject,
		option.WithEndpoint("http://"+gcpAddr(t, "9050")), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestIntegration_GCP_BigQuery_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newBigQueryClient(t)
	ctx := t.Context()
	dsID := "it_" + gcpName("ds")[3:]
	ds := client.Dataset(dsID)

	require.NoError(t, ds.Create(ctx, &bigquery.DatasetMetadata{Location: "US", Description: "d1"}))

	md, err := ds.Update(ctx, bigquery.DatasetMetadataToUpdate{Description: "d2"}, "")
	require.NoError(t, err)
	assert.Equal(t, "d2", md.Description)

	schema, err := bigquery.InferSchema(bqPerson{})
	require.NoError(t, err)

	tbl := ds.Table("people")
	require.NoError(t, tbl.Create(ctx, &bigquery.TableMetadata{Schema: schema}))
	require.NoError(t, tbl.Inserter().Put(ctx, []bqPerson{{"ann", 30}, {"bob", 41}}))

	q := client.Query("SELECT name FROM `" + gcpProject + "." + dsID + ".people` WHERE age > 35")
	rit, err := q.Read(ctx)
	require.NoError(t, err)

	var rows [][]bigquery.Value

	for {
		var row []bigquery.Value

		nerr := rit.Next(&row)
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		rows = append(rows, row)
	}

	assert.Equal(t, [][]bigquery.Value{{"bob"}}, rows)

	tables := 0
	tit := ds.Tables(ctx)

	for {
		if _, nerr := tit.Next(); nerr != nil {
			break
		}

		tables++
	}

	assert.Equal(t, 1, tables)

	require.NoError(t, tbl.Delete(ctx))
	require.NoError(t, ds.Delete(ctx))
}

func TestIntegration_GCP_BigQuery_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newBigQueryClient(t)

	_, err := client.Dataset("it_absent_" + gcpName("x")[3:]).Metadata(t.Context())

	var gerr *googleapi.Error

	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)
}
