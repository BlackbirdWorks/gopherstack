package app

import (
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpbigquerybackend "github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

type bigQueryFixture struct {
	client *bigquery.Client
	gcs    *gcpstoragebackend.InMemoryBackend
}

func newBigQueryFixture(t *testing.T) *bigQueryFixture {
	t.Helper()

	gcs := gcpstoragebackend.NewHandler(gcpstoragebackend.NewInMemoryBackend())
	bq := gcpbigquerybackend.NewHandler(gcpbigquerybackend.NewInMemoryBackend())

	wireGCPBigQuery(map[string]service.Registerable{"GCPStorage": gcs, "GCPBigQuery": bq})

	e := echo.New()
	e.Any("/*", bq.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	client, err := bigquery.NewClient(t.Context(), "proj", option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = gcs.Backend.CreateBucket("proj", gcpstoragebackend.Patch{"name": []byte(`"lake"`)})
	require.NoError(t, err)

	return &bigQueryFixture{client: client, gcs: gcs.Backend}
}

func TestBigQueryLoadFromAndExtractToGCS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		object   string
		content  string
		format   bigquery.DataFormat
		skip     int64
		wantRows int
	}{
		{"csv", "in/people.csv", "id,name\n1,ann\n2,bob\n", bigquery.CSV, 1, 2},
		{
			"ndjson", "in/people.json",
			`{"id":1,"name":"ann"}` + "\n" + `{"id":2,"name":"bob"}` + "\n" + `{"id":3,"name":"cy"}` + "\n",
			bigquery.JSON, 0, 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newBigQueryFixture(t)
			ctx := t.Context()

			_, err := fx.gcs.PutObject(
				"lake", tt.object, []byte(tt.content), gcpstoragebackend.Patch{}, gcpstoragebackend.Preconds{},
			)
			require.NoError(t, err)

			require.NoError(t, fx.client.Dataset("d").Create(ctx, nil))

			ref := bigquery.NewGCSReference("gs://lake/" + tt.object)
			ref.SourceFormat = tt.format
			ref.SkipLeadingRows = tt.skip
			ref.Schema = bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "name", Type: bigquery.StringFieldType},
			}

			tbl := fx.client.Dataset("d").Table("people")
			job, err := tbl.LoaderFrom(ref).Run(ctx)
			require.NoError(t, err)

			status, err := job.Wait(ctx)
			require.NoError(t, err)
			require.NoError(t, status.Err())

			md, err := tbl.Metadata(ctx)
			require.NoError(t, err)
			assert.Equal(t, uint64(tt.wantRows), md.NumRows)

			ext := bigquery.NewGCSReference("gs://lake/out/people.json")
			ext.DestinationFormat = bigquery.JSON
			job, err = tbl.ExtractorTo(ext).Run(ctx)
			require.NoError(t, err)

			status, err = job.Wait(ctx)
			require.NoError(t, err)
			require.NoError(t, status.Err())

			obj, err := fx.gcs.GetObject("lake", "out/people.json", 0, gcpstoragebackend.Preconds{})
			require.NoError(t, err)
			assert.Contains(t, string(obj.Data), `{"id":1,"name":"ann"}`)
		})
	}
}
