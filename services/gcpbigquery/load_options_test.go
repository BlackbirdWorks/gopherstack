package gcpbigquery_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bqv2 "google.golang.org/api/bigquery/v2"
)

func runLoad(t *testing.T, loader *bigquery.Loader) *bigquery.JobStatus {
	t.Helper()

	status, _ := runLoadJob(t, loader)

	return status
}

func runLoadJob(t *testing.T, loader *bigquery.Loader) (*bigquery.JobStatus, string) {
	t.Helper()

	job, err := loader.Run(t.Context())
	require.NoError(t, err)

	status, err := job.Wait(t.Context())
	require.NoError(t, err)

	return status, job.ID()
}

func TestLoadCSVQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		quote   string
		data    string
		wantErr string
		want    [][]bigquery.Value
		zero    bool
	}{
		{
			"custom quote", "'", "1,'a,b'\n2,'it''s'\n", "",
			[][]bigquery.Value{{int64(1), "a,b"}, {int64(2), "it's"}}, false,
		},
		{"quoted newline", "'", "1,'a\nb'\n", "", [][]bigquery.Value{{int64(1), "a\nb"}}, false},
		{"no quote", "", "1,\"x\"\n", "", [][]bigquery.Value{{int64(1), "\"x\""}}, true},
		{"multi char", "''", "1,a\n", "quote must be a single character", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "q")
			env.objects.data["b/in.csv"] = []byte(tt.data)

			ref := bigquery.NewGCSReference("gs://b/in.csv")
			ref.Quote, ref.ForceZeroQuote = tt.quote, tt.zero
			ref.Schema = bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "s", Type: bigquery.StringFieldType},
			}

			status := runLoad(t, ds.Table("t").LoaderFrom(ref))
			if tt.wantErr != "" {
				require.Error(t, status.Err())
				assert.Contains(t, status.Err().Error(), tt.wantErr)

				return
			}

			require.NoError(t, status.Err())
			assert.Equal(t, tt.want, env.rows(t, "SELECT id, s FROM q.t ORDER BY id"))
		})
	}
}

func TestLoadSchemaUpdateOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		opts    []string
		schema  bigquery.Schema
		want    [][]bigquery.Value
	}{
		{
			name: "addition", opts: []string{"ALLOW_FIELD_ADDITION"},
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "extra", Type: bigquery.StringFieldType},
			},
			want: [][]bigquery.Value{{int64(1), nil}, {int64(2), "x"}},
		},
		{
			name: "addition denied",
			schema: bigquery.Schema{
				{Name: "id", Type: bigquery.IntegerFieldType}, {Name: "extra", Type: bigquery.StringFieldType},
			},
			wantErr: "Cannot add fields (field: extra)",
		},
		{
			name: "relaxation", opts: []string{"ALLOW_FIELD_RELAXATION"},
			schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			want:   [][]bigquery.Value{{int64(1)}, {int64(2)}},
		},
		{
			name:    "relaxation denied",
			schema:  bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
			wantErr: "Field id has changed mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "su")
			_, err := ds.Table("t").Metadata(t.Context())
			require.Error(t, err)
			require.NoError(t, ds.Table("t").Create(t.Context(), &bigquery.TableMetadata{
				Schema: bigquery.Schema{
					{
						Name:     "id",
						Type:     bigquery.IntegerFieldType,
						Required: tt.name == "relaxation denied" || tt.name == "relaxation",
					},
				},
			}))
			env.exec(t, "INSERT su.t (id) VALUES (1)")

			env.objects.data["b/in.csv"] = []byte("2,x\n")
			if len(tt.schema) == 1 {
				env.objects.data["b/in.csv"] = []byte("2\n")
			}

			ref := bigquery.NewGCSReference("gs://b/in.csv")
			ref.Schema = tt.schema
			loader := ds.Table("t").LoaderFrom(ref)
			loader.SchemaUpdateOptions = tt.opts

			status := runLoad(t, loader)
			if tt.wantErr != "" {
				require.Error(t, status.Err())
				assert.Contains(t, status.Err().Error(), tt.wantErr)

				md, merr := ds.Table("t").Metadata(t.Context())
				require.NoError(t, merr)
				assert.Len(t, md.Schema, 1)

				return
			}

			require.NoError(t, status.Err())
			assert.Equal(t, tt.want, env.rows(t, "SELECT * FROM su.t ORDER BY id"))

			md, merr := ds.Table("t").Metadata(t.Context())
			require.NoError(t, merr)
			assert.False(t, md.Schema[0].Required)
		})
	}
}

func TestLoadBadRecordsAndErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxBad   int64
		wantFail bool
		wantRows int
		wantErrs int
	}{
		{"within limit", 2, false, 1, 2},
		{"over limit", 1, true, 0, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "bad")
			env.objects.data["b/in.csv"] = []byte("1\nx\ny\n")

			ref := bigquery.NewGCSReference("gs://b/in.csv")
			ref.MaxBadRecords = tt.maxBad
			ref.Schema = bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}

			status, jobID := runLoadJob(t, ds.Table("t").LoaderFrom(ref))
			assert.Len(t, status.Errors, tt.wantErrs)

			if tt.wantFail {
				require.Error(t, status.Err())

				_, err := ds.Table("t").Metadata(t.Context())
				require.Error(t, err, "a failed load leaves no table behind")

				return
			}

			require.NoError(t, status.Err())
			assert.Len(t, env.rows(t, "SELECT id FROM bad.t"), tt.wantRows)

			raw, err := env.policyService(t).Jobs.Get(sdkProject, jobID).Context(t.Context()).Do()
			require.NoError(t, err)
			assert.EqualValues(t, 2, raw.Statistics.Load.BadRecords)
		})
	}
}

func TestLoadHivePartitioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode string
		want [][]bigquery.Value
	}{
		{"auto", "AUTO", [][]bigquery.Value{{int64(1), int64(2024), "us"}, {int64(2), int64(2025), "eu"}}},
		{"strings", "STRINGS", [][]bigquery.Value{{int64(1), "2024", "us"}, {int64(2), "2025", "eu"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			env.dataset(t, "hv")
			env.objects.data["b/h/year=2024/r=us/f.csv"] = []byte("1\n")
			env.objects.data["b/h/year=2025/r=eu/f.csv"] = []byte("2\n")

			job, err := env.policyService(t).Jobs.Insert(sdkProject, &bqv2.Job{Configuration: &bqv2.JobConfiguration{
				Load: &bqv2.JobConfigurationLoad{
					SourceUris:   []string{"gs://b/h/*"},
					SourceFormat: "CSV",
					Schema: &bqv2.TableSchema{
						Fields: []*bqv2.TableFieldSchema{{Name: "id", Type: "INTEGER"}},
					},
					DestinationTable: &bqv2.TableReference{ProjectId: sdkProject, DatasetId: "hv", TableId: "t"},
					HivePartitioningOptions: &bqv2.HivePartitioningOptions{
						Mode: tt.mode, SourceUriPrefix: "gs://b/h/",
					},
				},
			}}).Context(t.Context()).Do()
			require.NoError(t, err)

			done, err := env.client.JobFromID(t.Context(), job.JobReference.JobId)
			require.NoError(t, err)

			status, err := done.Wait(t.Context())
			require.NoError(t, err)
			require.NoError(t, status.Err())
			assert.Equal(t, tt.want, env.rows(t, "SELECT id, year, r FROM hv.t ORDER BY id"))
		})
	}
}

func TestLoadCaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr string
		files   int
		bytes   int64
	}{
		{"file count", "Too many source files", 1, 0},
		{"byte budget", "limit of this emulator", 0, 3},
		{"within", "", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "cap")
			env.objects.data["b/a.csv"] = []byte("1\n2\n")
			env.objects.data["b/b.csv"] = []byte("3\n4\n")
			env.backend.SetLoadLimits(tt.files, tt.bytes)

			ref := bigquery.NewGCSReference("gs://b/*.csv")
			ref.Schema = bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}

			status := runLoad(t, ds.Table("t").LoaderFrom(ref))
			if tt.wantErr != "" {
				require.Error(t, status.Err())
				assert.Contains(t, status.Err().Error(), tt.wantErr)

				return
			}

			require.NoError(t, status.Err())
		})
	}
}
