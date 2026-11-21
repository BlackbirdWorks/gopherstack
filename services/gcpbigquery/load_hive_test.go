package gcpbigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bqv2 "google.golang.org/api/bigquery/v2"
)

func TestLoadHiveRejectsInvalidPartitionColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		object string
		mode   string
		prefix string
	}{
		{name: "auto invalid name", object: "b/h/bad key=1/f.csv", mode: "AUTO", prefix: "gs://b/h/"},
		{name: "auto leading digit", object: "b/h/1k=1/f.csv", mode: "AUTO", prefix: "gs://b/h/"},
		{name: "custom unknown type", object: "b/h/1/f.csv", mode: "CUSTOM", prefix: "gs://b/h/{dt:FOO}"},
		{name: "custom invalid name", object: "b/h/1/f.csv", mode: "CUSTOM", prefix: "gs://b/h/{a-b:STRING}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "hv")
			env.objects.data[tt.object] = []byte("1\n")

			job, err := env.policyService(t).Jobs.Insert(sdkProject, &bqv2.Job{Configuration: &bqv2.JobConfiguration{
				Load: &bqv2.JobConfigurationLoad{
					SourceUris:   []string{"gs://b/h/*"},
					SourceFormat: "CSV",
					Schema: &bqv2.TableSchema{
						Fields: []*bqv2.TableFieldSchema{{Name: "id", Type: "INTEGER"}},
					},
					DestinationTable: &bqv2.TableReference{ProjectId: sdkProject, DatasetId: "hv", TableId: "t"},
					HivePartitioningOptions: &bqv2.HivePartitioningOptions{
						Mode: tt.mode, SourceUriPrefix: tt.prefix,
					},
				},
			}}).Context(t.Context()).Do()
			require.NoError(t, err)

			done, err := env.client.JobFromID(t.Context(), job.JobReference.JobId)
			require.NoError(t, err)

			status, err := done.Wait(t.Context())
			require.NoError(t, err)
			require.Error(t, status.Err())

			_, err = ds.Table("t").Metadata(t.Context())
			assert.Error(t, err, "a failed load creates no table")
		})
	}
}
