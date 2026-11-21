package gcpbigquery_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAppendSchemaMerge(t *testing.T) {
	t.Parallel()

	rec := func(fields ...*bigquery.FieldSchema) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: "rec", Type: bigquery.RecordFieldType, Schema: fields}
	}
	str := func(name string) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: bigquery.StringFieldType}
	}
	num := func(name string) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: bigquery.IntegerFieldType}
	}

	tests := []struct {
		name     string
		data     string
		wantErr  string
		opts     []string
		existing bigquery.Schema
		loaded   bigquery.Schema
		wantSub  int
	}{
		{
			name:     "nullable to required",
			data:     `{"s":"x"}`,
			existing: bigquery.Schema{str("s")},
			loaded:   bigquery.Schema{{Name: "s", Type: bigquery.StringFieldType, Required: true}},
			opts:     []string{"ALLOW_FIELD_RELAXATION"},
			wantErr:  "Field s has changed mode from NULLABLE to REQUIRED",
		},
		{
			name:     "nullable to repeated",
			data:     `{"s":["x"]}`,
			existing: bigquery.Schema{str("s")},
			loaded:   bigquery.Schema{{Name: "s", Type: bigquery.StringFieldType, Repeated: true}},
			opts:     []string{"ALLOW_FIELD_RELAXATION"},
			wantErr:  "Field s has changed mode from NULLABLE to REPEATED",
		},
		{
			name:     "nested addition denied",
			data:     `{"rec":{"a":"x","b":"y"}}`,
			existing: bigquery.Schema{rec(str("a"))},
			loaded:   bigquery.Schema{rec(str("a"), str("b"))},
			wantErr:  "Cannot add fields (field: rec.b)",
			wantSub:  1,
		},
		{
			name:     "nested type change",
			data:     `{"rec":{"a":1}}`,
			existing: bigquery.Schema{rec(str("a"))},
			loaded:   bigquery.Schema{rec(num("a"))},
			opts:     []string{"ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"},
			wantErr:  "Field rec.a has changed type from STRING to INTEGER",
			wantSub:  1,
		},
		{
			name:     "nested addition",
			data:     `{"rec":{"a":"x","b":"y"}}`,
			existing: bigquery.Schema{rec(str("a"))},
			loaded:   bigquery.Schema{rec(str("a"), str("b"))},
			opts:     []string{"ALLOW_FIELD_ADDITION"},
			wantSub:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDK(t)
			ds := env.dataset(t, "sm")
			require.NoError(t, ds.Table("t").Create(t.Context(), &bigquery.TableMetadata{Schema: tt.existing}))

			env.objects.data["b/in.json"] = []byte(tt.data + "\n")

			ref := bigquery.NewGCSReference("gs://b/in.json")
			ref.SourceFormat = bigquery.JSON
			ref.Schema = tt.loaded
			loader := ds.Table("t").LoaderFrom(ref)
			loader.SchemaUpdateOptions = tt.opts

			status := runLoad(t, loader)
			md, err := ds.Table("t").Metadata(t.Context())
			require.NoError(t, err)

			if tt.wantErr != "" {
				require.Error(t, status.Err())
				assert.Contains(t, status.Err().Error(), tt.wantErr)
				assert.Len(t, md.Schema, len(tt.existing), "a failed load leaves the schema unchanged")
			} else {
				require.NoError(t, status.Err())
			}

			if tt.wantSub > 0 {
				require.Len(t, md.Schema, 1)
				assert.Len(t, md.Schema[0].Schema, tt.wantSub)
			}
		})
	}
}
