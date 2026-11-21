package appsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errProbe = errors.New("schema unavailable")

type schemaDDB struct {
	DynamoDBBackend

	err    error
	put    map[string]any
	schema []string
}

func (d *schemaDDB) KeyAttributeNames(context.Context, string) ([]string, error) {
	return d.schema, d.err
}

func (d *schemaDDB) PutItemRaw(_ context.Context, _ string, item map[string]any, _ *DDBExpression) error {
	d.put = item

	return nil
}

func TestDeltaSortKeyOrdersCompositeKey(t *testing.T) {
	t.Parallel()

	key := map[string]any{"a": strAttr("range"), "z": strAttr("hash")}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		err    error
		name   string
		want   string
		schema []string
	}{
		{name: "hash first", schema: []string{"z", "a"}, want: "03:04:05:hash#range:7"},
		{name: "range listed first", schema: []string{"a", "z"}, want: "03:04:05:range#hash:7"},
		{name: "no schema falls back to name order", err: errProbe, want: "03:04:05:range#hash:7"},
		{name: "schema mismatch falls back", schema: []string{"z"}, want: "03:04:05:range#hash:7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ddb := &schemaDDB{schema: tt.schema, err: tt.err}
			vs := versionedSource{ddb: ddb, dsName: "ds", cfg: &DynamoDBDataSourceConfig{
				TableName: "base", DeltaSyncConfig: &DeltaSyncConfig{DeltaSyncTableName: "delta"},
			}}

			item := map[string]any{attrVersion: numAttr(7)}
			require.NoError(t, vs.logDelta(t.Context(), key, item, at, deltaLayout{}))
			assert.Equal(t, strAttr(tt.want), ddb.put[attrDeltaSK])
		})
	}
}
