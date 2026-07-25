package timestreamquery //nolint:testpackage // needs unexported marshalColumnInfos for the wire-shape check.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarshalColumnInfos_PreservesNestedUnion verifies that marshalColumnInfos
// (used by both the Query and PrepareQuery responses) round-trips the full
// ColumnType nested union (types.Type on the wire: ScalarType |
// ArrayColumnInfo | RowColumnInfo | TimeSeriesMeasureValueColumnInfo), not
// just ScalarType. An earlier version hand-picked only ScalarType out of
// ColumnInfo.Type, silently dropping the other three union members whenever
// they were set -- a wire-shape bug for any complex (array/row/timeseries)
// column type.
func TestMarshalColumnInfos_PreservesNestedUnion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkType func(t *testing.T, typ map[string]any)
		name      string
		col       ColumnInfo
	}{
		{
			name: "scalar column",
			col:  scalarColumnInfo("measure_value", ScalarTypeDouble),
			checkType: func(t *testing.T, typ map[string]any) {
				t.Helper()
				assert.Equal(t, ScalarTypeDouble, typ["ScalarType"])
				assert.NotContains(t, typ, "ArrayColumnInfo")
			},
		},
		{
			name: "array column",
			col: ColumnInfo{
				Name: "tags",
				Type: ColumnType{ArrayColumnInfo: &ColumnInfo{
					Name: "tag",
					Type: ColumnType{ScalarType: ScalarTypeVarchar},
				}},
			},
			checkType: func(t *testing.T, typ map[string]any) {
				t.Helper()
				arrayInfo, ok := typ["ArrayColumnInfo"].(map[string]any)
				require.True(t, ok, "ArrayColumnInfo must survive marshalling")
				assert.Equal(t, "tag", arrayInfo["Name"])
				innerType := arrayInfo["Type"].(map[string]any)
				assert.Equal(t, ScalarTypeVarchar, innerType["ScalarType"])
			},
		},
		{
			name: "row column",
			col: ColumnInfo{
				Name: "point",
				Type: ColumnType{RowColumnInfo: []ColumnInfo{
					scalarColumnInfo("x", ScalarTypeDouble),
					scalarColumnInfo("y", ScalarTypeDouble),
				}},
			},
			checkType: func(t *testing.T, typ map[string]any) {
				t.Helper()
				rowInfo, ok := typ["RowColumnInfo"].([]any)
				require.True(t, ok, "RowColumnInfo must survive marshalling")
				assert.Len(t, rowInfo, 2)
			},
		},
		{
			name: "timeseries column",
			col: ColumnInfo{
				Name: "series",
				Type: ColumnType{
					TimeSeriesMeasureValueColumnInfo: &ColumnInfo{
						Name: "v",
						Type: ColumnType{ScalarType: ScalarTypeBigint},
					},
				},
			},
			checkType: func(t *testing.T, typ map[string]any) {
				t.Helper()
				tsInfo, ok := typ["TimeSeriesMeasureValueColumnInfo"].(map[string]any)
				require.True(t, ok, "TimeSeriesMeasureValueColumnInfo must survive marshalling")
				assert.Equal(t, "v", tsInfo["Name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			marshalled := marshalColumnInfos([]ColumnInfo{tt.col})
			require.Len(t, marshalled, 1)

			// Round-trip through JSON, exactly as the handler's response does.
			raw, err := json.Marshal(marshalled)
			require.NoError(t, err)

			var decoded []map[string]any

			require.NoError(t, json.Unmarshal(raw, &decoded))
			require.Len(t, decoded, 1)

			typ, ok := decoded[0]["Type"].(map[string]any)
			require.True(t, ok, "Type must be present")
			tt.checkType(t, typ)
		})
	}
}

// TestMarshalColumnInfos_OmitsEmptyName verifies Name is omitted (not an
// empty string) when a ColumnInfo has no name -- matching real AWS's
// *string-typed, optional Name field (unset for array element columns).
func TestMarshalColumnInfos_OmitsEmptyName(t *testing.T) {
	t.Parallel()

	marshalled := marshalColumnInfos([]ColumnInfo{{Type: ColumnType{ScalarType: ScalarTypeVarchar}}})
	require.Len(t, marshalled, 1)
	_, hasName := marshalled[0]["Name"]
	assert.False(t, hasName, "Name must be omitted when empty")
}
