package pipes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func TestBuildTimestreamRecords(t *testing.T) {
	t.Parallel()

	dims := []pipes.TimestreamDimensionMapping{
		{DimensionName: "host", DimensionValue: "$.body.host", DimensionValueType: "VARCHAR"},
		{DimensionName: "env", DimensionValue: "prod", DimensionValueType: "VARCHAR"},
	}
	single := []pipes.TimestreamSingleMeasureMapping{
		{MeasureName: "cpu", MeasureValue: "$.body.cpu", MeasureValueType: "DOUBLE"},
	}

	tests := []struct {
		check   func(t *testing.T, recs []pipes.TimestreamRecord)
		events  []any
		name    string
		wantErr string
		tp      pipes.TimestreamParameters
	}{
		{
			name: "epoch_default_unit",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.body.ts", VersionValue: "3", DimensionMappings: dims, SingleMeasureMappings: single,
			},
			events: []any{map[string]any{"body": `{"ts":1700000000000,"host":"h1","cpu":1.5}`}},
			check: func(t *testing.T, recs []pipes.TimestreamRecord) {
				t.Helper()
				require.Len(t, recs, 1)
				assert.Equal(t, "1700000000000", recs[0].Time)
				assert.Equal(t, "MILLISECONDS", recs[0].TimeUnit)
				assert.Equal(t, int64(3), recs[0].Version)
				assert.Equal(t, "1.5", recs[0].MeasureValue)
				assert.Equal(t, []pipes.TimestreamRecordValue{
					{Name: "host", Value: "h1", Type: "VARCHAR"}, {Name: "env", Value: "prod", Type: "VARCHAR"},
				}, recs[0].Dimensions)
			},
		},
		{
			name: "epoch_seconds",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.t", EpochTimeUnit: "SECONDS", VersionValue: "1",
				DimensionMappings:     []pipes.TimestreamDimensionMapping{{DimensionName: "d", DimensionValue: "x"}},
				SingleMeasureMappings: []pipes.TimestreamSingleMeasureMapping{{MeasureName: "m", MeasureValue: "$.v"}},
			},
			events: []any{map[string]any{"t": 1700000000.0, "v": true}},
			check: func(t *testing.T, recs []pipes.TimestreamRecord) {
				t.Helper()
				require.Len(t, recs, 1)
				assert.Equal(t, "1700000000", recs[0].Time)
				assert.Equal(t, "SECONDS", recs[0].TimeUnit)
				assert.Equal(t, "true", recs[0].MeasureValue)
			},
		},
		{
			name: "timestamp_format",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.t", TimeFieldType: "TIMESTAMP_FORMAT", TimestampFormat: "yyyy-MM-dd'T'HH:mm:ss'Z'",
				VersionValue: "1", DimensionMappings: dims, SingleMeasureMappings: single,
			},
			events: []any{map[string]any{"t": "2023-11-14T22:13:20Z", "body": `{"host":"h","cpu":2}`}},
			check: func(t *testing.T, recs []pipes.TimestreamRecord) {
				t.Helper()
				require.Len(t, recs, 1)
				assert.Equal(t, "1700000000000", recs[0].Time)
				assert.Equal(t, "MILLISECONDS", recs[0].TimeUnit)
			},
		},
		{
			name: "multi_measure_batch",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.ts", VersionValue: "1", DimensionMappings: dims[1:],
				MultiMeasureMappings: []pipes.TimestreamMultiMeasureMapping{{
					MultiMeasureName: "usage",
					MultiMeasureAttributeMappings: []pipes.TimestreamMultiMeasureAttributeMapping{
						{MultiMeasureAttributeName: "mem", MeasureValue: "$.mem", MeasureValueType: "BIGINT"},
					},
				}},
			},
			events: []any{
				map[string]any{"ts": 1.0, "mem": 5.0}, map[string]any{"ts": 2.0, "mem": 6.0},
			},
			check: func(t *testing.T, recs []pipes.TimestreamRecord) {
				t.Helper()
				require.Len(t, recs, 2)
				assert.Equal(t, "MULTI", recs[1].MeasureValueType)
				assert.Equal(t, "usage", recs[1].MeasureName)
				assert.Equal(
					t,
					[]pipes.TimestreamRecordValue{{Name: "mem", Value: "6", Type: "BIGINT"}},
					recs[1].MeasureValues,
				)
			},
		},
		{
			name: "missing_path",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.ts", VersionValue: "1", DimensionMappings: dims, SingleMeasureMappings: single,
			},
			events:  []any{map[string]any{"ts": 1.0, "body": `{"cpu":1}`}},
			wantErr: "not found in event",
		},
		{
			name: "bad_version",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.ts", VersionValue: "0", DimensionMappings: dims[1:], SingleMeasureMappings: single,
			},
			events:  []any{map[string]any{"ts": 1.0}},
			wantErr: "VersionValue",
		},
		{
			name: "bad_epoch",
			tp: pipes.TimestreamParameters{
				TimeValue: "$.ts", VersionValue: "1", DimensionMappings: dims[1:], SingleMeasureMappings: single,
			},
			events:  []any{map[string]any{"ts": "yesterday"}},
			wantErr: "not an epoch number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			recs, err := pipes.BuildTimestreamRecords(&tt.tp, tt.events)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			tt.check(t, recs)
		})
	}
}
