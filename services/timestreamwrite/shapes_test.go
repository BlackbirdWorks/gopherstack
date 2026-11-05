package timestreamwrite_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func shapeBackend(t *testing.T, now *time.Time) *timestreamwrite.InMemoryBackend {
	t.Helper()

	b := timestreamwrite.NewInMemoryBackend()
	b.SetClock(func() time.Time { return *now })

	_, err := b.CreateDatabase("db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "tbl", nil, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours: 1, MagneticStoreRetentionPeriodInDays: 1,
		},
	})
	require.NoError(t, err)

	return b
}

func shapeRecords(at time.Time) []timestreamwrite.Record {
	ts := strconv.FormatInt(at.UnixMilli(), 10)

	return []timestreamwrite.Record{
		{
			MeasureName: "cpu", MeasureValue: "1", MeasureValueType: "DOUBLE", Time: ts, TimeUnit: "MILLISECONDS",
			Dimensions: []timestreamwrite.Dimension{{Name: "host", Value: "a"}, {Name: "region", Value: "us"}},
		},
		{
			MeasureName: "cpu", MeasureValue: "2", MeasureValueType: "DOUBLE", Time: ts, TimeUnit: "MILLISECONDS",
			Dimensions: []timestreamwrite.Dimension{{Name: "region", Value: "eu"}, {Name: "host", Value: "b"}},
		},
		{
			MeasureName: "stats", MeasureValueType: "MULTI", Time: ts, TimeUnit: "MILLISECONDS",
			Dimensions: []timestreamwrite.Dimension{{Name: "host", Value: "a"}},
			MeasureValues: []timestreamwrite.MeasureValue{
				{Name: "mem", Value: "5", Type: "BIGINT"},
			},
		},
	}
}

func TestTableShapes_SurviveRetentionAndRestore(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := t0
	b := shapeBackend(t, &now)

	_, err := b.WriteRecords("db", "tbl", shapeRecords(t0))
	require.NoError(t, err)

	want := []timestreamwrite.RecordShape{
		{MeasureName: "cpu", MeasureValueType: "DOUBLE", Dimensions: []string{"host", "region"}},
		{
			MeasureName: "stats", MeasureValueType: "MULTI", Dimensions: []string{"host"},
			Attributes: []timestreamwrite.ShapeMeasure{{Name: "mem", Type: "BIGINT"}},
		},
	}
	assert.Equal(t, want, b.TableShapes("db", "tbl"), "dimension order differences collapse into one shape")

	now = t0.Add(48 * time.Hour)
	b.SweepRetention(t.Context())

	data, err := b.TableData("db", "tbl", now)
	require.NoError(t, err)
	assert.Empty(t, data.Records, "records aged out")
	assert.Equal(t, want, b.TableShapes("db", "tbl"), "the schema outlives its records")

	snap, err := b.Snapshot()
	require.NoError(t, err)

	restored := shapeBackend(t, &now)
	require.NoError(t, restored.Restore(t.Context(), snap))
	assert.Equal(t, want, restored.TableShapes("db", "tbl"))
}

func TestTableShapes_RebuiltFromRecordsWhenSnapshotLacksThem(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	b := shapeBackend(t, &t0)

	_, err := b.WriteRecords("db", "tbl", shapeRecords(t0))
	require.NoError(t, err)

	snap, err := b.Snapshot()
	require.NoError(t, err)

	legacy := stripShapes(t, snap)
	restored := shapeBackend(t, &t0)
	require.NoError(t, restored.Restore(t.Context(), legacy))
	assert.Len(t, restored.TableShapes("db", "tbl"), 2)
}

func TestTableShapes_UnknownTable(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	b := shapeBackend(t, &t0)
	assert.Empty(t, b.TableShapes("db", "missing"))
}

func stripShapes(t *testing.T, snap []byte) []byte {
	t.Helper()

	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(snap, &m))
	delete(m, "shapes")

	out, err := json.Marshal(m)
	require.NoError(t, err)

	return out
}
