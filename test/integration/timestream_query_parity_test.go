package integration_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	twsdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_TimestreamQuery_ReadsWrittenRecords writes records through the
// Timestream Write SDK and reads them back through SQL on the Query SDK.
func TestIntegration_TimestreamQuery_ReadsWrittenRecords(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	write := createTimestreamWriteClient(t)
	query := createTimestreamQueryClient(t)
	ctx := t.Context()

	db := "tsq-" + uuid.NewString()[:8]
	_, err := write.CreateDatabase(ctx, &twsdk.CreateDatabaseInput{DatabaseName: aws.String(db)})
	require.NoError(t, err)
	_, err = write.CreateTable(ctx, &twsdk.CreateTableInput{DatabaseName: aws.String(db), TableName: aws.String("cpu")})
	require.NoError(t, err)

	now := time.Now()
	rec := func(host, val string, ago time.Duration) twtypes.Record {
		return twtypes.Record{
			MeasureName: aws.String("cpu"), MeasureValue: aws.String(val), MeasureValueType: twtypes.MeasureValueTypeDouble,
			Time: aws.String(strconv.FormatInt(now.Add(-ago).UnixMilli(), 10)), TimeUnit: twtypes.TimeUnitMilliseconds,
			Dimensions: []twtypes.Dimension{{Name: aws.String("host"), Value: aws.String(host)}},
		}
	}

	_, err = write.WriteRecords(ctx, &twsdk.WriteRecordsInput{
		DatabaseName: aws.String(db), TableName: aws.String("cpu"),
		Records: []twtypes.Record{rec("a", "10", 3*time.Minute), rec("a", "30", 2*time.Minute), rec("b", "5", time.Minute)},
	})
	require.NoError(t, err)

	out, err := query.Query(ctx, &tqsdk.QueryInput{QueryString: aws.String(
		`SELECT host, avg(measure_value::double) AS avg_cpu, count(*) AS n FROM "` + db + `"."cpu" ` +
			`WHERE time > ago(1h) GROUP BY host ORDER BY host`)})
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)

	assert.Equal(t, "a", aws.ToString(out.Rows[0].Data[0].ScalarValue))
	assert.Equal(t, "20.0", aws.ToString(out.Rows[0].Data[1].ScalarValue))
	assert.Equal(t, "2", aws.ToString(out.Rows[0].Data[2].ScalarValue))
	assert.Equal(t, "DOUBLE", string(out.ColumnInfo[1].Type.ScalarType))
	assert.Equal(t, "BIGINT", string(out.ColumnInfo[2].Type.ScalarType))
}
