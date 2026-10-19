package marketplacemetering_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/marketplacemetering"
	"github.com/aws/aws-sdk-go-v2/service/marketplacemetering/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mm "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	require.NoError(t, src.backend.RegisterProduct(mm.ProductSpec{
		Code: "persisted", Dimensions: []mm.Dimension{{Name: "Users", Value: intValue(3)}},
	}))

	_, err := src.backend.Subscribe("persisted", testAccount)
	require.NoError(t, err)

	rec, err := src.client.MeterUsage(t.Context(), &sdk.MeterUsageInput{
		ProductCode: aws.String("persisted"), UsageDimension: aws.String("Users"),
		Timestamp: aws.Time(src.now().Add(-time.Minute)), UsageQuantity: new(int32(2)),
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got := dst.backend.UsageRecords("persisted")
	require.Len(t, got, 1)
	assert.Equal(t, aws.ToString(rec.MeteringRecordId), got[0].ID)

	dup, err := dst.client.BatchMeterUsage(t.Context(), &sdk.BatchMeterUsageInput{
		ProductCode: aws.String("persisted"),
		UsageRecords: []types.UsageRecord{{
			Dimension: aws.String("Users"), Timestamp: aws.Time(got[0].Timestamp.Time), Quantity: new(int32(2)),
			CustomerAWSAccountId: aws.String(testAccount),
		}},
	})
	require.NoError(t, err)
	require.Len(t, dup.Results, 1)
	assert.Equal(t, "DuplicateRecord", string(dup.Results[0].Status))

	dst.handler.Reset()
	assert.Empty(t, dst.backend.UsageRecords("persisted"))
}

func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	t.Parallel()

	require.Error(t, newFixture(t).handler.Restore(t.Context(), []byte("{")))
}
