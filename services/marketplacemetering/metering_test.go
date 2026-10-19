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

func TestMeterUsage(t *testing.T) {
	t.Parallel()

	strict := mm.ProductSpec{Code: "strict-product", Dimensions: []mm.Dimension{{Name: "Users", Value: intValue(5)}}}

	tests := []struct {
		setup    func(t *testing.T, f *fixture)
		mutate   func(in *sdk.MeterUsageInput, now time.Time)
		name     string
		wantCode string
	}{
		{name: "ok", mutate: func(*sdk.MeterUsageInput, time.Time) {}},
		{
			name:     "too old",
			mutate:   func(in *sdk.MeterUsageInput, now time.Time) { in.Timestamp = aws.Time(now.Add(-7 * time.Hour)) },
			wantCode: "TimestampOutOfBoundsException",
		},
		{
			name:     "too far ahead",
			mutate:   func(in *sdk.MeterUsageInput, now time.Time) { in.Timestamp = aws.Time(now.Add(2 * time.Hour)) },
			wantCode: "TimestampOutOfBoundsException",
		},
		{
			name:     "negative quantity",
			mutate:   func(in *sdk.MeterUsageInput, _ time.Time) { in.UsageQuantity = new(int32(-1)) },
			wantCode: "ValidationException",
		},
		{
			name:     "empty dimension",
			mutate:   func(in *sdk.MeterUsageInput, _ time.Time) { in.UsageDimension = aws.String("") },
			wantCode: "InvalidUsageDimensionException",
		},
		{
			name:     "empty product",
			mutate:   func(in *sdk.MeterUsageInput, _ time.Time) { in.ProductCode = aws.String("") },
			wantCode: "InvalidProductCodeException",
		},
		{
			name: "allocations add up",
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.UsageQuantity = new(int32(5))
				in.UsageAllocations = []types.UsageAllocation{
					{
						AllocatedUsageQuantity: new(int32(2)),
						Tags:                   []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}},
					},
					{
						AllocatedUsageQuantity: new(int32(3)),
						Tags:                   []types.Tag{{Key: aws.String("team"), Value: aws.String("b")}},
					},
				}
			},
		},
		{
			name: "allocations do not add up",
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.UsageQuantity = new(int32(5))
				in.UsageAllocations = []types.UsageAllocation{{AllocatedUsageQuantity: new(int32(2))}}
			},
			wantCode: "InvalidUsageAllocationsException",
		},
		{
			name: "duplicate allocation tags",
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.UsageQuantity = new(int32(4))
				in.UsageAllocations = []types.UsageAllocation{
					{AllocatedUsageQuantity: new(int32(2))}, {AllocatedUsageQuantity: new(int32(2))},
				}
			},
			wantCode: "InvalidUsageAllocationsException",
		},
		{
			name: "bad allocation tag",
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.UsageQuantity = new(int32(1))
				in.UsageAllocations = []types.UsageAllocation{
					{
						AllocatedUsageQuantity: new(int32(1)),
						Tags:                   []types.Tag{{Key: aws.String("k"), Value: aws.String("v<>")}},
					},
				}
			},
			wantCode: "InvalidTagException",
		},
		{
			name: "declared product known dimension",
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(strict))
				_, err := f.backend.Subscribe(strict.Code, testAccount)
				require.NoError(t, err)
			},
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.ProductCode = aws.String(strict.Code)
				in.UsageDimension = aws.String("Users")
			},
		},
		{
			name: "declared product unknown dimension",
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(strict))
				_, err := f.backend.Subscribe(strict.Code, testAccount)
				require.NoError(t, err)
			},
			mutate:   func(in *sdk.MeterUsageInput, _ time.Time) { in.ProductCode = aws.String(strict.Code) },
			wantCode: "InvalidUsageDimensionException",
		},
		{
			name: "declared product without subscription",
			setup: func(t *testing.T, f *fixture) {
				t.Helper()
				require.NoError(t, f.backend.RegisterProduct(strict))
			},
			mutate: func(in *sdk.MeterUsageInput, _ time.Time) {
				in.ProductCode = aws.String(strict.Code)
				in.UsageDimension = aws.String("Users")
			},
			wantCode: "CustomerNotEntitledException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			if tt.setup != nil {
				tt.setup(t, f)
			}

			in := &sdk.MeterUsageInput{
				ProductCode: aws.String(testProduct), UsageDimension: aws.String("Hosts"),
				Timestamp: aws.Time(f.now().Add(-10 * time.Minute)), UsageQuantity: new(int32(3)),
			}
			tt.mutate(in, f.now())

			out, err := f.client.MeterUsage(t.Context(), in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.MeteringRecordId))
			assert.Len(t, f.backend.UsageRecords(aws.ToString(in.ProductCode)), 1)
		})
	}
}

func TestMeterUsageIdempotency(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ts := aws.Time(f.now().Add(-time.Hour))
	base := func() *sdk.MeterUsageInput {
		return &sdk.MeterUsageInput{
			ProductCode: aws.String(
				testProduct,
			),
			UsageDimension: aws.String("Hosts"),
			Timestamp:      ts,
			UsageQuantity:  new(int32(3)),
		}
	}

	first, err := f.client.MeterUsage(t.Context(), base())
	require.NoError(t, err)

	again, err := f.client.MeterUsage(t.Context(), base())
	require.NoError(t, err)
	assert.Equal(t, first.MeteringRecordId, again.MeteringRecordId)

	changed := base()
	changed.UsageQuantity = new(int32(4))
	_, err = f.client.MeterUsage(t.Context(), changed)
	assert.Equal(t, "DuplicateRequestException", apiCode(err))

	tokened := base()
	tokened.UsageDimension = aws.String("Cores")
	tokened.ClientToken = aws.String("tok-1")
	one, err := f.client.MeterUsage(t.Context(), tokened)
	require.NoError(t, err)

	replay, err := f.client.MeterUsage(t.Context(), tokened)
	require.NoError(t, err)
	assert.Equal(t, one.MeteringRecordId, replay.MeteringRecordId)

	tokened.UsageQuantity = new(int32(9))
	_, err = f.client.MeterUsage(t.Context(), tokened)
	assert.Equal(t, "IdempotencyConflictException", apiCode(err))

	assert.Len(t, f.backend.UsageRecords(testProduct), 2)
}

func TestMeterUsageDryRun(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	_, err := f.client.MeterUsage(t.Context(), &sdk.MeterUsageInput{
		ProductCode: aws.String(testProduct), UsageDimension: aws.String("Hosts"),
		Timestamp: aws.Time(f.now()), DryRun: aws.Bool(true),
	})
	assert.Equal(t, "DryRunOperation", apiCode(err))
	assert.Empty(t, f.backend.UsageRecords(testProduct))
}

func TestBatchMeterUsage(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	spec := mm.ProductSpec{Code: "batch-product", Dimensions: []mm.Dimension{{Name: "Users", Value: intValue(5)}}}
	require.NoError(t, f.backend.RegisterProduct(spec))

	sub, err := f.backend.Subscribe(spec.Code, testAccount)
	require.NoError(t, err)

	ts := aws.Time(f.now().Add(-30 * time.Minute))
	record := func(mod func(r *types.UsageRecord)) types.UsageRecord {
		r := types.UsageRecord{
			Dimension: aws.String(
				"Users",
			),
			Timestamp:          ts,
			Quantity:           new(int32(2)),
			CustomerIdentifier: aws.String(sub.CustomerIdentifier),
		}
		mod(&r)

		return r
	}

	in := &sdk.BatchMeterUsageInput{
		ProductCode: aws.String(spec.Code),
		UsageRecords: []types.UsageRecord{
			record(func(*types.UsageRecord) {}),
			record(func(r *types.UsageRecord) {
				r.CustomerIdentifier = nil
				r.CustomerAWSAccountId = aws.String(otherAccount)
			}),
			record(func(r *types.UsageRecord) { r.Dimension = aws.String("Bogus") }),
			record(func(r *types.UsageRecord) { r.Timestamp = aws.Time(f.now().Add(-8 * time.Hour)) }),
		},
	}

	out, err := f.client.BatchMeterUsage(t.Context(), in)
	require.NoError(t, err)
	require.Len(t, out.Results, 2)
	assert.Equal(t, types.UsageRecordResultStatusSuccess, out.Results[0].Status)
	assert.NotEmpty(t, aws.ToString(out.Results[0].MeteringRecordId))
	assert.Equal(t, types.UsageRecordResultStatusCustomerNotSubscribed, out.Results[1].Status)
	require.Len(t, out.UnprocessedRecords, 2)
	assert.Equal(t, "Bogus", aws.ToString(out.UnprocessedRecords[0].Dimension))

	retry, err := f.client.BatchMeterUsage(t.Context(), &sdk.BatchMeterUsageInput{
		ProductCode: aws.String(spec.Code), UsageRecords: in.UsageRecords[:1],
	})
	require.NoError(t, err)
	require.Len(t, retry.Results, 1)
	assert.Equal(t, types.UsageRecordResultStatusDuplicateRecord, retry.Results[0].Status)
	assert.Equal(t, out.Results[0].MeteringRecordId, retry.Results[0].MeteringRecordId)

	assert.Len(t, f.backend.UsageRecords(spec.Code), 1)
}

func TestBatchMeterUsageValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       *sdk.BatchMeterUsageInput
		name     string
		wantCode string
	}{
		{
			name: "no records",
			in: &sdk.BatchMeterUsageInput{
				ProductCode:  aws.String(testProduct),
				UsageRecords: []types.UsageRecord{},
			},
			wantCode: "ValidationException",
		},
		{
			name: "no customer",
			in: &sdk.BatchMeterUsageInput{
				ProductCode:  aws.String(testProduct),
				UsageRecords: []types.UsageRecord{{Dimension: aws.String("d"), Timestamp: aws.Time(time.Now())}},
			},
			wantCode: "InvalidCustomerIdentifierException",
		},
		{
			name: "no product",
			in: &sdk.BatchMeterUsageInput{
				UsageRecords: []types.UsageRecord{{
					CustomerIdentifier: aws.String("c"), Dimension: aws.String("d"), Timestamp: aws.Time(time.Now()),
				}},
			},
			wantCode: "InvalidProductCodeException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newFixture(t).client.BatchMeterUsage(t.Context(), tt.in)
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}
