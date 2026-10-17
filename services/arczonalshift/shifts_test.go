package arczonalshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/arczonalshift"
	"github.com/aws/aws-sdk-go-v2/service/arczonalshift/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) start(t *testing.T, lb, az, expires string) *sdk.StartZonalShiftOutput {
	t.Helper()

	out, err := f.client.StartZonalShift(t.Context(), &sdk.StartZonalShiftInput{
		ResourceIdentifier: aws.String(
			lb,
		),
		AwayFrom:  aws.String(az),
		Comment:   aws.String("drill"),
		ExpiresIn: aws.String(expires),
	})
	require.NoError(t, err)

	return out
}

func TestZonalShiftLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	lb := f.loadBalancer(t, "web", 2)
	before := time.Now()

	started := f.start(t, lb, "us-east-1a", "20m")
	assert.Equal(t, types.ZonalShiftStatusActive, started.Status)
	assert.Equal(t, "use1-az1", aws.ToString(started.AwayFrom), "zone names resolve to zone IDs")
	assert.Equal(t, "drill", aws.ToString(started.Comment))
	assert.Equal(t, lb, aws.ToString(started.ResourceIdentifier))
	assert.NotEmpty(t, aws.ToString(started.ZonalShiftId))
	assert.WithinDuration(t, before.Add(20*time.Minute), aws.ToTime(started.ExpiryTime), time.Minute)

	managed, err := f.client.ListManagedResources(ctx, &sdk.ListManagedResourcesInput{})
	require.NoError(t, err)
	require.Len(t, managed.Items, 1)

	item := managed.Items[0]
	assert.Equal(t, lb, aws.ToString(item.Arn))
	assert.Equal(t, "web", aws.ToString(item.Name))
	assert.Equal(t, []string{"use1-az1", "use1-az2"}, item.AvailabilityZones)
	assert.Equal(t, map[string]float32{"use1-az1": 0, "use1-az2": 1}, item.AppliedWeights)
	assert.Equal(t, types.ZonalAutoshiftStatusDisabled, item.ZonalAutoshiftStatus)
	assert.Equal(t, types.ZonalAutoshiftStatusDisabled, item.PracticeRunStatus)
	require.Len(t, item.ZonalShifts, 1)
	assert.Equal(t, types.AppliedStatusApplied, item.ZonalShifts[0].AppliedStatus)
	assert.Equal(t, aws.ToString(started.ZonalShiftId), aws.ToString(item.ZonalShifts[0].ZonalShiftId))

	got, err := f.client.GetManagedResource(ctx, &sdk.GetManagedResourceInput{ResourceIdentifier: aws.String(lb)})
	require.NoError(t, err)
	assert.Equal(t, "web", aws.ToString(got.Name))
	assert.InDelta(t, float32(0), got.AppliedWeights["use1-az1"], 0.0001)
	assert.Len(t, got.ZonalShifts, 1)

	upd, err := f.client.UpdateZonalShift(ctx, &sdk.UpdateZonalShiftInput{
		ZonalShiftId: started.ZonalShiftId, Comment: aws.String("longer"), ExpiresIn: aws.String("3h"),
	})
	require.NoError(t, err)
	assert.Equal(t, "longer", aws.ToString(upd.Comment))
	assert.WithinDuration(t, before.Add(3*time.Hour), aws.ToTime(upd.ExpiryTime), time.Minute)

	canceled, err := f.client.CancelZonalShift(ctx, &sdk.CancelZonalShiftInput{ZonalShiftId: started.ZonalShiftId})
	require.NoError(t, err)
	assert.Equal(t, types.ZonalShiftStatusCanceled, canceled.Status)

	_, err = f.client.CancelZonalShift(ctx, &sdk.CancelZonalShiftInput{ZonalShiftId: started.ZonalShiftId})
	var conflict *types.ConflictException
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, types.ConflictExceptionReasonZonalShiftStatusNotActive, conflict.Reason)
	assert.Equal(t, aws.ToString(started.ZonalShiftId), aws.ToString(conflict.ZonalShiftId))

	_, err = f.client.UpdateZonalShift(
		ctx,
		&sdk.UpdateZonalShiftInput{ZonalShiftId: started.ZonalShiftId, Comment: aws.String("x")},
	)
	require.ErrorAs(t, err, &conflict)

	managed, err = f.client.ListManagedResources(ctx, &sdk.ListManagedResourcesInput{})
	require.NoError(t, err)
	assert.Empty(t, managed.Items[0].ZonalShifts, "a canceled shift no longer applies")
	assert.InDelta(t, float32(1), managed.Items[0].AppliedWeights["use1-az1"], 0.0001)
}

func TestZonalShiftListFilters(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	lb1, lb2 := f.loadBalancer(t, "one", 3), f.loadBalancer(t, "two", 3)

	active := f.start(t, lb1, "us-east-1a", "1h")
	canceled := f.start(t, lb1, "us-east-1b", "1h")
	_, err := f.client.CancelZonalShift(ctx, &sdk.CancelZonalShiftInput{ZonalShiftId: canceled.ZonalShiftId})
	require.NoError(t, err)
	other := f.start(t, lb2, "us-east-1a", "1h")

	ids := func(in *sdk.ListZonalShiftsInput) []string {
		out, lErr := f.client.ListZonalShifts(ctx, in)
		require.NoError(t, lErr)

		s := make([]string, 0, len(out.Items))
		for _, i := range out.Items {
			s = append(s, aws.ToString(i.ZonalShiftId))
		}

		return s
	}

	assert.ElementsMatch(
		t,
		[]string{
			aws.ToString(active.ZonalShiftId),
			aws.ToString(canceled.ZonalShiftId),
			aws.ToString(other.ZonalShiftId),
		},
		ids(&sdk.ListZonalShiftsInput{}),
	)
	assert.ElementsMatch(
		t,
		[]string{aws.ToString(active.ZonalShiftId), aws.ToString(other.ZonalShiftId)},
		ids(&sdk.ListZonalShiftsInput{Status: types.ZonalShiftStatusActive}),
	)
	assert.Equal(
		t,
		[]string{aws.ToString(canceled.ZonalShiftId)},
		ids(&sdk.ListZonalShiftsInput{Status: types.ZonalShiftStatusCanceled}),
	)
	assert.Empty(t, ids(&sdk.ListZonalShiftsInput{Status: types.ZonalShiftStatusExpired}))
	assert.Equal(
		t,
		[]string{aws.ToString(other.ZonalShiftId)},
		ids(&sdk.ListZonalShiftsInput{ResourceIdentifier: aws.String(lb2)}),
	)

	p := sdk.NewListZonalShiftsPaginator(f.client, &sdk.ListZonalShiftsInput{MaxResults: aws.Int32(2)})
	pages := 0

	for p.HasMorePages() {
		_, pErr := p.NextPage(ctx)
		require.NoError(t, pErr)

		pages++
	}

	assert.Equal(t, 2, pages)

	_, err = f.client.ListZonalShifts(ctx, &sdk.ListZonalShiftsInput{Status: "BOGUS"})
	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, types.ValidationExceptionReasonInvalidStatus, ve.Reason)
}

func TestStartZonalShiftErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	lb := f.loadBalancer(t, "web", 2)
	missing := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/ghost/1234567890abcdef"
	nlb := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/gwy/g/1234567890abcdef"

	tests := []struct {
		name       string
		resource   string
		az         string
		expires    string
		wantCode   string
		wantReason string
	}{
		{"bad expiry unit", lb, "us-east-1a", "90d", "ValidationException", "InvalidExpiresIn"},
		{"expiry above 72h", lb, "us-east-1a", "73h", "ValidationException", "InvalidExpiresIn"},
		{"expiry zero", lb, "us-east-1a", "0m", "ValidationException", "InvalidExpiresIn"},
		{"zone not on the resource", lb, "us-east-1c", "10m", "ValidationException", "InvalidAz"},
		{"resource type unsupported", nlb, "us-east-1a", "10m", "ValidationException", "InvalidResourceIdentifier"},
		{"resource not found", missing, "us-east-1a", "10m", "ResourceNotFoundException", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.StartZonalShift(t.Context(), &sdk.StartZonalShiftInput{
				ResourceIdentifier: aws.String(
					tt.resource,
				),
				AwayFrom:  aws.String(tt.az),
				Comment:   aws.String("c"),
				ExpiresIn: aws.String(tt.expires),
			})
			assert.Equal(t, tt.wantCode, apiCode(err))

			var ve *types.ValidationException
			if tt.wantReason != "" {
				require.ErrorAs(t, err, &ve)
				assert.Equal(t, types.ValidationExceptionReason(tt.wantReason), ve.Reason)
			}
		})
	}
}

func TestStartZonalShiftConflicts(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	lb := f.loadBalancer(t, "web", 2)
	f.start(t, lb, "us-east-1a", "10m")

	reason := func(az string) types.ConflictExceptionReason {
		_, err := f.client.StartZonalShift(t.Context(), &sdk.StartZonalShiftInput{
			ResourceIdentifier: aws.String(
				lb,
			),
			AwayFrom:  aws.String(az),
			Comment:   aws.String("c"),
			ExpiresIn: aws.String("10m"),
		})

		var ce *types.ConflictException
		require.ErrorAs(t, err, &ce)

		return ce.Reason
	}

	assert.Equal(t, types.ConflictExceptionReasonZonalShiftAlreadyExists, reason("us-east-1a"))
	assert.Equal(
		t,
		types.ConflictExceptionReasonSimultaneousZonalShiftsConflict,
		reason("use1-az2"),
		"shifting away from every zone is refused",
	)
}

func TestUpdateZonalShiftErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	started := f.start(t, f.loadBalancer(t, "web", 2), "us-east-1a", "10m")

	_, err := f.client.UpdateZonalShift(t.Context(), &sdk.UpdateZonalShiftInput{ZonalShiftId: started.ZonalShiftId})
	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, types.ValidationExceptionReasonMissingValue, ve.Reason)

	_, err = f.client.UpdateZonalShift(
		t.Context(),
		&sdk.UpdateZonalShiftInput{ZonalShiftId: started.ZonalShiftId, ExpiresIn: aws.String("99d")},
	)
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, types.ValidationExceptionReasonInvalidExpiresIn, ve.Reason)

	_, err = f.client.UpdateZonalShift(
		t.Context(),
		&sdk.UpdateZonalShiftInput{ZonalShiftId: aws.String("nope"), Comment: aws.String("c")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.CancelZonalShift(t.Context(), &sdk.CancelZonalShiftInput{ZonalShiftId: aws.String("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestManagedResourceErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	_, err := f.client.GetManagedResource(
		t.Context(),
		&sdk.GetManagedResourceInput{
			ResourceIdentifier: aws.String(
				"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/ghost/1",
			),
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.GetManagedResource(
		t.Context(),
		&sdk.GetManagedResourceInput{ResourceIdentifier: aws.String("arn:aws:s3:::bucket")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	out, err := f.client.ListManagedResources(t.Context(), &sdk.ListManagedResourcesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Items)
}
