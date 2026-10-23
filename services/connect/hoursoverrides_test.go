package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func overrideCfg(day types.OverrideDays, from, to int32) types.HoursOfOperationOverrideConfig {
	return types.HoursOfOperationOverrideConfig{
		Day:       day,
		StartTime: &types.OverrideTimeSlice{Hours: aws.Int32(from), Minutes: aws.Int32(0)},
		EndTime:   &types.OverrideTimeSlice{Hours: aws.Int32(to), Minutes: aws.Int32(0)},
	}
}

func (f *fixture) newHours(t *testing.T, inst, name string, parents ...string) *sdk.CreateHoursOfOperationOutput {
	t.Helper()

	in := &sdk.CreateHoursOfOperationInput{
		InstanceId: aws.String(inst), Name: aws.String(name), TimeZone: aws.String("UTC"),
		Config: []types.HoursOfOperationConfig{{
			Day:       types.HoursOfOperationDaysMonday,
			StartTime: &types.HoursOfOperationTimeSlice{Hours: aws.Int32(9), Minutes: aws.Int32(0)},
			EndTime:   &types.HoursOfOperationTimeSlice{Hours: aws.Int32(17), Minutes: aws.Int32(0)},
		}},
	}
	for _, p := range parents {
		in.ParentHoursOfOperationConfigs = append(in.ParentHoursOfOperationConfigs,
			types.ParentHoursOfOperationConfig{HoursOfOperationId: aws.String(p)})
	}

	out, err := f.client.CreateHoursOfOperation(t.Context(), in)
	require.NoError(t, err)

	return out
}

func TestHoursOfOperationOverrideValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "ovrvalid")
	hours := f.newHours(t, inst, "base")

	tests := []struct {
		mutate func(in *sdk.CreateHoursOfOperationOverrideInput)
		name   string
		code   string
	}{
		{
			name:   "bad date",
			mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) { in.EffectiveFrom = aws.String("2026/12/24") },
			code:   "InvalidParameterException",
		},
		{
			name:   "till before from",
			mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) { in.EffectiveTill = aws.String("2026-12-01") },
			code:   "InvalidParameterException",
		},
		{name: "standard needs config", mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) {
			in.Config = []types.HoursOfOperationOverrideConfig{}
		}, code: "InvalidParameterException"},
		{name: "bad day", mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) {
			in.Config = []types.HoursOfOperationOverrideConfig{overrideCfg("FUNDAY", 1, 2)}
		}, code: "InvalidParameterException"},
		{name: "bad hour", mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) {
			in.Config = []types.HoursOfOperationOverrideConfig{overrideCfg(types.OverrideDaysMonday, 1, 30)}
		}, code: "InvalidParameterException"},
		{
			name:   "unknown hours",
			mutate: func(in *sdk.CreateHoursOfOperationOverrideInput) { in.HoursOfOperationId = aws.String("gone") },
			code:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateHoursOfOperationOverrideInput{
				InstanceId: aws.String(
					inst,
				),
				HoursOfOperationId: hours.HoursOfOperationId,
				Name:               aws.String("o-" + tt.name),
				EffectiveFrom:      aws.String("2026-12-24"),
				EffectiveTill:      aws.String("2026-12-26"),
				Config: []types.HoursOfOperationOverrideConfig{
					overrideCfg(types.OverrideDaysThursday, 10, 12),
				},
			}
			tt.mutate(in)
			_, err := f.client.CreateHoursOfOperationOverride(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestHoursOfOperationOverrideLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "ovr"))
	hours := f.newHours(t, aws.ToString(inst), "base")

	created, err := f.client.CreateHoursOfOperationOverride(t.Context(), &sdk.CreateHoursOfOperationOverrideInput{
		InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId, Name: aws.String("xmas"),
		EffectiveFrom: aws.String("2026-12-24"), EffectiveTill: aws.String("2026-12-26"),
		Config: []types.HoursOfOperationOverrideConfig{overrideCfg(types.OverrideDaysThursday, 10, 12)},
	})
	require.NoError(t, err)

	_, err = f.client.CreateHoursOfOperationOverride(t.Context(), &sdk.CreateHoursOfOperationOverrideInput{
		InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId, Name: aws.String("xmas"),
		EffectiveFrom: aws.String("2026-12-24"), EffectiveTill: aws.String("2026-12-26"),
		Config: []types.HoursOfOperationOverrideConfig{overrideCfg(types.OverrideDaysThursday, 10, 12)},
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.DescribeHoursOfOperationOverride(t.Context(), &sdk.DescribeHoursOfOperationOverrideInput{
		InstanceId:                 inst,
		HoursOfOperationId:         hours.HoursOfOperationId,
		HoursOfOperationOverrideId: created.HoursOfOperationOverrideId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.OverrideTypeStandard, got.HoursOfOperationOverride.OverrideType)
	assert.Equal(t, "2026-12-24", aws.ToString(got.HoursOfOperationOverride.EffectiveFrom))

	eff, err := f.client.GetEffectiveHoursOfOperations(t.Context(), &sdk.GetEffectiveHoursOfOperationsInput{
		InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId,
		FromDate: aws.String("2026-12-21"), ToDate: aws.String("2026-12-25"),
	})
	require.NoError(t, err)
	require.Len(t, eff.EffectiveHoursOfOperationList, 5)
	assert.Equal(t, "UTC", aws.ToString(eff.TimeZone))
	assert.Len(t, eff.EffectiveHoursOfOperationList[0].OperationalHours, 1, "monday uses the base schedule")
	assert.Equal(t, int32(9), aws.ToInt32(eff.EffectiveHoursOfOperationList[0].OperationalHours[0].Start.Hours))
	assert.Equal(
		t,
		int32(10),
		aws.ToInt32(eff.EffectiveHoursOfOperationList[3].OperationalHours[0].Start.Hours),
		"thursday override",
	)
	require.Len(t, eff.EffectiveOverrideHoursList, 1)
	assert.Equal(t, "2026-12-24", aws.ToString(eff.EffectiveOverrideHoursList[0].Date))

	_, err = f.client.UpdateHoursOfOperationOverride(t.Context(), &sdk.UpdateHoursOfOperationOverrideInput{
		InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId,
		HoursOfOperationOverrideId: created.HoursOfOperationOverrideId, OverrideType: types.OverrideTypeClosed,
	})
	require.NoError(t, err)

	list, err := f.client.ListHoursOfOperationOverrides(t.Context(), &sdk.ListHoursOfOperationOverridesInput{
		InstanceId: inst, HoursOfOperationId: hours.HoursOfOperationId,
	})
	require.NoError(t, err)
	require.Len(t, list.HoursOfOperationOverrideList, 1)
	assert.Equal(t, types.OverrideTypeClosed, list.HoursOfOperationOverrideList[0].OverrideType)

	dst := f.restored(t)
	got, err = dst.client.DescribeHoursOfOperationOverride(t.Context(), &sdk.DescribeHoursOfOperationOverrideInput{
		InstanceId:                 inst,
		HoursOfOperationId:         hours.HoursOfOperationId,
		HoursOfOperationOverrideId: created.HoursOfOperationOverrideId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.OverrideTypeClosed, got.HoursOfOperationOverride.OverrideType)

	_, err = f.client.DeleteHoursOfOperationOverride(t.Context(), &sdk.DeleteHoursOfOperationOverrideInput{
		InstanceId:                 inst,
		HoursOfOperationId:         hours.HoursOfOperationId,
		HoursOfOperationOverrideId: created.HoursOfOperationOverrideId,
	})
	require.NoError(t, err)

	_, err = f.client.DeleteHoursOfOperationOverride(t.Context(), &sdk.DeleteHoursOfOperationOverrideInput{
		InstanceId:                 inst,
		HoursOfOperationId:         hours.HoursOfOperationId,
		HoursOfOperationOverrideId: created.HoursOfOperationOverrideId,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestParentHoursOfOperation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "parents"))
	parent := f.newHours(t, aws.ToString(inst), "parent")
	child := f.newHours(t, aws.ToString(inst), "child", aws.ToString(parent.HoursOfOperationId))
	other := f.newHours(t, aws.ToString(inst), "other")

	got, err := f.client.DescribeHoursOfOperation(t.Context(),
		&sdk.DescribeHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: child.HoursOfOperationId})
	require.NoError(t, err)
	require.Len(t, got.HoursOfOperation.ParentHoursOfOperations, 1)
	assert.Equal(t, "parent", aws.ToString(got.HoursOfOperation.ParentHoursOfOperations[0].Name))

	kids, err := f.client.ListChildHoursOfOperations(t.Context(),
		&sdk.ListChildHoursOfOperationsInput{InstanceId: inst, HoursOfOperationId: parent.HoursOfOperationId})
	require.NoError(t, err)
	require.Len(t, kids.ChildHoursOfOperationsSummaryList, 1)
	assert.Equal(t, "child", aws.ToString(kids.ChildHoursOfOperationsSummaryList[0].Name))

	_, err = f.client.DeleteHoursOfOperation(t.Context(),
		&sdk.DeleteHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: parent.HoursOfOperationId})
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.AssociateHoursOfOperations(t.Context(), &sdk.AssociateHoursOfOperationsInput{
		InstanceId:         inst,
		HoursOfOperationId: parent.HoursOfOperationId,
		ParentHoursOfOperationConfigs: []types.ParentHoursOfOperationConfig{
			{HoursOfOperationId: child.HoursOfOperationId},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err), "cycle")

	_, err = f.client.AssociateHoursOfOperations(t.Context(), &sdk.AssociateHoursOfOperationsInput{
		InstanceId:         inst,
		HoursOfOperationId: child.HoursOfOperationId,
		ParentHoursOfOperationConfigs: []types.ParentHoursOfOperationConfig{
			{HoursOfOperationId: other.HoursOfOperationId},
		},
	})
	require.NoError(t, err)

	dst := f.restored(t)
	got, err = dst.client.DescribeHoursOfOperation(t.Context(),
		&sdk.DescribeHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: child.HoursOfOperationId})
	require.NoError(t, err)
	assert.Len(t, got.HoursOfOperation.ParentHoursOfOperations, 2)

	_, err = f.client.DisassociateHoursOfOperations(t.Context(), &sdk.DisassociateHoursOfOperationsInput{
		InstanceId:         inst,
		HoursOfOperationId: child.HoursOfOperationId,
		ParentHoursOfOperationIds: []string{
			aws.ToString(parent.HoursOfOperationId),
			aws.ToString(other.HoursOfOperationId),
		},
	})
	require.NoError(t, err)

	_, err = f.client.DisassociateHoursOfOperations(t.Context(), &sdk.DisassociateHoursOfOperationsInput{
		InstanceId: inst, HoursOfOperationId: child.HoursOfOperationId,
		ParentHoursOfOperationIds: []string{aws.ToString(parent.HoursOfOperationId)},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteHoursOfOperation(t.Context(),
		&sdk.DeleteHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: parent.HoursOfOperationId})
	require.NoError(t, err)
}
