package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// mergeWarmThroughput overlays the units set in in onto cur; unset units keep
// their current value.
func mergeWarmThroughput(cur *models.WarmThroughput, in *types.WarmThroughput) *models.WarmThroughput {
	if in == nil {
		return cur
	}

	out := models.WarmThroughput{}
	if cur != nil {
		out = *cur
	}

	if in.ReadUnitsPerSecond != nil {
		out.ReadUnitsPerSecond = aws.Int64(*in.ReadUnitsPerSecond)
	}

	if in.WriteUnitsPerSecond != nil {
		out.WriteUnitsPerSecond = aws.Int64(*in.WriteUnitsPerSecond)
	}

	return &out
}

// warmThroughputDescription reports stored warm throughput as ACTIVE: the
// emulator applies it immediately, so there is no UPDATING window.
func warmThroughputDescription(wt *models.WarmThroughput) *models.WarmThroughputDescription {
	if wt == nil {
		return nil
	}

	return &models.WarmThroughputDescription{
		ReadUnitsPerSecond:  wt.ReadUnitsPerSecond,
		WriteUnitsPerSecond: wt.WriteUnitsPerSecond,
		Status:              models.TableStatusActive,
	}
}

func validateIndexWarmThroughput(wt *types.WarmThroughput) error {
	if wt != nil && wt.ReadUnitsPerSecond == nil && wt.WriteUnitsPerSecond == nil {
		return NewValidationException(
			"One or more parameter values were invalid: " +
				"WarmThroughput must specify ReadUnitsPerSecond, WriteUnitsPerSecond, or both",
		)
	}

	return nil
}

func validateCreateIndexWarmThroughput(gsis []types.GlobalSecondaryIndex) error {
	for _, gsi := range gsis {
		if err := validateIndexWarmThroughput(gsi.WarmThroughput); err != nil {
			return err
		}
	}

	return nil
}

func validateUpdateIndexWarmThroughput(updates []types.GlobalSecondaryIndexUpdate) error {
	for _, u := range updates {
		switch {
		case u.Create != nil:
			if err := validateIndexWarmThroughput(u.Create.WarmThroughput); err != nil {
				return err
			}
		case u.Update != nil:
			if err := validateIndexWarmThroughput(u.Update.WarmThroughput); err != nil {
				return err
			}
		}
	}

	return nil
}

func gsiWarmThroughputSDK(wt *models.WarmThroughput) *types.GlobalSecondaryIndexWarmThroughputDescription {
	d := warmThroughputDescription(wt)
	if d == nil {
		return nil
	}

	return &types.GlobalSecondaryIndexWarmThroughputDescription{
		ReadUnitsPerSecond:  d.ReadUnitsPerSecond,
		WriteUnitsPerSecond: d.WriteUnitsPerSecond,
		Status:              types.IndexStatus(d.Status),
	}
}
