package opensearch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestCreateDomain_AutoTuneOptions proves CreateDomain's AutoTuneOptionsInput
// (types.AutoTuneOptionsInput, opensearch@v1.75.4 types/types.go:444-460;
// wire key "AutoTuneOptions", serializers.go:1215-1219) is no longer dropped:
// DomainStatus.AutoTuneOptions (types.AutoTuneOptionsOutput,
// deserializers.go:21601-21603) echoes State/UseOffPeakWindow back.
func TestCreateDomain_AutoTuneOptions(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	out, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("at-create-dom"),
		AutoTuneOptions: &types.AutoTuneOptionsInput{
			DesiredState:     types.AutoTuneDesiredStateEnabled,
			UseOffPeakWindow: aws.Bool(true),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DomainStatus.AutoTuneOptions)
	assert.Equal(t, types.AutoTuneStateEnabled, out.DomainStatus.AutoTuneOptions.State)
	assert.True(t, aws.ToBool(out.DomainStatus.AutoTuneOptions.UseOffPeakWindow))

	desc, err := client.DescribeDomain(ctx, &opensearchsdk.DescribeDomainInput{DomainName: aws.String("at-create-dom")})
	require.NoError(t, err)
	require.NotNil(t, desc.DomainStatus.AutoTuneOptions)
	assert.Equal(t, types.AutoTuneStateEnabled, desc.DomainStatus.AutoTuneOptions.State)
}

// TestCreateDomain_AutoTuneOptions_InvalidDesiredState proves DesiredState is
// validated against its documented closed enum (ENABLED/DISABLED,
// opensearch@v1.75.4 types/enums.go:118-135).
func TestCreateDomain_AutoTuneOptions_InvalidDesiredState(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)

	_, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName:      aws.String("at-bad-dom"),
		AutoTuneOptions: &types.AutoTuneOptionsInput{DesiredState: "SORT_OF"},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}

// TestUpdateDomainConfig_AutoTuneOptions proves UpdateDomainConfig's
// AutoTuneOptions (types.AutoTuneOptions, types/types.go:414-439; wire key
// "AutoTuneOptions", serializers.go:7824-7828) is stored and echoed back
// through DescribeDomainConfig.AutoTuneOptions (types.AutoTuneOptionsStatus,
// deserializers.go:20749-20752), including the RollbackOnDisable member that
// CreateDomain's own shape doesn't carry.
func TestUpdateDomainConfig_AutoTuneOptions(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{DomainName: aws.String("at-update-dom")})
	require.NoError(t, err)

	startAt := time.Date(2027, 1, 3, 2, 0, 0, 0, time.UTC)

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String("at-update-dom"),
		AutoTuneOptions: &types.AutoTuneOptions{
			DesiredState:      types.AutoTuneDesiredStateEnabled,
			RollbackOnDisable: types.RollbackOnDisableNoRollback,
			MaintenanceSchedules: []types.AutoTuneMaintenanceSchedule{
				{
					StartAt:                     aws.Time(startAt),
					CronExpressionForRecurrence: aws.String("cron(0 2 ? * SUN *)"),
					Duration:                    &types.Duration{Unit: types.TimeUnitHours, Value: aws.Int64(2)},
				},
			},
		},
	})
	require.NoError(t, err)

	cfg, err := client.DescribeDomainConfig(ctx, &opensearchsdk.DescribeDomainConfigInput{
		DomainName: aws.String("at-update-dom"),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.DomainConfig.AutoTuneOptions)
	require.NotNil(t, cfg.DomainConfig.AutoTuneOptions.Options)
	require.NotNil(t, cfg.DomainConfig.AutoTuneOptions.Status)

	opts := cfg.DomainConfig.AutoTuneOptions.Options
	assert.Equal(t, types.AutoTuneDesiredStateEnabled, opts.DesiredState)
	assert.Equal(t, types.RollbackOnDisableNoRollback, opts.RollbackOnDisable)
	require.Len(t, opts.MaintenanceSchedules, 1)
	assert.Equal(t, "cron(0 2 ? * SUN *)", aws.ToString(opts.MaintenanceSchedules[0].CronExpressionForRecurrence))
	assert.True(t, startAt.Equal(aws.ToTime(opts.MaintenanceSchedules[0].StartAt)))

	status := cfg.DomainConfig.AutoTuneOptions.Status
	assert.Equal(t, types.AutoTuneStateEnabled, status.State)
	assert.False(t, aws.ToTime(status.CreationDate).IsZero())
	assert.False(t, aws.ToTime(status.UpdateDate).IsZero())
}

// TestUpdateDomainConfig_AutoTuneOptions_RollbackRequiresSchedule proves the
// doc's stated constraint: "If you specify DEFAULT_ROLLBACK, you must
// include a MaintenanceSchedule in the request. Otherwise, OpenSearch
// Service is unable to perform the rollback" (types.AutoTuneOptions'
// RollbackOnDisable doc comment, types/types.go:426-429).
func TestUpdateDomainConfig_AutoTuneOptions_RollbackRequiresSchedule(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{DomainName: aws.String("at-rollback-dom")})
	require.NoError(t, err)

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String("at-rollback-dom"),
		AutoTuneOptions: &types.AutoTuneOptions{
			DesiredState:      types.AutoTuneDesiredStateDisabled,
			RollbackOnDisable: types.RollbackOnDisableDefaultRollback,
		},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}

// TestUpdateDomainConfig_AutoTuneOptions_InvalidDurationUnit proves
// Duration.Unit is validated against TimeUnit's documented closed enum
// ("HOURS" is its only member, opensearch@v1.75.4 types/enums.go:1715-1727).
func TestUpdateDomainConfig_AutoTuneOptions_InvalidDurationUnit(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{DomainName: aws.String("at-badunit-dom")})
	require.NoError(t, err)

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String("at-badunit-dom"),
		AutoTuneOptions: &types.AutoTuneOptions{
			MaintenanceSchedules: []types.AutoTuneMaintenanceSchedule{
				{Duration: &types.Duration{Unit: "DAYS", Value: aws.Int64(1)}},
			},
		},
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve)
}

// TestDescribeDomainAutoTunes_DerivedFromSchedules proves
// DescribeDomainAutoTunes derives its AutoTune entries from real, configured
// MaintenanceSchedules (one entry per schedule, AutoTuneType
// SCHEDULED_ACTION) rather than a fabricated canned entry, and reports none
// at all when no schedule was ever configured.
func TestDescribeDomainAutoTunes_DerivedFromSchedules(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName:      aws.String("at-tunes-dom"),
		AutoTuneOptions: &types.AutoTuneOptionsInput{DesiredState: types.AutoTuneDesiredStateEnabled},
	})
	require.NoError(t, err)

	// No schedule configured yet -- honest empty list, not a fabricated entry.
	out, err := client.DescribeDomainAutoTunes(ctx, &opensearchsdk.DescribeDomainAutoTunesInput{
		DomainName: aws.String("at-tunes-dom"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.AutoTunes)

	startAt1 := time.Date(2027, 2, 1, 3, 0, 0, 0, time.UTC)
	startAt2 := time.Date(2027, 2, 8, 3, 0, 0, 0, time.UTC)

	_, err = client.UpdateDomainConfig(ctx, &opensearchsdk.UpdateDomainConfigInput{
		DomainName: aws.String("at-tunes-dom"),
		AutoTuneOptions: &types.AutoTuneOptions{
			DesiredState: types.AutoTuneDesiredStateEnabled,
			MaintenanceSchedules: []types.AutoTuneMaintenanceSchedule{
				{StartAt: aws.Time(startAt1)},
				{StartAt: aws.Time(startAt2)},
			},
		},
	})
	require.NoError(t, err)

	out, err = client.DescribeDomainAutoTunes(ctx, &opensearchsdk.DescribeDomainAutoTunesInput{
		DomainName: aws.String("at-tunes-dom"),
	})
	require.NoError(t, err)
	require.Len(t, out.AutoTunes, 2)

	for _, at := range out.AutoTunes {
		assert.Equal(t, types.AutoTuneTypeScheduledAction, at.AutoTuneType)
		require.NotNil(t, at.AutoTuneDetails)
		require.NotNil(t, at.AutoTuneDetails.ScheduledAutoTuneDetails)
		assert.NotEmpty(t, at.AutoTuneDetails.ScheduledAutoTuneDetails.ActionType)
		assert.NotEmpty(t, aws.ToString(at.AutoTuneDetails.ScheduledAutoTuneDetails.Action))
	}
}

// TestDescribeDomainAutoTunes_DisabledReportsNoSchedules proves that when
// Auto-Tune is DISABLED, DescribeDomainAutoTunes reports no scheduled
// actions even if MaintenanceSchedules were previously configured -- a
// disabled domain has nothing genuinely scheduled to report.
func TestDescribeDomainAutoTunes_DisabledReportsNoSchedules(t *testing.T) {
	t.Parallel()

	h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
		DomainName: aws.String("at-disabled-dom"),
		AutoTuneOptions: &types.AutoTuneOptionsInput{
			DesiredState: types.AutoTuneDesiredStateDisabled,
			MaintenanceSchedules: []types.AutoTuneMaintenanceSchedule{
				{StartAt: aws.Time(time.Now())},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDomainAutoTunes(ctx, &opensearchsdk.DescribeDomainAutoTunesInput{
		DomainName: aws.String("at-disabled-dom"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.AutoTunes)
}
