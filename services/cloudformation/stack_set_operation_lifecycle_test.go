package cloudformation_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

const emptyStackSetTemplate = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}`

func opResultStatuses(t *testing.T, b *cloudformation.InMemoryBackend, set, opID string) []string {
	t.Helper()

	p, err := b.ListStackSetOperationResults(set, opID, 0, "", nil)
	require.NoError(t, err)

	out := make([]string, 0, len(p.Data))
	for _, r := range p.Data {
		out = append(out, r.Status)
	}

	return out
}

func opStatus(t *testing.T, b *cloudformation.InMemoryBackend, set, opID string) string {
	t.Helper()

	op, err := b.DescribeStackSetOperation(set, opID)
	require.NoError(t, err)

	return op.Status
}

func TestStackSetOperation_FailureTolerance(t *testing.T) {
	t.Parallel()

	accounts := []string{"111111111111", "222222222222", "333333333333", "444444444444"}

	tests := []struct {
		prefs         *cloudformation.OperationPreferences
		name          string
		wantStatus    string
		wantResults   []string
		wantInstances int
		wantFailed    int
	}{
		{
			name:          "default_zero_tolerance",
			prefs:         nil,
			wantStatus:    "FAILED",
			wantResults:   []string{"SUCCEEDED", "FAILED", "CANCELLED", "CANCELLED"},
			wantInstances: 2,
			wantFailed:    1,
		},
		{
			name:          "count_one",
			prefs:         &cloudformation.OperationPreferences{FailureToleranceCount: int32p(1)},
			wantStatus:    "FAILED",
			wantResults:   []string{"SUCCEEDED", "FAILED", "FAILED", "CANCELLED"},
			wantInstances: 3,
			wantFailed:    2,
		},
		{
			name: "count_within_tolerance",
			prefs: &cloudformation.OperationPreferences{
				FailureToleranceCount: int32p(3),
				MaxConcurrentCount:    int32p(4),
				ConcurrencyMode:       "SOFT_FAILURE_TOLERANCE",
			},
			wantStatus:    "SUCCEEDED",
			wantResults:   []string{"SUCCEEDED", "FAILED", "FAILED", "FAILED"},
			wantInstances: 4,
			wantFailed:    3,
		},
		{
			name:          "percentage_rounds_down",
			prefs:         &cloudformation.OperationPreferences{FailureTolerancePercentage: int32p(50)},
			wantStatus:    "FAILED",
			wantResults:   []string{"SUCCEEDED", "FAILED", "FAILED", "FAILED"},
			wantInstances: 4,
			wantFailed:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			_, err := b.CreateStackSet("ft-ss", "", exportTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			opID, err := b.CreateStackInstances(
				t.Context(), "ft-ss", accounts, nil, []string{"us-east-1"}, "",
				cloudformation.WithOperationPreferences(tt.prefs),
			)
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			op, err := b.DescribeStackSetOperation("ft-ss", opID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, op.Status)
			assert.Equal(t, tt.wantFailed, op.FailedCount)
			assert.NotNil(t, op.EndedAt)
			assert.Equal(t, tt.wantResults, opResultStatuses(t, b, "ft-ss", opID))

			instances, err := b.ListStackInstances("ft-ss", 0, "", cloudformation.ListStackInstancesFilter{})
			require.NoError(t, err)
			assert.Len(t, instances.Data, tt.wantInstances)
		})
	}
}

func TestStackSetOperation_RegionOrderAndConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefs       *cloudformation.OperationPreferences
		name        string
		wantRegions []string
	}{
		{
			name:        "request_order_default",
			prefs:       nil,
			wantRegions: []string{"us-east-1", "us-east-1", "us-west-2", "us-west-2"},
		},
		{
			name:        "region_order_sequential",
			prefs:       &cloudformation.OperationPreferences{RegionOrder: []string{"us-west-2", "us-east-1"}},
			wantRegions: []string{"us-west-2", "us-west-2", "us-east-1", "us-east-1"},
		},
		{
			name: "parallel_interleaves_regions",
			prefs: &cloudformation.OperationPreferences{
				RegionConcurrencyType: "PARALLEL",
				RegionOrder:           []string{"us-west-2", "us-east-1"},
			},
			wantRegions: []string{"us-west-2", "us-east-1", "us-west-2", "us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			_, err := b.CreateStackSet("ro-ss", "", emptyStackSetTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			opID, err := b.CreateStackInstances(
				t.Context(), "ro-ss", []string{"111111111111", "222222222222"}, nil,
				[]string{"us-east-1", "us-west-2"}, "", cloudformation.WithOperationPreferences(tt.prefs),
			)
			require.NoError(t, err)
			b.WaitForStackSetOperations()
			assert.Equal(t, "SUCCEEDED", opStatus(t, b, "ro-ss", opID))

			instances, err := b.ListStackInstances("ro-ss", 0, "", cloudformation.ListStackInstancesFilter{})
			require.NoError(t, err)

			got := make([]string, 0, len(instances.Data))
			for _, i := range instances.Data {
				got = append(got, i.Region)
			}

			assert.Equal(t, tt.wantRegions, got)
		})
	}
}

func TestStackSetOperation_SequentialRegionAbort(t *testing.T) {
	t.Parallel()

	b := cloudformation.NewInMemoryBackend()
	_, err := b.CreateStackSet("abort-ss", "", exportTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	opID, err := b.CreateStackInstances(
		t.Context(), "abort-ss", []string{"111111111111", "222222222222"}, nil,
		[]string{"us-east-1", "us-west-2"}, "",
	)
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	assert.Equal(t, "FAILED", opStatus(t, b, "abort-ss", opID))
	assert.Equal(t, []string{"SUCCEEDED", "CANCELLED", "FAILED", "CANCELLED"}, opResultStatuses(t, b, "abort-ss", opID))
}

func TestStackSetOperation_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *cloudformation.InMemoryBackend, opID string)
		name string
	}{
		{
			name: "runs_to_success",
			run: func(t *testing.T, b *cloudformation.InMemoryBackend, opID string) {
				t.Helper()

				assert.Equal(t, "RUNNING", opStatus(t, b, "life-ss", opID))
				assert.Equal(t, []string{"SUCCEEDED", "PENDING", "PENDING"}, opResultStatuses(t, b, "life-ss", opID))

				time.Sleep(time.Minute)
				synctest.Wait()
				assert.Equal(t, "RUNNING", opStatus(t, b, "life-ss", opID))

				time.Sleep(time.Minute)
				synctest.Wait()
				assert.Equal(t, "SUCCEEDED", opStatus(t, b, "life-ss", opID))
				assert.Equal(
					t,
					[]string{"SUCCEEDED", "SUCCEEDED", "SUCCEEDED"},
					opResultStatuses(t, b, "life-ss", opID),
				)
			},
		},
		{
			name: "stop_cancels_remaining",
			run: func(t *testing.T, b *cloudformation.InMemoryBackend, opID string) {
				t.Helper()

				require.NoError(t, b.StopStackSetOperation("life-ss", opID))
				assert.Equal(t, "STOPPING", opStatus(t, b, "life-ss", opID))
				require.ErrorIs(t, b.StopStackSetOperation("life-ss", opID), cloudformation.ErrOperationNotRunning)

				time.Sleep(time.Minute)
				synctest.Wait()

				op, err := b.DescribeStackSetOperation("life-ss", opID)
				require.NoError(t, err)
				assert.Equal(t, "STOPPED", op.Status)
				assert.NotNil(t, op.EndedAt)
				assert.Equal(
					t,
					[]string{"SUCCEEDED", "CANCELLED", "CANCELLED"},
					opResultStatuses(t, b, "life-ss", opID),
				)

				instances, err := b.ListStackInstances("life-ss", 0, "", cloudformation.ListStackInstancesFilter{})
				require.NoError(t, err)
				assert.Len(t, instances.Data, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := cloudformation.NewInMemoryBackend()
				b.SetStackSetBatchDelay(time.Minute)
				_, err := b.CreateStackSet("life-ss", "", emptyStackSetTemplate, cloudformation.StackSetOptions{})
				require.NoError(t, err)

				opID, err := b.CreateStackInstances(
					t.Context(), "life-ss", []string{"111111111111", "222222222222", "333333333333"}, nil,
					[]string{"us-east-1"}, "",
				)
				require.NoError(t, err)
				synctest.Wait()

				tt.run(t, b, opID)
				b.WaitForStackSetOperations()
			})
		})
	}
}

func TestStackSetOperation_QueuesPerStackSet(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := cloudformation.NewInMemoryBackend()
		b.SetStackSetBatchDelay(time.Minute)
		_, err := b.CreateStackSet("q-ss", "", emptyStackSetTemplate, cloudformation.StackSetOptions{})
		require.NoError(t, err)

		first, err := b.CreateStackInstances(
			t.Context(), "q-ss", []string{"111111111111", "222222222222"}, nil, []string{"us-east-1"}, "",
		)
		require.NoError(t, err)
		second, err := b.UpdateStackInstances("q-ss", []string{"111111111111"}, nil, []string{"us-east-1"}, "")
		require.NoError(t, err)
		synctest.Wait()

		assert.Equal(t, "RUNNING", opStatus(t, b, "q-ss", first))
		assert.Equal(t, "QUEUED", opStatus(t, b, "q-ss", second))
		require.ErrorIs(t, b.StopStackSetOperation("q-ss", second), cloudformation.ErrOperationNotRunning)
		require.ErrorIs(t, b.DeleteStackSet("q-ss"), cloudformation.ErrOperationInProgress)

		time.Sleep(time.Minute)
		synctest.Wait()

		assert.Equal(t, "SUCCEEDED", opStatus(t, b, "q-ss", first))
		assert.Equal(t, "SUCCEEDED", opStatus(t, b, "q-ss", second))
		b.WaitForStackSetOperations()
	})
}

func TestStackSetOperation_PreferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefs *cloudformation.OperationPreferences
		name  string
	}{
		{name: "both_tolerance", prefs: &cloudformation.OperationPreferences{
			FailureToleranceCount: int32p(1), FailureTolerancePercentage: int32p(10),
		}},
		{name: "both_concurrency", prefs: &cloudformation.OperationPreferences{
			MaxConcurrentCount: int32p(1), MaxConcurrentPercentage: int32p(10),
		}},
		{
			name:  "percentage_over_100",
			prefs: &cloudformation.OperationPreferences{MaxConcurrentPercentage: int32p(101)},
		},
		{name: "zero_concurrency", prefs: &cloudformation.OperationPreferences{MaxConcurrentCount: int32p(0)}},
		{name: "bad_mode", prefs: &cloudformation.OperationPreferences{ConcurrencyMode: "FAST"}},
		{name: "bad_region_type", prefs: &cloudformation.OperationPreferences{RegionConcurrencyType: "SOME"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			_, err := b.CreateStackSet("val-ss", "", emptyStackSetTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			_, err = b.CreateStackInstances(
				t.Context(), "val-ss", []string{"111111111111"}, nil, []string{"us-east-1"}, "",
				cloudformation.WithOperationPreferences(tt.prefs),
			)
			require.ErrorIs(t, err, cloudformation.ErrInvalidOperationPreferences)
		})
	}
}

func TestStackSetOperation_PreferencesWire(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateStackSet(t.Context(), &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("wire-ss"),
		TemplateBody: aws.String(exportTemplate),
	})
	require.NoError(t, err)

	out, err := client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String("wire-ss"),
		Accounts:     []string{"111111111111", "222222222222"},
		Regions:      []string{"us-east-1"},
		OperationPreferences: &types.StackSetOperationPreferences{
			FailureToleranceCount: aws.Int32(1),
			MaxConcurrentCount:    aws.Int32(1),
			RegionOrder:           []string{"us-east-1"},
			RegionConcurrencyType: types.RegionConcurrencyTypeSequential,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStackSetOperation(t.Context(), &cfnsdk.DescribeStackSetOperationInput{
		StackSetName: aws.String("wire-ss"),
		OperationId:  out.OperationId,
	})
	require.NoError(t, err)

	op := desc.StackSetOperation
	assert.Equal(t, types.StackSetOperationStatusSucceeded, op.Status)
	assert.NotNil(t, op.EndTimestamp)
	require.NotNil(t, op.StatusDetails)
	assert.EqualValues(t, 1, aws.ToInt32(op.StatusDetails.FailedStackInstancesCount))
	require.NotNil(t, op.OperationPreferences)
	assert.EqualValues(t, 1, aws.ToInt32(op.OperationPreferences.FailureToleranceCount))
	assert.EqualValues(t, 1, aws.ToInt32(op.OperationPreferences.MaxConcurrentCount))
	assert.Equal(t, []string{"us-east-1"}, op.OperationPreferences.RegionOrder)
	assert.Equal(t, types.RegionConcurrencyTypeSequential, op.OperationPreferences.RegionConcurrencyType)

	_, err = client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String("wire-ss"),
		Accounts:     []string{"333333333333"},
		Regions:      []string{"us-east-1"},
		OperationPreferences: &types.StackSetOperationPreferences{
			FailureToleranceCount:      aws.Int32(1),
			FailureTolerancePercentage: aws.Int32(10),
		},
	})
	require.Error(t, err)
}
