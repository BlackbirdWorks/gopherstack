package integration_test

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	guarddutytypes "github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// guardDutyDetectorMu serializes exclusive use of GuardDuty's single
// account/region detector (bd gopherstack-ba9l) between
// TestIntegration_GuardDuty_DetectorLifecycle and
// TestIntegration_GuardDuty_FilterLifecycle, the two tests in this file that
// each need a detector to exist for their full duration. GuardDuty allows
// only one detector per account/region -- confirmed, correct AWS behavior,
// not a gopherstack bug -- so both tests calling CreateDetector concurrently
// against the shared server races on a 409 ConflictException.
//
// Region-per-test isolation was considered and ruled out: every gopherstack
// service backend resolves its account/region exactly once, at Provider.Init
// (server startup) time, from process-wide config
// (pkgs/service.AccountRegionOrDefault) -- never per request. Every
// integration test in this package additionally shares one server
// process/container (see TestMain in main_test.go). So giving each test's
// SDK client a different config.WithRegion would be purely client-side
// decoration: both clients would still hit the exact same single
// server-side GuardDuty backend instance, with the same one-detector limit,
// and the race would persist unchanged.
//
// A shared get-or-create detector (the issue's other suggested fallback)
// does not work either: TestIntegration_GuardDuty_DetectorLifecycle
// specifically exercises DeleteDetector as its own lifecycle assertion, so
// if it shared a live detector with TestIntegration_GuardDuty_FilterLifecycle
// running concurrently, that delete would invalidate FilterLifecycle's
// precondition mid-test regardless of who created the detector.
//
// This mutex instead serializes just the two tests' create-through-delete
// critical sections against each other, so whichever acquires it first runs
// its full detector lifecycle to completion (including any cleanup delete)
// before the other starts. t.Parallel() is kept on both -- they still run
// concurrently with any other (present or future) parallel subtests in this
// package; only these two specifically no longer race on the shared
// single-detector resource.
//
//nolint:gochecknoglobals // deliberate test-only mutex serializing these two subtests
var guardDutyDetectorMu sync.Mutex

// createGuardDutyClient returns a GuardDuty client pointed at the shared test container.
func createGuardDutyClient(t *testing.T) *guarddutysdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return guarddutysdk.NewFromConfig(cfg, func(o *guarddutysdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_GuardDuty_DetectorLifecycle drives create→get→list→delete of a detector.
func TestIntegration_GuardDuty_DetectorLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "full_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Serialized against TestIntegration_GuardDuty_FilterLifecycle --
			// see guardDutyDetectorMu's doc comment. Unlock is registered as
			// a cleanup before the detector-delete cleanup below so it runs
			// last (t.Cleanup is LIFO), after this test's full lifecycle
			// (including its own explicit DeleteDetector call and its
			// cleanup) has finished.
			guardDutyDetectorMu.Lock()
			t.Cleanup(guardDutyDetectorMu.Unlock)

			ctx := t.Context()
			client := createGuardDutyClient(t)

			createOut, err := client.CreateDetector(ctx, &guarddutysdk.CreateDetectorInput{
				Enable: aws.Bool(true),
			})
			require.NoError(t, err, "CreateDetector should succeed")
			detectorID := aws.ToString(createOut.DetectorId)
			require.NotEmpty(t, detectorID, "detector id must be returned")

			// context.WithoutCancel, not ctx directly: t.Context() is
			// canceled just before Cleanup functions run (testing.T.Context
			// doc), so a cleanup using the canceled ctx would silently fail
			// to delete the detector -- a real leak this test used to carry
			// unnoticed only because the explicit DeleteDetector call below
			// (still on the live ctx) already removes it first, making this
			// cleanup a no-op either way. Fixed for real regardless, since a
			// failed assertion above would otherwise skip that explicit
			// delete and leave the detector behind for guardDutyDetectorMu's
			// next owner.
			cleanupCtx := context.WithoutCancel(ctx)
			t.Cleanup(func() {
				_, _ = client.DeleteDetector(
					cleanupCtx,
					&guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)},
				)
			})

			getOut, err := client.GetDetector(ctx, &guarddutysdk.GetDetectorInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "GetDetector should succeed")
			assert.Equal(t, guarddutytypes.DetectorStatusEnabled, getOut.Status)

			listOut, err := client.ListDetectors(ctx, &guarddutysdk.ListDetectorsInput{})
			require.NoError(t, err, "ListDetectors should succeed")
			assert.Contains(t, listOut.DetectorIds, detectorID, "created detector should appear in list")

			_, err = client.DeleteDetector(ctx, &guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "DeleteDetector should succeed")
		})
	}
}

// TestIntegration_GuardDuty_GetMemberDetectors drives
// CreateMembers -> GetMemberDetectors via the real AWS SDK v2 client.
// GetMemberDetectorsOutput requires MemberDataSourceConfigurations; the SDK
// leaves it nil when the server names the field wrong, so a decoded
// non-nil, non-empty slice is the only proof the real wire key
// (deserializers.go: "members", not "memberDataSources") round-trips
// (gopherstack-lx5h).
func TestIntegration_GuardDuty_GetMemberDetectors(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	guardDutyDetectorMu.Lock()
	t.Cleanup(guardDutyDetectorMu.Unlock)

	ctx := t.Context()
	client := createGuardDutyClient(t)

	createOut, err := client.CreateDetector(ctx, &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err, "CreateDetector should succeed")
	detectorID := aws.ToString(createOut.DetectorId)

	cleanupCtx := context.WithoutCancel(ctx)
	t.Cleanup(func() {
		_, _ = client.DeleteDetector(
			cleanupCtx,
			&guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)},
		)
	})

	const memberAccountID = "222222222222"

	_, err = client.CreateMembers(ctx, &guarddutysdk.CreateMembersInput{
		DetectorId: aws.String(detectorID),
		AccountDetails: []guarddutytypes.AccountDetail{
			{AccountId: aws.String(memberAccountID), Email: aws.String("member@example.com")},
		},
	})
	require.NoError(t, err, "CreateMembers should succeed")

	getOut, err := client.GetMemberDetectors(ctx, &guarddutysdk.GetMemberDetectorsInput{
		DetectorId: aws.String(detectorID),
		AccountIds: []string{memberAccountID},
	})
	require.NoError(t, err, "GetMemberDetectors should succeed")
	require.Len(
		t,
		getOut.MemberDataSourceConfigurations,
		1,
		"real key is members, mapping to MemberDataSourceConfigurations",
	)
	assert.Equal(t, memberAccountID, aws.ToString(getOut.MemberDataSourceConfigurations[0].AccountId))
}

// TestIntegration_GuardDuty_FilterLifecycle drives detector→filter create→get→list→delete.
func TestIntegration_GuardDuty_FilterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name       string
		filterName string
	}{
		{name: "full_lifecycle", filterName: "integ-filter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Serialized against TestIntegration_GuardDuty_DetectorLifecycle --
			// see guardDutyDetectorMu's doc comment.
			guardDutyDetectorMu.Lock()
			t.Cleanup(guardDutyDetectorMu.Unlock)

			ctx := t.Context()
			client := createGuardDutyClient(t)

			detOut, err := client.CreateDetector(ctx, &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
			require.NoError(t, err, "CreateDetector should succeed")
			detectorID := aws.ToString(detOut.DetectorId)

			// context.WithoutCancel, not ctx directly: this test has no
			// explicit DeleteDetector call of its own (unlike
			// DetectorLifecycle), so this cleanup is the ONLY thing that
			// removes the detector. t.Context() is canceled just before
			// Cleanup functions run (testing.T.Context doc), so using ctx
			// here would make DeleteDetector fail immediately on a canceled
			// context every time, silently leaking the detector (the error
			// is discarded) -- exactly the pre-existing latent bug that
			// surfaced as a spurious CreateDetector 409 once
			// guardDutyDetectorMu started running this test back-to-back
			// with TestIntegration_GuardDuty_DetectorLifecycle in the same
			// process.
			cleanupCtx := context.WithoutCancel(ctx)
			t.Cleanup(func() {
				_, _ = client.DeleteDetector(
					cleanupCtx,
					&guarddutysdk.DeleteDetectorInput{DetectorId: aws.String(detectorID)},
				)
			})

			_, err = client.CreateFilter(ctx, &guarddutysdk.CreateFilterInput{
				DetectorId: aws.String(detectorID),
				Name:       aws.String(tt.filterName),
				FindingCriteria: &guarddutytypes.FindingCriteria{
					Criterion: map[string]guarddutytypes.Condition{
						"severity": {GreaterThanOrEqual: aws.Int64(7)},
					},
				},
			})
			require.NoError(t, err, "CreateFilter should succeed")

			getOut, err := client.GetFilter(ctx, &guarddutysdk.GetFilterInput{
				DetectorId: aws.String(detectorID),
				FilterName: aws.String(tt.filterName),
			})
			require.NoError(t, err, "GetFilter should succeed")
			assert.Equal(t, tt.filterName, aws.ToString(getOut.Name))

			listOut, err := client.ListFilters(ctx, &guarddutysdk.ListFiltersInput{DetectorId: aws.String(detectorID)})
			require.NoError(t, err, "ListFilters should succeed")
			assert.Contains(t, listOut.FilterNames, tt.filterName, "created filter should appear in list")

			_, err = client.DeleteFilter(ctx, &guarddutysdk.DeleteFilterInput{
				DetectorId: aws.String(detectorID),
				FilterName: aws.String(tt.filterName),
			})
			require.NoError(t, err, "DeleteFilter should succeed")
		})
	}
}
