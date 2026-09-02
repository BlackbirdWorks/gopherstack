package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// Test_DeleteFamily_UnknownResourceIsInvalidRequest covers twelve operations
// whose own deserializeOpError switch (iot@v1.77.4/deserializers.go,
// confirmed by direct per-op read) declares no ResourceNotFoundException
// case, unlike almost every other Delete/not-found path in this service.
// gopherstack's backend previously wrapped the generic ErrResourceNotFound/
// ErrThingGroupNotFound sentinel for an unknown resource, which
// writeIoTError rendered as ResourceNotFoundException -- a code none of
// these operations' real deserializer switches match, so each fell to its
// switch's default case and produced a *smithy.GenericAPIError instead of
// any typed exception. InvalidRequestException is the only client-fault
// type each of these operations declares.
func Test_DeleteFamily_UnknownResourceIsInvalidRequest(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *iotsdk.Client {
		t.Helper()

		backend := iot.NewInMemoryBackend()
		h := iot.NewHandler(backend, nil)

		return newTestIoTClient(t, h)
	}

	assertInvalidRequest := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		var ire *types.InvalidRequestException
		require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
	}

	t.Run("DeleteAuditSuppression", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteAuditSuppression(t.Context(), &iotsdk.DeleteAuditSuppressionInput{
			CheckName:          aws.String("no-such-check"),
			ResourceIdentifier: &types.ResourceIdentifier{},
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteMitigationAction", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteMitigationAction(t.Context(), &iotsdk.DeleteMitigationActionInput{
			ActionName: aws.String("no-such-action"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteBillingGroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteBillingGroup(t.Context(), &iotsdk.DeleteBillingGroupInput{
			BillingGroupName: aws.String("no-such-group"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("PutVerificationStateOnViolation", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.PutVerificationStateOnViolation(t.Context(), &iotsdk.PutVerificationStateOnViolationInput{
			ViolationId:       aws.String("no-such-violation"),
			VerificationState: types.VerificationStateTruePositive,
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteV2LoggingLevel", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteV2LoggingLevel(t.Context(), &iotsdk.DeleteV2LoggingLevelInput{
			TargetName: aws.String("no-such-target"),
			TargetType: types.LogTargetTypeThingGroup,
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteFleetMetric", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteFleetMetric(t.Context(), &iotsdk.DeleteFleetMetricInput{
			MetricName: aws.String("no-such-metric"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteCustomMetric", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteCustomMetric(t.Context(), &iotsdk.DeleteCustomMetricInput{
			MetricName: aws.String("no-such-metric"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteDimension", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteDimension(t.Context(), &iotsdk.DeleteDimensionInput{
			Name: aws.String("no-such-dimension"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteSecurityProfile", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteSecurityProfile(t.Context(), &iotsdk.DeleteSecurityProfileInput{
			SecurityProfileName: aws.String("no-such-profile"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteThingGroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteThingGroup(t.Context(), &iotsdk.DeleteThingGroupInput{
			ThingGroupName: aws.String("no-such-group"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("DeleteDynamicThingGroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.DeleteDynamicThingGroup(t.Context(), &iotsdk.DeleteDynamicThingGroupInput{
			ThingGroupName: aws.String("no-such-dynamic-group"),
		})
		assertInvalidRequest(t, err)
	})

	t.Run("ListThingRegistrationTaskReports", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		_, err := client.ListThingRegistrationTaskReports(t.Context(), &iotsdk.ListThingRegistrationTaskReportsInput{
			TaskId:     aws.String("no-such-task"),
			ReportType: types.ReportTypeErrors,
		})
		assertInvalidRequest(t, err)
	})
}

// Test_CancelJob_TerminalStateIsInvalidRequest and
// Test_DeleteThing_ConflictIsInvalidRequest cover the two remaining
// single-sentinel-only overrides (CancelJob's InvalidStateTransitionException
// and DeleteThing's DeleteConflictException, neither declared by their real
// operation).
func Test_CancelJob_TerminalStateIsInvalidRequest(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	client := newTestIoTClient(t, h)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
		JobId:   aws.String("cancel-terminal-job"),
		Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/my-thing"},
	})
	require.NoError(t, err)

	_, err = client.CancelJob(ctx, &iotsdk.CancelJobInput{JobId: aws.String("cancel-terminal-job")})
	require.NoError(t, err)

	_, err = client.CancelJob(ctx, &iotsdk.CancelJobInput{JobId: aws.String("cancel-terminal-job")})
	require.Error(t, err)

	var ire *types.InvalidRequestException
	require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
}

// Test_DeleteThing_ConflictIsInvalidRequest proves DeleteThing's
// has-attached-principals path is wire-shape-wrong. DeleteThing's own
// deserializeOpError switch declares ResourceNotFoundException AND
// VersionConflictException, but no DeleteConflictException -- unlike
// gopherstack's prior mapping, which rendered ErrDeleteConflict as
// DeleteConflictException, a code this operation's real deserializer switch
// never matches.
func Test_DeleteThing_ConflictIsInvalidRequest(t *testing.T) {
	t.Parallel()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)
	client := newTestIoTClient(t, h)
	ctx := t.Context()

	_, err := client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String("conflict-thing")})
	require.NoError(t, err)

	_, err = client.AttachThingPrincipal(ctx, &iotsdk.AttachThingPrincipalInput{
		ThingName: aws.String("conflict-thing"),
		Principal: aws.String("arn:aws:iot:us-east-1:000000000000:cert/deadbeef"),
	})
	require.NoError(t, err)

	_, err = client.DeleteThing(ctx, &iotsdk.DeleteThingInput{ThingName: aws.String("conflict-thing")})
	require.Error(t, err)

	var ire *types.InvalidRequestException
	require.ErrorAs(t, err, &ire, "expected a typed InvalidRequestException, got: %v", err)
}
