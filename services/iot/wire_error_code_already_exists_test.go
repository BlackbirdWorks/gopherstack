package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// Test_CreateFamily_AlreadyExistsIsConflict covers four Create operations
// whose own deserializeOpError switch (iot@v1.77.4/deserializers.go,
// confirmed by direct per-op read) declares ConflictException, not
// ResourceAlreadyExistsException -- the code writeIoTError's shared
// ErrAlreadyExists case renders by default (correct for the ~150 other
// Create ops in this service that DO declare it). A real client's
// deserializer never matched ResourceAlreadyExistsException for these four,
// so errors.As on the typed exception it wrote never fired.
func Test_CreateFamily_AlreadyExistsIsConflict(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *iotsdk.Client {
		t.Helper()

		backend := iot.NewInMemoryBackend()
		h := iot.NewHandler(backend, nil)

		return newTestIoTClient(t, h)
	}

	assertConflict := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		var ce *types.ConflictException
		require.ErrorAs(t, err, &ce, "expected a typed ConflictException, got: %v", err)
	}

	t.Run("CreateCommand", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		in := &iotsdk.CreateCommandInput{
			CommandId:   aws.String("dup-command"),
			DisplayName: aws.String("dup"),
			Namespace:   types.CommandNamespaceAWSIoT,
		}
		_, err := client.CreateCommand(ctx, in)
		require.NoError(t, err)

		_, err = client.CreateCommand(ctx, in)
		assertConflict(t, err)
	})

	t.Run("CreatePackage", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		in := &iotsdk.CreatePackageInput{PackageName: aws.String("dup-package")}
		_, err := client.CreatePackage(ctx, in)
		require.NoError(t, err)

		_, err = client.CreatePackage(ctx, in)
		assertConflict(t, err)
	})

	t.Run("CreatePackageVersion", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		_, err := client.CreatePackage(ctx, &iotsdk.CreatePackageInput{PackageName: aws.String("pkg")})
		require.NoError(t, err)

		in := &iotsdk.CreatePackageVersionInput{
			PackageName: aws.String("pkg"),
			VersionName: aws.String("dup-version"),
		}
		_, err = client.CreatePackageVersion(ctx, in)
		require.NoError(t, err)

		_, err = client.CreatePackageVersion(ctx, in)
		assertConflict(t, err)
	})

	t.Run("CreateJobTemplate", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		in := &iotsdk.CreateJobTemplateInput{
			JobTemplateId: aws.String("dup-template"),
			Description:   aws.String("dup"),
		}
		_, err := client.CreateJobTemplate(ctx, in)
		require.NoError(t, err)

		_, err = client.CreateJobTemplate(ctx, in)
		assertConflict(t, err)
	})
}

// Test_StartMitigationTaskFamily_AlreadyExistsIsTaskAlreadyExists covers
// StartAuditMitigationActionsTask and StartDetectMitigationActionsTask,
// whose own deserializeOpError switch declares TaskAlreadyExistsException --
// a code writeIoTError has never rendered at all (the not-found half of
// each already correctly declares/renders ResourceNotFoundException and is
// untouched by this fix).
func Test_StartMitigationTaskFamily_AlreadyExistsIsTaskAlreadyExists(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *iotsdk.Client {
		t.Helper()

		backend := iot.NewInMemoryBackend()
		h := iot.NewHandler(backend, nil)

		return newTestIoTClient(t, h)
	}

	assertTaskAlreadyExists := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		var tae *types.TaskAlreadyExistsException
		require.ErrorAs(t, err, &tae, "expected a typed TaskAlreadyExistsException, got: %v", err)
	}

	t.Run("StartAuditMitigationActionsTask", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		in := &iotsdk.StartAuditMitigationActionsTaskInput{
			TaskId:             aws.String("dup-audit-task"),
			ClientRequestToken: aws.String("token-1"),
			Target:             &types.AuditMitigationActionsTaskTarget{},
			AuditCheckToActionsMapping: map[string][]string{
				"AUTHENTICATED_COGNITO_ROLE_OVERLY_PERMISSIVE_CHECK": {"some-action"},
			},
		}
		_, err := client.StartAuditMitigationActionsTask(ctx, in)
		require.NoError(t, err)

		_, err = client.StartAuditMitigationActionsTask(ctx, in)
		assertTaskAlreadyExists(t, err)
	})

	t.Run("StartDetectMitigationActionsTask", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()
		in := &iotsdk.StartDetectMitigationActionsTaskInput{
			TaskId:             aws.String("dup-detect-task"),
			ClientRequestToken: aws.String("token-1"),
			Target:             &types.DetectMitigationActionsTaskTarget{},
			Actions:            []string{"some-action"},
		}
		_, err := client.StartDetectMitigationActionsTask(ctx, in)
		require.NoError(t, err)

		_, err = client.StartDetectMitigationActionsTask(ctx, in)
		assertTaskAlreadyExists(t, err)
	})
}
