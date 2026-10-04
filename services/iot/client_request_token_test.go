package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ClientRequestToken must be unique per resource (api_op_CreateAuditSuppression.go, Start*MitigationActionsTask).
func TestClientRequestToken_UniquePerResource(t *testing.T) {
	t.Parallel()

	t.Run("audit-suppression", func(t *testing.T) {
		t.Parallel()

		client := newIoTTestClient(t)
		ctx := t.Context()
		create := func(cert, token string) error {
			_, err := client.CreateAuditSuppression(ctx, &iotsdk.CreateAuditSuppressionInput{
				CheckName:            aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
				ClientRequestToken:   aws.String(token),
				ResourceIdentifier:   &types.ResourceIdentifier{DeviceCertificateId: aws.String(cert)},
				SuppressIndefinitely: aws.Bool(true),
			})

			return err
		}

		require.NoError(t, create("cert-a", "tok-1"))
		require.NoError(t, create("cert-b", "tok-2"))

		err := create("cert-c", "tok-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ResourceAlreadyExistsException")

		_, err = client.DeleteAuditSuppression(ctx, &iotsdk.DeleteAuditSuppressionInput{
			CheckName:          aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
			ResourceIdentifier: &types.ResourceIdentifier{DeviceCertificateId: aws.String("cert-a")},
		})
		require.NoError(t, err)
		require.NoError(t, create("cert-c", "tok-1"), "deleting the suppression releases its token")
	})

	t.Run("audit-mitigation-task", func(t *testing.T) {
		t.Parallel()

		client := newIoTTestClient(t)
		ctx := t.Context()
		start := func(id, token string) error {
			_, err := client.StartAuditMitigationActionsTask(ctx, &iotsdk.StartAuditMitigationActionsTaskInput{
				TaskId:             aws.String(id),
				ClientRequestToken: aws.String(token),
				Target:             &types.AuditMitigationActionsTaskTarget{},
				AuditCheckToActionsMapping: map[string][]string{
					"AUTHENTICATED_COGNITO_ROLE_OVERLY_PERMISSIVE_CHECK": {"some-action"},
				},
			})

			return err
		}

		require.NoError(t, start("t1", "tok-1"))

		var tae *types.TaskAlreadyExistsException
		require.ErrorAs(t, start("t2", "tok-1"), &tae)
		require.NoError(t, start("t2", "tok-2"))
	})

	t.Run("detect-mitigation-task", func(t *testing.T) {
		t.Parallel()

		client := newIoTTestClient(t)
		ctx := t.Context()
		start := func(id, token string) error {
			_, err := client.StartDetectMitigationActionsTask(ctx, &iotsdk.StartDetectMitigationActionsTaskInput{
				TaskId:             aws.String(id),
				ClientRequestToken: aws.String(token),
				Target:             &types.DetectMitigationActionsTaskTarget{},
				Actions:            []string{"some-action"},
			})

			return err
		}

		require.NoError(t, start("d1", "tok-1"))

		var tae *types.TaskAlreadyExistsException
		require.ErrorAs(t, start("d2", "tok-1"), &tae)
		require.NoError(t, start("d2", "tok-2"))
	})
}
