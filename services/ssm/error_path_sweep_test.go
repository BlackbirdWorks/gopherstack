package ssm_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

var errKMSKeyNotFound = errors.New("kms: key not found")

// failingKMS is a ssm.KMSEncryptor whose EncryptSSM always fails, simulating
// a bad KMS key ID for TestPutParameter_InvalidKMSKey_RealClient.
type failingKMS struct{}

func (failingKMS) EncryptSSM(string, []byte) ([]byte, error) {
	return nil, errKMSKeyNotFound
}

func (failingKMS) DecryptSSM([]byte) ([]byte, error) {
	return nil, errKMSKeyNotFound
}

// TestAddTagsToResource_UnknownParameter_RealClient covers a wire-shape error
// bug: tagging a Parameter resource that doesn't exist raised ErrParameterNotFound
// ("ParameterNotFound"), but AddTagsToResource's own deserializer
// (awsAwsjson11_deserializeOpErrorAddTagsToResource, ssm@v1.73.4 deserializers.go)
// models InvalidResourceId/InvalidResourceType/TooManyTagsError/TooManyUpdates,
// not ParameterNotFound — a real client's errors.As(&InvalidResourceId{}) never
// matched.
func TestAddTagsToResource_UnknownParameter_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.AddTagsToResource(ctx, &ssmsdk.AddTagsToResourceInput{
		ResourceId:   aws.String("/no/such/param"),
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter,
		Tags:         []ssmtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var ire *ssmtypes.InvalidResourceId
	require.ErrorAs(t, err, &ire, "expected a real InvalidResourceId from the SDK deserializer")
}

func TestRemoveTagsFromResource_UnknownParameter_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RemoveTagsFromResource(ctx, &ssmsdk.RemoveTagsFromResourceInput{
		ResourceId:   aws.String("/no/such/param"),
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter,
		TagKeys:      []string{"k"},
	})
	require.Error(t, err)

	var ire *ssmtypes.InvalidResourceId
	require.ErrorAs(t, err, &ire, "expected a real InvalidResourceId from the SDK deserializer")
}

func TestListTagsForResource_UnknownParameter_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.ListTagsForResource(ctx, &ssmsdk.ListTagsForResourceInput{
		ResourceId:   aws.String("/no/such/param"),
		ResourceType: ssmtypes.ResourceTypeForTaggingParameter,
	})
	require.Error(t, err)

	var ire *ssmtypes.InvalidResourceId
	require.ErrorAs(t, err, &ire, "expected a real InvalidResourceId from the SDK deserializer")
}

// TestDeleteActivation_UnknownID_RealClient covers a fabricated-code bug:
// gopherstack raised a wire code of "ActivationNotFound", which does not
// appear anywhere in ssm@v1.73.4's types/errors.go or deserializers.go — not
// a real AWS SSM error at all. DeleteActivation's own deserializer models
// InvalidActivationId ("The activation ID isn't valid...") for this case.
func TestDeleteActivation_UnknownID_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.DeleteActivation(ctx, &ssmsdk.DeleteActivationInput{
		ActivationId: aws.String("no-such-activation"),
	})
	require.Error(t, err)

	var iaid *ssmtypes.InvalidActivationId
	require.ErrorAs(t, err, &iaid, "expected a real InvalidActivationId from the SDK deserializer")
}

// TestDeleteMaintenanceWindow_UnknownID_RealClient covers a should-not-error
// bug: DeleteMaintenanceWindow's own deserializer
// (awsAwsjson11_deserializeOpErrorDeleteMaintenanceWindow) models only
// InternalServerError — no not-found exception at all — matching this
// family's other idempotent Delete ops. gopherstack raised one anyway.
func TestDeleteMaintenanceWindow_UnknownID_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.DeleteMaintenanceWindow(ctx, &ssmsdk.DeleteMaintenanceWindowInput{
		WindowId: aws.String("mw-no-such-window"),
	})
	require.NoError(t, err, "DeleteMaintenanceWindow on an unknown ID must be idempotent success, not an error")
}

// TestDeleteOpsItem_UnknownID_RealClient covers a should-not-error bug:
// DeleteOpsItem's own SDK doc comment (api_op_DeleteOpsItem.go) states "This
// operation is idempotent. The system doesn't throw an exception if you
// repeatedly call this operation for the same OpsItem." gopherstack raised
// ErrOpsItemNotFound, which DeleteOpsItem's deserializer doesn't model either
// (only OpsItemInvalidParameterException/InternalServerError).
func TestDeleteOpsItem_UnknownID_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.DeleteOpsItem(ctx, &ssmsdk.DeleteOpsItemInput{
		OpsItemId: aws.String("oi-no-such-item"),
	})
	require.NoError(t, err, "DeleteOpsItem on an unknown ID must be idempotent success per its own SDK doc comment")
}

// TestDeletePatchBaseline_UnknownID_RealClient covers a should-not-error bug:
// DeletePatchBaseline's own deserializer models only
// ResourceInUseException/InternalServerError — no not-found exception,
// matching this family's other idempotent Delete ops (DeleteMaintenanceWindow,
// DeleteOpsItem). gopherstack raised one anyway.
func TestDeletePatchBaseline_UnknownID_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.DeletePatchBaseline(ctx, &ssmsdk.DeletePatchBaselineInput{
		BaselineId: aws.String("pb-no-such-baseline"),
	})
	require.NoError(t, err, "DeletePatchBaseline on an unknown ID must be idempotent success, not an error")
}

// TestGetPatchBaselineForPatchGroup_NoExplicitMapping_RealClient covers a
// should-not-error bug: GetPatchBaselineForPatchGroup's own deserializer
// models NO exceptions at all besides InternalServerError. Real AWS always
// resolves a patch group to a baseline — falling back to the AWS-managed
// default baseline for the OS when no explicit mapping was registered — the
// same fallback GetDefaultPatchBaseline already implements. gopherstack
// instead raised an unmodeled DoesNotExistException for this case.
func TestGetPatchBaselineForPatchGroup_NoExplicitMapping_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	out, err := client.GetPatchBaselineForPatchGroup(ctx, &ssmsdk.GetPatchBaselineForPatchGroupInput{
		PatchGroup:      aws.String("unmapped-patch-group"),
		OperatingSystem: ssmtypes.OperatingSystemWindows,
	})
	require.NoError(t, err, "GetPatchBaselineForPatchGroup must fall back to the default baseline, not error")
	require.NotEmpty(t, aws.ToString(out.BaselineId), "must resolve to the AWS-managed default baseline ID")
}

// TestPutParameter_InvalidKMSKey_RealClient covers a missing-error bug:
// encryptSSMValue raises ErrInvalidKeyID (wire "InvalidKeyId", exactly what
// PutParameter's own deserializer models) when the KMS backend rejects the
// key, but handler.go's classifySSMError never checked for it, so it fell
// through to the default case and surfaced as an opaque 500
// InternalServerError instead of the modeled 400 InvalidKeyId.
func TestPutParameter_InvalidKMSKey_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend().WithKMS(failingKMS{})
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String("/wire-fixes/kms-param"),
		Value: aws.String("secret"),
		Type:  ssmtypes.ParameterTypeSecureString,
		KeyId: aws.String("bad-key"),
	})
	require.Error(t, err)

	var ik *ssmtypes.InvalidKeyId
	require.ErrorAs(t, err, &ik, "expected a real InvalidKeyId from the SDK deserializer")
}
