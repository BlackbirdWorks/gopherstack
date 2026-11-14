package gcpworkflows_test

import (
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpkms"
)

const (
	keyRing   = "projects/" + project + "/locations/" + location + "/keyRings/r"
	symKey    = keyRing + "/cryptoKeys/sym"
	signKey   = keyRing + "/cryptoKeys/sign"
	otherKey  = "projects/" + project + "/locations/europe-west1/keyRings/r/cryptoKeys/sym"
	missing   = keyRing + "/cryptoKeys/none"
	trivialWF = "- step:\n    return: ok\n"
)

func newKMS(t *testing.T) *gcpkms.InMemoryBackend {
	t.Helper()

	b := gcpkms.NewInMemoryBackend()

	for _, loc := range []string{location, "europe-west1"} {
		_, err := b.CreateKeyRing(&kmspb.CreateKeyRingRequest{
			Parent: "projects/" + project + "/locations/" + loc, KeyRingId: "r",
		})
		require.NoError(t, err)
	}

	for _, k := range []struct {
		parent, id string
		purpose    kmspb.CryptoKey_CryptoKeyPurpose
	}{
		{keyRing, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT},
		{keyRing, "sign", kmspb.CryptoKey_ASYMMETRIC_SIGN},
		{"projects/" + project + "/locations/europe-west1/keyRings/r", "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT},
	} {
		ck := &kmspb.CryptoKey{Purpose: k.purpose}
		if k.purpose == kmspb.CryptoKey_ASYMMETRIC_SIGN {
			ck.VersionTemplate = &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256}
		}

		_, err := b.CreateCryptoKey(&kmspb.CreateCryptoKeyRequest{Parent: k.parent, CryptoKeyId: k.id, CryptoKey: ck})
		require.NoError(t, err)
	}

	return b
}

func TestCreateWorkflowCMEK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   string
		code  codes.Code
		wired bool
	}{
		{name: "ok", key: symKey, wired: true},
		{name: "missing", key: missing, wired: true, code: codes.InvalidArgument},
		{name: "other region", key: otherKey, wired: true, code: codes.InvalidArgument},
		{name: "wrong purpose", key: signKey, wired: true, code: codes.InvalidArgument},
		{name: "unwired", key: missing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.wired {
				e.handler.Backend.SetKeyChecker(newKMS(t))
			}

			_, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
				Parent: parent, WorkflowId: "w", Workflow: &workflowspb.Workflow{
					SourceCode:    &workflowspb.Workflow_SourceContents{SourceContents: trivialWF},
					CryptoKeyName: tt.key,
				},
			})
			if tt.code == codes.OK {
				require.NoError(t, err)

				return
			}

			require.Equal(t, tt.code, code(err))
		})
	}
}

func TestDisabledKeyBlocksExecutions(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	kb := newKMS(t)
	e.handler.Backend.SetKeyChecker(kb)

	w := e.createWorkflow(t, "w", trivialWF, func(w *workflowspb.Workflow) { w.CryptoKeyName = symKey })
	e.execute(t, w.GetName(), "")

	_, err := kb.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name:  symKey + "/cryptoKeyVersions/1",
			State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)

	_, err = e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{Parent: w.GetName()})
	require.Equal(t, codes.FailedPrecondition, code(err))
	require.ErrorContains(t, err, "The provided Cloud KMS key version has been disabled.")
}
