package gcpiam_test

import (
	"context"
	"testing"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/services/gcpiam"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	sa := e.account(t, "persist-sa")
	gone := e.account(t, "persist-gone")
	userKey, _ := createKey(t, e, sa)
	role := createRole(t, e, project, "persistRole", "iam.roles.get")

	signedKey, _, err := e.signBlob(t, sa.GetName(), []byte("before"))
	require.NoError(t, err)

	_, err = e.raw.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sa.GetName(), Policy: &iampb.Policy{
		Bindings: []*iampb.Binding{{Role: "roles/viewer", Members: []string{"user:a@example.com"}}},
	}})
	require.NoError(t, err)

	require.NoError(t, e.client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: gone.GetName()}))

	snap := e.handler.Backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	fresh := newEnv(t)
	require.NoError(t, fresh.handler.Backend.Restore(ctx, snap))

	got, err := fresh.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: sa.GetName()})
	require.NoError(t, err)
	assert.Equal(t, sa.GetUniqueId(), got.GetUniqueId())

	keyAfter, err := fresh.client.GetServiceAccountKey(ctx, &adminpb.GetServiceAccountKeyRequest{
		Name: userKey.GetName(), PublicKeyType: adminpb.ServiceAccountPublicKeyType_TYPE_X509_PEM_FILE,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, keyAfter.GetPublicKeyData())

	pol, err := fresh.raw.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sa.GetName()})
	require.NoError(t, err)
	assert.Len(t, pol.GetBindings(), 1)

	r, err := fresh.client.GetRole(ctx, &adminpb.GetRoleRequest{Name: role.GetName()})
	require.NoError(t, err)
	assert.Equal(t, role.GetEtag(), r.GetEtag())

	resignedKey, sig, err := fresh.signBlob(t, sa.GetName(), []byte("after"))
	require.NoError(t, err)
	assert.Equal(t, signedKey, resignedKey, "system-managed key survives restore")

	pub := parsePublicKey(t, &adminpb.ServiceAccountKey{PublicKeyData: systemPublicKey(t, fresh, sa, resignedKey)})
	require.NoError(t, verifySHA256(pub, []byte("after"), sig))

	_, err = fresh.client.GetServiceAccount(ctx, &adminpb.GetServiceAccountRequest{Name: gone.GetName()})
	requireCode(t, codes.NotFound, err)

	_, err = fresh.client.UndeleteServiceAccount(
		ctx,
		&adminpb.UndeleteServiceAccountRequest{Name: project + "/serviceAccounts/" + gone.GetUniqueId()},
	)
	require.NoError(t, err, "soft-deleted accounts stay undeletable across restore")
}

func TestRestoreRejectsCorruptAndResets(t *testing.T) {
	t.Parallel()

	b := gcpiam.NewInMemoryBackend()
	require.Error(t, b.Restore(context.Background(), []byte("{not json")))

	b.Reset()
	assert.NotEmpty(t, b.Snapshot(context.Background()))
}
