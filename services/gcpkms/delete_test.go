package gcpkms_test

import (
	"testing"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func fieldMask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func TestDeleteCryptoKeyVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		func(k *kmspb.CryptoKey) { k.DestroyScheduledDuration = durationpb.New(24 * time.Hour) })
	v2, err := e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()})
	require.NoError(t, err)

	del := func(name string) error {
		op, derr := e.client.DeleteCryptoKeyVersion(t.Context(), &kmspb.DeleteCryptoKeyVersionRequest{Name: name})
		if derr != nil {
			return derr
		}

		return op.Wait(t.Context())
	}

	assert.Equal(t, codes.FailedPrecondition, status.Code(del(v2.GetName())), "enabled")

	_, err = e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{Name: v2.GetName()})
	require.NoError(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(del(v2.GetName())), "scheduled")

	e.clk.advance(25 * time.Hour)
	require.NoError(t, del(v2.GetName()))

	_, err = e.client.GetCryptoKeyVersion(t.Context(), &kmspb.GetCryptoKeyVersionRequest{Name: v2.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Equal(t, codes.NotFound, status.Code(del(v2.GetName())), "already deleted")
	assert.Equal(t, codes.FailedPrecondition, status.Code(del(key.GetName()+"/cryptoKeyVersions/1")), "primary")

	v3, err := e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()})
	require.NoError(t, err)
	assert.Equal(t, key.GetName()+"/cryptoKeyVersions/3", v3.GetName(), "version ids are never reused")

	list, err := drain(e.client.ListCryptoKeyVersions(t.Context(),
		&kmspb.ListCryptoKeyVersionsRequest{Parent: key.GetName()}).Next, (*kmspb.CryptoKeyVersion).GetName)
	require.NoError(t, err)
	assert.Len(t, list, 2)
}

func TestDeleteCryptoKeyAndRetiredResources(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256,
		func(k *kmspb.CryptoKey) { k.DestroyScheduledDuration = durationpb.New(24 * time.Hour) })
	sym := e.key(t, ring, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
	v1 := key.GetName() + "/cryptoKeyVersions/1"

	_, err := e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: key.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "live version blocks deletion")

	_, err = e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{Name: v1})
	require.NoError(t, err)
	e.clk.advance(25 * time.Hour)

	op, err := e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	meta, err := op.Metadata()
	require.NoError(t, err)
	assert.Contains(t, meta.GetRetiredResource(), location+"/retiredResources/")

	_, err = e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: key.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: key.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.CreateCryptoKey(t.Context(), &kmspb.CreateCryptoKeyRequest{
		Parent: ring, CryptoKeyId: "k",
		CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_MAC, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm: kmspb.CryptoKeyVersion_HMAC_SHA256,
		}},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "retired names cannot be reused")

	rr, err := e.client.GetRetiredResource(
		t.Context(),
		&kmspb.GetRetiredResourceRequest{Name: meta.GetRetiredResource()},
	)
	require.NoError(t, err)
	assert.Equal(t, key.GetName(), rr.GetOriginalResource())
	assert.Equal(t, "cloudkms.googleapis.com/CryptoKey", rr.GetResourceType())
	assert.True(t, e.clk.now().Equal(rr.GetDeleteTime().AsTime()))

	list, err := drain(
		e.client.ListRetiredResources(t.Context(), &kmspb.ListRetiredResourcesRequest{Parent: location}).Next,
		(*kmspb.RetiredResource).GetName,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{rr.GetName()}, list)

	_, err = e.client.GetRetiredResource(
		t.Context(),
		&kmspb.GetRetiredResourceRequest{Name: location + "/retiredResources/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.GetRetiredResource(t.Context(), &kmspb.GetRetiredResourceRequest{Name: "bad"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	dop, err := e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: sym.GetName()})
	require.NoError(t, err, "a primary version goes with its key")
	require.NoError(t, dop.Wait(t.Context()))
}

func TestRetiredResourcesPersist(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256,
		func(k *kmspb.CryptoKey) { k.DestroyScheduledDuration = durationpb.New(24 * time.Hour) })

	_, err := e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{
		Name: key.GetName() + "/cryptoKeyVersions/1",
	})
	require.NoError(t, err)
	e.clk.advance(25 * time.Hour)

	op, err := e.client.DeleteCryptoKeyVersion(t.Context(), &kmspb.DeleteCryptoKeyVersionRequest{
		Name: key.GetName() + "/cryptoKeyVersions/1",
	})
	require.NoError(t, err)
	require.NoError(t, op.Wait(t.Context()))

	dop, err := e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	require.NoError(t, dop.Wait(t.Context()))

	snap := e.handler.Snapshot(t.Context())

	fresh := newEnv(t)
	require.NoError(t, fresh.handler.Restore(t.Context(), snap))

	list, err := drain(fresh.client.ListRetiredResources(t.Context(),
		&kmspb.ListRetiredResourcesRequest{Parent: location}).Next, (*kmspb.RetiredResource).GetOriginalResource)
	require.NoError(t, err)
	assert.Equal(t, []string{key.GetName()}, list)
}

func TestDeleteOperationServed(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "ops")
	sym := e.key(t, ring, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)

	op, err := e.client.DeleteCryptoKey(t.Context(), &kmspb.DeleteCryptoKeyRequest{Name: sym.GetName()})
	require.NoError(t, err)

	got, err := e.client.LROClient.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err)
	assert.True(t, got.GetDone())
	assert.Equal(t, op.Name(), got.GetName())

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))

	_, err = dst.client.LROClient.GetOperation(t.Context(), &longrunningpb.GetOperationRequest{Name: op.Name()})
	require.NoError(t, err, "operations persist")

	_, err = e.client.LROClient.GetOperation(
		t.Context(), &longrunningpb.GetOperationRequest{Name: location + "/operations/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))
}
