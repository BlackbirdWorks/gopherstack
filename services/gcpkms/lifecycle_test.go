package gcpkms_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func TestKeyRings(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	first := e.ring(t, "alpha")

	tests := []struct {
		call func() error
		name string
		want codes.Code
	}{
		{name: "duplicate", call: func() error {
			_, err := e.client.CreateKeyRing(
				t.Context(),
				&kmspb.CreateKeyRingRequest{Parent: location, KeyRingId: "alpha"},
			)

			return err
		}, want: codes.AlreadyExists},
		{name: "bad id", call: func() error {
			_, err := e.client.CreateKeyRing(
				t.Context(),
				&kmspb.CreateKeyRingRequest{Parent: location, KeyRingId: "has space"},
			)

			return err
		}, want: codes.InvalidArgument},
		{name: "bad parent", call: func() error {
			_, err := e.client.CreateKeyRing(
				t.Context(),
				&kmspb.CreateKeyRingRequest{Parent: "projects/p", KeyRingId: "x"},
			)

			return err
		}, want: codes.InvalidArgument},
		{name: "get", call: func() error {
			_, err := e.client.GetKeyRing(t.Context(), &kmspb.GetKeyRingRequest{Name: first})

			return err
		}, want: codes.OK},
		{name: "get missing", call: func() error {
			_, err := e.client.GetKeyRing(t.Context(), &kmspb.GetKeyRingRequest{Name: location + "/keyRings/zzz"})

			return err
		}, want: codes.NotFound},
		{name: "filter bad field", call: func() error {
			it := e.client.ListKeyRings(t.Context(), &kmspb.ListKeyRingsRequest{Parent: location, Filter: "bogus = a"})
			_, err := it.Next()

			return err
		}, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, status.Code(tt.call()))
		})
	}
}

func TestListPagination(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for i := range 5 {
		e.ring(t, fmt.Sprintf("ring%d", i))
	}

	ring := location + "/keyRings/ring0"
	for i := range 3 {
		e.key(t, ring, fmt.Sprintf("key%d", i), kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)
	}

	rings := e.client.ListKeyRings(t.Context(), &kmspb.ListKeyRingsRequest{Parent: location, PageSize: 2})

	var names []string

	for {
		r, err := rings.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		names = append(names, r.GetName())
	}

	require.Len(t, names, 5)
	assert.Equal(t, location+"/keyRings/ring0", names[0])
	assert.Equal(t, location+"/keyRings/ring4", names[4])

	keys := e.client.ListCryptoKeys(t.Context(), &kmspb.ListCryptoKeysRequest{Parent: ring, PageSize: 2})
	count := 0

	for {
		_, err := keys.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		count++
	}

	assert.Equal(t, 3, count)

	_, err := e.client.ListCryptoKeys(t.Context(), &kmspb.ListCryptoKeysRequest{Parent: location + "/keyRings/none"}).
		Next()
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestCreateCryptoKey(t *testing.T) {
	t.Parallel()

	future := timestamppb.New(time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC))
	sym := kmspb.CryptoKey_ENCRYPT_DECRYPT

	tests := []struct {
		key  *kmspb.CryptoKey
		name string
		want codes.Code
	}{
		{&kmspb.CryptoKey{Purpose: sym}, "default symmetric", codes.OK},
		{&kmspb.CryptoKey{Purpose: kmspb.CryptoKey_MAC}, "mac needs algorithm", codes.InvalidArgument},
		{&kmspb.CryptoKey{}, "purpose required", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: kmspb.CryptoKey_KEY_ENCAPSULATION}, "kem unsupported", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: sym, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm: kmspb.CryptoKeyVersion_HMAC_SHA256,
		}}, "algorithm purpose mismatch", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: sym, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			ProtectionLevel: kmspb.ProtectionLevel_EXTERNAL,
		}}, "external level", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN, VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
			Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_SECP256K1_SHA256,
		}}, "secp256k1 unsupported", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: sym, Labels: map[string]string{"env": "dev"}}, "labels", codes.OK},
		{&kmspb.CryptoKey{
			Purpose: sym, NextRotationTime: future,
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(48 * time.Hour)},
		}, "rotation", codes.OK},
		{&kmspb.CryptoKey{
			Purpose:          sym,
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(48 * time.Hour)},
		}, "period without next time", codes.InvalidArgument},
		{&kmspb.CryptoKey{
			Purpose: sym, NextRotationTime: future,
			RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(time.Hour)},
		}, "period too short", codes.InvalidArgument},
		{&kmspb.CryptoKey{Purpose: sym, NextRotationTime: timestamppb.New(time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC))},
			"rotation in past", codes.InvalidArgument},
		{
			&kmspb.CryptoKey{Purpose: sym, DestroyScheduledDuration: durationpb.New(time.Hour)},
			"destroy ttl short",
			codes.InvalidArgument,
		},
		{
			&kmspb.CryptoKey{Purpose: sym, DestroyScheduledDuration: durationpb.New(48 * time.Hour)},
			"destroy ttl ok",
			codes.OK,
		},
		{
			&kmspb.CryptoKey{Purpose: sym, CryptoKeyBackend: "projects/p/locations/l/ekmConnections/c"},
			"ekm backend",
			codes.Unimplemented,
		},
	}

	e := newEnv(t)
	ring := e.ring(t, "r")

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := e.client.CreateCryptoKey(t.Context(), &kmspb.CreateCryptoKeyRequest{
				Parent: ring, CryptoKeyId: fmt.Sprintf("k%d", i), CryptoKey: tt.key,
			})
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if err != nil {
				return
			}

			assert.Equal(t, fmt.Sprintf("%s/cryptoKeys/k%d", ring, i), out.GetName())
			assert.NotNil(t, out.GetCreateTime())
			assert.NotNil(t, out.GetDestroyScheduledDuration())
			assert.Equal(t, kmspb.ProtectionLevel_SOFTWARE, out.GetVersionTemplate().GetProtectionLevel())

			if tt.key.GetPurpose() == kmspb.CryptoKey_ENCRYPT_DECRYPT {
				assert.Equal(t, out.GetName()+"/cryptoKeyVersions/1", out.GetPrimary().GetName())
				assert.Equal(t, kmspb.CryptoKeyVersion_ENABLED, out.GetPrimary().GetState())
				assert.Equal(
					t,
					kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
					out.GetVersionTemplate().GetAlgorithm(),
				)
			}
		})
	}
}

func TestCreateCryptoKeyEdgeCases(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	req := func(id string, skip bool, k *kmspb.CryptoKey) *kmspb.CreateCryptoKeyRequest {
		return &kmspb.CreateCryptoKeyRequest{
			Parent:                     ring,
			CryptoKeyId:                id,
			CryptoKey:                  k,
			SkipInitialVersionCreation: skip,
		}
	}

	_, err := e.client.CreateCryptoKey(
		t.Context(),
		req("dup", false, &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}),
	)
	require.NoError(t, err)

	_, err = e.client.CreateCryptoKey(
		t.Context(),
		req("dup", false, &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}),
	)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.CreateCryptoKey(t.Context(), &kmspb.CreateCryptoKeyRequest{
		Parent:      location + "/keyRings/missing",
		CryptoKeyId: "k",
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))

	skipped, err := e.client.CreateCryptoKey(
		t.Context(),
		req("skip", true, &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}),
	)
	require.NoError(t, err)
	assert.Nil(t, skipped.GetPrimary())

	_, err = e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: skipped.GetName(), Plaintext: []byte("x")})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "no primary yet")

	v, err := e.client.CreateCryptoKeyVersion(
		t.Context(),
		&kmspb.CreateCryptoKeyVersionRequest{Parent: skipped.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, skipped.GetName()+"/cryptoKeyVersions/1", v.GetName())

	got, err := e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: skipped.GetName()})
	require.NoError(t, err)
	assert.Equal(t, v.GetName(), got.GetPrimary().GetName(), "first version becomes primary")

	imp, err := e.client.CreateCryptoKey(t.Context(), req("imp", false, &kmspb.CryptoKey{
		Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT, ImportOnly: true,
	}))
	require.NoError(t, err)
	assert.Nil(t, imp.GetPrimary())

	_, err = e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: imp.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestUpdateCryptoKey(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	next := timestamppb.New(time.Date(2030, 3, 1, 0, 0, 0, 0, time.UTC))

	tests := []struct {
		in    *kmspb.CryptoKey
		check func(t *testing.T, k *kmspb.CryptoKey)
		name  string
		paths []string
		want  codes.Code
	}{
		{
			name: "labels", in: &kmspb.CryptoKey{Labels: map[string]string{"a": "b"}}, paths: []string{"labels"},
			check: func(t *testing.T, k *kmspb.CryptoKey) {
				t.Helper()
				assert.Equal(t, map[string]string{"a": "b"}, k.GetLabels())
			},
		},
		{name: "no mask", in: &kmspb.CryptoKey{}, want: codes.InvalidArgument},
		{name: "immutable purpose", in: &kmspb.CryptoKey{}, paths: []string{"purpose"}, want: codes.InvalidArgument},
		{
			name: "rotation",
			in: &kmspb.CryptoKey{
				NextRotationTime: next,
				RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(72 * time.Hour)},
			},
			paths: []string{"rotation_period", "next_rotation_time"},
			check: func(t *testing.T, k *kmspb.CryptoKey) {
				t.Helper()
				assert.Equal(t, 72*time.Hour, k.GetRotationPeriod().AsDuration())
				assert.True(t, next.AsTime().Equal(k.GetNextRotationTime().AsTime()))
			},
		},
		{
			name: "period only derives next",
			in: &kmspb.CryptoKey{
				RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(48 * time.Hour)},
			},
			paths: []string{"rotation_period"},
			check: func(t *testing.T, k *kmspb.CryptoKey) {
				t.Helper()
				assert.True(t, time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC).Equal(k.GetNextRotationTime().AsTime()))
			},
		},
		{
			name: "bad period",
			in: &kmspb.CryptoKey{
				RotationSchedule: &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(time.Hour)},
			},
			paths: []string{"rotation_period"}, want: codes.InvalidArgument,
		},
		{
			name: "template level",
			in: &kmspb.CryptoKey{
				VersionTemplate: &kmspb.CryptoKeyVersionTemplate{ProtectionLevel: kmspb.ProtectionLevel_HSM},
			},
			paths: []string{"version_template.protection_level"},
			check: func(t *testing.T, k *kmspb.CryptoKey) {
				t.Helper()
				assert.Equal(t, kmspb.ProtectionLevel_HSM, k.GetVersionTemplate().GetProtectionLevel())
				assert.Equal(
					t,
					kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
					k.GetVersionTemplate().GetAlgorithm(),
				)
			},
		},
		{
			name: "template wrong algorithm",
			in: &kmspb.CryptoKey{
				VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_HMAC_SHA256},
			},
			paths: []string{"version_template.algorithm"}, want: codes.InvalidArgument,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := e.key(t, ring, fmt.Sprintf("k%d", i), kmspb.CryptoKey_ENCRYPT_DECRYPT,
				kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
			in := tt.in
			in.Name = key.GetName()

			out, err := e.client.UpdateCryptoKey(t.Context(), &kmspb.UpdateCryptoKeyRequest{
				CryptoKey: in, UpdateMask: mask(tt.paths...),
			})
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if tt.check != nil {
				tt.check(t, out)
			}
		})
	}

	_, err := e.client.UpdateCryptoKey(t.Context(), &kmspb.UpdateCryptoKeyRequest{
		CryptoKey: &kmspb.CryptoKey{Name: ring + "/cryptoKeys/none"}, UpdateMask: mask("labels"),
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestPrimaryVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION)
	mac := e.key(t, ring, "m", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)

	v2, err := e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()})
	require.NoError(t, err)

	_, err = e.client.UpdateCryptoKeyVersion(t.Context(), &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: v2.GetName(), State: kmspb.CryptoKeyVersion_DISABLED},
		UpdateMask:       mask("state"),
	})
	require.NoError(t, err)

	tests := []struct {
		name string
		key  string
		id   string
		want codes.Code
	}{
		{"disabled version", key.GetName(), "2", codes.FailedPrecondition},
		{"missing version", key.GetName(), "9", codes.NotFound},
		{"bad id", key.GetName(), "x", codes.InvalidArgument},
		{"mac has no primary", mac.GetName(), "1", codes.FailedPrecondition},
		{"missing key", ring + "/cryptoKeys/none", "1", codes.NotFound},
		{"same primary", key.GetName(), "1", codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, perr := e.client.UpdateCryptoKeyPrimaryVersion(t.Context(), &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
				Name: tt.key, CryptoKeyVersionId: tt.id,
			})
			assert.Equal(t, tt.want, status.Code(perr))
		})
	}
}

func TestVersionStates(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		func(k *kmspb.CryptoKey) { k.DestroyScheduledDuration = durationpb.New(48 * time.Hour) })

	v1 := key.GetPrimary().GetName()

	v2, err := e.client.CreateCryptoKeyVersion(t.Context(), &kmspb.CreateCryptoKeyVersionRequest{Parent: key.GetName()})
	require.NoError(t, err)

	enc, err := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: v2.GetName(), Plaintext: []byte("data")})
	require.NoError(t, err)

	setState := func(name string, s kmspb.CryptoKeyVersion_CryptoKeyVersionState) (*kmspb.CryptoKeyVersion, error) {
		return e.client.UpdateCryptoKeyVersion(t.Context(), &kmspb.UpdateCryptoKeyVersionRequest{
			CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: name, State: s}, UpdateMask: mask("state"),
		})
	}
	decrypt := func() error {
		_, cerr := e.client.Decrypt(
			t.Context(),
			&kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: enc.GetCiphertext()},
		)

		return cerr
	}

	require.NoError(t, decrypt())

	got, err := setState(v2.GetName(), kmspb.CryptoKeyVersion_DISABLED)
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DISABLED, got.GetState())
	assert.Equal(t, codes.FailedPrecondition, status.Code(decrypt()), "disabled version cannot decrypt")

	_, err = e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: v2.GetName(), Plaintext: []byte("x")})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = setState(v2.GetName(), kmspb.CryptoKeyVersion_ENABLED)
	require.NoError(t, err)
	require.NoError(t, decrypt())

	_, err = setState(v2.GetName(), kmspb.CryptoKeyVersion_DESTROYED)
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "destroyed is not settable")

	_, err = e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{Name: v1})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "primary cannot be destroyed")

	destroyed, err := e.client.DestroyCryptoKeyVersion(
		t.Context(),
		&kmspb.DestroyCryptoKeyVersionRequest{Name: v2.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DESTROY_SCHEDULED, destroyed.GetState())
	assert.True(t, e.clk.now().Add(48*time.Hour).Equal(destroyed.GetDestroyTime().AsTime()))
	assert.Equal(t, codes.FailedPrecondition, status.Code(decrypt()))

	_, err = setState(v2.GetName(), kmspb.CryptoKeyVersion_ENABLED)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "scheduled version cannot be enabled directly")

	restored, err := e.client.RestoreCryptoKeyVersion(
		t.Context(),
		&kmspb.RestoreCryptoKeyVersionRequest{Name: v2.GetName()},
	)
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DISABLED, restored.GetState())
	assert.Nil(t, restored.GetDestroyTime())

	_, err = e.client.RestoreCryptoKeyVersion(t.Context(), &kmspb.RestoreCryptoKeyVersionRequest{Name: v2.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = e.client.DestroyCryptoKeyVersion(t.Context(), &kmspb.DestroyCryptoKeyVersionRequest{Name: v2.GetName()})
	require.NoError(t, err)

	e.clk.advance(47 * time.Hour)

	pending, err := e.client.GetCryptoKeyVersion(t.Context(), &kmspb.GetCryptoKeyVersionRequest{Name: v2.GetName()})
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DESTROY_SCHEDULED, pending.GetState())

	e.clk.advance(2 * time.Hour)

	done, err := e.client.GetCryptoKeyVersion(t.Context(), &kmspb.GetCryptoKeyVersionRequest{Name: v2.GetName()})
	require.NoError(t, err)
	assert.Equal(t, kmspb.CryptoKeyVersion_DESTROYED, done.GetState())
	assert.NotNil(t, done.GetDestroyEventTime())
	assert.Equal(t, codes.FailedPrecondition, status.Code(decrypt()), "destroyed key material is gone")

	_, err = e.client.RestoreCryptoKeyVersion(t.Context(), &kmspb.RestoreCryptoKeyVersionRequest{Name: v2.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = e.client.GetCryptoKeyVersion(
		t.Context(),
		&kmspb.GetCryptoKeyVersionRequest{Name: key.GetName() + "/cryptoKeyVersions/9"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	versions := e.client.ListCryptoKeyVersions(
		t.Context(),
		&kmspb.ListCryptoKeyVersionsRequest{Parent: key.GetName(), PageSize: 1},
	)
	count := 0

	for {
		if _, err = versions.Next(); errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		count++
	}

	assert.Equal(t, 2, count)
}

func TestAutomaticRotation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	start := e.clk.now()

	key := e.key(t, ring, "k", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		func(k *kmspb.CryptoKey) {
			k.NextRotationTime = timestamppb.New(start.Add(2 * time.Hour))
			k.RotationSchedule = &kmspb.CryptoKey_RotationPeriod{RotationPeriod: durationpb.New(24 * time.Hour)}
		})

	enc, err := e.client.Encrypt(t.Context(), &kmspb.EncryptRequest{Name: key.GetName(), Plaintext: []byte("old")})
	require.NoError(t, err)

	e.clk.advance(time.Hour)

	got, err := e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	assert.Equal(t, key.GetName()+"/cryptoKeyVersions/1", got.GetPrimary().GetName(), "not yet due")

	e.clk.advance(time.Hour + time.Minute)

	got, err = e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	assert.Equal(t, key.GetName()+"/cryptoKeyVersions/2", got.GetPrimary().GetName())
	assert.True(t, start.Add(26*time.Hour).Equal(got.GetNextRotationTime().AsTime()))

	dec, err := e.client.Decrypt(
		t.Context(),
		&kmspb.DecryptRequest{Name: key.GetName(), Ciphertext: enc.GetCiphertext()},
	)
	require.NoError(t, err)
	assert.Equal(t, "old", string(dec.GetPlaintext()))
	assert.False(t, dec.GetUsedPrimary())

	e.clk.advance(72 * time.Hour)
	e.handler.Backend.Sweep()

	got, err = e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	assert.Equal(t, key.GetName()+"/cryptoKeyVersions/3", got.GetPrimary().GetName(), "one rotation per sweep")
	assert.True(t, got.GetNextRotationTime().AsTime().After(e.clk.now()))
}

func TestRotationWithoutPeriodIsOneShot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	key := e.key(t, e.ring(t, "r"), "k", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256,
		func(k *kmspb.CryptoKey) { k.NextRotationTime = timestamppb.New(e.clk.now().Add(time.Hour)) })

	e.clk.advance(2 * time.Hour)

	got, err := e.client.GetCryptoKey(t.Context(), &kmspb.GetCryptoKeyRequest{Name: key.GetName()})
	require.NoError(t, err)
	assert.Nil(t, got.GetNextRotationTime())
	assert.Nil(t, got.GetPrimary(), "non-symmetric keys have no primary")

	vs := e.client.ListCryptoKeyVersions(t.Context(), &kmspb.ListCryptoKeyVersionsRequest{Parent: key.GetName()})
	count := 0

	for {
		if _, err = vs.Next(); errors.Is(err, iterator.Done) {
			break
		}

		require.NoError(t, err)

		count++
	}

	assert.Equal(t, 2, count)
}

func TestIamPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ring := e.ring(t, "r")
	key := e.key(t, ring, "k", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)

	for _, resource := range []string{ring, key.GetName()} {
		h := e.client.ResourceIAM(resource)

		p, err := h.Policy(t.Context())
		require.NoError(t, err)
		assert.Empty(t, p.Members("roles/cloudkms.admin"))

		p.Add("user:a@example.com", "roles/cloudkms.admin")
		require.NoError(t, h.SetPolicy(t.Context(), p))

		got, err := e.client.ResourceIAM(resource).Policy(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"user:a@example.com"}, got.Members("roles/cloudkms.admin"))

		perms, err := h.TestPermissions(t.Context(), []string{"cloudkms.cryptoKeys.get"})
		require.NoError(t, err)
		assert.Equal(t, []string{"cloudkms.cryptoKeys.get"}, perms)
	}

	_, err := e.client.ResourceIAM(ring + "/cryptoKeys/none").Policy(t.Context())
	assert.Equal(t, codes.NotFound, status.Code(err))

	stale := &iampb.Policy{Etag: []byte("stale"), Bindings: []*iampb.Binding{{Role: "r", Members: []string{"user:b"}}}}
	_, err = e.handler.Backend.SetIamPolicy(&iampb.SetIamPolicyRequest{Resource: key.GetName(), Policy: stale})
	assert.Equal(t, codes.Aborted, status.Code(err))
}
