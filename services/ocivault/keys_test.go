package ocivault_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestCreateKeyShapes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "shapes")
	m := e.mgmt(t, v)

	tests := []struct {
		shape   *keymanagement.KeyShape
		name    string
		wantErr string
	}{
		{name: "aes128", shape: shape(keymanagement.KeyShapeAlgorithmAes, 16, "")},
		{name: "aes192", shape: shape(keymanagement.KeyShapeAlgorithmAes, 24, "")},
		{name: "aes256", shape: shape(keymanagement.KeyShapeAlgorithmAes, 32, "")},
		{name: "rsa2048", shape: shape(keymanagement.KeyShapeAlgorithmRsa, 256, "")},
		{name: "rsa3072", shape: shape(keymanagement.KeyShapeAlgorithmRsa, 384, "")},
		{name: "ecdsa256", shape: shape(keymanagement.KeyShapeAlgorithmEcdsa, 32, keymanagement.KeyShapeCurveIdP256)},
		{name: "ecdsa384", shape: shape(keymanagement.KeyShapeAlgorithmEcdsa, 48, keymanagement.KeyShapeCurveIdP384)},
		{name: "ecdsa521", shape: shape(keymanagement.KeyShapeAlgorithmEcdsa, 66, keymanagement.KeyShapeCurveIdP521)},
		{name: "aes bad length", shape: shape(keymanagement.KeyShapeAlgorithmAes, 20, ""), wantErr: "InvalidParameter"},
		{name: "rsa bad length", shape: shape(keymanagement.KeyShapeAlgorithmRsa, 32, ""), wantErr: "InvalidParameter"},
		{
			name:    "ecdsa curve mismatch",
			shape:   shape(keymanagement.KeyShapeAlgorithmEcdsa, 32, keymanagement.KeyShapeCurveIdP384),
			wantErr: "InvalidParameter",
		},
		{
			name:    "ecdsa no curve",
			shape:   shape(keymanagement.KeyShapeAlgorithmEcdsa, 48, ""),
			wantErr: "InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
				CompartmentId: new(testCompartment), DisplayName: new(tt.name), KeyShape: tt.shape,
			})
			if tt.wantErr != "" {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantErr, se.GetCode())
				assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			k := resp.Key
			assert.Equal(t, keymanagement.KeyLifecycleStateEnabling, k.LifecycleState)
			assert.Equal(t, keymanagement.KeyProtectionModeHsm, k.ProtectionMode)
			assert.Equal(t, tt.shape.Algorithm, k.KeyShape.Algorithm)
			assert.Equal(t, *tt.shape.Length, *k.KeyShape.Length)
			assert.Equal(t, *v.Id, *k.VaultId)
			assert.True(t, strings.HasPrefix(*k.Id, "ocid1.key.oc1."+oci.Region+"."))
			assert.True(t, strings.HasPrefix(*k.CurrentKeyVersion, "ocid1.keyversion.oc1."))

			ver, err := m.GetKeyVersion(
				t.Context(),
				keymanagement.GetKeyVersionRequest{KeyId: k.Id, KeyVersionId: k.CurrentKeyVersion},
			)
			require.NoError(t, err)
			assert.Equal(t, keymanagement.KeyVersionOriginInternal, ver.Origin)

			if tt.shape.Algorithm == keymanagement.KeyShapeAlgorithmAes {
				assert.Nil(t, ver.PublicKey)
			} else {
				assert.True(t, strings.HasPrefix(*ver.PublicKey, "-----BEGIN PUBLIC KEY-----"))
			}
		})
	}
}

func TestProtectionModeStored(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	m := e.mgmt(t, e.mkVault(t, "modes"))

	soft := e.mkKey(t, m, "soft", aes256(), keymanagement.CreateKeyDetailsProtectionModeSoftware)
	assert.Equal(t, keymanagement.KeyProtectionModeSoftware, soft.ProtectionMode)

	_, err := m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("ext"), KeyShape: aes256(),
		ProtectionMode: keymanagement.CreateKeyDetailsProtectionModeExternal})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	list, err := m.ListKeys(t.Context(), keymanagement.ListKeysRequest{
		CompartmentId: new(testCompartment), ProtectionMode: keymanagement.ListKeysProtectionModeSoftware,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "soft", *list.Items[0].DisplayName)
}

func TestKeyStateMachine(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	m := e.mgmt(t, e.mkVault(t, "states"))
	k := e.mkKey(t, m, "k", aes256(), "")

	get := func() keymanagement.Key {
		r, err := m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: k.Id})
		require.NoError(t, err)

		return r.Key
	}

	assert.Equal(t, keymanagement.KeyLifecycleStateEnabled, get().LifecycleState)

	dis, err := m.DisableKey(ctx, keymanagement.DisableKeyRequest{KeyId: k.Id})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.KeyLifecycleStateDisabling, dis.LifecycleState)
	assert.Equal(t, keymanagement.KeyLifecycleStateDisabled, get().LifecycleState)

	_, err = m.DisableKey(ctx, keymanagement.DisableKeyRequest{KeyId: k.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = m.EnableKey(ctx, keymanagement.EnableKeyRequest{KeyId: k.Id})
	require.NoError(t, err)

	at := common.SDKTime{Time: e.clock.Now().Add(8 * 24 * time.Hour)}
	_, err = m.ScheduleKeyDeletion(ctx, keymanagement.ScheduleKeyDeletionRequest{
		KeyId: k.Id, TimeOfDeletion: &at,
	})
	require.NoError(t, err)
	assert.Equal(t, keymanagement.KeyLifecycleStatePendingDeletion, get().LifecycleState)

	_, err = m.EnableKey(ctx, keymanagement.EnableKeyRequest{KeyId: k.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = m.CancelKeyDeletion(ctx, keymanagement.CancelKeyDeletionRequest{KeyId: k.Id})
	require.NoError(t, err)
	got := get()
	assert.Equal(t, keymanagement.KeyLifecycleStateEnabled, got.LifecycleState)
	assert.Nil(t, got.TimeOfDeletion)

	_, err = m.ScheduleKeyDeletion(ctx, keymanagement.ScheduleKeyDeletionRequest{KeyId: k.Id})
	require.NoError(t, err)
	e.clock.Advance(31 * 24 * time.Hour)
	assert.Equal(t, keymanagement.KeyLifecycleStateDeleted, get().LifecycleState)
}

func TestKeyUpdateAndList(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	m := e.mgmt(t, e.mkVault(t, "list"))

	a := e.mkKey(t, m, "a", aes256(), "")
	e.clock.Advance(time.Second)
	e.mkKey(t, m, "b", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")

	upd, err := m.UpdateKey(ctx, keymanagement.UpdateKeyRequest{
		KeyId: a.Id, IfMatch: nil,
		DisplayName: new("a2"), FreeformTags: map[string]string{"x": "y"},
	})
	require.NoError(t, err)
	assert.Equal(t, "a2", *upd.DisplayName)

	tests := []struct {
		req  keymanagement.ListKeysRequest
		name string
		want []string
	}{
		{keymanagement.ListKeysRequest{}, "all newest first", []string{"b", "a2"}},
		{keymanagement.ListKeysRequest{Algorithm: keymanagement.ListKeysAlgorithmRsa}, "rsa only", []string{"b"}},
		{keymanagement.ListKeysRequest{Length: new(32)}, "length 32", []string{"a2"}},
		{
			keymanagement.ListKeysRequest{
				SortBy:    keymanagement.ListKeysSortByDisplayname,
				SortOrder: keymanagement.ListKeysSortOrderAsc,
			},
			"by name",
			[]string{"a2", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := tt.req
			req.CompartmentId = new(testCompartment)

			resp, cerr := m.ListKeys(ctx, req)
			require.NoError(t, cerr)

			var got []string
			for _, k := range resp.Items {
				got = append(got, *k.DisplayName)
			}

			assert.Equal(t, tt.want, got)
		})
	}

	_, err = m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: new("ocid1.key.oc1..none")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestKeyVersions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	m := e.mgmt(t, e.mkVault(t, "versions"))
	k := e.mkKey(t, m, "k", shape(keymanagement.KeyShapeAlgorithmEcdsa, 32, keymanagement.KeyShapeCurveIdP256), "")

	v2, err := m.CreateKeyVersion(ctx, keymanagement.CreateKeyVersionRequest{KeyId: k.Id})
	require.NoError(t, err)
	assert.NotEqual(t, *k.CurrentKeyVersion, *v2.Id)
	assert.NotEmpty(t, *v2.PublicKey)

	got, err := m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: k.Id})
	require.NoError(t, err)
	assert.Equal(t, *v2.Id, *got.CurrentKeyVersion)

	list, err := m.ListKeyVersions(ctx, keymanagement.ListKeyVersionsRequest{KeyId: k.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	_, err = m.ScheduleKeyVersionDeletion(
		ctx,
		keymanagement.ScheduleKeyVersionDeletionRequest{KeyId: k.Id, KeyVersionId: v2.Id},
	)
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "current version cannot be deleted")

	old := k.CurrentKeyVersion
	sched, err := m.ScheduleKeyVersionDeletion(
		ctx,
		keymanagement.ScheduleKeyVersionDeletionRequest{KeyId: k.Id, KeyVersionId: old},
	)
	require.NoError(t, err)
	assert.Equal(t, keymanagement.KeyVersionLifecycleStateSchedulingDeletion, sched.LifecycleState)

	_, err = m.CancelKeyVersionDeletion(
		ctx,
		keymanagement.CancelKeyVersionDeletionRequest{KeyId: k.Id, KeyVersionId: old},
	)
	require.NoError(t, err)

	_, err = m.ScheduleKeyVersionDeletion(
		ctx,
		keymanagement.ScheduleKeyVersionDeletionRequest{KeyId: k.Id, KeyVersionId: old},
	)
	require.NoError(t, err)
	e.clock.Advance(31 * 24 * time.Hour)

	list, err = m.ListKeyVersions(ctx, keymanagement.ListKeyVersionsRequest{KeyId: k.Id})
	require.NoError(t, err)

	states := map[string]keymanagement.KeyVersionSummaryLifecycleStateEnum{}
	for _, s := range list.Items {
		states[*s.Id] = s.LifecycleState
	}

	assert.Equal(t, keymanagement.KeyVersionSummaryLifecycleStateDeleted, states[*old])
}

func TestAutoRotation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	m := e.mgmt(t, e.mkVault(t, "rot"))

	resp, err := m.CreateKey(ctx, keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("auto"), KeyShape: aes256(),
		IsAutoRotationEnabled:  new(true),
		AutoKeyRotationDetails: &keymanagement.AutoKeyRotationDetails{RotationIntervalInDays: new(60)}})
	require.NoError(t, err)
	require.NotNil(t, resp.AutoKeyRotationDetails.TimeOfNextRotation)

	_, err = m.CreateKey(ctx, keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("bad"), KeyShape: aes256(),
		IsAutoRotationEnabled:  new(true),
		AutoKeyRotationDetails: &keymanagement.AutoKeyRotationDetails{RotationIntervalInDays: new(5)}})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	e.clock.Advance(61 * 24 * time.Hour)

	got, err := m.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: resp.Id})
	require.NoError(t, err)
	assert.NotEqual(t, *resp.CurrentKeyVersion, *got.CurrentKeyVersion)
	assert.Equal(
		t,
		keymanagement.AutoKeyRotationDetailsLastRotationStatusSuccess,
		got.AutoKeyRotationDetails.LastRotationStatus,
	)

	list, err := m.ListKeyVersions(ctx, keymanagement.ListKeyVersionsRequest{KeyId: resp.Id})
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	off, err := m.UpdateKey(ctx, keymanagement.UpdateKeyRequest{
		KeyId: resp.Id, IsAutoRotationEnabled: new(false),
	})
	require.NoError(t, err)
	assert.False(t, *off.IsAutoRotationEnabled)
}
