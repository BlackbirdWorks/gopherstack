package ocivault_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

type secretEnv struct {
	e   *env
	v   keymanagement.Vault
	key keymanagement.Key
	m   keymanagement.KmsManagementClient
}

func newSecretEnv(t *testing.T) *secretEnv {
	t.Helper()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "secrets")
	m := e.mgmt(t, v)

	return &secretEnv{e: e, v: v, m: m, key: e.mkKey(t, m, "master", aes256(), "")}
}

func content(s string) vault.Base64SecretContentDetails {
	return vault.Base64SecretContentDetails{Content: new(b64([]byte(s)))}
}

func (se *secretEnv) create(t *testing.T, name, body string, mut func(*vault.CreateSecretDetails)) vault.Secret {
	t.Helper()

	d := vault.CreateSecretDetails{
		CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id, SecretName: new(name),
		SecretContent: content(body),
	}
	if mut != nil {
		mut(&d)
	}

	r, err := se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{CreateSecretDetails: d})
	require.NoError(t, err)

	return r.Secret
}

func (se *secretEnv) update(t *testing.T, id *string, d vault.UpdateSecretDetails) vault.Secret {
	t.Helper()

	r, err := se.e.vsecret.UpdateSecret(t.Context(), vault.UpdateSecretRequest{SecretId: id, UpdateSecretDetails: d})
	require.NoError(t, err)

	return r.Secret
}

func (se *secretEnv) bundle(
	t *testing.T,
	id *string,
	mut func(*secrets.GetSecretBundleRequest),
) (secrets.SecretBundle, error) {
	t.Helper()

	req := secrets.GetSecretBundleRequest{SecretId: id}
	if mut != nil {
		mut(&req)
	}

	r, err := se.e.bundles.GetSecretBundle(t.Context(), req)

	return r.SecretBundle, err
}

func bundleText(t *testing.T, b secrets.SecretBundle) string {
	t.Helper()

	c, ok := b.SecretBundleContent.(secrets.Base64SecretBundleContentDetails)
	require.True(t, ok)

	return string(unb64(t, *c.Content))
}

func TestSecretCreateAndRetrieve(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()

	s := se.create(t, "db-password", "hunter2", func(d *vault.CreateSecretDetails) {
		d.Description = new("prod db")
		d.FreeformTags = map[string]string{"team": "core"}
		d.Metadata = map[string]any{"owner": "ops"}
	})
	assert.Equal(t, vault.SecretLifecycleStateCreating, s.LifecycleState)
	assert.True(t, strings.HasPrefix(*s.Id, "ocid1.vaultsecret.oc1."+oci.Region+"."))
	assert.EqualValues(t, 1, *s.CurrentVersionNumber)

	got, err := se.e.vsecret.GetSecret(ctx, vault.GetSecretRequest{SecretId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, vault.SecretLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, "prod db", *got.Description)
	assert.Equal(t, *se.key.Id, *got.KeyId)
	assert.Equal(t, "ops", got.Metadata["owner"])

	b, err := se.bundle(t, s.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "hunter2", bundleText(t, b))
	assert.EqualValues(t, 1, *b.VersionNumber)
	assert.Equal(
		t,
		[]secrets.SecretBundleStagesEnum{secrets.SecretBundleStagesCurrent, secrets.SecretBundleStagesLatest},
		b.Stages,
	)
	assert.Equal(t, "ops", b.Metadata["owner"])

	byName, err := se.e.bundles.GetSecretBundleByName(
		ctx,
		secrets.GetSecretBundleByNameRequest{SecretName: new("db-password"), VaultId: se.v.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "hunter2", bundleText(t, byName.SecretBundle))
}

func TestSecretContentEncryptedAtRest(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	s := se.create(t, "at-rest", "plain-marker-value", nil)

	snap := string(se.e.backend.Snapshot(t.Context()))
	assert.NotContains(t, snap, "plain-marker-value")
	assert.NotContains(t, snap, b64([]byte("plain-marker-value")))

	restored := newEnv(t, envOpts{})
	require.NoError(t, restored.backend.Restore(t.Context(), []byte(snap)))

	r := &secretEnv{e: restored, v: se.v, key: se.key}
	b, err := r.bundle(t, s.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "plain-marker-value", bundleText(t, b))
}

func stagedSecret(t *testing.T) (*secretEnv, vault.Secret) {
	t.Helper()

	se := newSecretEnv(t)
	s := se.create(t, "rotating", "v1", nil)

	se.e.clock.Advance(time.Second)
	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("v2")})
	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("v3")})
	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: vault.Base64SecretContentDetails{
		Content: new(b64([]byte("v4"))), Stage: vault.SecretContentDetailsStagePending,
	}})

	return se, s
}

func TestSecretVersionStages(t *testing.T) {
	t.Parallel()

	se, s := stagedSecret(t)

	list, err := se.e.vsecret.ListSecretVersions(t.Context(), vault.ListSecretVersionsRequest{SecretId: s.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 4)

	want := map[int64][]vault.SecretVersionSummaryStagesEnum{
		1: {vault.SecretVersionSummaryStagesDeprecated},
		2: {vault.SecretVersionSummaryStagesPrevious},
		3: {vault.SecretVersionSummaryStagesCurrent},
		4: {vault.SecretVersionSummaryStagesPending, vault.SecretVersionSummaryStagesLatest},
	}
	for _, it := range list.Items {
		assert.Equal(t, want[*it.VersionNumber], it.Stages, "version %d", *it.VersionNumber)
	}

	summaries, err := se.e.bundles.ListSecretBundleVersions(t.Context(), secrets.ListSecretBundleVersionsRequest{
		SecretId: s.Id, SortOrder: secrets.ListSecretBundleVersionsSortOrderDesc,
	})
	require.NoError(t, err)
	require.Len(t, summaries.Items, 4)
	assert.EqualValues(t, 4, *summaries.Items[0].VersionNumber)
}

func TestSecretBundleSelectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(*secrets.GetSecretBundleRequest)
		name string
		want string
	}{
		{nil, "default current", "v3"},
		{func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStagePrevious }, "previous", "v2"},
		{func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStagePending }, "pending", "v4"},
		{func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStageLatest }, "latest", "v4"},
		{
			func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStageDeprecated },
			"deprecated",
			"v1",
		},
		{func(r *secrets.GetSecretBundleRequest) { r.VersionNumber = new(int64(1)) }, "by number", "v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se, s := stagedSecret(t)

			b, err := se.bundle(t, s.Id, tt.mut)
			require.NoError(t, err)
			assert.Equal(t, tt.want, bundleText(t, b))
		})
	}
}

func TestSecretPromotionAndRollback(t *testing.T) {
	t.Parallel()

	se, s := stagedSecret(t)

	promoted := se.update(t, s.Id, vault.UpdateSecretDetails{CurrentVersionNumber: new(int64(4))})
	assert.EqualValues(t, 4, *promoted.CurrentVersionNumber)

	b, err := se.bundle(
		t,
		s.Id,
		func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStagePrevious },
	)
	require.NoError(t, err)
	assert.Equal(t, "v3", bundleText(t, b))

	rollback := se.update(t, s.Id, vault.UpdateSecretDetails{CurrentVersionNumber: new(int64(3))})
	assert.EqualValues(t, 3, *rollback.CurrentVersionNumber)

	ver, err := se.e.vsecret.GetSecretVersion(t.Context(), vault.GetSecretVersionRequest{
		SecretId: s.Id, SecretVersionNumber: new(int64(3)),
	})
	require.NoError(t, err)
	assert.Contains(t, ver.Stages, vault.SecretVersionStagesCurrent)

	_, err = se.e.vsecret.UpdateSecret(t.Context(), vault.UpdateSecretRequest{
		SecretId: s.Id, CurrentVersionNumber: new(int64(99)),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSecretVersionNameSelection(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	s := se.create(t, "named", "one", func(d *vault.CreateSecretDetails) {
		d.SecretContent = vault.Base64SecretContentDetails{Content: new(b64([]byte("one"))), Name: new("first")}
	})
	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("two")})

	b, err := se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) { r.SecretVersionName = new("first") })
	require.NoError(t, err)
	assert.Equal(t, "one", bundleText(t, b))
	assert.Equal(t, "first", *b.VersionName)

	_, err = se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) {
		r.SecretVersionName = new("first")
		r.Stage = secrets.GetSecretBundleStageCurrent
	})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) { r.SecretVersionName = new("nope") })
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSecretCreateValidation(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	se.create(t, "taken", "x", nil)

	rsaKey := se.e.mkKey(t, se.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")
	disabled := se.e.mkKey(t, se.m, "dis", aes256(), "")
	_, err := se.m.DisableKey(t.Context(), keymanagement.DisableKeyRequest{KeyId: disabled.Id})
	require.NoError(t, err)

	otherVault := se.e.mkVault(t, "other")
	foreign := se.e.mkKey(t, se.e.mgmt(t, otherVault), "foreign", aes256(), "")

	huge := make([]byte, 25*1024+1)

	tests := []struct {
		mut      func(*vault.CreateSecretDetails)
		name     string
		wantCode string
		status   int
	}{
		{
			func(d *vault.CreateSecretDetails) { d.SecretName = new("taken") },
			"duplicate name",
			"Conflict",
			http.StatusConflict,
		},
		{
			func(d *vault.CreateSecretDetails) { d.SecretName = new("bad name!") },
			"bad name",
			"InvalidParameter",
			http.StatusBadRequest,
		},
		{
			func(d *vault.CreateSecretDetails) { d.KeyId = rsaKey.Id },
			"rsa key",
			"InvalidParameter",
			http.StatusBadRequest,
		},
		{
			func(d *vault.CreateSecretDetails) { d.KeyId = disabled.Id },
			"disabled key",
			"IncorrectState",
			http.StatusConflict,
		},
		{
			func(d *vault.CreateSecretDetails) { d.KeyId = foreign.Id },
			"key from other vault",
			"NotAuthorizedOrNotFound",
			http.StatusNotFound,
		},
		{
			func(d *vault.CreateSecretDetails) { d.KeyId = new("ocid1.key.oc1..none") },
			"unknown key",
			"NotAuthorizedOrNotFound",
			http.StatusNotFound,
		},
		{
			func(d *vault.CreateSecretDetails) { d.VaultId = new("ocid1.vault.oc1..none") },
			"unknown vault",
			"NotAuthorizedOrNotFound",
			http.StatusNotFound,
		},
		{func(d *vault.CreateSecretDetails) {
			d.SecretContent = vault.Base64SecretContentDetails{Content: new("***")}
		}, "content not base64", "InvalidParameter", http.StatusBadRequest},
		{func(d *vault.CreateSecretDetails) {
			d.SecretContent = vault.Base64SecretContentDetails{Content: new(b64(huge))}
		}, "content too large", "InvalidParameter", http.StatusBadRequest},
		{func(d *vault.CreateSecretDetails) {
			d.RotationConfig = &vault.RotationConfig{}
		}, "rotation unsupported", "InvalidParameter", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := vault.CreateSecretDetails{
				CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id,
				SecretName: new("fresh-" + strings.ReplaceAll(tt.name, " ", "-")), SecretContent: content("x"),
			}
			tt.mut(&d)

			_, cerr := se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{CreateSecretDetails: d})
			s := svcErr(t, cerr)
			assert.Equal(t, tt.status, s.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, s.GetCode())
		})
	}
}

func TestSecretDeletionLifecycle(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()
	s := se.create(t, "ephemeral", "bye", nil)

	state := func() vault.SecretLifecycleStateEnum {
		r, err := se.e.vsecret.GetSecret(ctx, vault.GetSecretRequest{SecretId: s.Id})
		require.NoError(t, err)

		return r.LifecycleState
	}

	at := common.SDKTime{Time: se.e.clock.Now().Add(2 * 24 * time.Hour)}
	_, err := se.e.vsecret.ScheduleSecretDeletion(ctx, vault.ScheduleSecretDeletionRequest{
		SecretId: s.Id, TimeOfDeletion: &at,
	})
	require.NoError(t, err)
	assert.Equal(t, vault.SecretLifecycleStatePendingDeletion, state())

	_, err = se.bundle(t, s.Id, nil)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = se.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{SecretId: s.Id, Description: new("x")})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = se.e.vsecret.CancelSecretDeletion(ctx, vault.CancelSecretDeletionRequest{SecretId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, vault.SecretLifecycleStateActive, state())

	b, err := se.bundle(t, s.Id, nil)
	require.NoError(t, err)
	assert.Equal(t, "bye", bundleText(t, b))

	_, err = se.e.vsecret.ScheduleSecretDeletion(ctx, vault.ScheduleSecretDeletionRequest{SecretId: s.Id})
	require.NoError(t, err)

	se.e.clock.Advance(31 * 24 * time.Hour)
	assert.Equal(t, vault.SecretLifecycleStateDeleted, state())

	reused := se.create(t, "ephemeral", "again", nil)
	assert.NotEqual(t, *s.Id, *reused.Id)

	_, err = se.e.vsecret.CancelSecretDeletion(ctx, vault.CancelSecretDeletionRequest{SecretId: s.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())
}

func TestSecretVersionDeletion(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()
	s := se.create(t, "versions", "v1", nil)
	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("v2")})

	_, err := se.e.vsecret.ScheduleSecretVersionDeletion(
		ctx,
		vault.ScheduleSecretVersionDeletionRequest{SecretId: s.Id, SecretVersionNumber: new(int64(2))},
	)
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "current version")

	_, err = se.e.vsecret.ScheduleSecretVersionDeletion(
		ctx,
		vault.ScheduleSecretVersionDeletionRequest{SecretId: s.Id, SecretVersionNumber: new(int64(1))},
	)
	require.NoError(t, err)

	ver, err := se.e.vsecret.GetSecretVersion(
		ctx,
		vault.GetSecretVersionRequest{SecretId: s.Id, SecretVersionNumber: new(int64(1))},
	)
	require.NoError(t, err)
	assert.NotNil(t, ver.TimeOfDeletion)

	_, err = se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) { r.VersionNumber = new(int64(1)) })
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = se.e.vsecret.CancelSecretVersionDeletion(
		ctx,
		vault.CancelSecretVersionDeletionRequest{SecretId: s.Id, SecretVersionNumber: new(int64(1))},
	)
	require.NoError(t, err)

	b, err := se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) { r.VersionNumber = new(int64(1)) })
	require.NoError(t, err)
	assert.Equal(t, "v1", bundleText(t, b))

	_, err = se.e.vsecret.ScheduleSecretVersionDeletion(
		ctx,
		vault.ScheduleSecretVersionDeletionRequest{SecretId: s.Id, SecretVersionNumber: new(int64(1))},
	)
	require.NoError(t, err)
	se.e.clock.Advance(31 * 24 * time.Hour)

	list, err := se.e.vsecret.ListSecretVersions(ctx, vault.ListSecretVersionsRequest{SecretId: s.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.EqualValues(t, 2, *list.Items[0].VersionNumber)
}

func TestSecretRules(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()

	reuse := vault.SecretReuseRule{IsEnforcedOnDeletedSecretVersions: new(false)}
	s := se.create(t, "ruled", "same", func(d *vault.CreateSecretDetails) {
		d.SecretRules = []vault.SecretRule{reuse, vault.SecretExpiryRule{
			SecretVersionExpiryInterval: new("P30D"), IsSecretContentRetrievalBlockedOnExpiry: new(true),
		}}
	})
	require.Len(t, s.SecretRules, 2)
	require.NotNil(t, s.TimeOfCurrentVersionExpiry)
	assert.WithinDuration(t, se.e.clock.Now().Add(30*24*time.Hour), s.TimeOfCurrentVersionExpiry.Time, time.Minute)

	_, err := se.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{SecretId: s.Id, SecretContent: content("same")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("different")})

	got, err := se.e.vsecret.GetSecret(ctx, vault.GetSecretRequest{SecretId: s.Id})
	require.NoError(t, err)
	assert.Len(t, got.SecretRules, 2)

	se.e.clock.Advance(31 * 24 * time.Hour)

	_, err = se.bundle(t, s.Id, nil)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "expired content is blocked")

	bad := vault.CreateSecretDetails{
		CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id, SecretName: new("bad-rule"),
		SecretRules: []vault.SecretRule{vault.SecretExpiryRule{SecretVersionExpiryInterval: new("soon")}},
	}
	_, err = se.e.vsecret.CreateSecret(ctx, vault.CreateSecretRequest{CreateSecretDetails: bad})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestSecretKeyRotationKeepsOldVersionsReadable(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	s := se.create(t, "rot", "before", nil)

	_, err := se.m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: se.key.Id})
	require.NoError(t, err)

	se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("after")})

	for _, c := range []struct {
		stage secrets.GetSecretBundleStageEnum
		want  string
	}{{secrets.GetSecretBundleStageCurrent, "after"}, {secrets.GetSecretBundleStagePrevious, "before"}} {
		b, cerr := se.bundle(t, s.Id, func(r *secrets.GetSecretBundleRequest) { r.Stage = c.stage })
		require.NoError(t, cerr)
		assert.Equal(t, c.want, bundleText(t, b))
	}

	_, err = se.m.DisableKey(t.Context(), keymanagement.DisableKeyRequest{KeyId: se.key.Id})
	require.NoError(t, err)

	_, err = se.bundle(t, s.Id, nil)
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "key must be usable to read content")
}

func TestListSecrets(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()

	for _, n := range []string{"c", "a", "b"} {
		se.create(t, n, n, nil)
		se.e.clock.Advance(time.Second)
	}

	gone := se.create(t, "gone", "g", nil)
	_, err := se.e.vsecret.ScheduleSecretDeletion(ctx, vault.ScheduleSecretDeletionRequest{SecretId: gone.Id})
	require.NoError(t, err)

	tests := []struct {
		req  vault.ListSecretsRequest
		name string
		want []string
	}{
		{
			vault.ListSecretsRequest{LifecycleState: vault.SecretSummaryLifecycleStateActive},
			"active newest first",
			[]string{"b", "a", "c"},
		},
		{
			vault.ListSecretsRequest{
				SortBy:         vault.ListSecretsSortByName,
				SortOrder:      vault.ListSecretsSortOrderAsc,
				LifecycleState: vault.SecretSummaryLifecycleStateActive,
			},
			"by name",
			[]string{"a", "b", "c"},
		},
		{vault.ListSecretsRequest{Name: new("a")}, "by exact name", []string{"a"}},
		{
			vault.ListSecretsRequest{LifecycleState: vault.SecretSummaryLifecycleStatePendingDeletion},
			"pending",
			[]string{"gone"},
		},
		{vault.ListSecretsRequest{VaultId: new("ocid1.vault.oc1..other")}, "other vault", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := tt.req
			req.CompartmentId = new(testCompartment)
			req.Limit = new(2)

			var got []string

			for {
				r, cerr := se.e.vsecret.ListSecrets(ctx, req)
				require.NoError(t, cerr)

				for _, it := range r.Items {
					got = append(got, *it.SecretName)
				}

				if r.OpcNextPage == nil {
					break
				}

				req.Page = r.OpcNextPage
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSecretIfMatchAndCompartmentMove(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	ctx := t.Context()
	s := se.create(t, "etag", "x", nil)

	got, err := se.e.vsecret.GetSecret(ctx, vault.GetSecretRequest{SecretId: s.Id})
	require.NoError(t, err)
	require.NotNil(t, got.Etag)

	_, err = se.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{
		SecretId: s.Id, IfMatch: new("bogus"), Description: new("n"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = se.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{
		SecretId: s.Id, IfMatch: got.Etag, Description: new("n"),
	})
	require.NoError(t, err)

	_, err = se.e.vsecret.ChangeSecretCompartment(ctx, vault.ChangeSecretCompartmentRequest{
		SecretId:      s.Id,
		CompartmentId: new("ocid1.compartment.oc1..moved"),
	})
	require.NoError(t, err)

	moved, err := se.e.vsecret.GetSecret(ctx, vault.GetSecretRequest{SecretId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..moved", *moved.CompartmentId)

	raw, err := json.Marshal(moved.Secret)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "currentVersionNumber")
}

func TestSecretOnPendingDeletionVault(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	_, err := se.e.vaults.ScheduleVaultDeletion(
		t.Context(),
		keymanagement.ScheduleVaultDeletionRequest{VaultId: se.v.Id},
	)
	require.NoError(t, err)

	_, err = se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{
		CompartmentId: new(
			testCompartment,
		),
		VaultId:       se.v.Id,
		KeyId:         se.key.Id,
		SecretName:    new("late"),
		SecretContent: content("x"),
	})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = se.m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new("late"), KeyShape: aes256()})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())
}
