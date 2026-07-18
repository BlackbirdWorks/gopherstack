package iam_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *iam.InMemoryBackend) string
		verify func(t *testing.T, b *iam.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *iam.InMemoryBackend) string {
				user, err := b.CreateUser("testuser", "/", "")
				if err != nil {
					return ""
				}

				return user.UserName
			},
			verify: func(t *testing.T, b *iam.InMemoryBackend, id string) {
				t.Helper()

				user, err := b.GetUser(id)
				require.NoError(t, err)
				assert.Equal(t, id, user.UserName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *iam.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *iam.InMemoryBackend, _ string) {
				t.Helper()

				users, err := b.ListUsers("", 0)
				require.NoError(t, err)
				assert.Empty(t, users.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := iam.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := iam.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_FullStateSnapshotRestore exercises a full-state
// Snapshot->Restore round trip covering every resource converted to
// pkgs/store in Phase 3.3 (users, roles, policies + versions/attachments,
// groups + membership, access keys, instance profiles + role attachment,
// SAML/OIDC providers, login profiles, service-specific credentials, virtual
// MFA devices, signing/server certificates, delegation requests) plus
// representative comprehensiveBackend state (SSH keys) and account-level
// state (password policy, account alias) that must NOT regress across the
// datalayer swap.
func TestInMemoryBackend_FullStateSnapshotRestore(t *testing.T) {
	t.Parallel()

	const testPolicyDoc = `{"Version":"2012-10-17",` +
		`"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	ctx := t.Context()
	b := iam.NewInMemoryBackend()

	// ---- Core entities ----
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	role, err := b.CreateRole("app-role", "/", testPolicyDoc, "")
	require.NoError(t, err)

	policy, err := b.CreatePolicy("app-policy", "/", testPolicyDoc)
	require.NoError(t, err)

	_, err = b.CreatePolicyVersion(policy.Arn, testPolicyDoc, false)
	require.NoError(t, err)

	require.NoError(t, b.AttachUserPolicy("alice", policy.Arn))
	require.NoError(t, b.AttachRolePolicy(role.RoleName, policy.Arn))

	_, err = b.CreateGroup("app-group", "/")
	require.NoError(t, err)
	require.NoError(t, b.AddUserToGroup("app-group", "alice"))

	ak, err := b.CreateAccessKey("alice")
	require.NoError(t, err)

	ip, err := b.CreateInstanceProfile("app-profile", "/")
	require.NoError(t, err)
	require.NoError(t, b.AddRoleToInstanceProfile(ip.InstanceProfileName, role.RoleName))

	_, err = b.CreateSAMLProvider("saml-idp", `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"/>`)
	require.NoError(t, err)

	_, err = b.CreateOpenIDConnectProvider(
		"https://token.actions.githubusercontent.com",
		[]string{"sts.amazonaws.com"},
		[]string{strings.Repeat("a", 40)},
	)
	require.NoError(t, err)

	_, err = b.CreateLoginProfile("alice", "P@ssw0rd123!", false)
	require.NoError(t, err)

	cred, err := b.CreateServiceSpecificCredential("alice", "codecommit.amazonaws.com")
	require.NoError(t, err)

	mfa, err := b.CreateVirtualMFADevice("alice-mfa", "/")
	require.NoError(t, err)

	_, err = b.UploadSigningCertificate("alice", "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----")
	require.NoError(t, err)

	_, err = b.UploadServerCertificate("app-cert", "/", "body", "chain")
	require.NoError(t, err)

	delegation, err := b.CreateDelegationRequest("123456789012")
	require.NoError(t, err)

	require.NoError(t, b.CreateAccountAlias("acme-corp"))
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{MinimumPasswordLength: 12}))

	// ---- comprehensiveBackend state (separate mutex; see gopherstack-gjp) ----
	sshKey, err := b.UploadSSHPublicKey("alice", strings.Repeat("k", 20))
	require.NoError(t, err)

	b.RecordServiceAccess(role.Arn, "s3", "s3.amazonaws.com")

	// ---- Snapshot / Restore round trip ----
	snap := b.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := iam.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	// ---- Verify every converted table survived ----
	gotUser, err := fresh.GetUser("alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", gotUser.UserName)

	gotRole, err := fresh.GetRole(role.RoleName)
	require.NoError(t, err)
	assert.Equal(t, role.Arn, gotRole.Arn)

	gotPolicy, err := fresh.GetPolicy(policy.Arn)
	require.NoError(t, err)
	assert.Equal(t, policy.PolicyName, gotPolicy.PolicyName)

	versions, err := fresh.ListPolicyVersions(policy.Arn)
	require.NoError(t, err)
	assert.Len(t, versions, 2) // v1 (implicit) + the version created above

	attachedToUser, err := fresh.GetPoliciesForUser("alice")
	require.NoError(t, err)
	assert.NotEmpty(t, attachedToUser)

	gotGroup, err := fresh.GetGroup("app-group")
	require.NoError(t, err)
	assert.Equal(t, "app-group", gotGroup.GroupName)

	groupUsers, err := fresh.GetGroupUsers("app-group")
	require.NoError(t, err)
	require.Len(t, groupUsers, 1)
	assert.Equal(t, "alice", groupUsers[0].UserName)

	gotAK, err := fresh.GetUserByAccessKeyID(ak.AccessKeyID)
	require.NoError(t, err)
	assert.Equal(t, "alice", gotAK.UserName)

	gotIP, err := fresh.GetInstanceProfile(ip.InstanceProfileName)
	require.NoError(t, err)
	require.Len(t, gotIP.Roles, 1)
	assert.Equal(t, role.RoleName, gotIP.Roles[0])

	samlProviders, err := fresh.ListSAMLProviders()
	require.NoError(t, err)
	assert.Len(t, samlProviders, 1)

	oidcProviders, err := fresh.ListOpenIDConnectProviders()
	require.NoError(t, err)
	assert.Len(t, oidcProviders, 1)

	gotLP, err := fresh.GetLoginProfile("alice")
	require.NoError(t, err)
	assert.Equal(t, "alice", gotLP.UserName)

	creds, err := fresh.ListServiceSpecificCredentials("alice", "")
	require.NoError(t, err)
	require.Len(t, creds, 1)
	assert.Equal(t, cred.ServiceSpecificCredentialID, creds[0].ServiceSpecificCredentialID)

	mfaDevices, err := fresh.ListVirtualMFADevices("", 0)
	require.NoError(t, err)
	require.Len(t, mfaDevices.Data, 1)
	assert.Equal(t, mfa.SerialNumber, mfaDevices.Data[0].SerialNumber)

	signingCerts, err := fresh.ListSigningCertificates("alice")
	require.NoError(t, err)
	assert.Len(t, signingCerts, 1)

	serverCerts, err := fresh.ListServerCertificates("")
	require.NoError(t, err)
	assert.Len(t, serverCerts, 1)

	require.NoError(t, fresh.AcceptDelegationRequest(delegation.DelegationID))

	// ---- Verify raw (un-converted) relation/index state survived ----
	aliases := fresh.ListAccountAliases()
	assert.Contains(t, aliases, "acme-corp")

	pp := fresh.GetAccountPasswordPolicy()
	require.NotNil(t, pp)
	assert.Equal(t, 12, pp.MinimumPasswordLength)

	// ---- Verify comprehensiveBackend state survived ----
	gotSSHKey, err := fresh.GetSSHPublicKey("alice", sshKey.SSHPublicKeyID)
	require.NoError(t, err)
	assert.Equal(t, sshKey.Fingerprint, gotSSHKey.Fingerprint)
}

// Test_HandlerPersistence_RoundTrip verifies that a Handler.Snapshot/Restore
// cycle preserves state that previously lived outside backendSnapshot:
// Handler-level resource tags (instance profiles, MFA devices, SAML/OIDC
// providers, server certificates) and comprehensiveBackend state (SSH public
// keys). Both were silently dropped by every persistence restore before this
// fix.
func Test_HandlerPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := echo.New()
	h1, b1 := newTestHandler(t)

	_, err := b1.CreateInstanceProfile("PersistedProfile", "/")
	require.NoError(t, err)

	tagReq := iamRequest("TagInstanceProfile", map[string]string{
		"InstanceProfileName": "PersistedProfile",
		"Tags.member.1.Key":   "Team",
		"Tags.member.1.Value": "Platform",
	})
	tagRec := httptest.NewRecorder()
	require.NoError(t, h1.Handler()(e.NewContext(tagReq, tagRec)))
	require.Equal(t, http.StatusOK, tagRec.Code)

	_, err = b1.CreateUser("keyholder", "/", "")
	require.NoError(t, err)
	_, err = b1.UploadSSHPublicKey("keyholder", "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDPersistedKeyBody")
	require.NoError(t, err)

	blob := h1.Snapshot(ctx)
	require.NotEmpty(t, blob)

	// Restore into a brand-new handler/backend pair.
	h2, _ := newTestHandler(t)
	require.NoError(t, h2.Restore(ctx, blob))

	// Instance profile tags must have round-tripped.
	listTagsReq := iamRequest("ListInstanceProfileTags", map[string]string{
		"InstanceProfileName": "PersistedProfile",
	})
	listTagsRec := httptest.NewRecorder()
	require.NoError(t, h2.Handler()(e.NewContext(listTagsReq, listTagsRec)))
	assert.Equal(t, http.StatusOK, listTagsRec.Code)
	assert.Contains(t, listTagsRec.Body.String(), "Platform")

	// SSH public keys (comprehensiveBackend state) must have round-tripped.
	listKeysReq := iamRequest("ListSSHPublicKeys", map[string]string{"UserName": "keyholder"})
	listKeysRec := httptest.NewRecorder()
	require.NoError(t, h2.Handler()(e.NewContext(listKeysReq, listKeysRec)))
	assert.Equal(t, http.StatusOK, listKeysRec.Code)
	assert.Contains(t, listKeysRec.Body.String(), "keyholder")
}

func TestPolicyVersionPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	// Seed data for all new ops.
	require.NoError(t, b.CreateAccountAlias("test-alias"))

	pol, err := b.CreatePolicy(
		"VersionedPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)
	policyArn := pol.Arn

	_, err = b.CreatePolicyVersion(
		policyArn,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		true,
	)
	require.NoError(t, err)

	_, err = b.CreateUser("svc-user", "/", "")
	require.NoError(t, err)
	_, err = b.CreateServiceSpecificCredential("svc-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	_, err = b.CreateVirtualMFADevice("MyMFA", "/")
	require.NoError(t, err)

	req, err := b.CreateDelegationRequest("111122223333")
	require.NoError(t, err)
	require.NoError(t, b.AcceptDelegationRequest(req.DelegationID))

	// Snapshot and restore.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Creating another version should work as there's already 1 extra version.
	pv, err := b2.CreatePolicyVersion(
		policyArn,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, "v3", pv.VersionID)
}
