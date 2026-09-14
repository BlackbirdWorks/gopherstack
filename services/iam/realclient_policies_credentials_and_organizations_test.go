package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestInlinePolicies_RealClient drives the Put/Get/List/Delete inline-policy
// family for roles, users and groups (gopherstack-n3zi: these 12 ops
// had no typed-client coverage anywhere in the repo).
func TestInlinePolicies_RealClient(t *testing.T) {
	t.Parallel()

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "role", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName:                 aws.String("inline-role"),
				AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)

			_, err = client.PutRolePolicy(t.Context(), &iamsdk.PutRolePolicyInput{
				RoleName:       aws.String("inline-role"),
				PolicyName:     aws.String("inline-pol"),
				PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			got, err := client.GetRolePolicy(t.Context(), &iamsdk.GetRolePolicyInput{
				RoleName:   aws.String("inline-role"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)
			assert.Equal(t, "inline-pol", aws.ToString(got.PolicyName))
			assert.Equal(t, "inline-role", aws.ToString(got.RoleName))
			assert.NotEmpty(t, aws.ToString(got.PolicyDocument))

			list, err := client.ListRolePolicies(t.Context(), &iamsdk.ListRolePoliciesInput{
				RoleName: aws.String("inline-role"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"inline-pol"}, list.PolicyNames)

			_, err = client.DeleteRolePolicy(t.Context(), &iamsdk.DeleteRolePolicyInput{
				RoleName:   aws.String("inline-role"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)

			list2, err := client.ListRolePolicies(t.Context(), &iamsdk.ListRolePoliciesInput{
				RoleName: aws.String("inline-role"),
			})
			require.NoError(t, err)
			assert.Empty(t, list2.PolicyNames)
		}},
		{name: "user", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("inline-user")})
			require.NoError(t, err)

			_, err = client.PutUserPolicy(t.Context(), &iamsdk.PutUserPolicyInput{
				UserName:       aws.String("inline-user"),
				PolicyName:     aws.String("inline-pol"),
				PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			got, err := client.GetUserPolicy(t.Context(), &iamsdk.GetUserPolicyInput{
				UserName:   aws.String("inline-user"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)
			assert.Equal(t, "inline-pol", aws.ToString(got.PolicyName))
			assert.Equal(t, "inline-user", aws.ToString(got.UserName))

			list, err := client.ListUserPolicies(t.Context(), &iamsdk.ListUserPoliciesInput{
				UserName: aws.String("inline-user"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"inline-pol"}, list.PolicyNames)

			_, err = client.DeleteUserPolicy(t.Context(), &iamsdk.DeleteUserPolicyInput{
				UserName:   aws.String("inline-user"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)

			list2, err := client.ListUserPolicies(t.Context(), &iamsdk.ListUserPoliciesInput{
				UserName: aws.String("inline-user"),
			})
			require.NoError(t, err)
			assert.Empty(t, list2.PolicyNames)
		}},
		{name: "group", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("inline-group")})
			require.NoError(t, err)

			_, err = client.PutGroupPolicy(t.Context(), &iamsdk.PutGroupPolicyInput{
				GroupName:      aws.String("inline-group"),
				PolicyName:     aws.String("inline-pol"),
				PolicyDocument: aws.String(doc),
			})
			require.NoError(t, err)

			got, err := client.GetGroupPolicy(t.Context(), &iamsdk.GetGroupPolicyInput{
				GroupName:  aws.String("inline-group"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)
			assert.Equal(t, "inline-pol", aws.ToString(got.PolicyName))
			assert.Equal(t, "inline-group", aws.ToString(got.GroupName))

			list, err := client.ListGroupPolicies(t.Context(), &iamsdk.ListGroupPoliciesInput{
				GroupName: aws.String("inline-group"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"inline-pol"}, list.PolicyNames)

			_, err = client.DeleteGroupPolicy(t.Context(), &iamsdk.DeleteGroupPolicyInput{
				GroupName:  aws.String("inline-group"),
				PolicyName: aws.String("inline-pol"),
			})
			require.NoError(t, err)

			list2, err := client.ListGroupPolicies(t.Context(), &iamsdk.ListGroupPoliciesInput{
				GroupName: aws.String("inline-group"),
			})
			require.NoError(t, err)
			assert.Empty(t, list2.PolicyNames)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestManagedPolicyGetters_RealClient covers GetPolicy and GetPolicyVersion.
func TestManagedPolicyGetters_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	created, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("gp-policy"),
		PolicyDocument: aws.String(doc),
	})
	require.NoError(t, err)

	gp, err := client.GetPolicy(t.Context(), &iamsdk.GetPolicyInput{PolicyArn: created.Policy.Arn})
	require.NoError(t, err)
	require.NotNil(t, gp.Policy)
	assert.Equal(t, "gp-policy", aws.ToString(gp.Policy.PolicyName))
	assert.Equal(t, aws.ToString(created.Policy.Arn), aws.ToString(gp.Policy.Arn))
	assert.Equal(t, "v1", aws.ToString(gp.Policy.DefaultVersionId))

	gpv, err := client.GetPolicyVersion(t.Context(), &iamsdk.GetPolicyVersionInput{
		PolicyArn: created.Policy.Arn,
		VersionId: aws.String("v1"),
	})
	require.NoError(t, err)
	require.NotNil(t, gpv.PolicyVersion)
	assert.True(t, gpv.PolicyVersion.IsDefaultVersion)
	assert.NotEmpty(t, aws.ToString(gpv.PolicyVersion.Document))
}

// TestTagUntagFamily_RealClient covers the Tag*/Untag* pairs for role, user,
// policy, instance profile, MFA device, OIDC provider, SAML provider and
// server certificate -- 16 ops, none previously driven by a typed client.
func TestTagUntagFamily_RealClient(t *testing.T) {
	t.Parallel()

	tag := types.Tag{Key: aws.String("k"), Value: aws.String("v")}
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "role", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName: aws.String("tag-role"), AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)

			_, err = client.TagRole(
				t.Context(),
				&iamsdk.TagRoleInput{RoleName: aws.String("tag-role"), Tags: []types.Tag{tag}},
			)
			require.NoError(t, err)

			list, err := client.ListRoleTags(t.Context(), &iamsdk.ListRoleTagsInput{RoleName: aws.String("tag-role")})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)
			assert.Equal(t, "k", aws.ToString(list.Tags[0].Key))

			_, err = client.UntagRole(
				t.Context(),
				&iamsdk.UntagRoleInput{RoleName: aws.String("tag-role"), TagKeys: []string{"k"}},
			)
			require.NoError(t, err)

			list2, err := client.ListRoleTags(t.Context(), &iamsdk.ListRoleTagsInput{RoleName: aws.String("tag-role")})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "user", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("tag-user")})
			require.NoError(t, err)

			_, err = client.TagUser(
				t.Context(),
				&iamsdk.TagUserInput{UserName: aws.String("tag-user"), Tags: []types.Tag{tag}},
			)
			require.NoError(t, err)

			list, err := client.ListUserTags(t.Context(), &iamsdk.ListUserTagsInput{UserName: aws.String("tag-user")})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagUser(
				t.Context(),
				&iamsdk.UntagUserInput{UserName: aws.String("tag-user"), TagKeys: []string{"k"}},
			)
			require.NoError(t, err)

			list2, err := client.ListUserTags(t.Context(), &iamsdk.ListUserTagsInput{UserName: aws.String("tag-user")})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "policy", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			created, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
				PolicyName: aws.String("tag-policy"),
				PolicyDocument: aws.String(
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				),
			})
			require.NoError(t, err)

			_, err = client.TagPolicy(
				t.Context(),
				&iamsdk.TagPolicyInput{PolicyArn: created.Policy.Arn, Tags: []types.Tag{tag}},
			)
			require.NoError(t, err)

			list, err := client.ListPolicyTags(t.Context(), &iamsdk.ListPolicyTagsInput{PolicyArn: created.Policy.Arn})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagPolicy(
				t.Context(),
				&iamsdk.UntagPolicyInput{PolicyArn: created.Policy.Arn, TagKeys: []string{"k"}},
			)
			require.NoError(t, err)

			list2, err := client.ListPolicyTags(t.Context(), &iamsdk.ListPolicyTagsInput{PolicyArn: created.Policy.Arn})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "instanceprofile", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.CreateInstanceProfile(t.Context(), &iamsdk.CreateInstanceProfileInput{
				InstanceProfileName: aws.String("tag-ip"),
			})
			require.NoError(t, err)

			_, err = client.TagInstanceProfile(t.Context(), &iamsdk.TagInstanceProfileInput{
				InstanceProfileName: aws.String("tag-ip"), Tags: []types.Tag{tag},
			})
			require.NoError(t, err)

			list, err := client.ListInstanceProfileTags(t.Context(), &iamsdk.ListInstanceProfileTagsInput{
				InstanceProfileName: aws.String("tag-ip"),
			})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagInstanceProfile(t.Context(), &iamsdk.UntagInstanceProfileInput{
				InstanceProfileName: aws.String("tag-ip"), TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			list2, err := client.ListInstanceProfileTags(t.Context(), &iamsdk.ListInstanceProfileTagsInput{
				InstanceProfileName: aws.String("tag-ip"),
			})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "mfadevice", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			created, err := client.CreateVirtualMFADevice(t.Context(), &iamsdk.CreateVirtualMFADeviceInput{
				VirtualMFADeviceName: aws.String("tag-mfa"),
			})
			require.NoError(t, err)

			_, err = client.TagMFADevice(t.Context(), &iamsdk.TagMFADeviceInput{
				SerialNumber: created.VirtualMFADevice.SerialNumber, Tags: []types.Tag{tag},
			})
			require.NoError(t, err)

			list, err := client.ListMFADeviceTags(t.Context(), &iamsdk.ListMFADeviceTagsInput{
				SerialNumber: created.VirtualMFADevice.SerialNumber,
			})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagMFADevice(t.Context(), &iamsdk.UntagMFADeviceInput{
				SerialNumber: created.VirtualMFADevice.SerialNumber, TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			list2, err := client.ListMFADeviceTags(t.Context(), &iamsdk.ListMFADeviceTagsInput{
				SerialNumber: created.VirtualMFADevice.SerialNumber,
			})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "oidcprovider", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			created, err := client.CreateOpenIDConnectProvider(t.Context(), &iamsdk.CreateOpenIDConnectProviderInput{
				Url:            aws.String("https://tag-oidc.example.com"),
				ThumbprintList: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			})
			require.NoError(t, err)

			_, err = client.TagOpenIDConnectProvider(t.Context(), &iamsdk.TagOpenIDConnectProviderInput{
				OpenIDConnectProviderArn: created.OpenIDConnectProviderArn, Tags: []types.Tag{tag},
			})
			require.NoError(t, err)

			list, err := client.ListOpenIDConnectProviderTags(t.Context(), &iamsdk.ListOpenIDConnectProviderTagsInput{
				OpenIDConnectProviderArn: created.OpenIDConnectProviderArn,
			})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagOpenIDConnectProvider(t.Context(), &iamsdk.UntagOpenIDConnectProviderInput{
				OpenIDConnectProviderArn: created.OpenIDConnectProviderArn, TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			list2, err := client.ListOpenIDConnectProviderTags(t.Context(), &iamsdk.ListOpenIDConnectProviderTagsInput{
				OpenIDConnectProviderArn: created.OpenIDConnectProviderArn,
			})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "samlprovider", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			created, err := client.CreateSAMLProvider(t.Context(), &iamsdk.CreateSAMLProviderInput{
				Name: aws.String("tag-saml"), SAMLMetadataDocument: aws.String("<md/>"),
			})
			require.NoError(t, err)

			_, err = client.TagSAMLProvider(t.Context(), &iamsdk.TagSAMLProviderInput{
				SAMLProviderArn: created.SAMLProviderArn, Tags: []types.Tag{tag},
			})
			require.NoError(t, err)

			list, err := client.ListSAMLProviderTags(t.Context(), &iamsdk.ListSAMLProviderTagsInput{
				SAMLProviderArn: created.SAMLProviderArn,
			})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagSAMLProvider(t.Context(), &iamsdk.UntagSAMLProviderInput{
				SAMLProviderArn: created.SAMLProviderArn, TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			list2, err := client.ListSAMLProviderTags(t.Context(), &iamsdk.ListSAMLProviderTagsInput{
				SAMLProviderArn: created.SAMLProviderArn,
			})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}},
		{name: "servercertificate", run: func(t *testing.T) {
			t.Helper()

			h := iam.NewHandler(iam.NewInMemoryBackend())
			client := newTestIAMClient(t, h)

			_, err := client.UploadServerCertificate(t.Context(), &iamsdk.UploadServerCertificateInput{
				ServerCertificateName: aws.String("tag-cert"),
				CertificateBody:       aws.String("-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----"),
				PrivateKey:            aws.String("-----BEGIN PRIVATE KEY-----\nMA==\n-----END PRIVATE KEY-----"),
			})
			require.NoError(t, err)

			_, err = client.TagServerCertificate(t.Context(), &iamsdk.TagServerCertificateInput{
				ServerCertificateName: aws.String("tag-cert"), Tags: []types.Tag{tag},
			})
			require.NoError(t, err)

			list, err := client.ListServerCertificateTags(t.Context(), &iamsdk.ListServerCertificateTagsInput{
				ServerCertificateName: aws.String("tag-cert"),
			})
			require.NoError(t, err)
			require.Len(t, list.Tags, 1)

			_, err = client.UntagServerCertificate(t.Context(), &iamsdk.UntagServerCertificateInput{
				ServerCertificateName: aws.String("tag-cert"), TagKeys: []string{"k"},
			})
			require.NoError(t, err)

			list2, err := client.ListServerCertificateTags(t.Context(), &iamsdk.ListServerCertificateTagsInput{
				ServerCertificateName: aws.String("tag-cert"),
			})
			require.NoError(t, err)
			assert.Empty(t, list2.Tags)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestServerCertificateLifecycle_RealClient covers Get/List/Update/Delete
// ServerCertificate (Upload is already covered elsewhere; kept here only as
// setup).
func TestServerCertificateLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.UploadServerCertificate(t.Context(), &iamsdk.UploadServerCertificateInput{
		ServerCertificateName: aws.String("lc-cert"),
		CertificateBody:       aws.String("-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----"),
		PrivateKey:            aws.String("-----BEGIN PRIVATE KEY-----\nMA==\n-----END PRIVATE KEY-----"),
	})
	require.NoError(t, err)

	got, err := client.GetServerCertificate(t.Context(), &iamsdk.GetServerCertificateInput{
		ServerCertificateName: aws.String("lc-cert"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.ServerCertificate)
	assert.Equal(t, "lc-cert", aws.ToString(got.ServerCertificate.ServerCertificateMetadata.ServerCertificateName))
	assert.NotEmpty(t, aws.ToString(got.ServerCertificate.CertificateBody))

	listed, err := client.ListServerCertificates(t.Context(), &iamsdk.ListServerCertificatesInput{})
	require.NoError(t, err)
	var found bool
	for _, m := range listed.ServerCertificateMetadataList {
		if aws.ToString(m.ServerCertificateName) == "lc-cert" {
			found = true
		}
	}
	assert.True(t, found, "lc-cert should be in ListServerCertificates")

	_, err = client.UpdateServerCertificate(t.Context(), &iamsdk.UpdateServerCertificateInput{
		ServerCertificateName:    aws.String("lc-cert"),
		NewServerCertificateName: aws.String("lc-cert-renamed"),
	})
	require.NoError(t, err)

	renamed, err := client.GetServerCertificate(t.Context(), &iamsdk.GetServerCertificateInput{
		ServerCertificateName: aws.String("lc-cert-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lc-cert-renamed",
		aws.ToString(renamed.ServerCertificate.ServerCertificateMetadata.ServerCertificateName))

	_, err = client.DeleteServerCertificate(t.Context(), &iamsdk.DeleteServerCertificateInput{
		ServerCertificateName: aws.String("lc-cert-renamed"),
	})
	require.NoError(t, err)

	_, err = client.GetServerCertificate(t.Context(), &iamsdk.GetServerCertificateInput{
		ServerCertificateName: aws.String("lc-cert-renamed"),
	})
	require.Error(t, err)
}

// TestSSHPublicKeyLifecycle_RealClient covers Upload/Get/Update/Delete.
func TestSSHPublicKeyLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("ssh-user")})
	require.NoError(t, err)

	// A real, parseable ssh-rsa authorized_keys line is required: GetSSHPublicKey's
	// Encoding=SSH path round-trips through ssh.ParseAuthorizedKey (ssh_keys.go), which
	// a made-up/truncated base64 body fails, unlike AWS's own key-format validation.
	//nolint:lll // ssh-rsa key body cannot be wrapped without becoming invalid
	body := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCtRrefcU79SFvkuGBm0j5SHj4DTH8cPoLSShYJopMJ2tg6EYASjsQ1AnLYqABe49OF+NDj3eWzCBaJHPg1w99OWa10N8F7Kstd+H4KBBSTakQ8XnOsVrQgi1AmZ/nzR4hLd5Z9pF6A6TREpuv0+sCUl1Y5OQtA6lLT+rzXe1epxDYeMtzjlpUK1inNlz0XAMS2+/j7k/GHGf8qtETclk5+rkwZ9YAxbj0Oba49s/tgZxV00wZWZ75npx/k1F/A5HHHmuZKN/HtXgOnzltX8FnYTvBiwlNZwAUfVuI7M3gW/o8P7mD8peI/RV4hqwfaJ4NAIwtiBcOwWEIDjr2AkwAz test@example.com"

	uploaded, err := client.UploadSSHPublicKey(t.Context(), &iamsdk.UploadSSHPublicKeyInput{
		UserName:         aws.String("ssh-user"),
		SSHPublicKeyBody: aws.String(body),
	})
	require.NoError(t, err)
	require.NotNil(t, uploaded.SSHPublicKey)
	keyID := aws.ToString(uploaded.SSHPublicKey.SSHPublicKeyId)
	assert.NotEmpty(t, keyID)
	assert.Equal(t, "Active", string(uploaded.SSHPublicKey.Status))

	got, err := client.GetSSHPublicKey(t.Context(), &iamsdk.GetSSHPublicKeyInput{
		UserName:       aws.String("ssh-user"),
		SSHPublicKeyId: aws.String(keyID),
		Encoding:       types.EncodingTypeSsh,
	})
	require.NoError(t, err)
	assert.Equal(t, body, aws.ToString(got.SSHPublicKey.SSHPublicKeyBody))

	_, err = client.UpdateSSHPublicKey(t.Context(), &iamsdk.UpdateSSHPublicKeyInput{
		UserName:       aws.String("ssh-user"),
		SSHPublicKeyId: aws.String(keyID),
		Status:         types.StatusTypeInactive,
	})
	require.NoError(t, err)

	got2, err := client.GetSSHPublicKey(t.Context(), &iamsdk.GetSSHPublicKeyInput{
		UserName:       aws.String("ssh-user"),
		SSHPublicKeyId: aws.String(keyID),
		Encoding:       types.EncodingTypeSsh,
	})
	require.NoError(t, err)
	assert.Equal(t, "Inactive", string(got2.SSHPublicKey.Status))

	_, err = client.DeleteSSHPublicKey(t.Context(), &iamsdk.DeleteSSHPublicKeyInput{
		UserName:       aws.String("ssh-user"),
		SSHPublicKeyId: aws.String(keyID),
	})
	require.NoError(t, err)

	_, err = client.GetSSHPublicKey(t.Context(), &iamsdk.GetSSHPublicKeyInput{
		UserName:       aws.String("ssh-user"),
		SSHPublicKeyId: aws.String(keyID),
		Encoding:       types.EncodingTypeSsh,
	})
	require.Error(t, err)
}

// TestAccountPasswordPolicyAndAliases_RealClient covers Get/Update/Delete
// AccountPasswordPolicy and ListAccountAliases.
func TestAccountPasswordPolicyAndAliases_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	defPP, err := client.GetAccountPasswordPolicy(t.Context(), &iamsdk.GetAccountPasswordPolicyInput{})
	require.NoError(t, err)
	require.NotNil(t, defPP.PasswordPolicy)
	assert.EqualValues(t, 8, aws.ToInt32(defPP.PasswordPolicy.MinimumPasswordLength))

	// AllowUsersToChangePassword is a plain (non-pointer) bool on the real
	// Input struct, and the query-protocol serializer only transmits a bool
	// field when true (aws-sdk-go-v2/service/iam@v1.63.0 serializers.go:15960)
	// -- a real client has no way to distinguish "explicitly false" from
	// "omitted" on the wire, so it is left unset here and expected to land on
	// its documented default (false) rather than asserted as an explicit send.
	_, err = client.UpdateAccountPasswordPolicy(t.Context(), &iamsdk.UpdateAccountPasswordPolicyInput{
		MinimumPasswordLength: aws.Int32(16),
		RequireSymbols:        true,
		RequireNumbers:        true,
	})
	require.NoError(t, err)

	updated, err := client.GetAccountPasswordPolicy(t.Context(), &iamsdk.GetAccountPasswordPolicyInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 16, aws.ToInt32(updated.PasswordPolicy.MinimumPasswordLength))
	assert.True(t, updated.PasswordPolicy.RequireSymbols)
	assert.False(t, updated.PasswordPolicy.AllowUsersToChangePassword)

	_, err = client.DeleteAccountPasswordPolicy(t.Context(), &iamsdk.DeleteAccountPasswordPolicyInput{})
	require.NoError(t, err)

	reset, err := client.GetAccountPasswordPolicy(t.Context(), &iamsdk.GetAccountPasswordPolicyInput{})
	require.NoError(t, err)
	assert.EqualValues(t, 8, aws.ToInt32(reset.PasswordPolicy.MinimumPasswordLength))

	_, err = client.CreateAccountAlias(
		t.Context(),
		&iamsdk.CreateAccountAliasInput{AccountAlias: aws.String("acme-corp")},
	)
	require.NoError(t, err)

	aliases, err := client.ListAccountAliases(t.Context(), &iamsdk.ListAccountAliasesInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"acme-corp"}, aliases.AccountAliases)
}

// TestMFACleanup_RealClient covers DeactivateMFADevice, ResyncMFADevice and
// DeleteVirtualMFADevice.
func TestMFACleanup_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("mfa-user")})
	require.NoError(t, err)

	created, err := client.CreateVirtualMFADevice(t.Context(), &iamsdk.CreateVirtualMFADeviceInput{
		VirtualMFADeviceName: aws.String("mfa-cleanup-device"),
	})
	require.NoError(t, err)
	serial := aws.ToString(created.VirtualMFADevice.SerialNumber)

	_, err = client.EnableMFADevice(t.Context(), &iamsdk.EnableMFADeviceInput{
		UserName:            aws.String("mfa-user"),
		SerialNumber:        aws.String(serial),
		AuthenticationCode1: aws.String("123456"),
		AuthenticationCode2: aws.String("654321"),
	})
	require.NoError(t, err)

	_, err = client.ResyncMFADevice(t.Context(), &iamsdk.ResyncMFADeviceInput{
		UserName:            aws.String("mfa-user"),
		SerialNumber:        aws.String(serial),
		AuthenticationCode1: aws.String("111111"),
		AuthenticationCode2: aws.String("222222"),
	})
	require.NoError(t, err)

	_, err = client.DeactivateMFADevice(t.Context(), &iamsdk.DeactivateMFADeviceInput{
		UserName:     aws.String("mfa-user"),
		SerialNumber: aws.String(serial),
	})
	require.NoError(t, err)

	_, err = client.DeleteVirtualMFADevice(t.Context(), &iamsdk.DeleteVirtualMFADeviceInput{
		SerialNumber: aws.String(serial),
	})
	require.NoError(t, err)

	list, err := client.ListVirtualMFADevices(t.Context(), &iamsdk.ListVirtualMFADevicesInput{})
	require.NoError(t, err)
	for _, d := range list.VirtualMFADevices {
		assert.NotEqual(t, serial, aws.ToString(d.SerialNumber))
	}
}

// TestServiceSpecificCredentials_RealClient covers Create/List/Reset/Update/Delete.
func TestServiceSpecificCredentials_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("svc-cred-user")})
	require.NoError(t, err)

	created, err := client.CreateServiceSpecificCredential(t.Context(), &iamsdk.CreateServiceSpecificCredentialInput{
		UserName:    aws.String("svc-cred-user"),
		ServiceName: aws.String("codecommit.amazonaws.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.ServiceSpecificCredential)
	credID := aws.ToString(created.ServiceSpecificCredential.ServiceSpecificCredentialId)
	origPassword := aws.ToString(created.ServiceSpecificCredential.ServicePassword)
	assert.NotEmpty(t, credID)
	assert.NotEmpty(t, origPassword)

	listed, err := client.ListServiceSpecificCredentials(t.Context(), &iamsdk.ListServiceSpecificCredentialsInput{
		UserName: aws.String("svc-cred-user"),
	})
	require.NoError(t, err)
	require.Len(t, listed.ServiceSpecificCredentials, 1)
	assert.Equal(t, credID, aws.ToString(listed.ServiceSpecificCredentials[0].ServiceSpecificCredentialId))

	_, err = client.UpdateServiceSpecificCredential(t.Context(), &iamsdk.UpdateServiceSpecificCredentialInput{
		UserName:                    aws.String("svc-cred-user"),
		ServiceSpecificCredentialId: aws.String(credID),
		Status:                      types.StatusTypeInactive,
	})
	require.NoError(t, err)

	listed2, err := client.ListServiceSpecificCredentials(t.Context(), &iamsdk.ListServiceSpecificCredentialsInput{
		UserName: aws.String("svc-cred-user"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StatusTypeInactive, listed2.ServiceSpecificCredentials[0].Status)

	reset, err := client.ResetServiceSpecificCredential(t.Context(), &iamsdk.ResetServiceSpecificCredentialInput{
		UserName:                    aws.String("svc-cred-user"),
		ServiceSpecificCredentialId: aws.String(credID),
	})
	require.NoError(t, err)
	assert.NotEqual(t, origPassword, aws.ToString(reset.ServiceSpecificCredential.ServicePassword))

	_, err = client.DeleteServiceSpecificCredential(t.Context(), &iamsdk.DeleteServiceSpecificCredentialInput{
		UserName:                    aws.String("svc-cred-user"),
		ServiceSpecificCredentialId: aws.String(credID),
	})
	require.NoError(t, err)

	listed3, err := client.ListServiceSpecificCredentials(t.Context(), &iamsdk.ListServiceSpecificCredentialsInput{
		UserName: aws.String("svc-cred-user"),
	})
	require.NoError(t, err)
	assert.Empty(t, listed3.ServiceSpecificCredentials)
}

// TestAccessKeyLastUsed_RealClient covers GetAccessKeyLastUsed.
func TestAccessKeyLastUsed_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("akey-user")})
	require.NoError(t, err)

	key, err := client.CreateAccessKey(t.Context(), &iamsdk.CreateAccessKeyInput{UserName: aws.String("akey-user")})
	require.NoError(t, err)

	out, err := client.GetAccessKeyLastUsed(t.Context(), &iamsdk.GetAccessKeyLastUsedInput{
		AccessKeyId: key.AccessKey.AccessKeyId,
	})
	require.NoError(t, err)
	assert.Equal(t, "akey-user", aws.ToString(out.UserName))
	require.NotNil(t, out.AccessKeyLastUsed)
}

// TestRoleUserGroupUpdates_RealClient covers UpdateAssumeRolePolicy,
// UpdateRoleDescription, PutRolePermissionsBoundary, UpdateGroup and UpdateUser.
func TestRoleUserGroupUpdates_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName: aws.String("update-role"), AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17"}`),
	})
	require.NoError(t, err)

	newDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = client.UpdateAssumeRolePolicy(t.Context(), &iamsdk.UpdateAssumeRolePolicyInput{
		RoleName: aws.String("update-role"), PolicyDocument: aws.String(newDoc),
	})
	require.NoError(t, err)

	roleAfter, err := client.GetRole(t.Context(), &iamsdk.GetRoleInput{RoleName: aws.String("update-role")})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(roleAfter.Role.AssumeRolePolicyDocument))

	updRole, err := client.UpdateRoleDescription(t.Context(), &iamsdk.UpdateRoleDescriptionInput{
		RoleName: aws.String("update-role"), Description: aws.String("updated description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(updRole.Role.Description))

	_, err = client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName: aws.String("boundary-policy"),
		PolicyDocument: aws.String(
			`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		),
	})
	require.NoError(t, err)
	boundaryArn := "arn:aws:iam::000000000000:policy/boundary-policy"

	_, err = client.PutRolePermissionsBoundary(t.Context(), &iamsdk.PutRolePermissionsBoundaryInput{
		RoleName: aws.String("update-role"), PermissionsBoundary: aws.String(boundaryArn),
	})
	require.NoError(t, err)

	roleWithBoundary, err := client.GetRole(t.Context(), &iamsdk.GetRoleInput{RoleName: aws.String("update-role")})
	require.NoError(t, err)
	require.NotNil(t, roleWithBoundary.Role.PermissionsBoundary)
	assert.Equal(t, boundaryArn, aws.ToString(roleWithBoundary.Role.PermissionsBoundary.PermissionsBoundaryArn))

	_, err = client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("update-group")})
	require.NoError(t, err)

	_, err = client.UpdateGroup(t.Context(), &iamsdk.UpdateGroupInput{
		GroupName: aws.String("update-group"), NewGroupName: aws.String("update-group-renamed"),
	})
	require.NoError(t, err)

	renamedGroup, err := client.GetGroup(
		t.Context(),
		&iamsdk.GetGroupInput{GroupName: aws.String("update-group-renamed")},
	)
	require.NoError(t, err)
	assert.Equal(t, "update-group-renamed", aws.ToString(renamedGroup.Group.GroupName))

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("update-user")})
	require.NoError(t, err)

	_, err = client.UpdateUser(t.Context(), &iamsdk.UpdateUserInput{
		UserName: aws.String("update-user"), NewUserName: aws.String("update-user-renamed"),
	})
	require.NoError(t, err)

	renamedUser, err := client.GetUser(t.Context(), &iamsdk.GetUserInput{UserName: aws.String("update-user-renamed")})
	require.NoError(t, err)
	assert.Equal(t, "update-user-renamed", aws.ToString(renamedUser.User.UserName))
}

// TestCredentialReportAndOIDC_RealClient covers Generate/GetCredentialReport
// and AddClientIDToOpenIDConnectProvider.
func TestCredentialReportAndOIDC_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("report-user")})
	require.NoError(t, err)

	gen, err := client.GenerateCredentialReport(t.Context(), &iamsdk.GenerateCredentialReportInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, string(gen.State))

	report, err := client.GetCredentialReport(t.Context(), &iamsdk.GetCredentialReportInput{})
	require.NoError(t, err)
	require.NotEmpty(t, report.Content)
	assert.Contains(t, string(report.Content), "report-user")
	require.NotNil(t, report.GeneratedTime)

	provider, err := client.CreateOpenIDConnectProvider(t.Context(), &iamsdk.CreateOpenIDConnectProviderInput{
		Url:            aws.String("https://oidc-clientid.example.com"),
		ThumbprintList: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	})
	require.NoError(t, err)

	_, err = client.AddClientIDToOpenIDConnectProvider(t.Context(), &iamsdk.AddClientIDToOpenIDConnectProviderInput{
		OpenIDConnectProviderArn: provider.OpenIDConnectProviderArn,
		ClientID:                 aws.String("my-client-id"),
	})
	require.NoError(t, err)

	got, err := client.GetOpenIDConnectProvider(t.Context(), &iamsdk.GetOpenIDConnectProviderInput{
		OpenIDConnectProviderArn: provider.OpenIDConnectProviderArn,
	})
	require.NoError(t, err)
	assert.Contains(t, got.ClientIDList, "my-client-id")
}

// TestPolicySimulationAndContextKeys_RealClient covers SimulateCustomPolicy,
// GetContextKeysForPrincipalPolicy and GetHumanReadableSummary.
func TestPolicySimulationAndContextKeys_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	sim, err := client.SimulateCustomPolicy(t.Context(), &iamsdk.SimulateCustomPolicyInput{
		PolicyInputList: []string{doc},
		ActionNames:     []string{"s3:GetObject"},
	})
	require.NoError(t, err)
	require.Len(t, sim.EvaluationResults, 1)
	assert.Equal(t, "s3:GetObject", aws.ToString(sim.EvaluationResults[0].EvalActionName))
	assert.Equal(t, types.PolicyEvaluationDecisionTypeAllowed, sim.EvaluationResults[0].EvalDecision)

	docWithCondition := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` +
		`"Condition":{"StringEquals":{"aws:SourceIp":"10.0.0.1"}}}]}`

	ctxKeys, err := client.GetContextKeysForPrincipalPolicy(t.Context(), &iamsdk.GetContextKeysForPrincipalPolicyInput{
		PolicySourceArn: aws.String("arn:aws:iam::000000000000:role/whatever"),
		PolicyInputList: []string{docWithCondition},
	})
	require.NoError(t, err)
	assert.Contains(t, ctxKeys.ContextKeyNames, "aws:SourceIp")

	created, err := client.CreateDelegationRequest(t.Context(), &iamsdk.CreateDelegationRequestInput{
		Description:         aws.String("hrs test"),
		NotificationChannel: aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
		RequestorWorkflowId: aws.String("workflow-hrs-test"),
		SessionDuration:     aws.Int32(3600),
		Permissions: &types.DelegationPermission{
			PolicyTemplateArn: aws.String("arn:aws:iam::aws:policy/ReadOnlyAccess"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.DelegationRequestId)

	entityArn := "arn:aws:iam::000000000000:delegation-request/" + aws.ToString(created.DelegationRequestId)

	hrs, err := client.GetHumanReadableSummary(t.Context(), &iamsdk.GetHumanReadableSummaryInput{
		EntityArn: aws.String(entityArn),
		Locale:    aws.String("en"),
	})
	require.NoError(t, err)
	assert.Equal(t, "en", aws.ToString(hrs.Locale))
	assert.Equal(t, types.SummaryStateTypeNotSupported, hrs.SummaryState)
}

// TestServiceLinkedRoleDeletion_RealClient covers DeleteServiceLinkedRole and
// GetServiceLinkedRoleDeletionStatus.
func TestServiceLinkedRoleDeletion_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	created, err := client.CreateServiceLinkedRole(t.Context(), &iamsdk.CreateServiceLinkedRoleInput{
		AWSServiceName: aws.String("elasticbeanstalk.amazonaws.com"),
	})
	require.NoError(t, err)

	del, err := client.DeleteServiceLinkedRole(t.Context(), &iamsdk.DeleteServiceLinkedRoleInput{
		RoleName: created.Role.RoleName,
	})
	require.NoError(t, err)
	taskID := aws.ToString(del.DeletionTaskId)
	assert.NotEmpty(t, taskID)

	status, err := client.GetServiceLinkedRoleDeletionStatus(
		t.Context(),
		&iamsdk.GetServiceLinkedRoleDeletionStatusInput{
			DeletionTaskId: aws.String(taskID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.DeletionTaskStatusTypeSucceeded, status.Status)
}

// TestOrganizationsAccessReportAndFeatures_RealClient covers
// Generate/GetOrganizationsAccessReport and ListOrganizationsFeatures.
func TestOrganizationsAccessReportAndFeatures_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	gen, err := client.GenerateOrganizationsAccessReport(t.Context(), &iamsdk.GenerateOrganizationsAccessReportInput{
		EntityPath: aws.String("o-abc123/r-abc/ou-abc/123456789012"),
	})
	require.NoError(t, err)
	jobID := aws.ToString(gen.JobId)
	assert.NotEmpty(t, jobID)

	report, err := client.GetOrganizationsAccessReport(t.Context(), &iamsdk.GetOrganizationsAccessReportInput{
		JobId: aws.String(jobID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.JobStatusTypeCompleted, report.JobStatus)

	features, err := client.ListOrganizationsFeatures(t.Context(), &iamsdk.ListOrganizationsFeaturesInput{})
	require.NoError(t, err)
	assert.NotNil(t, features.EnabledFeatures)
}

// TestOrganizationsRootManagementAndOutboundFederation_RealClient covers the
// Enable/Disable pairs for organizations root management and outbound web
// identity federation, plus GetOutboundWebIdentityFederationInfo.
func TestOrganizationsRootManagementAndOutboundFederation_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.EnableOrganizationsRootCredentialsManagement(
		t.Context(), &iamsdk.EnableOrganizationsRootCredentialsManagementInput{},
	)
	require.NoError(t, err)

	_, err = client.DisableOrganizationsRootCredentialsManagement(
		t.Context(), &iamsdk.DisableOrganizationsRootCredentialsManagementInput{},
	)
	require.NoError(t, err)

	_, err = client.EnableOrganizationsRootSessions(t.Context(), &iamsdk.EnableOrganizationsRootSessionsInput{})
	require.NoError(t, err)

	_, err = client.DisableOrganizationsRootSessions(t.Context(), &iamsdk.DisableOrganizationsRootSessionsInput{})
	require.NoError(t, err)

	// A fresh backend starts with outbound federation already enabled
	// (services/iam/store.go's NewInMemoryBackendWithConfig sets
	// outboundFederationEnabled: true) -- disable first to establish a known
	// false baseline rather than assuming one.
	_, err = client.DisableOutboundWebIdentityFederation(
		t.Context(), &iamsdk.DisableOutboundWebIdentityFederationInput{},
	)
	require.NoError(t, err)

	before, err := client.GetOutboundWebIdentityFederationInfo(
		t.Context(), &iamsdk.GetOutboundWebIdentityFederationInfoInput{},
	)
	require.NoError(t, err)
	assert.False(t, before.JwtVendingEnabled)

	enabled, err := client.EnableOutboundWebIdentityFederation(
		t.Context(), &iamsdk.EnableOutboundWebIdentityFederationInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(enabled.IssuerIdentifier))

	after, err := client.GetOutboundWebIdentityFederationInfo(
		t.Context(), &iamsdk.GetOutboundWebIdentityFederationInfoInput{},
	)
	require.NoError(t, err)
	assert.True(t, after.JwtVendingEnabled)
	assert.Equal(t, aws.ToString(enabled.IssuerIdentifier), aws.ToString(after.IssuerIdentifier))

	_, err = client.DisableOutboundWebIdentityFederation(
		t.Context(), &iamsdk.DisableOutboundWebIdentityFederationInput{},
	)
	require.NoError(t, err)

	final, err := client.GetOutboundWebIdentityFederationInfo(
		t.Context(), &iamsdk.GetOutboundWebIdentityFederationInfoInput{},
	)
	require.NoError(t, err)
	assert.False(t, final.JwtVendingEnabled)
}
