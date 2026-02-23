package iam_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/iam"
)

func newIAMBackend(t *testing.T) *iam.InMemoryBackend {
	t.Helper()

	return iam.NewInMemoryBackend()
}

func TestListAttachedUserPolicies(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.CreateUser("alice", "/")
	require.NoError(t, err)

	pol, err := b.CreatePolicy("MyPolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	require.NoError(t, b.AttachUserPolicy("alice", pol.Arn))

	policies, err := b.ListAttachedUserPolicies("alice")
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, pol.Arn, policies[0].PolicyArn)
	assert.Equal(t, "MyPolicy", policies[0].PolicyName)
}

func TestListAttachedUserPolicies_UserNotFound(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.ListAttachedUserPolicies("nobody")
	require.Error(t, err)
}

func TestListAttachedRolePolicies(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.CreateRole("MyRole", "/", `{}`)
	require.NoError(t, err)

	pol, err := b.CreatePolicy("RolePolicy", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	require.NoError(t, b.AttachRolePolicy("MyRole", pol.Arn))

	policies, err := b.ListAttachedRolePolicies("MyRole")
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, pol.Arn, policies[0].PolicyArn)
	assert.Equal(t, "RolePolicy", policies[0].PolicyName)
}

func TestListAttachedRolePolicies_RoleNotFound(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.ListAttachedRolePolicies("NoRole")
	require.Error(t, err)
}

func TestAttachUserPolicy_Idempotent(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.CreateUser("bob", "/")
	require.NoError(t, err)

	pol, err := b.CreatePolicy("Pol", "/", `{}`)
	require.NoError(t, err)

	// Attach twice — should not duplicate.
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))
	require.NoError(t, b.AttachUserPolicy("bob", pol.Arn))

	policies, err := b.ListAttachedUserPolicies("bob")
	require.NoError(t, err)
	assert.Len(t, policies, 1)
}

func TestGetPolicy(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	pol, err := b.CreatePolicy("GetMe", "/", `{"Version":"2012-10-17"}`)
	require.NoError(t, err)

	got, err := b.GetPolicy(pol.Arn)
	require.NoError(t, err)
	assert.Equal(t, "GetMe", got.PolicyName)
	assert.Equal(t, pol.Arn, got.Arn)
}

func TestGetPolicy_NotFound(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	_, err := b.GetPolicy("arn:aws:iam::000000000000:policy/nosuchpolicy")
	require.Error(t, err)
}

func TestGetPolicyVersion(t *testing.T) {
	t.Parallel()

	b := newIAMBackend(t)

	pol, err := b.CreatePolicy("VersionedPol", "/", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	// "v1" is always the default version in Gopherstack.
	got, err := b.GetPolicyVersion(pol.Arn, "v1")
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, got.PolicyDocument)
}
