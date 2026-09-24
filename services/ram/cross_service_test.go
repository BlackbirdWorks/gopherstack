package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// fakeSiblingServices structurally satisfies ram's unexported
// siblingServices interface (matched by SetAppConfig's type assertion),
// mirroring how the real *CLI wires GetIAMHandler.
type fakeSiblingServices struct {
	iamHandler service.Registerable
}

func (f *fakeSiblingServices) GetIAMHandler() service.Registerable { return f.iamHandler }

// TestEnableSharingWithAwsOrganization_CreatesServiceLinkedRole proves
// EnableSharingWithAwsOrganization creates the RAM service-linked role in a
// wired IAM backend -- the cross-service effect
// aws_ram_sharing_with_organization's Read depends on via iam:GetRole for
// AWSServiceRoleForResourceAccessManager (services/ram/PARITY.md
// items_still_open). A second call is idempotent (no EntityAlreadyExists
// error), matching real AWS.
func TestEnableSharingWithAwsOrganization_CreatesServiceLinkedRole(t *testing.T) {
	t.Parallel()

	iamBk := iambackend.NewInMemoryBackendWithConfig(config.DefaultAccountID)
	iamHandler := iambackend.NewHandler(iamBk)

	ramBk := ram.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	ramBk.SetAppConfig(&fakeSiblingServices{iamHandler: iamHandler})
	h := ram.NewHandler(ramBk)

	require.NoError(t, h.Backend.EnableSharingWithAwsOrganization())

	role, err := iamBk.GetRole("AWSServiceRoleForResourceAccessManager")
	require.NoError(t, err)
	assert.Equal(t, "/aws-service-role/ram.amazonaws.com/", role.Path)
	assert.Contains(t, role.AssumeRolePolicyDocument, "ram.amazonaws.com")

	policies, err := iamBk.ListAttachedRolePolicies(role.RoleName)
	require.NoError(t, err)
	policyARNs := make([]string, 0, len(policies))

	for _, p := range policies {
		policyARNs = append(policyARNs, p.PolicyArn)
	}

	assert.Contains(
		t, policyARNs,
		"arn:aws:iam::aws:policy/aws-service-role/AWSResourceAccessManagerServiceRolePolicy",
	)

	// Idempotent: a second call must not error.
	require.NoError(t, h.Backend.EnableSharingWithAwsOrganization())
}

// TestEnableSharingWithAwsOrganization_NoIAMWired proves the op still
// succeeds (a documented no-op) when the IAM backend isn't wired -- e.g.
// unit tests that construct InMemoryBackend directly, with no sibling
// registry.
func TestEnableSharingWithAwsOrganization_NoIAMWired(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, b.EnableSharingWithAwsOrganization())
}
