package ram_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// fakeSiblingServices structurally satisfies ram's unexported
// siblingServices interface (matched by SetAppConfig's type assertion),
// mirroring how the real *CLI wires GetIAMHandler/GetOrganizationsHandler.
type fakeSiblingServices struct {
	iamHandler           service.Registerable
	organizationsHandler service.Registerable
}

func (f *fakeSiblingServices) GetIAMHandler() service.Registerable { return f.iamHandler }

func (f *fakeSiblingServices) GetOrganizationsHandler() service.Registerable {
	return f.organizationsHandler
}

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

// TestEnableSharingWithAwsOrganization_EnablesOrganizationsServiceAccess
// proves EnableSharingWithAwsOrganization enables "ram.amazonaws.com" as a
// trusted service principal in a wired Organizations backend -- matching
// real AWS, where the same call that creates RAM's service-linked role also
// registers RAM for org-wide access, which
// organizations:ListAWSServiceAccessForOrganization then lists.
func TestEnableSharingWithAwsOrganization_EnablesOrganizationsServiceAccess(t *testing.T) {
	t.Parallel()

	orgBk := organizationsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, _, err := orgBk.CreateOrganization("ALL")
	require.NoError(t, err)

	orgHandler := organizationsbackend.NewHandler(orgBk)

	ramBk := ram.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	ramBk.SetAppConfig(&fakeSiblingServices{organizationsHandler: orgHandler})
	h := ram.NewHandler(ramBk)

	require.NoError(t, h.Backend.EnableSharingWithAwsOrganization())

	principals, err := orgBk.ListAWSServiceAccessForOrganization()
	require.NoError(t, err)

	var found bool

	for _, p := range principals {
		if p.ServicePrincipal == "ram.amazonaws.com" {
			found = true
		}
	}

	assert.True(t, found, "ram.amazonaws.com should be enabled for org-wide access")

	// Idempotent: a second call must not error.
	require.NoError(t, h.Backend.EnableSharingWithAwsOrganization())
}

// TestEnableSharingWithAwsOrganization_NoOrganization proves that, when a
// wired Organizations backend has no organization, EnableSharingWithAwsOrganization
// returns ram's own ErrOperationNotPermitted -- Organizations'
// AWSOrganizationsNotInUseException isn't declared on RAM's
// EnableSharingWithAwsOrganization (ram@v1.39.4 deserializers.go), so it must
// not leak through as-is.
func TestEnableSharingWithAwsOrganization_NoOrganization(t *testing.T) {
	t.Parallel()

	orgBk := organizationsbackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	orgHandler := organizationsbackend.NewHandler(orgBk)

	ramBk := ram.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	ramBk.SetAppConfig(&fakeSiblingServices{organizationsHandler: orgHandler})
	h := ram.NewHandler(ramBk)

	err := h.Backend.EnableSharingWithAwsOrganization()
	require.Error(t, err)
	assert.ErrorIs(t, err, ram.ErrOperationNotPermitted)
}
