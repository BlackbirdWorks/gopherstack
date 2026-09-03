package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mgn"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
)

// fakeSiblingServices structurally satisfies mgn's unexported siblingServices
// interface (matched by SetAppConfig's type assertion), mirroring how the real
// *CLI wires GetOrganizationsHandler.
type fakeSiblingServices struct {
	orgHandler service.Registerable
}

func (f *fakeSiblingServices) GetEC2Handler() service.Registerable { return nil }

func (f *fakeSiblingServices) GetOrganizationsHandler() service.Registerable {
	return f.orgHandler
}

// TestListManagedAccounts_Pagination proves ListManagedAccounts pages through
// every account in the organization exactly once instead of returning them
// all on a single page with no cursor: the org's own management account plus
// two member accounts (3 total) requested at MaxResults=2 must split across
// two pages, with the second page's token yielding the remainder.
func TestListManagedAccounts_Pagination(t *testing.T) {
	t.Parallel()

	orgBk := organizationsbackend.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	orgHandler := organizationsbackend.NewHandler(orgBk)

	_, _, err := orgBk.CreateOrganization("ALL")
	require.NoError(t, err)

	_, err = orgBk.CreateAccount("member-1", "member-1@example.com", "OrganizationAccountAccessRole", "ALLOW", nil)
	require.NoError(t, err)
	_, err = orgBk.CreateAccount("member-2", "member-2@example.com", "OrganizationAccountAccessRole", "ALLOW", nil)
	require.NoError(t, err)

	backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)
	backend.SetAppConfig(&fakeSiblingServices{orgHandler: orgHandler})
	backend.InitializeService()

	h := mgn.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	page1, err := client.ListManagedAccounts(ctx, &mgnsdk.ListManagedAccountsInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 2)
	require.NotNil(t, page1.NextToken, "first page must return a cursor when more accounts remain")

	page2, err := client.ListManagedAccounts(ctx, &mgnsdk.ListManagedAccountsInput{
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Items, 1)
	require.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{}
	for _, a := range page1.Items {
		seen[aws.ToString(a.AccountId)] = true
	}

	for _, a := range page2.Items {
		id := aws.ToString(a.AccountId)
		require.False(t, seen[id], "account %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, 3)
	require.Contains(t, seen, rtTestAccountID)
}
