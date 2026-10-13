package mgn_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mgnsdk "github.com/aws/aws-sdk-go-v2/service/mgn"
	mgntypes "github.com/aws/aws-sdk-go-v2/service/mgn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
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

type fakeEC2Siblings struct{ ec2Handler service.Registerable }

func (f *fakeEC2Siblings) GetEC2Handler() service.Registerable { return f.ec2Handler }

func (f *fakeEC2Siblings) GetOrganizationsHandler() service.Registerable { return nil }

func TestStartTest_LaunchesImportedInstanceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		header       string
		row          string
		wantInstType string
	}{
		{
			name:         "imported_type",
			header:       "mgn:server:hostname,mgn:launch:instance-type",
			row:          "ec2a.example.com,m5.large",
			wantInstType: "m5.large",
		},
		{name: "default_type", header: "mgn:server:hostname", row: "ec2b.example.com", wantInstType: "t3.medium"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2Bk := ec2backend.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
			backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
			t.Cleanup(backend.Close)
			backend.SetAppConfig(&fakeEC2Siblings{ec2Handler: ec2backend.NewHandler(ec2Bk)})
			backend.InitializeService()

			h := mgn.NewHandler(backend)
			client := newRoundTripClient(t, h)
			s3 := newMockS3()
			h.Backend.SetS3Backend(s3)

			final := runImportAndWait(t, client, "ec2-bucket", tt.header+"\n"+tt.row+"\n", s3)
			require.Equal(t, mgntypes.ImportStatusSucceeded, final.Status)

			servers, err := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})
			require.NoError(t, err)
			require.Len(t, servers.Items, 1)

			id := aws.ToString(servers.Items[0].SourceServerID)

			require.Eventually(t, func() bool {
				out, e := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})

				return e == nil && len(out.Items) == 1 && out.Items[0].LifeCycle != nil &&
					out.Items[0].LifeCycle.State == mgntypes.LifeCycleStateReadyForTest
			}, defaultAsyncWait, defaultAsyncPoll)

			_, err = client.StartTest(t.Context(), &mgnsdk.StartTestInput{SourceServerIDs: []string{id}})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				out, e := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})

				return e == nil && len(out.Items) == 1 && out.Items[0].LaunchedInstance != nil
			}, defaultAsyncWait, defaultAsyncPoll)

			out, err := client.DescribeSourceServers(t.Context(), &mgnsdk.DescribeSourceServersInput{})
			require.NoError(t, err)

			instID := aws.ToString(out.Items[0].LaunchedInstance.Ec2InstanceID)
			require.NotEmpty(t, instID)

			found := false

			for _, in := range ec2Bk.DescribeInstances([]string{instID}, "") {
				found = true

				assert.Equal(t, tt.wantInstType, in.InstanceType)
			}

			require.True(t, found, "launched instance must exist in the EC2 backend")
		})
	}
}
