package detective //nolint:testpackage // needs seedGraph/seedMember for state CreateMembers can't produce.

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	detectivesdk "github.com/aws/aws-sdk-go-v2/service/detective"
	detectivetypes "github.com/aws/aws-sdk-go-v2/service/detective/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newSlice27DetectiveClient stands up the real aws-sdk-go-v2 detective client
// against an httptest server running this package's Handler, the same
// pattern as newTestDetectiveSDKClient in wire_sdk_roundtrip_test.go. Placed
// in package detective (not detective_test) so this file can reuse
// whitebox_test.go's unexported seedGraph/seedMember helpers instead of
// adding new exports to export_test.go.
func newSlice27DetectiveClient(t *testing.T, h *Handler) *detectivesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return detectivesdk.NewFromConfig(cfg, func(o *detectivesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestTypedSlice27RealClient drives detective's remaining typed-client-blind
// ops through the real aws-sdk-go-v2 client (gopherstack-n3zi typed slice 27).
func TestTypedSlice27RealClient(t *testing.T) {
	t.Parallel()

	t.Run("member lifecycle create get delete list", func(t *testing.T) {
		t.Parallel()

		backend := NewInMemoryBackend("000000000000", "us-east-1")
		client := newSlice27DetectiveClient(t, NewHandler(backend))
		ctx := t.Context()

		graphOut, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
		require.NoError(t, err)

		createOut, err := client.CreateMembers(ctx, &detectivesdk.CreateMembersInput{
			GraphArn: graphOut.GraphArn,
			Accounts: []detectivetypes.Account{
				{AccountId: aws.String("111111111111"), EmailAddress: aws.String("a@example.com")},
				{AccountId: aws.String("222222222222"), EmailAddress: aws.String("b@example.com")},
			},
		})
		require.NoError(t, err)
		require.Len(t, createOut.Members, 2)
		assert.Empty(t, createOut.UnprocessedAccounts)

		getOut, err := client.GetMembers(ctx, &detectivesdk.GetMembersInput{
			GraphArn:   graphOut.GraphArn,
			AccountIds: []string{"111111111111", "999999999999"},
		})
		require.NoError(t, err)
		require.Len(t, getOut.MemberDetails, 1)
		assert.Equal(t, "111111111111", aws.ToString(getOut.MemberDetails[0].AccountId))
		require.Len(t, getOut.UnprocessedAccounts, 1)
		assert.Equal(t, "999999999999", aws.ToString(getOut.UnprocessedAccounts[0].AccountId))

		listOut, err := client.ListMembers(ctx, &detectivesdk.ListMembersInput{GraphArn: graphOut.GraphArn})
		require.NoError(t, err)
		assert.Len(t, listOut.MemberDetails, 2)

		delOut, err := client.DeleteMembers(ctx, &detectivesdk.DeleteMembersInput{
			GraphArn:   graphOut.GraphArn,
			AccountIds: []string{"111111111111"},
		})
		require.NoError(t, err)
		require.Len(t, delOut.AccountIds, 1)
		assert.Equal(t, "111111111111", delOut.AccountIds[0])

		listOut2, err := client.ListMembers(ctx, &detectivesdk.ListMembersInput{GraphArn: graphOut.GraphArn})
		require.NoError(t, err)
		assert.Len(t, listOut2.MemberDetails, 1)
	})

	t.Run("member self status transitions reject disassociate monitor", func(t *testing.T) {
		t.Parallel()

		backend := NewInMemoryBackend("000000000000", "us-east-1")
		client := newSlice27DetectiveClient(t, NewHandler(backend))
		ctx := t.Context()

		graphARN := "arn:aws:detective:us-east-1:333333333333:graph:aaaabbbbcccc00001111222233336666"
		seedGraph(backend, graphARN)
		seedMember(backend, graphARN, backend.AccountID(), memberStatusInvited)

		_, err := client.RejectInvitation(ctx, &detectivesdk.RejectInvitationInput{GraphArn: aws.String(graphARN)})
		require.NoError(t, err)

		invOut, err := client.ListInvitations(ctx, &detectivesdk.ListInvitationsInput{})
		require.NoError(t, err)
		assert.Empty(t, invOut.Invitations, "rejected invitation must not remain a membership")

		graphARN2 := "arn:aws:detective:us-east-1:444444444444:graph:aaaabbbbcccc00001111222233337777"
		seedGraph(backend, graphARN2)
		seedMember(backend, graphARN2, backend.AccountID(), memberStatusEnabled)

		_, err = client.DisassociateMembership(
			ctx,
			&detectivesdk.DisassociateMembershipInput{GraphArn: aws.String(graphARN2)},
		)
		require.NoError(t, err)

		invOut2, err := client.ListInvitations(ctx, &detectivesdk.ListInvitationsInput{})
		require.NoError(t, err)
		assert.Empty(t, invOut2.Invitations, "disassociated membership must not remain")

		graphARN3 := "arn:aws:detective:us-east-1:555555555555:graph:aaaabbbbcccc00001111222233338888"
		seedGraph(backend, graphARN3)
		seedMember(backend, graphARN3, "666666666666", memberStatusAcceptedDisabled)

		_, err = client.StartMonitoringMember(ctx, &detectivesdk.StartMonitoringMemberInput{
			GraphArn:  aws.String(graphARN3),
			AccountId: aws.String("666666666666"),
		})
		require.NoError(t, err)

		gm, err := client.GetMembers(ctx, &detectivesdk.GetMembersInput{
			GraphArn:   aws.String(graphARN3),
			AccountIds: []string{"666666666666"},
		})
		require.NoError(t, err)
		require.Len(t, gm.MemberDetails, 1)
		assert.Equal(t, "ENABLED", string(gm.MemberDetails[0].Status))
	})

	t.Run("datasource packages batch get and update", func(t *testing.T) {
		t.Parallel()

		backend := NewInMemoryBackend("000000000000", "us-east-1")
		client := newSlice27DetectiveClient(t, NewHandler(backend))
		ctx := t.Context()

		graphOut, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
		require.NoError(t, err)

		_, err = client.CreateMembers(ctx, &detectivesdk.CreateMembersInput{
			GraphArn: graphOut.GraphArn,
			Accounts: []detectivetypes.Account{
				{AccountId: aws.String("111111111111"), EmailAddress: aws.String("a@example.com")},
			},
		})
		require.NoError(t, err)

		_, err = client.UpdateDatasourcePackages(ctx, &detectivesdk.UpdateDatasourcePackagesInput{
			GraphArn:           graphOut.GraphArn,
			DatasourcePackages: []detectivetypes.DatasourcePackage{detectivetypes.DatasourcePackageDetectiveCore},
		})
		require.NoError(t, err)

		listOut, err := client.ListDatasourcePackages(ctx, &detectivesdk.ListDatasourcePackagesInput{
			GraphArn: graphOut.GraphArn,
		})
		require.NoError(t, err)
		require.Contains(t, listOut.DatasourcePackages, "DETECTIVE_CORE")
		assert.Equal(
			t,
			detectivetypes.DatasourcePackageIngestStateStarted,
			listOut.DatasourcePackages["DETECTIVE_CORE"].DatasourcePackageIngestState,
		)

		bgOut, err := client.BatchGetGraphMemberDatasources(ctx, &detectivesdk.BatchGetGraphMemberDatasourcesInput{
			GraphArn:   graphOut.GraphArn,
			AccountIds: []string{"111111111111", "999999999999"},
		})
		require.NoError(t, err)
		require.Len(t, bgOut.MemberDatasources, 1)
		assert.Equal(t, "111111111111", aws.ToString(bgOut.MemberDatasources[0].AccountId))
		require.Contains(t, bgOut.MemberDatasources[0].DatasourcePackageIngestHistory, "DETECTIVE_CORE")
		require.Len(t, bgOut.UnprocessedAccounts, 1)

		missingGraphARN := "arn:aws:detective:us-east-1:999999999999:graph:doesnotexist000000000000000000"
		bmOut, err := client.BatchGetMembershipDatasources(ctx, &detectivesdk.BatchGetMembershipDatasourcesInput{
			GraphArns: []string{aws.ToString(graphOut.GraphArn), missingGraphARN},
		})
		require.NoError(t, err)
		require.Len(t, bmOut.MembershipDatasources, 1)
		assert.Equal(t, aws.ToString(graphOut.GraphArn), aws.ToString(bmOut.MembershipDatasources[0].GraphArn))
		require.Len(t, bmOut.UnprocessedGraphs, 1)
	})

	t.Run("investigations get list update state", func(t *testing.T) {
		t.Parallel()

		backend := NewInMemoryBackend("000000000000", "us-east-1")
		client := newSlice27DetectiveClient(t, NewHandler(backend))
		ctx := t.Context()

		graphOut, err := client.CreateGraph(ctx, &detectivesdk.CreateGraphInput{})
		require.NoError(t, err)

		startOut, err := client.StartInvestigation(ctx, &detectivesdk.StartInvestigationInput{
			GraphArn:       graphOut.GraphArn,
			EntityArn:      aws.String("arn:aws:iam::000000000000:user/testuser"),
			ScopeStartTime: aws.Time(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			ScopeEndTime:   aws.Time(time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)),
		})
		require.NoError(t, err)

		getOut, err := client.GetInvestigation(ctx, &detectivesdk.GetInvestigationInput{
			GraphArn:        graphOut.GraphArn,
			InvestigationId: startOut.InvestigationId,
		})
		require.NoError(t, err)
		assert.Equal(t, detectivetypes.StateActive, getOut.State)
		assert.Equal(t, detectivetypes.EntityTypeIamUser, getOut.EntityType)

		listOut, err := client.ListInvestigations(ctx, &detectivesdk.ListInvestigationsInput{
			GraphArn: graphOut.GraphArn,
		})
		require.NoError(t, err)
		require.Len(t, listOut.InvestigationDetails, 1)
		assert.Equal(
			t,
			aws.ToString(startOut.InvestigationId),
			aws.ToString(listOut.InvestigationDetails[0].InvestigationId),
		)

		_, err = client.UpdateInvestigationState(ctx, &detectivesdk.UpdateInvestigationStateInput{
			GraphArn:        graphOut.GraphArn,
			InvestigationId: startOut.InvestigationId,
			State:           detectivetypes.StateArchived,
		})
		require.NoError(t, err)

		getOut2, err := client.GetInvestigation(ctx, &detectivesdk.GetInvestigationInput{
			GraphArn:        graphOut.GraphArn,
			InvestigationId: startOut.InvestigationId,
		})
		require.NoError(t, err)
		assert.Equal(t, detectivetypes.StateArchived, getOut2.State)
	})

	t.Run("organization admin lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := NewInMemoryBackend("000000000000", "us-east-1")
		client := newSlice27DetectiveClient(t, NewHandler(backend))
		ctx := t.Context()

		_, err := client.EnableOrganizationAdminAccount(ctx, &detectivesdk.EnableOrganizationAdminAccountInput{
			AccountId: aws.String("777777777777"),
		})
		require.NoError(t, err)

		listOut, err := client.ListOrganizationAdminAccounts(ctx, &detectivesdk.ListOrganizationAdminAccountsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.Administrators, 1)
		assert.Equal(t, "777777777777", aws.ToString(listOut.Administrators[0].AccountId))
		graphARN := aws.ToString(listOut.Administrators[0].GraphArn)
		require.NotEmpty(t, graphARN)

		descOut, err := client.DescribeOrganizationConfiguration(
			ctx,
			&detectivesdk.DescribeOrganizationConfigurationInput{GraphArn: aws.String(graphARN)},
		)
		require.NoError(t, err)
		assert.False(t, descOut.AutoEnable)

		_, err = client.UpdateOrganizationConfiguration(ctx, &detectivesdk.UpdateOrganizationConfigurationInput{
			GraphArn:   aws.String(graphARN),
			AutoEnable: true,
		})
		require.NoError(t, err)

		descOut2, err := client.DescribeOrganizationConfiguration(
			ctx,
			&detectivesdk.DescribeOrganizationConfigurationInput{GraphArn: aws.String(graphARN)},
		)
		require.NoError(t, err)
		assert.True(t, descOut2.AutoEnable)

		_, err = client.DisableOrganizationAdminAccount(ctx, &detectivesdk.DisableOrganizationAdminAccountInput{})
		require.NoError(t, err)

		listOut2, err := client.ListOrganizationAdminAccounts(ctx, &detectivesdk.ListOrganizationAdminAccountsInput{})
		require.NoError(t, err)
		assert.Empty(t, listOut2.Administrators)
	})
}
