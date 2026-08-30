package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// TestDescribeOutboundConnections_ConnectionIDFilter proves the Filters
// (Name: "connection-id") member of DescribeOutboundConnectionsInput is
// actually applied -- previously the handler never read the request body at
// all (api_op_DescribeOutboundConnections.go: Filters/MaxResults/NextToken
// are all JSON-body-bound).
func TestDescribeOutboundConnections_ConnectionIDFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	ids := make([]string, 0, 3)

	for range 3 {
		out, err := client.CreateOutboundConnection(ctx, &opensearchsdk.CreateOutboundConnectionInput{
			ConnectionAlias: aws.String("alias"),
			LocalDomainInfo: &types.DomainInformationContainer{
				AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("local")},
			},
			RemoteDomainInfo: &types.DomainInformationContainer{
				AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("remote")},
			},
		})
		require.NoError(t, err)
		ids = append(ids, aws.ToString(out.ConnectionId))
	}

	// Filtering by a single connection ID must return exactly that
	// connection, not all three.
	described, err := client.DescribeOutboundConnections(ctx, &opensearchsdk.DescribeOutboundConnectionsInput{
		Filters: []types.Filter{
			{Name: aws.String("connection-id"), Values: []string{ids[1]}},
		},
	})
	require.NoError(t, err)
	require.Len(t, described.Connections, 1, "connection-id filter must exclude non-matching connections")
	assert.Equal(t, ids[1], aws.ToString(described.Connections[0].ConnectionId))

	// Unfiltered describes everything.
	all, err := client.DescribeOutboundConnections(ctx, &opensearchsdk.DescribeOutboundConnectionsInput{})
	require.NoError(t, err)
	assert.Len(t, all.Connections, 3)
}

// TestDescribeInboundConnections_MaxResultsPagination proves MaxResults/
// NextToken are honored: previously they were accepted on the wire but
// never read, so the response always contained every connection in one
// unbounded page.
func TestDescribeInboundConnections_MaxResultsPagination(t *testing.T) {
	t.Parallel()

	_, h := newTestHandlerAndBackend()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	for range 3 {
		_, err := client.CreateOutboundConnection(ctx, &opensearchsdk.CreateOutboundConnectionInput{
			ConnectionAlias: aws.String("alias"),
			LocalDomainInfo: &types.DomainInformationContainer{
				AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("local")},
			},
			RemoteDomainInfo: &types.DomainInformationContainer{
				AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("remote")},
			},
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeInboundConnections(ctx, &opensearchsdk.DescribeInboundConnectionsInput{
		MaxResults: 2,
	})
	require.NoError(t, err)
	require.Len(t, page1.Connections, 2, "MaxResults must cap the page size")
	require.NotNil(t, page1.NextToken, "a truncated result must carry a NextToken")
	assert.NotEmpty(t, *page1.NextToken)

	page2, err := client.DescribeInboundConnections(ctx, &opensearchsdk.DescribeInboundConnectionsInput{
		MaxResults: 2,
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Connections, 1, "the second page must return the remainder")

	seen := map[string]bool{}
	for _, c := range page1.Connections {
		seen[aws.ToString(c.ConnectionId)] = true
	}

	for _, c := range page2.Connections {
		assert.False(t, seen[aws.ToString(c.ConnectionId)], "NextToken must not re-return an item from page 1")
	}
}

// TestListApplications_StatusesFilter proves the Statuses query parameter
// (repeated "statuses" query values, api_op_ListApplications.go
// serializers.go) is applied. Every application this backend creates is
// implicitly ACTIVE (DeleteApplication removes its record immediately, no
// DELETING window -- see applications.go), so a Statuses filter that
// excludes ACTIVE must yield an empty list rather than every application.
func TestListApplications_StatusesFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
		Name: aws.String("app-one"),
	})
	require.NoError(t, err)

	active, err := client.ListApplications(ctx, &opensearchsdk.ListApplicationsInput{
		Statuses: []types.ApplicationStatus{types.ApplicationStatusActive},
	})
	require.NoError(t, err)
	assert.Len(t, active.ApplicationSummaries, 1, "ACTIVE filter must match the existing application")

	deleting, err := client.ListApplications(ctx, &opensearchsdk.ListApplicationsInput{
		Statuses: []types.ApplicationStatus{types.ApplicationStatusDeleting},
	})
	require.NoError(t, err)
	assert.Empty(t, deleting.ApplicationSummaries,
		"a status this backend never produces must yield an empty result, not every application")
}

// TestListDomainMaintenances_ActionFilter proves the Action query parameter
// is applied -- previously ListDomainMaintenances ignored both Action and
// Status entirely and returned every maintenance record on the domain.
func TestListDomainMaintenances_ActionFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	domainName := "maint-filter-domain"
	_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{DomainName: aws.String(domainName)})
	require.NoError(t, err)

	_, err = client.StartDomainMaintenance(ctx, &opensearchsdk.StartDomainMaintenanceInput{
		DomainName: aws.String(domainName),
		Action:     types.MaintenanceTypeRebootNode,
		NodeId:     aws.String("node-0"),
	})
	require.NoError(t, err)

	_, err = client.StartDomainMaintenance(ctx, &opensearchsdk.StartDomainMaintenanceInput{
		DomainName: aws.String(domainName),
		Action:     types.MaintenanceTypeRestartDashboard,
	})
	require.NoError(t, err)

	rebootOnly, err := client.ListDomainMaintenances(ctx, &opensearchsdk.ListDomainMaintenancesInput{
		DomainName: aws.String(domainName),
		Action:     types.MaintenanceTypeRebootNode,
	})
	require.NoError(t, err)
	require.Len(t, rebootOnly.DomainMaintenances, 1, "Action filter must exclude non-matching maintenance records")
	assert.Equal(t, types.MaintenanceTypeRebootNode, rebootOnly.DomainMaintenances[0].Action)

	unfiltered, err := client.ListDomainMaintenances(ctx, &opensearchsdk.ListDomainMaintenancesInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err)
	assert.Len(t, unfiltered.DomainMaintenances, 2)
}

// TestDescribePackages_PackageStatusFilter proves Filters entries other than
// "PackageID" (PackageName/PackageStatus/PackageType) are applied --
// previously only "PackageID" was ever honored, so e.g. a PackageStatus
// filter silently matched every package regardless of status.
func TestDescribePackages_PackageStatusFilter(t *testing.T) {
	t.Parallel()

	_, h := newTestHandlerAndBackend()
	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	_, err := client.CreatePackage(ctx, &opensearchsdk.CreatePackageInput{
		PackageName: aws.String("pkg-one"),
		PackageType: types.PackageTypeTxtDictionary,
		PackageSource: &types.PackageSource{
			S3BucketName: aws.String("pkg-bucket"),
			S3Key:        aws.String("pkg-key"),
		},
	})
	require.NoError(t, err)

	available, err := client.DescribePackages(ctx, &opensearchsdk.DescribePackagesInput{
		Filters: []types.DescribePackagesFilter{
			{Name: types.DescribePackagesFilterNamePackageStatus, Value: []string{"AVAILABLE"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, available.PackageDetailsList, 1, "matching PackageStatus filter must return the package")

	noneMatching, err := client.DescribePackages(ctx, &opensearchsdk.DescribePackagesInput{
		Filters: []types.DescribePackagesFilter{
			{Name: types.DescribePackagesFilterNamePackageStatus, Value: []string{"DELETING"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, noneMatching.PackageDetailsList, "a PackageStatus filter matching nothing must exclude the package")
}

// TestListMigrations_MaxResultsPagination proves MaxResults/NextToken are
// honored for ListMigrations, which previously ignored both and always
// returned the full unbounded result in one page. Migrations are seeded
// directly through the backend (StartMigration's own validation chain --
// resolveDataSourceRefLocked/resolveMigrationWorkspaceLocked -- is exercised
// elsewhere; what this test verifies is the read path's MaxResults/NextToken
// wire binding, so only ListMigrations itself goes through the real client).
func TestListMigrations_MaxResultsPagination(t *testing.T) {
	t.Parallel()

	backend, h := newTestHandlerAndBackend()

	app, err := backend.CreateApplication("migration-app", nil, nil, nil)
	require.NoError(t, err)

	domain, err := backend.CreateDomain(opensearch.CreateDomainInput{Name: "migration-domain"})
	require.NoError(t, err)

	for range 3 {
		_, startErr := backend.StartMigration(
			app.ID, domain.ARN,
			&opensearch.MigrationWorkspaceInput{CreateWorkspace: true, Name: "ws"},
			nil, "",
		)
		require.NoError(t, startErr)
	}

	client := newTestOpenSearchClient(t, h)
	ctx := t.Context()

	page1, err := client.ListMigrations(ctx, &opensearchsdk.ListMigrationsInput{
		ApplicationId: aws.String(app.ID),
		MaxResults:    2,
	})
	require.NoError(t, err)
	require.Len(t, page1.Migrations, 2, "MaxResults must cap the page size")
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListMigrations(ctx, &opensearchsdk.ListMigrationsInput{
		ApplicationId: aws.String(app.ID),
		MaxResults:    2,
		NextToken:     page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.Migrations, 1, "the second page must return the remainder")
}
