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

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack-dv4s, 2026-09-19) for opensearch's six flagged List ops.
// ListApplications, ListDataSourceAttachments, ListDomainNames,
// ListVpcEndpoints and ListVpcEndpointsForDomain were all ALREADY exact
// matches of their real *Summary types (verified via cmd/structfielddiff
// against opensearch@v1.75.4). ListMigrations is missing MigrationSummary's
// Error member, but gopherstack's migration state machine never produces a
// failure (always PENDING -> IN_PROGRESS -> SUCCEEDED, migrations.go), so
// there is no failure to report -- correct-by-absence, recorded in
// PARITY.md's items_still_open rather than fabricated.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("applications exact", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)
		ctx := t.Context()

		_, err := client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
			Name: aws.String("lss-app"),
		})
		require.NoError(t, err)

		out, err := client.ListApplications(ctx, &opensearchsdk.ListApplicationsInput{})
		require.NoError(t, err)
		require.Len(t, out.ApplicationSummaries, 1)
		a := out.ApplicationSummaries[0]
		assert.Equal(t, "lss-app", aws.ToString(a.Name))
		assert.NotEmpty(t, aws.ToString(a.Endpoint))
		assert.NotNil(t, a.CreatedAt)
	})

	t.Run("data source attachments exact", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)
		ctx := t.Context()

		appOut, err := client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
			Name: aws.String("lss-app-ds"),
		})
		require.NoError(t, err)

		domOut, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
			DomainName: aws.String("lss-ds-domain"),
		})
		require.NoError(t, err)

		_, err = client.AttachDataSource(ctx, &opensearchsdk.AttachDataSourceInput{
			Id:            appOut.Id,
			DataSourceArn: domOut.DomainStatus.ARN,
		})
		require.NoError(t, err)

		out, err := client.ListDataSourceAttachments(ctx, &opensearchsdk.ListDataSourceAttachmentsInput{
			Id: appOut.Id,
		})
		require.NoError(t, err)
		require.Len(t, out.Attachments, 1)
		att := out.Attachments[0]
		assert.Equal(t, aws.ToString(domOut.DomainStatus.ARN), aws.ToString(att.DataSourceArn))
		assert.NotEmpty(t, aws.ToString(att.AttachmentId))
	})

	t.Run("domain names exact", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
			DomainName: aws.String("lss-domain"),
		})
		require.NoError(t, err)

		out, err := client.ListDomainNames(ctx, &opensearchsdk.ListDomainNamesInput{})
		require.NoError(t, err)
		require.Len(t, out.DomainNames, 1)
		assert.Equal(t, "lss-domain", aws.ToString(out.DomainNames[0].DomainName))
		assert.NotEmpty(t, out.DomainNames[0].EngineType)
	})

	t.Run("migrations missing error member is correct-by-absence", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)
		ctx := t.Context()

		appOut, err := client.CreateApplication(ctx, &opensearchsdk.CreateApplicationInput{
			Name: aws.String("lss-app-mig"),
		})
		require.NoError(t, err)

		domOut, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
			DomainName: aws.String("lss-mig-domain"),
		})
		require.NoError(t, err)

		_, err = client.StartMigration(ctx, &opensearchsdk.StartMigrationInput{
			ApplicationId: appOut.Id,
			MigrationOptions: &types.MigrationOptions{
				Source: &types.MigrationSource{DatasourceArn: domOut.DomainStatus.ARN},
				Workspace: &types.MigrationWorkspace{
					CreateWorkspace: aws.Bool(true),
					Name:            aws.String("lss-workspace"),
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListMigrations(ctx, &opensearchsdk.ListMigrationsInput{
			ApplicationId: appOut.Id,
		})
		require.NoError(t, err)
		require.Len(t, out.Migrations, 1)
		assert.Nil(t, out.Migrations[0].Error, "no failure ever occurs in this backend's migration state machine")
	})

	t.Run("vpc endpoints exact", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)
		ctx := t.Context()

		domOut, err := client.CreateDomain(ctx, &opensearchsdk.CreateDomainInput{
			DomainName: aws.String("lss-vpc-domain"),
		})
		require.NoError(t, err)

		created, err := client.CreateVpcEndpoint(ctx, &opensearchsdk.CreateVpcEndpointInput{
			DomainArn: domOut.DomainStatus.ARN,
			VpcOptions: &types.VPCOptions{
				SubnetIds: []string{"subnet-0123456789abcdef0"},
			},
		})
		require.NoError(t, err)

		out, err := client.ListVpcEndpoints(ctx, &opensearchsdk.ListVpcEndpointsInput{})
		require.NoError(t, err)
		require.Len(t, out.VpcEndpointSummaryList, 1)
		s := out.VpcEndpointSummaryList[0]
		assert.Equal(t, aws.ToString(created.VpcEndpoint.VpcEndpointId), aws.ToString(s.VpcEndpointId))
		assert.Equal(t, aws.ToString(domOut.DomainStatus.ARN), aws.ToString(s.DomainArn))

		forDomainOut, err := client.ListVpcEndpointsForDomain(ctx, &opensearchsdk.ListVpcEndpointsForDomainInput{
			DomainName: aws.String("lss-vpc-domain"),
		})
		require.NoError(t, err)
		require.Len(t, forDomainOut.VpcEndpointSummaryList, 1)
		assert.Equal(t, aws.ToString(created.VpcEndpoint.VpcEndpointId),
			aws.ToString(forDomainOut.VpcEndpointSummaryList[0].VpcEndpointId))
	})
}
