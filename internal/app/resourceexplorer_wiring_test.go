package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	retypes "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestResourceExplorerWiringSearchesRealResources(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()

	sqsC := sqs.NewFromConfig(fx.cfg)
	re := resourceexplorer2.NewFromConfig(fx.cfg)

	_, err := sqsC.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("re2-orders"), Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	vpc, err := ec2.NewFromConfig(fx.cfg).CreateVpc(ctx, &ec2.CreateVpcInput{
		CidrBlock: aws.String("10.9.0.0/16"),
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeVpc,
			Tags:         []ec2types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		}},
	})
	require.NoError(t, err)

	_, err = re.CreateIndex(ctx, &resourceexplorer2.CreateIndexInput{})
	require.NoError(t, err)

	view, err := re.CreateView(ctx, &resourceexplorer2.CreateViewInput{
		ViewName:           aws.String("all"),
		IncludedProperties: []retypes.IncludedProperty{{Name: aws.String("tags")}},
	})
	require.NoError(t, err)

	_, err = re.AssociateDefaultView(ctx, &resourceexplorer2.AssociateDefaultViewInput{ViewArn: view.View.ViewArn})
	require.NoError(t, err)

	tests := []struct {
		name    string
		query   string
		wantARN string
	}{
		{name: "queue_by_service", query: "service:sqs re2-orders", wantARN: ":re2-orders"},
		{name: "queue_by_tag", query: "resourcetype:sqs:queue tag:env=prod", wantARN: ":re2-orders"},
		{name: "vpc_by_type", query: "resourcetype:ec2:vpc", wantARN: ":vpc/" + aws.ToString(vpc.Vpc.VpcId)},
		{name: "vpc_by_tag", query: "tag:env=prod service:ec2", wantARN: ":vpc/" + aws.ToString(vpc.Vpc.VpcId)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, serr := re.Search(t.Context(), &resourceexplorer2.SearchInput{QueryString: aws.String(tc.query)})
			require.NoError(t, serr)
			require.NotEmpty(t, out.Resources)

			var found bool

			for _, r := range out.Resources {
				found = found || containsStr(aws.ToString(r.Arn), tc.wantARN)
			}

			assert.True(t, found, "%s should list %s, got %d resources", tc.query, tc.wantARN, len(out.Resources))
		})
	}

	_, err = sqsC.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("re2-fresh")})
	require.NoError(t, err)

	fresh, err := re.Search(ctx, &resourceexplorer2.SearchInput{QueryString: aws.String("re2-fresh")})
	require.NoError(t, err)
	assert.Len(t, fresh.Resources, 1, "a resource created after a search is searchable at once")
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}

	return false
}

func TestResourceExplorerWiringOrganizationsAccess(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	re := resourceexplorer2.NewFromConfig(fx.cfg)

	before, err := re.GetAccountLevelServiceConfiguration(
		ctx,
		&resourceexplorer2.GetAccountLevelServiceConfigurationInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, retypes.AWSServiceAccessStatusDisabled, before.OrgConfiguration.AWSServiceAccessStatus)

	orgH, ok := serviceByName(fx.services)["Organizations"].(*organizationsbackend.Handler)
	require.True(t, ok)

	orgBk, ok := orgH.Backend.(*organizationsbackend.InMemoryBackend)
	require.True(t, ok)

	_, _, err = orgBk.CreateOrganization("ALL")
	require.NoError(t, err)
	require.NoError(t, orgBk.EnableAWSServiceAccess("resource-explorer-2.amazonaws.com"))

	after, err := re.GetAccountLevelServiceConfiguration(
		ctx,
		&resourceexplorer2.GetAccountLevelServiceConfigurationInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, retypes.AWSServiceAccessStatusEnabled, after.OrgConfiguration.AWSServiceAccessStatus)
	assert.NotNil(t, after.OrgConfiguration.ServiceLinkedRole)
}

func TestResourceExplorerWiringSearchesAllTaggingServices(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	ctx := t.Context()
	re := resourceexplorer2.NewFromConfig(fx.cfg)

	ampH, ok := serviceByName(fx.services)["AMP"].(*ampbackend.Handler)
	require.True(t, ok)

	ws, err := ampH.Backend.CreateWorkspace(ctx, ampbackend.CreateWorkspaceInput{Alias: "re2-untagged"})
	require.NoError(t, err)

	_, err = re.CreateIndex(ctx, &resourceexplorer2.CreateIndexInput{})
	require.NoError(t, err)

	view, err := re.CreateView(ctx, &resourceexplorer2.CreateViewInput{ViewName: aws.String("all")})
	require.NoError(t, err)

	_, err = re.AssociateDefaultView(ctx, &resourceexplorer2.AssociateDefaultViewInput{ViewArn: view.View.ViewArn})
	require.NoError(t, err)

	tests := []struct {
		name  string
		query string
	}{
		{name: "by_service", query: "service:aps"},
		{name: "by_id", query: "id:" + ws.WorkspaceID},
		{name: "negated_other_service", query: "service:aps -service:sqs"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, serr := re.Search(t.Context(), &resourceexplorer2.SearchInput{QueryString: aws.String(tc.query)})
			require.NoError(t, serr)

			var found bool

			for _, r := range out.Resources {
				found = found || containsStr(aws.ToString(r.Arn), ws.WorkspaceID)
			}

			assert.True(t, found, "%s should list the untagged AMP workspace", tc.query)
		})
	}
}
