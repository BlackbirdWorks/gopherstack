package ram_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

const (
	subnetA = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-aaa"
	subnetB = "arn:aws:ec2:us-east-1:000000000000:subnet/subnet-bbb"
	vpcA    = "arn:aws:ec2:us-east-1:000000000000:vpc/vpc-aaa"
	account = "000000000000"
)

func newShareClient(t *testing.T) *ramsdk.Client {
	t.Helper()

	return newTestRAMClient(t, ram.NewHandler(ram.NewInMemoryBackend(account, "us-east-1")))
}

func shareARN(out *ramsdk.CreateResourceShareOutput) string {
	return aws.ToString(out.ResourceShare.ResourceShareArn)
}

func TestRealClient_CreateResourceShareClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		second    *ramsdk.CreateResourceShareInput
		name      string
		wantErr   string
		wantSame  bool
		wantCount int
	}{
		{
			name:      "same_token_same_params_replays",
			second:    &ramsdk.CreateResourceShareInput{Name: aws.String("tok"), ClientToken: aws.String("t1")},
			wantSame:  true,
			wantCount: 1,
		},
		{
			name:    "same_token_other_params_mismatch",
			second:  &ramsdk.CreateResourceShareInput{Name: aws.String("other"), ClientToken: aws.String("t1")},
			wantErr: "IdempotentParameterMismatchException",
		},
		{
			name:      "no_token_creates_new",
			second:    &ramsdk.CreateResourceShareInput{Name: aws.String("tok")},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newShareClient(t)
			first, err := client.CreateResourceShare(t.Context(), &ramsdk.CreateResourceShareInput{
				Name: aws.String("tok"), ClientToken: aws.String("t1"),
			})
			require.NoError(t, err)

			second, err := client.CreateResourceShare(t.Context(), tt.second)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, shareARN(first) == shareARN(second))

			listed, err := client.GetResourceShares(t.Context(), &ramsdk.GetResourceSharesInput{
				ResourceOwner: ramtypes.ResourceOwnerSelf,
			})
			require.NoError(t, err)
			assert.Len(t, listed.ResourceShares, tt.wantCount)
		})
	}
}

func TestRealClient_PermissionClientToken(t *testing.T) {
	t.Parallel()

	const policy = `{"Effect":"Allow","Action":["ec2:RunInstances"]}`

	client := newShareClient(t)
	ctx := t.Context()

	create := func(name string) (*ramsdk.CreatePermissionOutput, error) {
		return client.CreatePermission(ctx, &ramsdk.CreatePermissionInput{
			Name: aws.String(name), ResourceType: aws.String("ec2:Subnet"),
			PolicyTemplate: aws.String(policy), ClientToken: aws.String("p1"),
		})
	}

	first, err := create("perm")
	require.NoError(t, err)

	replay, err := create("perm")
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.Permission.Arn), aws.ToString(replay.Permission.Arn))

	_, err = create("perm2")
	require.ErrorContains(t, err, "IdempotentParameterMismatchException")

	addVersion := func() (*ramsdk.CreatePermissionVersionOutput, error) {
		return client.CreatePermissionVersion(ctx, &ramsdk.CreatePermissionVersionInput{
			PermissionArn: first.Permission.Arn, PolicyTemplate: aws.String(policy), ClientToken: aws.String("v1"),
		})
	}

	v1, err := addVersion()
	require.NoError(t, err)

	v1Replay, err := addVersion()
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(v1.Permission.Version), aws.ToString(v1Replay.Permission.Version))

	versions, err := client.ListPermissionVersions(ctx, &ramsdk.ListPermissionVersionsInput{
		PermissionArn: first.Permission.Arn,
	})
	require.NoError(t, err)
	assert.Len(t, versions.Permissions, 2)
}

func TestRealClient_ShareConfigurationAndSources(t *testing.T) {
	t.Parallel()

	client := newShareClient(t)
	ctx := t.Context()

	created, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name: aws.String("src"),
		ResourceShareConfiguration: &ramtypes.ResourceShareConfiguration{
			RetainSharingOnAccountLeaveOrganization: aws.Bool(true),
		},
		Sources: []string{"111111111111"},
	})
	require.NoError(t, err)

	cfg := created.ResourceShare.ResourceShareConfiguration
	require.NotNil(t, cfg)
	assert.True(t, aws.ToBool(cfg.RetainSharingOnAccountLeaveOrganization))

	_, err = client.AssociateResourceShare(ctx, &ramsdk.AssociateResourceShareInput{
		ResourceShareArn: created.ResourceShare.ResourceShareArn, Sources: []string{"222222222222"},
	})
	require.NoError(t, err)

	list := func(in ramsdk.ListSourceAssociationsInput) []string {
		out, listErr := client.ListSourceAssociations(ctx, &in)
		require.NoError(t, listErr)

		ids := make([]string, 0, len(out.SourceAssociations))
		for _, s := range out.SourceAssociations {
			ids = append(ids, aws.ToString(s.SourceId))
			assert.Equal(t, shareARN(created), aws.ToString(s.ResourceShareArn))
		}

		return ids
	}

	assert.Equal(t, []string{"111111111111", "222222222222"}, list(ramsdk.ListSourceAssociationsInput{}))
	assert.Equal(t, []string{"222222222222"},
		list(ramsdk.ListSourceAssociationsInput{SourceId: aws.String("222222222222")}))
	assert.Empty(t, list(ramsdk.ListSourceAssociationsInput{ResourceShareArns: []string{"arn:none"}}))
	assert.Empty(t, list(ramsdk.ListSourceAssociationsInput{
		AssociationStatus: ramtypes.ResourceShareAssociationStatusDisassociated,
	}))

	_, err = client.DisassociateResourceShare(ctx, &ramsdk.DisassociateResourceShareInput{
		ResourceShareArn: created.ResourceShare.ResourceShareArn, Sources: []string{"111111111111"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"222222222222"}, list(ramsdk.ListSourceAssociationsInput{}))
	assert.Equal(t, []string{"111111111111"}, list(ramsdk.ListSourceAssociationsInput{
		AssociationStatus: ramtypes.ResourceShareAssociationStatusDisassociated,
	}))
}

func TestRealClient_ListFilters(t *testing.T) {
	t.Parallel()

	client := newShareClient(t)
	ctx := t.Context()
	self := ramtypes.ResourceOwnerSelf

	one, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name: aws.String("one"), ResourceArns: []string{subnetA, subnetB}, Principals: []string{account},
	})
	require.NoError(t, err)

	_, err = client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name: aws.String("two"), ResourceArns: []string{vpcA}, Principals: []string{account},
	})
	require.NoError(t, err)

	resources := func(in ramsdk.ListResourcesInput) int {
		in.ResourceOwner = self
		out, listErr := client.ListResources(ctx, &in)
		require.NoError(t, listErr)

		return len(out.Resources)
	}

	principals := func(in ramsdk.ListPrincipalsInput) []string {
		in.ResourceOwner = self
		out, listErr := client.ListPrincipals(ctx, &in)
		require.NoError(t, listErr)

		shares := make([]string, 0, len(out.Principals))
		for _, p := range out.Principals {
			shares = append(shares, aws.ToString(p.ResourceShareArn))
		}

		return shares
	}

	assert.Equal(t, 1, resources(ramsdk.ListResourcesInput{ResourceArns: []string{subnetB}}))
	assert.Equal(t, 0, resources(ramsdk.ListResourcesInput{Principal: aws.String("999999999999")}))
	assert.Equal(t, 3, resources(ramsdk.ListResourcesInput{Principal: aws.String(account)}))

	assert.Len(t, principals(ramsdk.ListPrincipalsInput{ResourceArn: aws.String(vpcA)}), 1)
	assert.NotContains(t, principals(ramsdk.ListPrincipalsInput{ResourceArn: aws.String(vpcA)}), shareARN(one))
	assert.Equal(t, []string{shareARN(one)},
		principals(ramsdk.ListPrincipalsInput{ResourceType: aws.String("ec2:Subnet")}))
	assert.Empty(t, principals(ramsdk.ListPrincipalsInput{Principals: []string{"123"}}))

	policies := func(principal string) int {
		out, polErr := client.GetResourcePolicies(ctx, &ramsdk.GetResourcePoliciesInput{
			ResourceArns: []string{subnetA, vpcA}, Principal: aws.String(principal),
		})
		require.NoError(t, polErr)

		return len(out.Policies)
	}

	assert.Equal(t, 2, policies(account))
	assert.Equal(t, 0, policies("999999999999"))
}

func TestRealClient_ListPermissionAssociationFilters(t *testing.T) {
	t.Parallel()

	client := newShareClient(t)
	ctx := t.Context()

	_, err := client.CreateResourceShare(ctx, &ramsdk.CreateResourceShareInput{
		Name: aws.String("p1"), ResourceArns: []string{subnetA},
	})
	require.NoError(t, err)

	tests := []struct {
		input ramsdk.ListPermissionAssociationsInput
		name  string
		empty bool
	}{
		{name: "unfiltered"},
		{name: "matching_type", input: ramsdk.ListPermissionAssociationsInput{ResourceType: aws.String("ec2:Subnet")}},
		{
			name:  "other_type",
			input: ramsdk.ListPermissionAssociationsInput{ResourceType: aws.String("ec2:VPC")}, empty: true,
		},
		{
			name:  "standard_feature_set",
			input: ramsdk.ListPermissionAssociationsInput{FeatureSet: ramtypes.PermissionFeatureSetStandard},
		},
		{
			name:  "policy_feature_set",
			input: ramsdk.ListPermissionAssociationsInput{FeatureSet: ramtypes.PermissionFeatureSetCreatedFromPolicy},
			empty: true,
		},
		{
			name: "associated_status",
			input: ramsdk.ListPermissionAssociationsInput{
				AssociationStatus: ramtypes.ResourceShareAssociationStatusAssociated,
			},
		},
		{
			name: "disassociated_status",
			input: ramsdk.ListPermissionAssociationsInput{
				AssociationStatus: ramtypes.ResourceShareAssociationStatusDisassociated,
			},
			empty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := client.ListPermissionAssociations(ctx, &tt.input)
			require.NoError(t, listErr)

			if tt.empty {
				assert.Empty(t, out.Permissions)
			} else {
				assert.NotEmpty(t, out.Permissions)
			}
		})
	}
}
