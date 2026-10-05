package workmail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

func newMembersWorkMailClient(t *testing.T) *workmailsdk.Client {
	t.Helper()

	return newWorkMailSDKClient(t, workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1")))
}

func TestCreateOrganization_DirectoryIdAndClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		secondAlias  string
		secondToken  string
		wantSameOrg  bool
		wantConflict bool
	}{
		{name: "same token and params", secondAlias: "alias-a", secondToken: "tok", wantSameOrg: true},
		{name: "new token duplicate alias", secondAlias: "alias-a", secondToken: "tok-2", wantConflict: true},
		{name: "same token other alias", secondAlias: "alias-b", secondToken: "tok", wantConflict: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newMembersWorkMailClient(t)
			ctx := t.Context()

			first, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
				Alias: aws.String("alias-a"), DirectoryId: aws.String("d-1234567890"), ClientToken: aws.String("tok"),
			})
			require.NoError(t, err)

			desc, err := client.DescribeOrganization(
				ctx,
				&workmailsdk.DescribeOrganizationInput{OrganizationId: first.OrganizationId},
			)
			require.NoError(t, err)
			assert.Equal(t, "d-1234567890", aws.ToString(desc.DirectoryId))

			second, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
				Alias: aws.String(
					tt.secondAlias,
				), DirectoryId: aws.String("d-1234567890"), ClientToken: aws.String(tt.secondToken),
			})
			if tt.wantConflict {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.OrganizationId), aws.ToString(second.OrganizationId))
		})
	}
}

func TestCreates_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *workmailsdk.Client, orgID *string, token string) string
		name string
	}{
		{
			name: "register mail domain",
			call: func(t *testing.T, client *workmailsdk.Client, orgID *string, token string) string {
				t.Helper()

				_, err := client.RegisterMailDomain(t.Context(), &workmailsdk.RegisterMailDomainInput{
					OrganizationId: orgID, DomainName: aws.String("replay.example"), ClientToken: aws.String(token),
				})
				require.NoError(t, err)

				return "ok"
			},
		},
		{
			name: "impersonation role",
			call: func(t *testing.T, client *workmailsdk.Client, orgID *string, token string) string {
				t.Helper()

				out, err := client.CreateImpersonationRole(t.Context(), &workmailsdk.CreateImpersonationRoleInput{
					OrganizationId: orgID, Name: aws.String("role"), Type: types.ImpersonationRoleTypeFullAccess,
					ClientToken: aws.String(token),
					Rules: []types.ImpersonationRule{{
						ImpersonationRuleId: aws.String(
							"r1",
						), Effect: types.AccessEffectAllow, TargetUsers: []string{"u"},
					}},
				})
				require.NoError(t, err)

				return aws.ToString(out.ImpersonationRoleId)
			},
		},
		{
			name: "mobile device access rule",
			call: func(t *testing.T, client *workmailsdk.Client, orgID *string, token string) string {
				t.Helper()

				out, err := client.CreateMobileDeviceAccessRule(
					t.Context(),
					&workmailsdk.CreateMobileDeviceAccessRuleInput{
						OrganizationId: orgID,
						Name:           aws.String("rule"),
						Effect:         types.MobileDeviceAccessRuleEffectAllow,
						ClientToken:    aws.String(token),
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.MobileDeviceAccessRuleId)
			},
		},
		{
			name: "identity center application",
			call: func(t *testing.T, client *workmailsdk.Client, _ *string, token string) string {
				t.Helper()

				out, err := client.CreateIdentityCenterApplication(
					t.Context(),
					&workmailsdk.CreateIdentityCenterApplicationInput{
						InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1"), Name: aws.String("app"),
						ClientToken: aws.String(token),
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.ApplicationArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newMembersWorkMailClient(t)
			orgID := newWorkMailOrg(t, client)

			token := "tok-" + uuid.NewString()[:8]
			first := tt.call(t, client, orgID, token)
			again := tt.call(t, client, orgID, token)
			assert.NotEmpty(t, first)
			assert.Equal(t, first, again, "a retry with the same token returns the first result")
		})
	}
}

func TestUpdateResource_AppliesTypeAndClearsDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    func(org, res *string) *workmailsdk.UpdateResourceInput
		wantType types.ResourceType
		wantDesc string
		wantErr  bool
	}{
		{
			name: "type changes",
			input: func(org, res *string) *workmailsdk.UpdateResourceInput {
				return &workmailsdk.UpdateResourceInput{
					OrganizationId: org,
					ResourceId:     res,
					Type:           types.ResourceTypeEquipment,
				}
			},
			wantType: types.ResourceTypeEquipment, wantDesc: "initial",
		},
		{
			name: "explicit empty description clears",
			input: func(org, res *string) *workmailsdk.UpdateResourceInput {
				return &workmailsdk.UpdateResourceInput{
					OrganizationId: org,
					ResourceId:     res,
					Description:    aws.String(""),
				}
			},
			wantType: types.ResourceTypeRoom, wantDesc: "",
		},
		{
			name: "omitted members stay",
			input: func(org, res *string) *workmailsdk.UpdateResourceInput {
				return &workmailsdk.UpdateResourceInput{OrganizationId: org, ResourceId: res}
			},
			wantType: types.ResourceTypeRoom, wantDesc: "initial",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newMembersWorkMailClient(t)
			ctx := t.Context()
			orgID := newWorkMailOrg(t, client)

			res, err := client.CreateResource(ctx, &workmailsdk.CreateResourceInput{
				OrganizationId: orgID, Name: aws.String("room-1"), Type: types.ResourceTypeRoom,
				Description: aws.String("initial"),
			})
			require.NoError(t, err)

			_, err = client.UpdateResource(ctx, tt.input(orgID, res.ResourceId))
			require.NoError(t, err)

			got, err := client.DescribeResource(ctx, &workmailsdk.DescribeResourceInput{
				OrganizationId: orgID, ResourceId: res.ResourceId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, got.Type)
			assert.Equal(t, tt.wantDesc, aws.ToString(got.Description))
		})
	}
}
