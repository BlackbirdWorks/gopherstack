package iam_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

type listPage struct {
	marker    *string
	count     int
	truncated bool
}

// TestListOps_HonourMaxItemsAndMarker pins MaxItems/Marker/IsTruncated on the list ops
// returning unbounded name or tag lists (e.g. api_op_ListUserPolicies.go).
func TestListOps_HonourMaxItemsAndMarker(t *testing.T) {
	t.Parallel()

	tests := []listPageCase{
		listPageCaseListUserPolicies(),
		listPageCaseListRolePolicies(),
		listPageCaseListGroupPolicies(),
		listPageCaseGetGroup(),
		listPageCaseListPolicyVersions(),
		listPageCaseListUserTags(),
		listPageCaseListRoleTags(),
		listPageCaseListPolicyTags(),
		listPageCaseListMFADeviceTags(),
		listPageCaseListInstanceProfilesForRole(),
		listPageCaseSimulateCustomPolicy(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIAMClient(t, iam.NewHandler(iam.NewInMemoryBackend()))
			tt.setup(t, client)

			first, err := tt.list(t.Context(), client, aws.Int32(2), nil)
			require.NoError(t, err)
			require.Equal(t, 2, first.count)
			require.True(t, first.truncated)
			require.NotEmpty(t, aws.ToString(first.marker))

			second, err := tt.list(t.Context(), client, aws.Int32(2), first.marker)
			require.NoError(t, err)
			require.Equal(t, 1, second.count)
			require.False(t, second.truncated)

			all, err := tt.list(t.Context(), client, nil, nil)
			require.NoError(t, err)
			require.Equal(t, 3, all.count)
			require.False(t, all.truncated)
		})
	}
}

func threeTags() []types.Tag {
	return []types.Tag{
		{Key: aws.String("a"), Value: aws.String("1")},
		{Key: aws.String("b"), Value: aws.String("2")},
		{Key: aws.String("c"), Value: aws.String("3")},
	}
}

func listPageNames() []string { return []string{"a", "b", "c"} }

type listPageCase struct {
	setup func(t *testing.T, c *iamsdk.Client)
	list  func(ctx context.Context, c *iamsdk.Client, limit *int32, marker *string) (listPage, error)
	name  string
}

func listPageCaseListUserPolicies() listPageCase {
	return listPageCase{
		name: "ListUserPolicies",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u")})
			require.NoError(t, err)
			for _, n := range listPageNames() {
				_, err = c.PutUserPolicy(t.Context(), &iamsdk.PutUserPolicyInput{
					UserName: aws.String("u"), PolicyName: aws.String(n), PolicyDocument: aws.String(testPolicyDoc),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListUserPolicies(ctx, &iamsdk.ListUserPoliciesInput{
				UserName: aws.String("u"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.PolicyNames), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListRolePolicies() listPageCase {
	return listPageCase{
		name: "ListRolePolicies",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName: aws.String("r"), AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)
			for _, n := range listPageNames() {
				_, err = c.PutRolePolicy(t.Context(), &iamsdk.PutRolePolicyInput{
					RoleName: aws.String("r"), PolicyName: aws.String(n), PolicyDocument: aws.String(testPolicyDoc),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListRolePolicies(ctx, &iamsdk.ListRolePoliciesInput{
				RoleName: aws.String("r"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.PolicyNames), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListGroupPolicies() listPageCase {
	return listPageCase{
		name: "ListGroupPolicies",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("g")})
			require.NoError(t, err)
			for _, n := range listPageNames() {
				_, err = c.PutGroupPolicy(t.Context(), &iamsdk.PutGroupPolicyInput{
					GroupName: aws.String(
						"g",
					),
					PolicyName:     aws.String(n),
					PolicyDocument: aws.String(testPolicyDoc),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListGroupPolicies(ctx, &iamsdk.ListGroupPoliciesInput{
				GroupName: aws.String("g"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.PolicyNames), o.IsTruncated}, nil
		},
	}
}

func listPageCaseGetGroup() listPageCase {
	return listPageCase{
		name: "GetGroup",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("g")})
			require.NoError(t, err)
			for _, n := range listPageNames() {
				_, err = c.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String(n)})
				require.NoError(t, err)
				_, err = c.AddUserToGroup(t.Context(), &iamsdk.AddUserToGroupInput{
					GroupName: aws.String("g"), UserName: aws.String(n),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.GetGroup(ctx, &iamsdk.GetGroupInput{GroupName: aws.String("g"), MaxItems: mx, Marker: m})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Users), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListPolicyVersions() listPageCase {
	return listPageCase{
		name: "ListPolicyVersions",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
				PolicyName: aws.String("p"), PolicyDocument: aws.String(testPolicyDoc),
			})
			require.NoError(t, err)
			for range 2 {
				_, err = c.CreatePolicyVersion(t.Context(), &iamsdk.CreatePolicyVersionInput{
					PolicyArn:      aws.String("arn:aws:iam::000000000000:policy/p"),
					PolicyDocument: aws.String(testPolicyDoc),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListPolicyVersions(ctx, &iamsdk.ListPolicyVersionsInput{
				PolicyArn: aws.String("arn:aws:iam::000000000000:policy/p"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Versions), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListUserTags() listPageCase {
	return listPageCase{
		name: "ListUserTags",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u")})
			require.NoError(t, err)
			_, err = c.TagUser(t.Context(), &iamsdk.TagUserInput{UserName: aws.String("u"), Tags: threeTags()})
			require.NoError(t, err)
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListUserTags(
				ctx,
				&iamsdk.ListUserTagsInput{UserName: aws.String("u"), MaxItems: mx, Marker: m},
			)
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Tags), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListRoleTags() listPageCase {
	return listPageCase{
		name: "ListRoleTags",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName: aws.String("r"), AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)
			_, err = c.TagRole(t.Context(), &iamsdk.TagRoleInput{RoleName: aws.String("r"), Tags: threeTags()})
			require.NoError(t, err)
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListRoleTags(
				ctx,
				&iamsdk.ListRoleTagsInput{RoleName: aws.String("r"), MaxItems: mx, Marker: m},
			)
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Tags), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListPolicyTags() listPageCase {
	return listPageCase{
		name: "ListPolicyTags",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
				PolicyName: aws.String("p"), PolicyDocument: aws.String(testPolicyDoc), Tags: threeTags(),
			})
			require.NoError(t, err)
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListPolicyTags(ctx, &iamsdk.ListPolicyTagsInput{
				PolicyArn: aws.String("arn:aws:iam::000000000000:policy/p"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Tags), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListMFADeviceTags() listPageCase {
	return listPageCase{
		name: "ListMFADeviceTags",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.TagMFADevice(t.Context(), &iamsdk.TagMFADeviceInput{
				SerialNumber: aws.String("arn:aws:iam::000000000000:mfa/m"), Tags: threeTags(),
			})
			require.NoError(t, err)
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListMFADeviceTags(ctx, &iamsdk.ListMFADeviceTagsInput{
				SerialNumber: aws.String("arn:aws:iam::000000000000:mfa/m"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.Tags), o.IsTruncated}, nil
		},
	}
}

func listPageCaseListInstanceProfilesForRole() listPageCase {
	return listPageCase{
		name: "ListInstanceProfilesForRole",
		setup: func(t *testing.T, c *iamsdk.Client) {
			t.Helper()
			_, err := c.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName: aws.String("r"), AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)
			for _, n := range listPageNames() {
				_, err = c.CreateInstanceProfile(t.Context(), &iamsdk.CreateInstanceProfileInput{
					InstanceProfileName: aws.String(n),
				})
				require.NoError(t, err)
				_, err = c.AddRoleToInstanceProfile(t.Context(), &iamsdk.AddRoleToInstanceProfileInput{
					InstanceProfileName: aws.String(n), RoleName: aws.String("r"),
				})
				require.NoError(t, err)
			}
		},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.ListInstanceProfilesForRole(ctx, &iamsdk.ListInstanceProfilesForRoleInput{
				RoleName: aws.String("r"), MaxItems: mx, Marker: m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.InstanceProfiles), o.IsTruncated}, nil
		},
	}
}

func listPageCaseSimulateCustomPolicy() listPageCase {
	return listPageCase{
		name:  "SimulateCustomPolicy",
		setup: func(*testing.T, *iamsdk.Client) {},
		list: func(ctx context.Context, c *iamsdk.Client, mx *int32, m *string) (listPage, error) {
			o, err := c.SimulateCustomPolicy(ctx, &iamsdk.SimulateCustomPolicyInput{
				PolicyInputList: []string{testPolicyDoc},
				ActionNames:     []string{"s3:GetObject", "s3:PutObject", "s3:ListBucket"},
				MaxItems:        mx,
				Marker:          m,
			})
			if err != nil {
				return listPage{}, err
			}

			return listPage{o.Marker, len(o.EvaluationResults), o.IsTruncated}, nil
		},
	}
}
