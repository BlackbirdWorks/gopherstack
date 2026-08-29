package iam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const testPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`

// TestListUsers_PathPrefixTruncation drives 3 users through ListUsers with a
// PathPrefix that matches only 2 of them, and MaxItems=1 so the backend's own
// unfiltered pagination window (1 item) never contains both matches in one
// call. api_op_ListUsers.go documents PathPrefix as a filter and IsTruncated/
// Marker as the pagination signal; a client that trusts IsTruncated must see
// both matches across pages, not just whichever one lands in the first
// unfiltered window.
func TestListUsers_PathPrefixTruncation(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	for _, u := range []struct{ name, path string }{
		{"a-match", "/team/"},
		{"b-other", "/other/"},
		{"c-match", "/team/"},
	} {
		_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
			UserName: aws.String(u.name),
			Path:     aws.String(u.path),
		})
		require.NoError(t, err)
	}

	var got []string
	marker := ""

	for range 5 {
		out, err := client.ListUsers(t.Context(), &iamsdk.ListUsersInput{
			PathPrefix: aws.String("/team/"),
			MaxItems:   aws.Int32(1),
			Marker:     aws.String(marker),
		})
		require.NoError(t, err)

		for _, u := range out.Users {
			got = append(got, aws.ToString(u.UserName))
		}

		if !out.IsTruncated {
			break
		}

		marker = aws.ToString(out.Marker)
	}

	require.ElementsMatch(t, []string{"a-match", "c-match"}, got)
}

// TestListRolesGroupsInstanceProfiles_PathPrefix is a basic-correctness
// regression for the same filteredPage helper ListUsers uses (handler_list_filters.go):
// ListRoles, ListGroups and ListInstanceProfiles all fetch-filter-repaginate
// through it, so a break there breaks all four identically.
func TestListRolesGroupsInstanceProfiles_PathPrefix(t *testing.T) {
	t.Parallel()

	t.Run("roles", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		for _, r := range []struct{ name, path string }{
			{"r-match", "/team/"},
			{"r-other", "/other/"},
		} {
			_, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
				RoleName:                 aws.String(r.name),
				Path:                     aws.String(r.path),
				AssumeRolePolicyDocument: aws.String("{}"),
			})
			require.NoError(t, err)
		}

		out, err := client.ListRoles(t.Context(), &iamsdk.ListRolesInput{PathPrefix: aws.String("/team/")})
		require.NoError(t, err)
		require.Len(t, out.Roles, 1)
		require.Equal(t, "r-match", aws.ToString(out.Roles[0].RoleName))
	})

	t.Run("groups", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		for _, g := range []struct{ name, path string }{
			{"g-match", "/team/"},
			{"g-other", "/other/"},
		} {
			_, err := client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{
				GroupName: aws.String(g.name),
				Path:      aws.String(g.path),
			})
			require.NoError(t, err)
		}

		out, err := client.ListGroups(t.Context(), &iamsdk.ListGroupsInput{PathPrefix: aws.String("/team/")})
		require.NoError(t, err)
		require.Len(t, out.Groups, 1)
		require.Equal(t, "g-match", aws.ToString(out.Groups[0].GroupName))
	})

	t.Run("instanceprofiles", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		for _, ip := range []struct{ name, path string }{
			{"ip-match", "/team/"},
			{"ip-other", "/other/"},
		} {
			_, err := client.CreateInstanceProfile(t.Context(), &iamsdk.CreateInstanceProfileInput{
				InstanceProfileName: aws.String(ip.name),
				Path:                aws.String(ip.path),
			})
			require.NoError(t, err)
		}

		out, err := client.ListInstanceProfiles(
			t.Context(), &iamsdk.ListInstanceProfilesInput{PathPrefix: aws.String("/team/")},
		)
		require.NoError(t, err)
		require.Len(t, out.InstanceProfiles, 1)
		require.Equal(t, "ip-match", aws.ToString(out.InstanceProfiles[0].InstanceProfileName))
	})
}

// TestListPolicies_OnlyAttached asserts ListPoliciesInput.OnlyAttached (real
// AWS: "the returned list contains only the policies that are attached to an
// IAM user, group, or role") excludes an unattached policy.
func TestListPolicies_OnlyAttached(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	_, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u1")})
	require.NoError(t, err)

	attached, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("attached-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("unattached-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
		UserName:  aws.String("u1"),
		PolicyArn: attached.Policy.Arn,
	})
	require.NoError(t, err)

	out, err := client.ListPolicies(t.Context(), &iamsdk.ListPoliciesInput{OnlyAttached: true})
	require.NoError(t, err)

	names := make([]string, 0, len(out.Policies))
	for _, p := range out.Policies {
		names = append(names, aws.ToString(p.PolicyName))
	}

	require.Equal(t, []string{"attached-policy"}, names)
}

// TestListPolicies_PolicyUsageFilter asserts ListPoliciesInput.PolicyUsageFilter
// separates a policy used as a permissions boundary from one used as a plain
// identity policy (api_op_ListPolicies.go: PermissionsPolicy | PermissionsBoundary).
func TestListPolicies_PolicyUsageFilter(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	boundary, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("boundary-only"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	identity, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("identity-only"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
		UserName:            aws.String("bounded-user"),
		PermissionsBoundary: boundary.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("plain-user")})
	require.NoError(t, err)

	_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
		UserName:  aws.String("plain-user"),
		PolicyArn: identity.Policy.Arn,
	})
	require.NoError(t, err)

	boundaryOut, err := client.ListPolicies(t.Context(), &iamsdk.ListPoliciesInput{
		PolicyUsageFilter: types.PolicyUsageTypePermissionsBoundary,
	})
	require.NoError(t, err)

	boundaryNames := make([]string, 0, len(boundaryOut.Policies))
	for _, p := range boundaryOut.Policies {
		boundaryNames = append(boundaryNames, aws.ToString(p.PolicyName))
	}

	require.Equal(t, []string{"boundary-only"}, boundaryNames)

	permOut, err := client.ListPolicies(t.Context(), &iamsdk.ListPoliciesInput{
		PolicyUsageFilter: types.PolicyUsageTypePermissionsPolicy,
	})
	require.NoError(t, err)

	permNames := make([]string, 0, len(permOut.Policies))
	for _, p := range permOut.Policies {
		permNames = append(permNames, aws.ToString(p.PolicyName))
	}

	require.Equal(t, []string{"identity-only"}, permNames)
}
