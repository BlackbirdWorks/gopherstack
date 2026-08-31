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

// TestListAttachedPolicies_PathPrefix asserts ListAttachedUserPolicies,
// ListAttachedGroupPolicies and ListAttachedRolePolicies all honor
// PathPrefix (real AWS: "the returned list contains only the policies that
// have their path matching this parameter", api_op_ListAttachedUserPolicies.go
// et al) -- each of the three previously read no PathPrefix/Marker/MaxItems
// at all and returned every attached policy unfiltered and unpaginated.
func TestListAttachedPolicies_PathPrefix(t *testing.T) {
	t.Parallel()

	t.Run("user", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		matchPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("match-policy"),
			Path:           aws.String("/team/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		otherPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("other-policy"),
			Path:           aws.String("/other/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u1")})
		require.NoError(t, err)

		for _, arn := range []*string{matchPolicy.Policy.Arn, otherPolicy.Policy.Arn} {
			_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
				UserName: aws.String("u1"), PolicyArn: arn,
			})
			require.NoError(t, err)
		}

		out, err := client.ListAttachedUserPolicies(t.Context(), &iamsdk.ListAttachedUserPoliciesInput{
			UserName:   aws.String("u1"),
			PathPrefix: aws.String("/team/"),
		})
		require.NoError(t, err)
		require.Len(t, out.AttachedPolicies, 1)
		require.Equal(t, "match-policy", aws.ToString(out.AttachedPolicies[0].PolicyName))
	})

	t.Run("group", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		matchPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("match-policy"),
			Path:           aws.String("/team/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		otherPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("other-policy"),
			Path:           aws.String("/other/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		_, err = client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("g1")})
		require.NoError(t, err)

		for _, arn := range []*string{matchPolicy.Policy.Arn, otherPolicy.Policy.Arn} {
			_, err = client.AttachGroupPolicy(t.Context(), &iamsdk.AttachGroupPolicyInput{
				GroupName: aws.String("g1"), PolicyArn: arn,
			})
			require.NoError(t, err)
		}

		out, err := client.ListAttachedGroupPolicies(t.Context(), &iamsdk.ListAttachedGroupPoliciesInput{
			GroupName:  aws.String("g1"),
			PathPrefix: aws.String("/team/"),
		})
		require.NoError(t, err)
		require.Len(t, out.AttachedPolicies, 1)
		require.Equal(t, "match-policy", aws.ToString(out.AttachedPolicies[0].PolicyName))
	})

	t.Run("role", func(t *testing.T) {
		t.Parallel()

		h := iam.NewHandler(iam.NewInMemoryBackend())
		client := newTestIAMClient(t, h)

		matchPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("match-policy"),
			Path:           aws.String("/team/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		otherPolicy, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
			PolicyName:     aws.String("other-policy"),
			Path:           aws.String("/other/"),
			PolicyDocument: aws.String(testPolicyDoc),
		})
		require.NoError(t, err)

		_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
			RoleName:                 aws.String("r1"),
			AssumeRolePolicyDocument: aws.String("{}"),
		})
		require.NoError(t, err)

		for _, arn := range []*string{matchPolicy.Policy.Arn, otherPolicy.Policy.Arn} {
			_, err = client.AttachRolePolicy(t.Context(), &iamsdk.AttachRolePolicyInput{
				RoleName: aws.String("r1"), PolicyArn: arn,
			})
			require.NoError(t, err)
		}

		out, err := client.ListAttachedRolePolicies(t.Context(), &iamsdk.ListAttachedRolePoliciesInput{
			RoleName:   aws.String("r1"),
			PathPrefix: aws.String("/team/"),
		})
		require.NoError(t, err)
		require.Len(t, out.AttachedPolicies, 1)
		require.Equal(t, "match-policy", aws.ToString(out.AttachedPolicies[0].PolicyName))
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

// TestListEntitiesForPolicy_PathPrefix asserts PathPrefix filters on each
// entity's OWN path (not the policy's path), per api_op_ListEntitiesForPolicy.go:
// "The path prefix for filtering the results." Entities are attached to the
// same policy under distinct paths; the filter must narrow to only those
// entities whose own path matches, across all three entity kinds.
func TestListEntitiesForPolicy_PathPrefix(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	pol, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("shared-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
		UserName: aws.String("u-team"), Path: aws.String("/team/"),
	})
	require.NoError(t, err)
	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
		UserName: aws.String("u-other"), Path: aws.String("/other/"),
	})
	require.NoError(t, err)
	_, err = client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{
		GroupName: aws.String("g-team"), Path: aws.String("/team/"),
	})
	require.NoError(t, err)
	_, err = client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{
		GroupName: aws.String("g-other"), Path: aws.String("/other/"),
	})
	require.NoError(t, err)
	_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName: aws.String("r-team"), Path: aws.String("/team/"),
		AssumeRolePolicyDocument: aws.String("{}"),
	})
	require.NoError(t, err)
	_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName: aws.String("r-other"), Path: aws.String("/other/"),
		AssumeRolePolicyDocument: aws.String("{}"),
	})
	require.NoError(t, err)

	for _, name := range []string{"u-team", "u-other"} {
		_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
			UserName: aws.String(name), PolicyArn: pol.Policy.Arn,
		})
		require.NoError(t, err)
	}
	for _, name := range []string{"g-team", "g-other"} {
		_, err = client.AttachGroupPolicy(t.Context(), &iamsdk.AttachGroupPolicyInput{
			GroupName: aws.String(name), PolicyArn: pol.Policy.Arn,
		})
		require.NoError(t, err)
	}
	for _, name := range []string{"r-team", "r-other"} {
		_, err = client.AttachRolePolicy(t.Context(), &iamsdk.AttachRolePolicyInput{
			RoleName: aws.String(name), PolicyArn: pol.Policy.Arn,
		})
		require.NoError(t, err)
	}

	out, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
		PolicyArn:  pol.Policy.Arn,
		PathPrefix: aws.String("/team/"),
	})
	require.NoError(t, err)

	require.Len(t, out.PolicyUsers, 1)
	require.Equal(t, "u-team", aws.ToString(out.PolicyUsers[0].UserName))
	require.Len(t, out.PolicyGroups, 1)
	require.Equal(t, "g-team", aws.ToString(out.PolicyGroups[0].GroupName))
	require.Len(t, out.PolicyRoles, 1)
	require.Equal(t, "r-team", aws.ToString(out.PolicyRoles[0].RoleName))
}

// TestListEntitiesForPolicy_MarkerResumesAcrossPageBoundary drives a policy
// attached to entities across all three kinds through ListEntitiesForPolicy
// with MaxItems=1, so a single-item backend window straddles the
// User/Group/Role boundary. Every entity must appear exactly once across the
// full walk, matching real AWS's documented single Marker/IsTruncated pair
// spanning the whole combined result (api_op_ListEntitiesForPolicy.go). Two
// users force at least one page break inside the User section itself, not
// just at a kind boundary.
func TestListEntitiesForPolicy_MarkerResumesAcrossPageBoundary(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	pol, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("paginated-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u1")})
	require.NoError(t, err)
	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u2")})
	require.NoError(t, err)
	_, err = client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("g1")})
	require.NoError(t, err)
	_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName: aws.String("r1"), AssumeRolePolicyDocument: aws.String("{}"),
	})
	require.NoError(t, err)

	for _, name := range []string{"u1", "u2"} {
		_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
			UserName: aws.String(name), PolicyArn: pol.Policy.Arn,
		})
		require.NoError(t, err)
	}
	_, err = client.AttachGroupPolicy(t.Context(), &iamsdk.AttachGroupPolicyInput{
		GroupName: aws.String("g1"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)
	_, err = client.AttachRolePolicy(t.Context(), &iamsdk.AttachRolePolicyInput{
		RoleName: aws.String("r1"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)

	var users, groups, roles []string

	marker := ""
	pageCount := 0

	for range 10 {
		out, callErr := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
			PolicyArn: pol.Policy.Arn,
			MaxItems:  aws.Int32(1),
			Marker:    aws.String(marker),
		})
		require.NoError(t, callErr)
		pageCount++

		items := len(out.PolicyUsers) + len(out.PolicyGroups) + len(out.PolicyRoles)
		require.LessOrEqual(t, items, 1, "MaxItems=1 must not return more than one entity per page")

		for _, u := range out.PolicyUsers {
			users = append(users, aws.ToString(u.UserName))
		}
		for _, g := range out.PolicyGroups {
			groups = append(groups, aws.ToString(g.GroupName))
		}
		for _, r := range out.PolicyRoles {
			roles = append(roles, aws.ToString(r.RoleName))
		}

		if !out.IsTruncated {
			break
		}

		marker = aws.ToString(out.Marker)
	}

	require.Equal(t, 4, pageCount, "4 entities at MaxItems=1 must take exactly 4 pages")
	require.Equal(t, []string{"u1", "u2"}, users)
	require.Equal(t, []string{"g1"}, groups)
	require.Equal(t, []string{"r1"}, roles)
}

// TestListEntitiesForPolicy_PolicyUsageFilter asserts PolicyUsageFilter
// separates entities that hold policyArn as a normal attached policy from
// entities that use it as their permissions boundary
// (api_op_ListEntitiesForPolicy.go: PermissionsPolicy vs PermissionsBoundary).
// Groups have no permissions boundary in real IAM, so this only exercises
// users and roles.
func TestListEntitiesForPolicy_PolicyUsageFilter(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	pol, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("usage-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("u-attached")})
	require.NoError(t, err)
	_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
		UserName: aws.String("u-attached"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = client.CreateUser(t.Context(), &iamsdk.CreateUserInput{
		UserName: aws.String("u-boundary"), PermissionsBoundary: pol.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName:                 aws.String("r-boundary"),
		AssumeRolePolicyDocument: aws.String("{}"),
		PermissionsBoundary:      pol.Policy.Arn,
	})
	require.NoError(t, err)

	permOut, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
		PolicyArn:         pol.Policy.Arn,
		PolicyUsageFilter: types.PolicyUsageTypePermissionsPolicy,
	})
	require.NoError(t, err)
	require.Len(t, permOut.PolicyUsers, 1)
	require.Equal(t, "u-attached", aws.ToString(permOut.PolicyUsers[0].UserName))
	require.Empty(t, permOut.PolicyRoles)

	boundaryOut, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
		PolicyArn:         pol.Policy.Arn,
		PolicyUsageFilter: types.PolicyUsageTypePermissionsBoundary,
	})
	require.NoError(t, err)
	require.Len(t, boundaryOut.PolicyUsers, 1)
	require.Equal(t, "u-boundary", aws.ToString(boundaryOut.PolicyUsers[0].UserName))
	require.Len(t, boundaryOut.PolicyRoles, 1)
	require.Equal(t, "r-boundary", aws.ToString(boundaryOut.PolicyRoles[0].RoleName))

	allOut, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
		PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)
	allUsers := make([]string, 0, len(allOut.PolicyUsers))
	for _, u := range allOut.PolicyUsers {
		allUsers = append(allUsers, aws.ToString(u.UserName))
	}
	require.ElementsMatch(t, []string{"u-attached", "u-boundary"}, allUsers)
	require.Len(t, allOut.PolicyRoles, 1)
	require.Equal(t, "r-boundary", aws.ToString(allOut.PolicyRoles[0].RoleName))
}

// TestListEntitiesForPolicy_ItemShape_RealClient checks that PolicyUser,
// PolicyGroup and PolicyRole's UserId/GroupId/RoleId (real fields per
// deserializers.go's awsAwsquery_deserializeDocumentPolicyUser/
// PolicyGroup/PolicyRole) round-trip. Each is a response-only field --
// distinct from the request-side entity name -- so this isolates response
// decode rather than exercising a bidirectional struct.
func TestListEntitiesForPolicy_ItemShape_RealClient(t *testing.T) {
	t.Parallel()

	h := iam.NewHandler(iam.NewInMemoryBackend())
	client := newTestIAMClient(t, h)

	pol, err := client.CreatePolicy(t.Context(), &iamsdk.CreatePolicyInput{
		PolicyName:     aws.String("shape-policy"),
		PolicyDocument: aws.String(testPolicyDoc),
	})
	require.NoError(t, err)

	user, err := client.CreateUser(t.Context(), &iamsdk.CreateUserInput{UserName: aws.String("shape-user")})
	require.NoError(t, err)
	group, err := client.CreateGroup(t.Context(), &iamsdk.CreateGroupInput{GroupName: aws.String("shape-group")})
	require.NoError(t, err)
	role, err := client.CreateRole(t.Context(), &iamsdk.CreateRoleInput{
		RoleName: aws.String("shape-role"), AssumeRolePolicyDocument: aws.String("{}"),
	})
	require.NoError(t, err)

	_, err = client.AttachUserPolicy(t.Context(), &iamsdk.AttachUserPolicyInput{
		UserName: aws.String("shape-user"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)
	_, err = client.AttachGroupPolicy(t.Context(), &iamsdk.AttachGroupPolicyInput{
		GroupName: aws.String("shape-group"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)
	_, err = client.AttachRolePolicy(t.Context(), &iamsdk.AttachRolePolicyInput{
		RoleName: aws.String("shape-role"), PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)

	out, err := client.ListEntitiesForPolicy(t.Context(), &iamsdk.ListEntitiesForPolicyInput{
		PolicyArn: pol.Policy.Arn,
	})
	require.NoError(t, err)

	require.Len(t, out.PolicyUsers, 1)
	require.Equal(t, aws.ToString(user.User.UserId), aws.ToString(out.PolicyUsers[0].UserId))
	require.NotEmpty(t, aws.ToString(out.PolicyUsers[0].UserId))

	require.Len(t, out.PolicyGroups, 1)
	require.Equal(t, aws.ToString(group.Group.GroupId), aws.ToString(out.PolicyGroups[0].GroupId))
	require.NotEmpty(t, aws.ToString(out.PolicyGroups[0].GroupId))

	require.Len(t, out.PolicyRoles, 1)
	require.Equal(t, aws.ToString(role.Role.RoleId), aws.ToString(out.PolicyRoles[0].RoleId))
	require.NotEmpty(t, aws.ToString(out.PolicyRoles[0].RoleId))
}
