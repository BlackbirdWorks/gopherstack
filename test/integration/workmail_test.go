package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	worktypes "github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createWorkMailClient returns a WorkMail client pointed at the shared test container.
func createWorkMailClient(t *testing.T) *workmailsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return workmailsdk.NewFromConfig(cfg, func(o *workmailsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// wmCreateOrg is a helper that creates an organization and registers a cleanup.
func wmCreateOrg(t *testing.T, client *workmailsdk.Client, alias string) string {
	t.Helper()

	ctx := t.Context()
	out, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String(alias),
	})
	require.NoError(t, err, "CreateOrganization(%q) should succeed", alias)
	orgID := aws.ToString(out.OrganizationId)
	require.NotEmpty(t, orgID)

	t.Cleanup(func() {
		_, _ = client.DeleteOrganization(t.Context(), &workmailsdk.DeleteOrganizationInput{
			OrganizationId:  aws.String(orgID),
			DeleteDirectory: true,
		})
	})

	return orgID
}

// TestIntegration_WorkMail_OrganizationLifecycle drives create→describe→list→delete.
func TestIntegration_WorkMail_OrganizationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name  string
		alias string
	}{
		{name: "basic", alias: "integ-org-lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.alias)

			descOut, err := client.DescribeOrganization(ctx, &workmailsdk.DescribeOrganizationInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "DescribeOrganization should succeed")
			assert.Equal(t, tt.alias, aws.ToString(descOut.Alias))
			assert.NotEmpty(t, descOut.OrganizationId)

			listOut, err := client.ListOrganizations(ctx, &workmailsdk.ListOrganizationsInput{})
			require.NoError(t, err, "ListOrganizations should succeed")

			found := false
			for _, o := range listOut.OrganizationSummaries {
				if aws.ToString(o.OrganizationId) == orgID {
					found = true

					break
				}
			}
			assert.True(t, found, "org should appear in list")
		})
	}
}

// TestIntegration_WorkMail_UserLifecycle drives create→describe→update→list→register→deregister→delete.
func TestIntegration_WorkMail_UserLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name        string
		orgAlias    string
		userName    string
		displayName string
	}{
		{name: "full_lifecycle", orgAlias: "integ-user-org", userName: "alice", displayName: "Alice Smith"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.orgAlias)

			createOut, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String(tt.userName),
				DisplayName:    aws.String(tt.displayName),
				Password:       aws.String("TestPass@1234"),
			})
			require.NoError(t, err, "CreateUser should succeed")
			userID := aws.ToString(createOut.UserId)
			require.NotEmpty(t, userID)

			descOut, err := client.DescribeUser(ctx, &workmailsdk.DescribeUserInput{
				OrganizationId: aws.String(orgID),
				UserId:         aws.String(userID),
			})
			require.NoError(t, err, "DescribeUser should succeed")
			assert.Equal(t, tt.userName, aws.ToString(descOut.Name))
			assert.Equal(t, tt.displayName, aws.ToString(descOut.DisplayName))
			assert.Equal(t, "DISABLED", string(descOut.State))

			_, err = client.UpdateUser(ctx, &workmailsdk.UpdateUserInput{
				OrganizationId: aws.String(orgID),
				UserId:         aws.String(userID),
				FirstName:      aws.String("Alice"),
				LastName:       aws.String("Smith"),
			})
			require.NoError(t, err, "UpdateUser should succeed")

			listOut, err := client.ListUsers(ctx, &workmailsdk.ListUsersInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "ListUsers should succeed")

			found := false
			for _, u := range listOut.Users {
				if aws.ToString(u.Id) == userID {
					found = true

					break
				}
			}
			assert.True(t, found, "user should appear in list")

			email := tt.userName + "@example.com"
			_, err = client.RegisterToWorkMail(ctx, &workmailsdk.RegisterToWorkMailInput{
				OrganizationId: aws.String(orgID),
				EntityId:       aws.String(userID),
				Email:          aws.String(email),
			})
			require.NoError(t, err, "RegisterToWorkMail should succeed")

			descEnabled, err := client.DescribeUser(ctx, &workmailsdk.DescribeUserInput{
				OrganizationId: aws.String(orgID),
				UserId:         aws.String(userID),
			})
			require.NoError(t, err)
			assert.Equal(t, "ENABLED", string(descEnabled.State))

			_, err = client.DeregisterFromWorkMail(ctx, &workmailsdk.DeregisterFromWorkMailInput{
				OrganizationId: aws.String(orgID),
				EntityId:       aws.String(userID),
			})
			require.NoError(t, err, "DeregisterFromWorkMail should succeed")

			_, err = client.DeleteUser(ctx, &workmailsdk.DeleteUserInput{
				OrganizationId: aws.String(orgID),
				UserId:         aws.String(userID),
			})
			require.NoError(t, err, "DeleteUser should succeed")
		})
	}
}

// TestIntegration_WorkMail_GroupLifecycle drives create→describe→list→member ops→delete.
func TestIntegration_WorkMail_GroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		orgAlias  string
		groupName string
	}{
		{name: "full_lifecycle", orgAlias: "integ-group-org", groupName: "integ-group"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.orgAlias)

			grpOut, err := client.CreateGroup(ctx, &workmailsdk.CreateGroupInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String(tt.groupName),
			})
			require.NoError(t, err, "CreateGroup should succeed")
			groupID := aws.ToString(grpOut.GroupId)
			require.NotEmpty(t, groupID)

			descOut, err := client.DescribeGroup(ctx, &workmailsdk.DescribeGroupInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
			})
			require.NoError(t, err, "DescribeGroup should succeed")
			assert.Equal(t, tt.groupName, aws.ToString(descOut.Name))

			listOut, err := client.ListGroups(ctx, &workmailsdk.ListGroupsInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "ListGroups should succeed")

			found := false
			for _, g := range listOut.Groups {
				if aws.ToString(g.Id) == groupID {
					found = true

					break
				}
			}
			assert.True(t, found, "created group should appear in list")

			// Create a user to add as member.
			userOut, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String("member-user"),
				DisplayName:    aws.String("Member"),
				Password:       aws.String("TestPass@1234"),
			})
			require.NoError(t, err)
			userID := aws.ToString(userOut.UserId)

			_, err = client.AssociateMemberToGroup(ctx, &workmailsdk.AssociateMemberToGroupInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
				MemberId:       aws.String(userID),
			})
			require.NoError(t, err, "AssociateMemberToGroup should succeed")

			membersOut, err := client.ListGroupMembers(ctx, &workmailsdk.ListGroupMembersInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
			})
			require.NoError(t, err, "ListGroupMembers should succeed")
			require.Len(t, membersOut.Members, 1)
			assert.Equal(t, userID, aws.ToString(membersOut.Members[0].Id))

			_, err = client.DisassociateMemberFromGroup(ctx, &workmailsdk.DisassociateMemberFromGroupInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
				MemberId:       aws.String(userID),
			})
			require.NoError(t, err, "DisassociateMemberFromGroup should succeed")

			_, err = client.DeleteGroup(ctx, &workmailsdk.DeleteGroupInput{
				OrganizationId: aws.String(orgID),
				GroupId:        aws.String(groupID),
			})
			require.NoError(t, err, "DeleteGroup should succeed")
		})
	}
}

// TestIntegration_WorkMail_ResourceLifecycle drives create→describe→list→delegate ops→delete.
func TestIntegration_WorkMail_ResourceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name         string
		orgAlias     string
		resourceName string
		resourceType worktypes.ResourceType
	}{
		{
			name:         "room",
			orgAlias:     "integ-resource-org",
			resourceName: "conf-room-a",
			resourceType: worktypes.ResourceTypeRoom,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.orgAlias)

			resOut, err := client.CreateResource(ctx, &workmailsdk.CreateResourceInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String(tt.resourceName),
				Type:           tt.resourceType,
			})
			require.NoError(t, err, "CreateResource should succeed")
			resID := aws.ToString(resOut.ResourceId)
			require.NotEmpty(t, resID)

			descOut, err := client.DescribeResource(ctx, &workmailsdk.DescribeResourceInput{
				OrganizationId: aws.String(orgID),
				ResourceId:     aws.String(resID),
			})
			require.NoError(t, err, "DescribeResource should succeed")
			assert.Equal(t, tt.resourceName, aws.ToString(descOut.Name))
			assert.Equal(t, tt.resourceType, descOut.Type)

			listOut, err := client.ListResources(ctx, &workmailsdk.ListResourcesInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "ListResources should succeed")

			found := false
			for _, r := range listOut.Resources {
				if aws.ToString(r.Id) == resID {
					found = true

					break
				}
			}
			assert.True(t, found, "resource should appear in list")

			_, err = client.UpdateResource(ctx, &workmailsdk.UpdateResourceInput{
				OrganizationId: aws.String(orgID),
				ResourceId:     aws.String(resID),
				Description:    aws.String("Updated description"),
			})
			require.NoError(t, err, "UpdateResource should succeed")

			_, err = client.DeleteResource(ctx, &workmailsdk.DeleteResourceInput{
				OrganizationId: aws.String(orgID),
				ResourceId:     aws.String(resID),
			})
			require.NoError(t, err, "DeleteResource should succeed")
		})
	}
}

// TestIntegration_WorkMail_MailDomains drives register→get→list→update-default→deregister.
func TestIntegration_WorkMail_MailDomains(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		orgAlias string
		domain   string
	}{
		{name: "domain_lifecycle", orgAlias: "integ-domain-org", domain: "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.orgAlias)

			_, err := client.RegisterMailDomain(ctx, &workmailsdk.RegisterMailDomainInput{
				OrganizationId: aws.String(orgID),
				DomainName:     aws.String(tt.domain),
			})
			require.NoError(t, err, "RegisterMailDomain should succeed")

			_, err = client.GetMailDomain(ctx, &workmailsdk.GetMailDomainInput{
				OrganizationId: aws.String(orgID),
				DomainName:     aws.String(tt.domain),
			})
			require.NoError(t, err, "GetMailDomain should succeed")

			listOut, err := client.ListMailDomains(ctx, &workmailsdk.ListMailDomainsInput{
				OrganizationId: aws.String(orgID),
			})
			require.NoError(t, err, "ListMailDomains should succeed")

			found := false
			for _, d := range listOut.MailDomains {
				if aws.ToString(d.DomainName) == tt.domain {
					found = true
					break
				}
			}
			assert.True(t, found, "domain should appear in list")

			_, err = client.DeregisterMailDomain(ctx, &workmailsdk.DeregisterMailDomainInput{
				OrganizationId: aws.String(orgID),
				DomainName:     aws.String(tt.domain),
			})
			require.NoError(t, err, "DeregisterMailDomain should succeed")
		})
	}
}

// TestIntegration_WorkMail_MailboxPermissions drives put→list→delete on mailbox permissions.
func TestIntegration_WorkMail_MailboxPermissions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name     string
		orgAlias string
	}{
		{name: "perms_lifecycle", orgAlias: "integ-perms-org"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createWorkMailClient(t)

			orgID := wmCreateOrg(t, client, tt.orgAlias)

			ownerOut, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String("owner"),
				DisplayName:    aws.String("Owner"),
				Password:       aws.String("TestPass@1234"),
			})
			require.NoError(t, err)
			ownerID := aws.ToString(ownerOut.UserId)

			granteeOut, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
				OrganizationId: aws.String(orgID),
				Name:           aws.String("grantee"),
				DisplayName:    aws.String("Grantee"),
				Password:       aws.String("TestPass@1234"),
			})
			require.NoError(t, err)
			granteeID := aws.ToString(granteeOut.UserId)

			_, err = client.PutMailboxPermissions(ctx, &workmailsdk.PutMailboxPermissionsInput{
				OrganizationId:   aws.String(orgID),
				EntityId:         aws.String(ownerID),
				GranteeId:        aws.String(granteeID),
				PermissionValues: []worktypes.PermissionType{worktypes.PermissionTypeFullAccess},
			})
			require.NoError(t, err, "PutMailboxPermissions should succeed")

			listOut, err := client.ListMailboxPermissions(ctx, &workmailsdk.ListMailboxPermissionsInput{
				OrganizationId: aws.String(orgID),
				EntityId:       aws.String(ownerID),
			})
			require.NoError(t, err, "ListMailboxPermissions should succeed")
			require.Len(t, listOut.Permissions, 1)
			assert.Equal(t, granteeID, aws.ToString(listOut.Permissions[0].GranteeId))

			_, err = client.DeleteMailboxPermissions(ctx, &workmailsdk.DeleteMailboxPermissionsInput{
				OrganizationId: aws.String(orgID),
				EntityId:       aws.String(ownerID),
				GranteeId:      aws.String(granteeID),
			})
			require.NoError(t, err, "DeleteMailboxPermissions should succeed")
		})
	}
}
