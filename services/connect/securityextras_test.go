package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecurityProfileExtras(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "secextra"))
	mod, err := f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId: inst, Name: aws.String("mod"), Content: aws.String(validFlow),
	})
	require.NoError(t, err)

	_, err = f.client.CreateSecurityProfile(t.Context(), &sdk.CreateSecurityProfileInput{
		InstanceId: inst, SecurityProfileName: aws.String("bad"),
		AllowedFlowModules: []types.FlowModule{{FlowModuleId: aws.String("gone"), Type: types.FlowModuleTypeMcp}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.CreateSecurityProfile(t.Context(), &sdk.CreateSecurityProfileInput{
		InstanceId: inst, SecurityProfileName: aws.String("bad"),
		Applications: []types.Application{{Namespace: aws.String("ns"), Type: "COBOL"}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	created, err := f.client.CreateSecurityProfile(t.Context(), &sdk.CreateSecurityProfileInput{
		InstanceId:          inst,
		SecurityProfileName: aws.String("extras"),
		AllowedFlowModules:  []types.FlowModule{{FlowModuleId: mod.Id, Type: types.FlowModuleTypeMcp}},
		Applications: []types.Application{
			{Namespace: aws.String("ns"), Type: types.ApplicationTypeMcp, ApplicationPermissions: []string{"Read"}},
		},
		AllowedAIAgents: []types.AIAgent{
			{Arn: aws.String("arn:aws:wisdom:us-east-1:123456789012:ai-agent/a"), Type: types.AIAgentTypeThirdParty},
		},
	})
	require.NoError(t, err)

	apps, err := f.client.ListSecurityProfileApplications(t.Context(), &sdk.ListSecurityProfileApplicationsInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId,
	})
	require.NoError(t, err)
	require.Len(t, apps.Applications, 1)
	assert.Equal(t, []string{"Read"}, apps.Applications[0].ApplicationPermissions)
	assert.NotNil(t, apps.LastModifiedTime)

	modules, err := f.client.ListSecurityProfileFlowModules(t.Context(), &sdk.ListSecurityProfileFlowModulesInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId,
	})
	require.NoError(t, err)
	require.Len(t, modules.AllowedFlowModules, 1)
	assert.Equal(t, aws.ToString(mod.Id), aws.ToString(modules.AllowedFlowModules[0].FlowModuleId))

	agents, err := f.client.ListSecurityProfileAIAgents(t.Context(), &sdk.ListSecurityProfileAIAgentsInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId,
	})
	require.NoError(t, err)
	assert.Len(t, agents.AllowedAIAgents, 1)

	_, err = f.client.UpdateSecurityProfile(t.Context(), &sdk.UpdateSecurityProfileInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId, Applications: []types.Application{},
	})
	require.NoError(t, err)

	dst := f.restored(t)
	apps, err = dst.client.ListSecurityProfileApplications(t.Context(), &sdk.ListSecurityProfileApplicationsInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId,
	})
	require.NoError(t, err)
	assert.Empty(t, apps.Applications)

	modules, err = dst.client.ListSecurityProfileFlowModules(t.Context(), &sdk.ListSecurityProfileFlowModulesInput{
		InstanceId: inst, SecurityProfileId: created.SecurityProfileId,
	})
	require.NoError(t, err)
	assert.Len(t, modules.AllowedFlowModules, 1, "unsupplied lists are kept by an update")

	_, err = f.client.ListSecurityProfileApplications(t.Context(), &sdk.ListSecurityProfileApplicationsInput{
		InstanceId: inst, SecurityProfileId: aws.String("gone"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestEntitySecurityProfiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "entprof"))
	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "ada", profile)
	admin := aws.String(f.securityProfileID(t, aws.ToString(inst), "Admin"))
	agent := aws.String(f.securityProfileID(t, aws.ToString(inst), "Agent"))

	_, err := f.client.AssociateSecurityProfiles(t.Context(), &sdk.AssociateSecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeUser, EntityArn: user.UserArn,
		SecurityProfiles: []types.SecurityProfileItem{{Id: aws.String("gone")}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.AssociateSecurityProfiles(t.Context(), &sdk.AssociateSecurityProfilesInput{
		InstanceId:       inst,
		EntityType:       types.EntityTypeUser,
		EntityArn:        aws.String("arn:aws:connect:us-east-1:123456789012:instance/x/agent/y"),
		SecurityProfiles: []types.SecurityProfileItem{{Id: admin}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "unknown user")

	_, err = f.client.AssociateSecurityProfiles(t.Context(), &sdk.AssociateSecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeUser, EntityArn: user.UserArn,
		SecurityProfiles: []types.SecurityProfileItem{{Id: admin}},
	})
	require.NoError(t, err)

	list, err := f.client.ListEntitySecurityProfiles(t.Context(), &sdk.ListEntitySecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeUser, EntityArn: user.UserArn,
	})
	require.NoError(t, err)
	assert.Len(t, list.SecurityProfiles, 2)

	described, err := f.client.DescribeUser(t.Context(), &sdk.DescribeUserInput{InstanceId: inst, UserId: user.UserId})
	require.NoError(t, err)
	assert.Contains(t, described.User.SecurityProfileIds, aws.ToString(admin))

	_, err = f.client.DisassociateSecurityProfiles(t.Context(), &sdk.DisassociateSecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeUser, EntityArn: user.UserArn,
		SecurityProfiles: []types.SecurityProfileItem{{Id: agent}},
	})
	require.NoError(t, err)

	_, err = f.client.DisassociateSecurityProfiles(t.Context(), &sdk.DisassociateSecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeUser, EntityArn: user.UserArn,
		SecurityProfiles: []types.SecurityProfileItem{{Id: agent}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "no longer assigned")

	const aiAgent = "arn:aws:wisdom:us-east-1:123456789012:ai-agent/a1"

	_, err = f.client.AssociateSecurityProfiles(t.Context(), &sdk.AssociateSecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeAiAgent, EntityArn: aws.String(aiAgent),
		SecurityProfiles: []types.SecurityProfileItem{{Id: agent}},
	})
	require.NoError(t, err)

	dst := f.restored(t)
	ai, err := dst.client.ListEntitySecurityProfiles(t.Context(), &sdk.ListEntitySecurityProfilesInput{
		InstanceId: inst, EntityType: types.EntityTypeAiAgent, EntityArn: aws.String(aiAgent),
	})
	require.NoError(t, err)
	require.Len(t, ai.SecurityProfiles, 1)
	assert.Equal(t, aws.ToString(agent), aws.ToString(ai.SecurityProfiles[0].Id))
}
