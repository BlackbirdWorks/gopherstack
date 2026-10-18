package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) securityProfileID(t *testing.T, inst, name string) string {
	t.Helper()

	list, err := f.client.ListSecurityProfiles(
		t.Context(),
		&sdk.ListSecurityProfilesInput{InstanceId: aws.String(inst)},
	)
	require.NoError(t, err)

	for _, p := range list.SecurityProfileSummaryList {
		if aws.ToString(p.Name) == name {
			return aws.ToString(p.Id)
		}
	}

	t.Fatalf("security profile %q not found", name)

	return ""
}

func (f *fixture) newUser(t *testing.T, inst, username, profile string) *sdk.CreateUserOutput {
	t.Helper()

	out, err := f.client.CreateUser(t.Context(), &sdk.CreateUserInput{
		InstanceId:         aws.String(inst),
		Username:           aws.String(username),
		Password:           aws.String("Passw0rdOK"),
		RoutingProfileId:   aws.String(profile),
		SecurityProfileIds: []string{f.securityProfileID(t, inst, "Agent")},
		PhoneConfig:        &types.UserPhoneConfig{PhoneType: types.PhoneTypeSoftPhone},
		IdentityInfo:       &types.UserIdentityInfo{FirstName: aws.String("Ada"), LastName: aws.String("Lovelace")},
		Tags:               map[string]string{"team": "a"},
	})
	require.NoError(t, err)

	return out
}

func TestUserLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "users")
	_, _, profile := f.defaultQueue(t, inst)
	user := f.newUser(t, inst, "ada", profile)

	got, err := f.client.DescribeUser(
		t.Context(),
		&sdk.DescribeUserInput{InstanceId: aws.String(inst), UserId: user.UserId},
	)
	require.NoError(t, err)
	assert.Equal(t, "ada", aws.ToString(got.User.Username))
	assert.Equal(t, "Lovelace", aws.ToString(got.User.IdentityInfo.LastName))
	assert.Equal(t, profile, aws.ToString(got.User.RoutingProfileId))
	assert.Equal(t, map[string]string{"team": "a"}, got.User.Tags)
	assert.NotNil(t, got.User.LastModifiedTime)

	_, err = f.client.UpdateUserIdentityInfo(t.Context(), &sdk.UpdateUserIdentityInfoInput{
		InstanceId: aws.String(
			inst,
		),
		UserId:       user.UserId,
		IdentityInfo: &types.UserIdentityInfo{FirstName: aws.String("Grace")},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateUserPhoneConfig(t.Context(), &sdk.UpdateUserPhoneConfigInput{
		InstanceId: aws.String(inst),
		UserId:     user.UserId,
		PhoneConfig: &types.UserPhoneConfig{
			PhoneType:       types.PhoneTypeDeskPhone,
			DeskPhoneNumber: aws.String("+15551230000"),
			AutoAccept:      true,
		},
	})
	require.NoError(t, err)

	got, err = f.client.DescribeUser(
		t.Context(),
		&sdk.DescribeUserInput{InstanceId: aws.String(inst), UserId: user.UserId},
	)
	require.NoError(t, err)
	assert.Equal(t, "Grace", aws.ToString(got.User.IdentityInfo.FirstName))
	assert.Equal(t, types.PhoneTypeDeskPhone, got.User.PhoneConfig.PhoneType)
	assert.True(t, got.User.PhoneConfig.AutoAccept)

	list, err := f.client.ListUsers(t.Context(), &sdk.ListUsersInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	require.Len(t, list.UserSummaryList, 1)

	_, err = f.client.DeleteUser(t.Context(), &sdk.DeleteUserInput{InstanceId: aws.String(inst), UserId: user.UserId})
	require.NoError(t, err)

	_, err = f.client.DescribeUser(
		t.Context(),
		&sdk.DescribeUserInput{InstanceId: aws.String(inst), UserId: user.UserId},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestUserValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "uservalid")
	_, _, profile := f.defaultQueue(t, inst)
	agent := f.securityProfileID(t, inst, "Agent")
	f.newUser(t, inst, "dup", profile)

	base := func() *sdk.CreateUserInput {
		return &sdk.CreateUserInput{
			InstanceId: aws.String(inst), Username: aws.String("fresh"), Password: aws.String("Passw0rdOK"),
			RoutingProfileId: aws.String(profile), SecurityProfileIds: []string{agent},
			PhoneConfig: &types.UserPhoneConfig{PhoneType: types.PhoneTypeSoftPhone},
		}
	}

	tests := []struct {
		mutate func(in *sdk.CreateUserInput)
		name   string
		code   string
	}{
		{
			name:   "duplicate",
			mutate: func(in *sdk.CreateUserInput) { in.Username = aws.String("DUP") },
			code:   "DuplicateResourceException",
		},
		{
			name:   "weak password",
			mutate: func(in *sdk.CreateUserInput) { in.Password = aws.String("short") },
			code:   "InvalidParameterException",
		},
		{
			name:   "no password",
			mutate: func(in *sdk.CreateUserInput) { in.Password = nil },
			code:   "InvalidParameterException",
		},
		{
			name:   "unknown profile",
			mutate: func(in *sdk.CreateUserInput) { in.RoutingProfileId = aws.String("nope") },
			code:   "ResourceNotFoundException",
		},
		{
			name:   "unknown security profile",
			mutate: func(in *sdk.CreateUserInput) { in.SecurityProfileIds = []string{"nope"} },
			code:   "ResourceNotFoundException",
		},
		{name: "desk phone needs number", mutate: func(in *sdk.CreateUserInput) {
			in.PhoneConfig = &types.UserPhoneConfig{PhoneType: types.PhoneTypeDeskPhone}
		}, code: "InvalidParameterException"},
		{
			name:   "unknown hierarchy",
			mutate: func(in *sdk.CreateUserInput) { in.HierarchyGroupId = aws.String("nope") },
			code:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := base()
			tt.mutate(in)
			_, err := f.client.CreateUser(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestUserAssignmentsAndStatus(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "assign")
	queue, _, profile := f.defaultQueue(t, inst)
	user := f.newUser(t, inst, "bob", profile)

	rp, err := f.client.CreateRoutingProfile(t.Context(), &sdk.CreateRoutingProfileInput{
		InstanceId: aws.String(
			inst,
		),
		Name:                   aws.String("Second"),
		Description:            aws.String("d"),
		DefaultOutboundQueueId: aws.String(queue),
		MediaConcurrencies:     []types.MediaConcurrency{{Channel: types.ChannelVoice, Concurrency: aws.Int32(1)}},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateUserRoutingProfile(t.Context(), &sdk.UpdateUserRoutingProfileInput{
		InstanceId: aws.String(inst), UserId: user.UserId, RoutingProfileId: rp.RoutingProfileId,
	})
	require.NoError(t, err)

	_, err = f.client.DeleteRoutingProfile(
		t.Context(),
		&sdk.DeleteRoutingProfileInput{InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	admin := f.securityProfileID(t, inst, "Admin")
	_, err = f.client.UpdateUserSecurityProfiles(t.Context(), &sdk.UpdateUserSecurityProfilesInput{
		InstanceId: aws.String(inst), UserId: user.UserId, SecurityProfileIds: []string{admin},
	})
	require.NoError(t, err)

	_, err = f.client.DeleteSecurityProfile(
		t.Context(),
		&sdk.DeleteSecurityProfileInput{InstanceId: aws.String(inst), SecurityProfileId: aws.String(admin)},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	statuses, err := f.client.ListAgentStatuses(t.Context(), &sdk.ListAgentStatusesInput{
		InstanceId: aws.String(inst), AgentStatusTypes: []types.AgentStatusType{types.AgentStatusTypeRoutable},
	})
	require.NoError(t, err)
	require.Len(t, statuses.AgentStatusSummaryList, 1)

	_, err = f.client.PutUserStatus(t.Context(), &sdk.PutUserStatusInput{
		InstanceId: aws.String(inst), UserId: user.UserId, AgentStatusId: statuses.AgentStatusSummaryList[0].Id,
	})
	require.NoError(t, err)

	data, err := f.client.GetCurrentUserData(t.Context(), &sdk.GetCurrentUserDataInput{
		InstanceId: aws.String(inst), Filters: &types.UserDataFilters{Agents: []string{aws.ToString(user.UserId)}},
	})
	require.NoError(t, err)
	require.Len(t, data.UserDataList, 1)
	assert.Equal(t, "Available", aws.ToString(data.UserDataList[0].Status.StatusName))
	assert.Equal(t, int32(1), data.UserDataList[0].MaxSlotsByChannel["VOICE"])

	_, err = f.client.GetCurrentUserData(
		t.Context(),
		&sdk.GetCurrentUserDataInput{InstanceId: aws.String(inst), Filters: &types.UserDataFilters{}},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestRoutingProfileQueues(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "rpq")
	basic, hours, _ := f.defaultQueue(t, inst)

	q2, err := f.client.CreateQueue(
		t.Context(),
		&sdk.CreateQueueInput{
			InstanceId:         aws.String(inst),
			Name:               aws.String("Two"),
			HoursOfOperationId: aws.String(hours),
		},
	)
	require.NoError(t, err)

	rp, err := f.client.CreateRoutingProfile(t.Context(), &sdk.CreateRoutingProfileInput{
		InstanceId: aws.String(
			inst,
		),
		Name:                   aws.String("Custom"),
		Description:            aws.String("d"),
		DefaultOutboundQueueId: aws.String(basic),
		MediaConcurrencies:     []types.MediaConcurrency{{Channel: types.ChannelChat, Concurrency: aws.Int32(3)}},
		QueueConfigs: []types.RoutingProfileQueueConfig{
			{
				Delay: aws.Int32(
					0,
				),
				Priority: aws.Int32(1),
				QueueReference: &types.RoutingProfileQueueReference{
					QueueId: aws.String(basic),
					Channel: types.ChannelChat,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = f.client.AssociateRoutingProfileQueues(t.Context(), &sdk.AssociateRoutingProfileQueuesInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		QueueConfigs: []types.RoutingProfileQueueConfig{
			{
				Delay: aws.Int32(
					5,
				),
				Priority:       aws.Int32(2),
				QueueReference: &types.RoutingProfileQueueReference{QueueId: q2.QueueId, Channel: types.ChannelChat},
			},
		},
	})
	require.NoError(t, err)

	_, err = f.client.UpdateRoutingProfileQueues(t.Context(), &sdk.UpdateRoutingProfileQueuesInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		QueueConfigs: []types.RoutingProfileQueueConfig{
			{
				Delay: aws.Int32(
					9,
				),
				Priority:       aws.Int32(4),
				QueueReference: &types.RoutingProfileQueueReference{QueueId: q2.QueueId, Channel: types.ChannelChat},
			},
		},
	})
	require.NoError(t, err)

	list, err := f.client.ListRoutingProfileQueues(
		t.Context(),
		&sdk.ListRoutingProfileQueuesInput{InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId},
	)
	require.NoError(t, err)
	require.Len(t, list.RoutingProfileQueueConfigSummaryList, 2)

	byQueue := map[string]types.RoutingProfileQueueConfigSummary{}
	for _, c := range list.RoutingProfileQueueConfigSummaryList {
		byQueue[aws.ToString(c.QueueId)] = c
	}

	assert.EqualValues(t, 9, byQueue[aws.ToString(q2.QueueId)].Delay)
	assert.EqualValues(t, 4, aws.ToInt32(byQueue[aws.ToString(q2.QueueId)].Priority))

	_, err = f.client.DisassociateRoutingProfileQueues(t.Context(), &sdk.DisassociateRoutingProfileQueuesInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		QueueReferences: []types.RoutingProfileQueueReference{{QueueId: aws.String(basic), Channel: types.ChannelChat}},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.DisassociateRoutingProfileQueues(t.Context(), &sdk.DisassociateRoutingProfileQueuesInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		QueueReferences: []types.RoutingProfileQueueReference{{QueueId: q2.QueueId, Channel: types.ChannelChat}},
	})
	require.NoError(t, err)

	_, err = f.client.DeleteQueue(
		t.Context(),
		&sdk.DeleteQueueInput{InstanceId: aws.String(inst), QueueId: aws.String(basic)},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.UpdateRoutingProfileName(t.Context(), &sdk.UpdateRoutingProfileNameInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId, Name: aws.String("Renamed"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateRoutingProfileConcurrency(t.Context(), &sdk.UpdateRoutingProfileConcurrencyInput{
		InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId,
		MediaConcurrencies: []types.MediaConcurrency{{Channel: types.ChannelVoice, Concurrency: aws.Int32(1)}},
	})
	require.NoError(t, err)

	got, err := f.client.DescribeRoutingProfile(
		t.Context(),
		&sdk.DescribeRoutingProfileInput{InstanceId: aws.String(inst), RoutingProfileId: rp.RoutingProfileId},
	)
	require.NoError(t, err)
	assert.Equal(t, "Renamed", aws.ToString(got.RoutingProfile.Name))
	require.Len(t, got.RoutingProfile.MediaConcurrencies, 1)
	assert.Equal(t, types.ChannelVoice, got.RoutingProfile.MediaConcurrencies[0].Channel)
}

func TestSecurityProfiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "secprof"))

	created, err := f.client.CreateSecurityProfile(t.Context(), &sdk.CreateSecurityProfileInput{
		InstanceId: inst, SecurityProfileName: aws.String("Auditors"), Description: aws.String("read only"),
		Permissions: []string{"Users.View", "Queues.View"}, Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	_, err = f.client.CreateSecurityProfile(
		t.Context(),
		&sdk.CreateSecurityProfileInput{InstanceId: inst, SecurityProfileName: aws.String("auditors")},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.UpdateSecurityProfile(t.Context(), &sdk.UpdateSecurityProfileInput{
		InstanceId:        inst,
		SecurityProfileId: created.SecurityProfileId,
		Permissions:       []string{"Users.View"},
		Description:       aws.String("new"),
	})
	require.NoError(t, err)

	perms, err := f.client.ListSecurityProfilePermissions(
		t.Context(),
		&sdk.ListSecurityProfilePermissionsInput{InstanceId: inst, SecurityProfileId: created.SecurityProfileId},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"Users.View"}, perms.Permissions)

	got, err := f.client.DescribeSecurityProfile(
		t.Context(),
		&sdk.DescribeSecurityProfileInput{InstanceId: inst, SecurityProfileId: created.SecurityProfileId},
	)
	require.NoError(t, err)
	assert.Equal(t, "new", aws.ToString(got.SecurityProfile.Description))
	assert.Equal(t, map[string]string{"k": "v"}, got.SecurityProfile.Tags)

	_, err = f.client.DeleteSecurityProfile(
		t.Context(),
		&sdk.DeleteSecurityProfileInput{InstanceId: inst, SecurityProfileId: created.SecurityProfileId},
	)
	require.NoError(t, err)
}

func TestHierarchy(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "hier"))

	_, err := f.client.CreateUserHierarchyGroup(
		t.Context(),
		&sdk.CreateUserHierarchyGroupInput{InstanceId: inst, Name: aws.String("EMEA")},
	)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = f.client.UpdateUserHierarchyStructure(t.Context(), &sdk.UpdateUserHierarchyStructureInput{
		InstanceId: inst,
		HierarchyStructure: &types.HierarchyStructureUpdate{
			LevelThree: &types.HierarchyLevelUpdate{Name: aws.String("Team")},
		},
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.UpdateUserHierarchyStructure(t.Context(), &sdk.UpdateUserHierarchyStructureInput{
		InstanceId: inst, HierarchyStructure: &types.HierarchyStructureUpdate{
			LevelOne: &types.HierarchyLevelUpdate{
				Name: aws.String("Region"),
			},
			LevelTwo: &types.HierarchyLevelUpdate{Name: aws.String("Team")},
		},
	})
	require.NoError(t, err)

	structure, err := f.client.DescribeUserHierarchyStructure(
		t.Context(),
		&sdk.DescribeUserHierarchyStructureInput{InstanceId: inst},
	)
	require.NoError(t, err)
	assert.Equal(t, "Region", aws.ToString(structure.HierarchyStructure.LevelOne.Name))
	assert.Nil(t, structure.HierarchyStructure.LevelThree)

	emea, err := f.client.CreateUserHierarchyGroup(
		t.Context(),
		&sdk.CreateUserHierarchyGroupInput{InstanceId: inst, Name: aws.String("EMEA")},
	)
	require.NoError(t, err)

	team, err := f.client.CreateUserHierarchyGroup(
		t.Context(),
		&sdk.CreateUserHierarchyGroupInput{
			InstanceId:    inst,
			Name:          aws.String("Support"),
			ParentGroupId: emea.HierarchyGroupId,
		},
	)
	require.NoError(t, err)

	got, err := f.client.DescribeUserHierarchyGroup(
		t.Context(),
		&sdk.DescribeUserHierarchyGroupInput{InstanceId: inst, HierarchyGroupId: team.HierarchyGroupId},
	)
	require.NoError(t, err)
	assert.Equal(t, "EMEA", aws.ToString(got.HierarchyGroup.HierarchyPath.LevelOne.Name))
	assert.Equal(t, "Support", aws.ToString(got.HierarchyGroup.HierarchyPath.LevelTwo.Name))

	_, err = f.client.CreateUserHierarchyGroup(
		t.Context(),
		&sdk.CreateUserHierarchyGroupInput{
			InstanceId:    inst,
			Name:          aws.String("Deep"),
			ParentGroupId: team.HierarchyGroupId,
		},
	)
	assert.Equal(t, "InvalidRequestException", apiCode(err))

	_, err = f.client.DeleteUserHierarchyGroup(
		t.Context(),
		&sdk.DeleteUserHierarchyGroupInput{InstanceId: inst, HierarchyGroupId: emea.HierarchyGroupId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.UpdateUserHierarchyGroupName(
		t.Context(),
		&sdk.UpdateUserHierarchyGroupNameInput{
			InstanceId:       inst,
			HierarchyGroupId: team.HierarchyGroupId,
			Name:             aws.String("Helpdesk"),
		},
	)
	require.NoError(t, err)

	_, _, profile := f.defaultQueue(t, aws.ToString(inst))
	user := f.newUser(t, aws.ToString(inst), "grouped", profile)
	_, err = f.client.UpdateUserHierarchy(
		t.Context(),
		&sdk.UpdateUserHierarchyInput{InstanceId: inst, UserId: user.UserId, HierarchyGroupId: team.HierarchyGroupId},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteUserHierarchyGroup(
		t.Context(),
		&sdk.DeleteUserHierarchyGroupInput{InstanceId: inst, HierarchyGroupId: team.HierarchyGroupId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	data, err := f.client.GetCurrentUserData(t.Context(), &sdk.GetCurrentUserDataInput{
		InstanceId: inst,
		Filters:    &types.UserDataFilters{UserHierarchyGroups: []string{aws.ToString(emea.HierarchyGroupId)}},
	})
	require.NoError(t, err)
	require.Len(t, data.UserDataList, 1)
	assert.Equal(t, aws.ToString(emea.HierarchyGroupId), aws.ToString(data.UserDataList[0].HierarchyPath.LevelOne.Id))
}

func TestAgentStatusesAndPrompts(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetResolvers(fakeResolvers{objects: map[string]bool{"audio/greeting.wav": true}})
	inst := aws.String(f.newInstance(t, "misc"))

	st, err := f.client.CreateAgentStatus(
		t.Context(),
		&sdk.CreateAgentStatusInput{InstanceId: inst, Name: aws.String("Lunch"), State: types.AgentStatusStateEnabled},
	)
	require.NoError(t, err)

	got, err := f.client.DescribeAgentStatus(
		t.Context(),
		&sdk.DescribeAgentStatusInput{InstanceId: inst, AgentStatusId: st.AgentStatusId},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AgentStatusTypeCustom, got.AgentStatus.Type)
	assert.EqualValues(t, 3, aws.ToInt32(got.AgentStatus.DisplayOrder))

	_, err = f.client.UpdateAgentStatus(
		t.Context(),
		&sdk.UpdateAgentStatusInput{
			InstanceId:    inst,
			AgentStatusId: st.AgentStatusId,
			State:         types.AgentStatusStateDisabled,
			Name:          aws.String("Break"),
		},
	)
	require.NoError(t, err)

	got, err = f.client.DescribeAgentStatus(
		t.Context(),
		&sdk.DescribeAgentStatusInput{InstanceId: inst, AgentStatusId: st.AgentStatusId},
	)
	require.NoError(t, err)
	assert.Equal(t, "Break", aws.ToString(got.AgentStatus.Name))
	assert.Equal(t, types.AgentStatusStateDisabled, got.AgentStatus.State)

	_, err = f.client.CreatePrompt(
		t.Context(),
		&sdk.CreatePromptInput{InstanceId: inst, Name: aws.String("p"), S3Uri: aws.String("s3://audio/missing.wav")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.CreatePrompt(
		t.Context(),
		&sdk.CreatePromptInput{InstanceId: inst, Name: aws.String("p"), S3Uri: aws.String("s3://audio/greeting.mp3")},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	p, err := f.client.CreatePrompt(
		t.Context(),
		&sdk.CreatePromptInput{
			InstanceId: inst,
			Name:       aws.String("Greeting"),
			S3Uri:      aws.String("s3://audio/greeting.wav"),
		},
	)
	require.NoError(t, err)

	_, err = f.client.UpdatePrompt(
		t.Context(),
		&sdk.UpdatePromptInput{InstanceId: inst, PromptId: p.PromptId, Description: aws.String("hello")},
	)
	require.NoError(t, err)

	dp, err := f.client.DescribePrompt(t.Context(), &sdk.DescribePromptInput{InstanceId: inst, PromptId: p.PromptId})
	require.NoError(t, err)
	assert.Equal(t, "hello", aws.ToString(dp.Prompt.Description))

	prompts, err := f.client.ListPrompts(t.Context(), &sdk.ListPromptsInput{InstanceId: inst})
	require.NoError(t, err)
	assert.Len(t, prompts.PromptSummaryList, 1)

	_, err = f.client.DeletePrompt(t.Context(), &sdk.DeletePromptInput{InstanceId: inst, PromptId: p.PromptId})
	require.NoError(t, err)
}
