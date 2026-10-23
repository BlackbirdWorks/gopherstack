package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validFlow = `{"Version":"2019-10-30","StartAction":"a1","Actions":[` +
	`{"Identifier":"a1","Type":"MessageParticipant","Parameters":{"Text":"hi"},` +
	`"Transitions":{"NextAction":"a2","Errors":[{"NextAction":"a2","ErrorType":"NoMatchingError"}]}},` +
	`{"Identifier":"a2","Type":"DisconnectParticipant","Parameters":{},"Transitions":{}}]}`

func TestContactFlowContentValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "flowvalid"))

	tests := []struct {
		name    string
		content string
		problem string
	}{
		{name: "not json", content: "nope", problem: "not valid flow language JSON"},
		{
			name:    "no version",
			content: `{"StartAction":"a","Actions":[{"Identifier":"a","Type":"X"}]}`,
			problem: "Version is required",
		},
		{
			name:    "no actions",
			content: `{"Version":"2019-10-30","StartAction":"a","Actions":[]}`,
			problem: "Actions must not be empty",
		},
		{
			name:    "bad start",
			content: `{"Version":"2019-10-30","StartAction":"zz","Actions":[{"Identifier":"a","Type":"X"}]}`,
			problem: `StartAction "zz"`,
		},
		{
			name: "dangling transition",
			content: `{"Version":"2019-10-30","StartAction":"a","Actions":[` +
				`{"Identifier":"a","Type":"X","Transitions":{"NextAction":"gone"}}]}`,
			problem: `unknown action "gone"`,
		},
		{
			name: "duplicate id",
			content: `{"Version":"2019-10-30","StartAction":"a","Actions":[` +
				`{"Identifier":"a","Type":"X"},{"Identifier":"a","Type":"Y"}]}`,
			problem: "duplicate action Identifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
				InstanceId: inst,
				Name:       aws.String("f-" + tt.name),
				Type:       types.ContactFlowTypeContactFlow,
				Content:    aws.String(tt.content),
			})
			require.Error(t, err)
			assert.Equal(t, "InvalidContactFlowException", apiCode(err))

			var invalid *types.InvalidContactFlowException
			require.ErrorAs(t, err, &invalid)
			require.NotEmpty(t, invalid.Problems)
			assert.Contains(t, aws.ToString(invalid.Problems[0].Message), tt.problem)
		})
	}
}

func TestContactFlowLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "flows"))

	created, err := f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId:  inst,
		Name:        aws.String("Inbound"),
		Type:        types.ContactFlowTypeContactFlow,
		Content:     aws.String(validFlow),
		Description: aws.String("d"),
		Tags:        map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Len(t, aws.ToString(created.FlowContentSha256), 64)

	_, err = f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId: inst,
		Name:       aws.String("inbound"),
		Type:       types.ContactFlowTypeContactFlow,
		Content:    aws.String(validFlow),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.DescribeContactFlow(
		t.Context(),
		&sdk.DescribeContactFlowInput{InstanceId: inst, ContactFlowId: created.ContactFlowId},
	)
	require.NoError(t, err)
	assert.JSONEq(t, validFlow, aws.ToString(got.ContactFlow.Content))
	assert.Equal(t, types.ContactFlowStatusPublished, got.ContactFlow.Status)
	assert.Equal(t, types.ContactFlowStateActive, got.ContactFlow.State)
	assert.EqualValues(t, 1, aws.ToInt64(got.ContactFlow.Version))

	newContent := `{"Version":"2019-10-30","StartAction":"x","Actions":[` +
		`{"Identifier":"x","Type":"DisconnectParticipant","Parameters":{},"Transitions":{}}]}`
	_, err = f.client.UpdateContactFlowContent(
		t.Context(),
		&sdk.UpdateContactFlowContentInput{
			InstanceId:    inst,
			ContactFlowId: created.ContactFlowId,
			Content:       aws.String(newContent),
		},
	)
	require.NoError(t, err)

	_, err = f.client.UpdateContactFlowContent(
		t.Context(),
		&sdk.UpdateContactFlowContentInput{
			InstanceId:    inst,
			ContactFlowId: created.ContactFlowId,
			Content:       aws.String("{}"),
		},
	)
	assert.Equal(t, "InvalidContactFlowException", apiCode(err))

	_, err = f.client.UpdateContactFlowName(
		t.Context(),
		&sdk.UpdateContactFlowNameInput{
			InstanceId:    inst,
			ContactFlowId: created.ContactFlowId,
			Name:          aws.String("Renamed"),
		},
	)
	require.NoError(t, err)

	_, err = f.client.UpdateContactFlowMetadata(t.Context(), &sdk.UpdateContactFlowMetadataInput{
		InstanceId: inst, ContactFlowId: created.ContactFlowId, ContactFlowState: types.ContactFlowStateArchived,
	})
	require.NoError(t, err)

	list, err := f.client.ListContactFlows(
		t.Context(),
		&sdk.ListContactFlowsInput{
			InstanceId:       inst,
			ContactFlowTypes: []types.ContactFlowType{types.ContactFlowTypeContactFlow},
		},
	)
	require.NoError(t, err)
	require.Len(t, list.ContactFlowSummaryList, 2, "the sample inbound flow plus the new one")

	var renamed *types.ContactFlowSummary

	for i := range list.ContactFlowSummaryList {
		if aws.ToString(list.ContactFlowSummaryList[i].Id) == aws.ToString(created.ContactFlowId) {
			renamed = &list.ContactFlowSummaryList[i]
		}
	}

	require.NotNil(t, renamed)
	assert.Equal(t, types.ContactFlowStateArchived, renamed.ContactFlowState)
	assert.Equal(t, "Renamed", aws.ToString(renamed.Name))

	none, err := f.client.ListContactFlows(
		t.Context(),
		&sdk.ListContactFlowsInput{
			InstanceId:       inst,
			ContactFlowTypes: []types.ContactFlowType{types.ContactFlowTypeAgentHold},
		},
	)
	require.NoError(t, err)
	require.Len(t, none.ContactFlowSummaryList, 1)
	assert.Equal(t, "Default agent hold", aws.ToString(none.ContactFlowSummaryList[0].Name))

	_, err = f.client.DeleteContactFlow(
		t.Context(),
		&sdk.DeleteContactFlowInput{InstanceId: inst, ContactFlowId: created.ContactFlowId},
	)
	require.NoError(t, err)
}

func TestContactFlowModules(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "modules"))

	_, err := f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId: inst, Name: aws.String("bad"), Content: aws.String(`{"Version":"1"}`),
	})
	assert.Equal(t, "InvalidContactFlowModuleException", apiCode(err))

	_, err = f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId: inst, Name: aws.String("badsettings"), Content: aws.String(validFlow), Settings: aws.String("{"),
	})
	assert.Equal(t, "InvalidContactFlowModuleException", apiCode(err))

	m, err := f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId:  inst,
		Name:        aws.String("Mod"),
		Content:     aws.String(validFlow),
		Settings:    aws.String(`{"InputParameters":[]}`),
		ClientToken: aws.String("tok"),
	})
	require.NoError(t, err)

	again, err := f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId:  inst,
		Name:        aws.String("Mod"),
		Content:     aws.String(validFlow),
		Settings:    aws.String(`{"InputParameters":[]}`),
		ClientToken: aws.String("tok"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(m.Id), aws.ToString(again.Id))

	_, err = f.client.CreateContactFlowModule(t.Context(), &sdk.CreateContactFlowModuleInput{
		InstanceId: inst, Name: aws.String("Mod"), Content: aws.String(validFlow), ClientToken: aws.String("other"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.UpdateContactFlowModuleMetadata(t.Context(), &sdk.UpdateContactFlowModuleMetadataInput{
		InstanceId:          inst,
		ContactFlowModuleId: m.Id,
		State:               types.ContactFlowModuleStateArchived,
		Name:                aws.String("Mod2"),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeContactFlowModule(
		t.Context(),
		&sdk.DescribeContactFlowModuleInput{InstanceId: inst, ContactFlowModuleId: m.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "Mod2", aws.ToString(got.ContactFlowModule.Name))
	assert.Equal(t, types.ContactFlowModuleStateArchived, got.ContactFlowModule.State)
	assert.JSONEq(t, `{"InputParameters":[]}`, aws.ToString(got.ContactFlowModule.Settings))

	active, err := f.client.ListContactFlowModules(
		t.Context(),
		&sdk.ListContactFlowModulesInput{InstanceId: inst, ContactFlowModuleState: types.ContactFlowModuleStateActive},
	)
	require.NoError(t, err)
	assert.Empty(t, active.ContactFlowModulesSummaryList)

	_, err = f.client.UpdateContactFlowModuleContent(
		t.Context(),
		&sdk.UpdateContactFlowModuleContentInput{
			InstanceId:          inst,
			ContactFlowModuleId: m.Id,
			Content:             aws.String(validFlow),
		},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteContactFlowModule(
		t.Context(),
		&sdk.DeleteContactFlowModuleInput{InstanceId: inst, ContactFlowModuleId: m.Id},
	)
	require.NoError(t, err)
}
