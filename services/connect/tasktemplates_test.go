package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tmplField(name string, typ types.TaskTemplateFieldType) types.TaskTemplateField {
	return types.TaskTemplateField{Id: &types.TaskTemplateFieldIdentifier{Name: aws.String(name)}, Type: typ}
}

func TestTaskTemplateCreateValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "tmplvalid"))

	tests := []struct {
		mutate func(in *sdk.CreateTaskTemplateInput)
		name   string
		code   string
	}{
		{name: "duplicate field", mutate: func(in *sdk.CreateTaskTemplateInput) {
			in.Fields = append(in.Fields, tmplField("Name", types.TaskTemplateFieldTypeText))
		}, code: "InvalidParameterException"},
		{name: "select without options", mutate: func(in *sdk.CreateTaskTemplateInput) {
			in.Fields = []types.TaskTemplateField{tmplField("Pick", types.TaskTemplateFieldTypeSingleSelect)}
		}, code: "InvalidParameterException"},
		{name: "bad type", mutate: func(in *sdk.CreateTaskTemplateInput) {
			in.Fields = []types.TaskTemplateField{tmplField("X", "WEIRD")}
		}, code: "InvalidParameterException"},
		{name: "constraint on unknown field", mutate: func(in *sdk.CreateTaskTemplateInput) {
			in.Constraints = &types.TaskTemplateConstraints{
				RequiredFields: []types.RequiredFieldInfo{
					{Id: &types.TaskTemplateFieldIdentifier{Name: aws.String("Ghost")}},
				},
			}
		}, code: "InvalidParameterException"},
		{
			name:   "missing flow",
			mutate: func(in *sdk.CreateTaskTemplateInput) { in.ContactFlowId = aws.String("gone") },
			code:   "ResourceNotFoundException",
		},
		{
			name:   "bad status",
			mutate: func(in *sdk.CreateTaskTemplateInput) { in.Status = "PAUSED" },
			code:   "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateTaskTemplateInput{
				InstanceId: inst, Name: aws.String("t-" + tt.name),
				Fields: []types.TaskTemplateField{tmplField("Name", types.TaskTemplateFieldTypeName)},
			}
			tt.mutate(in)
			_, err := f.client.CreateTaskTemplate(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestTaskTemplateLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "tmpl"))
	flow := f.newFlow(t, aws.ToString(inst), "tflow")

	created, err := f.client.CreateTaskTemplate(t.Context(), &sdk.CreateTaskTemplateInput{
		InstanceId: inst, Name: aws.String("followup"), ContactFlowId: aws.String(flow),
		Description: aws.String("d"), Fields: []types.TaskTemplateField{
			tmplField("Name", types.TaskTemplateFieldTypeName),
			{
				Id: &types.TaskTemplateFieldIdentifier{
					Name: aws.String("Pick"),
				},
				Type:                types.TaskTemplateFieldTypeSingleSelect,
				SingleSelectOptions: []string{"a", "b"},
			},
		},
		Defaults: &types.TaskTemplateDefaults{DefaultFieldValues: []types.TaskTemplateDefaultFieldValue{
			{Id: &types.TaskTemplateFieldIdentifier{Name: aws.String("Pick")}, DefaultValue: aws.String("a")},
		}},
	})
	require.NoError(t, err)

	_, err = f.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: created.Arn, Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	_, err = f.client.CreateTaskTemplate(t.Context(), &sdk.CreateTaskTemplateInput{
		InstanceId: inst, Name: aws.String("followup"),
		Fields: []types.TaskTemplateField{tmplField("Name", types.TaskTemplateFieldTypeName)},
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	got, err := f.client.GetTaskTemplate(
		t.Context(),
		&sdk.GetTaskTemplateInput{InstanceId: inst, TaskTemplateId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, types.TaskTemplateStatusActive, got.Status)
	assert.Equal(t, flow, aws.ToString(got.ContactFlowId))
	assert.Len(t, got.Fields, 2)
	assert.Equal(t, "v", got.Tags["k"])
	assert.NotNil(t, got.CreatedTime)

	upd, err := f.client.UpdateTaskTemplate(t.Context(), &sdk.UpdateTaskTemplateInput{
		InstanceId:     inst,
		TaskTemplateId: created.Id,
		Status:         types.TaskTemplateStatusInactive,
		Name:           aws.String("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(upd.Name))
	assert.Len(t, upd.Fields, 2)

	listTests := []struct {
		name string
		in   sdk.ListTaskTemplatesInput
		want int
	}{
		{name: "all", in: sdk.ListTaskTemplatesInput{}, want: 1},
		{name: "active", in: sdk.ListTaskTemplatesInput{Status: types.TaskTemplateStatusActive}, want: 0},
		{name: "inactive", in: sdk.ListTaskTemplatesInput{Status: types.TaskTemplateStatusInactive}, want: 1},
		{name: "name filter", in: sdk.ListTaskTemplatesInput{Name: aws.String("RENAM")}, want: 1},
		{name: "name miss", in: sdk.ListTaskTemplatesInput{Name: aws.String("zzz")}, want: 0},
	}
	for _, tt := range listTests {
		in := tt.in
		in.InstanceId = inst
		out, lerr := f.client.ListTaskTemplates(t.Context(), &in)
		require.NoError(t, lerr, tt.name)
		assert.Len(t, out.TaskTemplates, tt.want, tt.name)
	}

	_, err = f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId: inst, Name: aws.String("x"), TaskTemplateId: created.Id,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "inactive template")

	_, err = f.client.UpdateTaskTemplate(t.Context(), &sdk.UpdateTaskTemplateInput{
		InstanceId: inst, TaskTemplateId: created.Id, Status: types.TaskTemplateStatusActive,
	})
	require.NoError(t, err)

	task, err := f.client.StartTaskContact(t.Context(), &sdk.StartTaskContactInput{
		InstanceId: inst, Name: aws.String("x"), TaskTemplateId: created.Id,
	})
	require.NoError(t, err)

	_, err = f.client.UpdateContactTaskTemplate(t.Context(), &sdk.UpdateContactTaskTemplateInput{
		InstanceId: inst, ContactId: task.ContactId, TaskTemplateId: aws.String("gone"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.UpdateContactTaskTemplate(t.Context(), &sdk.UpdateContactTaskTemplateInput{
		InstanceId: inst, ContactId: task.ContactId, TaskTemplateId: created.Id,
	})
	require.NoError(t, err)

	dst := f.restored(t)
	got, err = dst.client.GetTaskTemplate(
		t.Context(),
		&sdk.GetTaskTemplateInput{InstanceId: inst, TaskTemplateId: created.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "renamed", aws.ToString(got.Name))

	_, err = f.client.DeleteTaskTemplate(
		t.Context(),
		&sdk.DeleteTaskTemplateInput{InstanceId: inst, TaskTemplateId: created.Id},
	)
	require.NoError(t, err)
	_, err = f.client.GetTaskTemplate(
		t.Context(),
		&sdk.GetTaskTemplateInput{InstanceId: inst, TaskTemplateId: created.Id},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
