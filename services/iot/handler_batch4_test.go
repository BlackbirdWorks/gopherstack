package iot_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// ---------------------------------------------------------------------------
// RegisterThing
// ---------------------------------------------------------------------------

func TestBackend_RegisterThing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *iot.RegisterThingInput
		name    string
	}{
		{
			name: "explicit_thing_name",
			input: &iot.RegisterThingInput{
				TemplateBody: `{"Parameters":{"ThingName":{"Type":"String"}}}`,
				Parameters:   map[string]string{"ThingName": "provisioned-thing"},
			},
		},
		{
			name: "generated_thing_name",
			input: &iot.RegisterThingInput{
				TemplateBody: `{"Parameters":{}}`,
			},
		},
		{
			name: "missing_template_body",
			input: &iot.RegisterThingInput{
				Parameters: map[string]string{"ThingName": "x"},
			},
			wantErr: iot.ErrValidation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()

			out, err := b.RegisterThing(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.CertificatePem)
			assert.NotEmpty(t, out.ResourceArns["thing"])
			assert.NotEmpty(t, out.ResourceArns["certificate"])
			assert.Equal(t, 1, b.ThingCount())
		})
	}
}

func TestBackend_RegisterThing_UpdatesExistingThing(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "existing-thing"})
	require.NoError(t, err)

	out, err := b.RegisterThing(&iot.RegisterThingInput{
		TemplateBody: `{"Parameters":{}}`,
		Parameters:   map[string]string{"ThingName": "existing-thing", "extra": "attr"},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, b.ThingCount())

	described, err := b.DescribeThing("existing-thing")
	require.NoError(t, err)
	assert.Equal(t, "attr", described.Attributes["extra"])
	assert.Equal(t, int64(2), described.Version)
	assert.Contains(t, out.ResourceArns["thing"], "existing-thing")
}

func TestHandler_RegisterThing(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	out := iotOK(t, h, http.MethodPost, "/things", map[string]any{
		"templateBody": `{"Parameters":{}}`,
		"parameters":   map[string]any{"ThingName": "http-thing"},
	})

	resourceArns, _ := out["resourceArns"].(map[string]any)
	require.NotNil(t, resourceArns)
	assert.Contains(t, resourceArns["thing"], "http-thing")
	assert.NotEmpty(t, out["certificatePem"])
}

// ---------------------------------------------------------------------------
// Bulk thing registration tasks
// ---------------------------------------------------------------------------

func TestBackend_ThingRegistrationTaskLifecycle(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	task, err := b.StartThingRegistrationTask(&iot.StartThingRegistrationTaskInput{
		TemplateBody:    `{}`,
		InputFileBucket: "my-bucket",
		InputFileKey:    "input.json",
		RoleArn:         "arn:aws:iam::000000000000:role/provisioning",
	})
	require.NoError(t, err)
	require.NotEmpty(t, task.TaskID)
	assert.Equal(t, "InProgress", task.Status)

	// Describe
	described, err := b.DescribeThingRegistrationTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, task.TaskID, described.TaskID)
	assert.Equal(t, "my-bucket", described.InputFileBucket)

	// List (unfiltered)
	all := b.ListThingRegistrationTasks("")
	assert.Len(t, all, 1)

	// List (filtered, no match yet since task is still InProgress)
	none := b.ListThingRegistrationTasks("Cancelled")
	assert.Empty(t, none)

	// Reports
	links, err := b.ListThingRegistrationTaskReports(task.TaskID, "RESULTS")
	require.NoError(t, err)
	assert.Len(t, links, 1)
	assert.Contains(t, links[0], task.TaskID)

	// Stop
	require.NoError(t, b.StopThingRegistrationTask(task.TaskID))

	described2, err := b.DescribeThingRegistrationTask(task.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "Cancelled", described2.Status)

	// Now the filtered list should find it.
	cancelled := b.ListThingRegistrationTasks("Cancelled")
	assert.Len(t, cancelled, 1)
}

func TestBackend_StartThingRegistrationTask_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *iot.StartThingRegistrationTaskInput
		name  string
	}{
		{name: "missing_all", input: &iot.StartThingRegistrationTaskInput{}},
		{
			name: "missing_role_arn",
			input: &iot.StartThingRegistrationTaskInput{
				TemplateBody:    "{}",
				InputFileBucket: "b",
				InputFileKey:    "k",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.StartThingRegistrationTask(tt.input)
			require.ErrorIs(t, err, iot.ErrValidation)
		})
	}
}

func TestBackend_ThingRegistrationTask_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	_, err := b.DescribeThingRegistrationTask("nonexistent")
	require.ErrorIs(t, err, iot.ErrRegistrationTaskNotFound)

	require.ErrorIs(t, b.StopThingRegistrationTask("nonexistent"), iot.ErrRegistrationTaskNotFound)

	_, err = b.ListThingRegistrationTaskReports("nonexistent", "")
	require.ErrorIs(t, err, iot.ErrRegistrationTaskNotFound)
}

func TestHandler_ThingRegistrationTaskLifecycle(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	out := iotOK(t, h, http.MethodPost, "/thing-registration-tasks", map[string]any{
		"templateBody":    `{}`,
		"inputFileBucket": "my-bucket",
		"inputFileKey":    "input.json",
		"roleArn":         "arn:aws:iam::000000000000:role/provisioning",
	})
	taskID, _ := out["taskId"].(string)
	require.NotEmpty(t, taskID)

	out2 := iotOK(t, h, http.MethodGet, "/thing-registration-tasks/"+taskID, nil)
	assert.Equal(t, "InProgress", out2["status"])

	out3 := iotOK(t, h, http.MethodGet, "/thing-registration-tasks", nil)
	taskIDs, _ := out3["taskIds"].([]any)
	assert.Len(t, taskIDs, 1)

	out4 := iotOK(t, h, http.MethodGet, "/thing-registration-tasks/"+taskID+"/reports", nil)
	links, _ := out4["resourceLinks"].([]any)
	assert.Len(t, links, 1)

	iotOK(t, h, http.MethodPut, "/thing-registration-tasks/"+taskID+"/cancel", nil)

	out5 := iotOK(t, h, http.MethodGet, "/thing-registration-tasks/"+taskID, nil)
	assert.Equal(t, "Cancelled", out5["status"])

	iotExpectError(t, h, "/thing-registration-tasks/does-not-exist")
}

// ---------------------------------------------------------------------------
// UpdateThingType
// ---------------------------------------------------------------------------

func TestBackend_UpdateThingType(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThingType(&iot.CreateThingTypeInput{
		ThingTypeName:        "Sensor",
		Description:          "original",
		SearchableAttributes: []string{"model"},
	})
	require.NoError(t, err)

	err = b.UpdateThingType(&iot.UpdateThingTypeInput{
		ThingTypeName:        "Sensor",
		Description:          "updated",
		SearchableAttributes: []string{"model", "firmware"},
	})
	require.NoError(t, err)

	tt, err := b.DescribeThingType("Sensor")
	require.NoError(t, err)
	assert.Equal(t, "updated", tt.Description)
	assert.ElementsMatch(t, []string{"model", "firmware"}, tt.SearchableAttributes)
}

func TestBackend_UpdateThingType_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	err := b.UpdateThingType(&iot.UpdateThingTypeInput{ThingTypeName: "missing", Description: "x"})
	require.ErrorIs(t, err, iot.ErrThingTypeNotFound)
}

func TestHandler_UpdateThingType(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPost, "/thing-types/Sensor", map[string]any{
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": "original",
		},
	})

	iotOK(t, h, http.MethodPatch, "/thing-types/Sensor", map[string]any{
		"thingTypeProperties": map[string]any{
			"thingTypeDescription": "updated",
			"searchableAttributes": []string{"a", "b"},
		},
	})

	out := iotOK(t, h, http.MethodGet, "/thing-types/Sensor", nil)
	props, _ := out["thingTypeProperties"].(map[string]any)
	assert.Equal(t, "updated", props["thingTypeDescription"])

	iotExpectError(t, h, "/thing-types/does-not-exist")
}

// ---------------------------------------------------------------------------
// UpdateThingGroupsForThing
// ---------------------------------------------------------------------------

func TestBackend_UpdateThingGroupsForThing(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "thing-1"})
	require.NoError(t, err)
	_, err = b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "group-a"})
	require.NoError(t, err)
	_, err = b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "group-b"})
	require.NoError(t, err)

	err = b.UpdateThingGroupsForThing(&iot.UpdateThingGroupsForThingInput{
		ThingName:        "thing-1",
		ThingGroupsToAdd: []string{"group-a", "group-b"},
	})
	require.NoError(t, err)

	members, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "group-a"})
	require.NoError(t, err)
	assert.Contains(t, members, "thing-1")

	err = b.UpdateThingGroupsForThing(&iot.UpdateThingGroupsForThingInput{
		ThingName:           "thing-1",
		ThingGroupsToRemove: []string{"group-a"},
	})
	require.NoError(t, err)

	members2, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "group-a"})
	require.NoError(t, err)
	assert.NotContains(t, members2, "thing-1")

	members3, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "group-b"})
	require.NoError(t, err)
	assert.Contains(t, members3, "thing-1")
}

func TestBackend_UpdateThingGroupsForThing_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(*iot.InMemoryBackend)
		input   *iot.UpdateThingGroupsForThingInput
		name    string
	}{
		{
			name:    "missing_thing_name",
			input:   &iot.UpdateThingGroupsForThingInput{},
			wantErr: iot.ErrValidation,
		},
		{
			name:    "thing_not_found",
			input:   &iot.UpdateThingGroupsForThingInput{ThingName: "nope"},
			wantErr: iot.ErrThingNotFound,
		},
		{
			name: "group_not_found",
			setup: func(b *iot.InMemoryBackend) {
				_, _ = b.CreateThing(&iot.CreateThingInput{ThingName: "thing-1"})
			},
			input: &iot.UpdateThingGroupsForThingInput{
				ThingName:        "thing-1",
				ThingGroupsToAdd: []string{"missing-group"},
			},
			wantErr: iot.ErrThingGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.UpdateThingGroupsForThing(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestHandler_UpdateThingGroupsForThing(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPost, "/things/thing-1", nil)
	iotOK(t, h, http.MethodPost, "/thing-groups/group-a", nil)

	iotOK(t, h, http.MethodPut, "/thing-groups/updateThingGroupsForThing", map[string]any{
		"thingName":        "thing-1",
		"thingGroupsToAdd": []string{"group-a"},
	})

	out := iotOK(t, h, http.MethodGet, "/thing-groups/group-a/things", nil)
	things, _ := out["things"].([]any)
	assert.Contains(t, things, "thing-1")
}

// ---------------------------------------------------------------------------
// ListThingPrincipalsV2
// ---------------------------------------------------------------------------

func TestBackend_ListThingPrincipalsV2(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "thing-1"})
	require.NoError(t, err)

	require.NoError(t, b.AttachThingPrincipal(&iot.AttachThingPrincipalInput{
		ThingName: "thing-1",
		Principal: "arn:aws:iot:us-east-1:000000000000:cert/abc",
	}))

	principals, err := b.ListThingPrincipalsV2("thing-1")
	require.NoError(t, err)
	require.Len(t, principals, 1)
	assert.Equal(t, "arn:aws:iot:us-east-1:000000000000:cert/abc", principals[0].Principal)
	assert.Equal(t, "NON_EXCLUSIVE_THING", principals[0].ThingPrincipalType)
}

func TestBackend_ListThingPrincipalsV2_NotFound(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()
	_, err := b.ListThingPrincipalsV2("missing-thing")
	require.ErrorIs(t, err, iot.ErrThingNotFound)
}

func TestHandler_ListThingPrincipalsV2(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	iotOK(t, h, http.MethodPost, "/things/thing-1", nil)
	iotOK(t, h, http.MethodPut, "/things/thing-1/principals", nil)

	out := iotOK(t, h, http.MethodGet, "/things/thing-1/principals-v2", nil)
	objs, _ := out["thingPrincipalObjects"].([]any)
	assert.Len(t, objs, 1)

	iotExpectError(t, h, "/things/does-not-exist/principals-v2")
}

// ---------------------------------------------------------------------------
// ManagedJobTemplate
// ---------------------------------------------------------------------------

func TestBackend_DescribeManagedJobTemplate(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	tmpl, err := b.DescribeManagedJobTemplate("AWS-Reboot", "")
	require.NoError(t, err)
	assert.Equal(t, "AWS-Reboot", tmpl.TemplateName)
	assert.NotEmpty(t, tmpl.TemplateArn)
	assert.NotEmpty(t, tmpl.Document)
	assert.NotEmpty(t, tmpl.DocumentParameters)

	_, err = b.DescribeManagedJobTemplate("does-not-exist", "")
	require.ErrorIs(t, err, iot.ErrManagedJobTemplateNotFound)
}

func TestBackend_ListManagedJobTemplates(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackend()

	all := b.ListManagedJobTemplates("")
	assert.GreaterOrEqual(t, len(all), 2)

	filtered := b.ListManagedJobTemplates("AWS-Reboot")
	require.Len(t, filtered, 1)
	assert.Equal(t, "AWS-Reboot", filtered[0].TemplateName)
}

func TestHandler_ManagedJobTemplate(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	out := iotOK(t, h, http.MethodGet, "/managed-job-templates", nil)
	templates, _ := out["managedJobTemplates"].([]any)
	assert.NotEmpty(t, templates)

	out2 := iotOK(t, h, http.MethodGet, "/managed-job-templates/AWS-Reboot", nil)
	assert.Equal(t, "AWS-Reboot", out2["templateName"])

	iotExpectError(t, h, "/managed-job-templates/does-not-exist")
}
