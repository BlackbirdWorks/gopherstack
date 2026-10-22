package connect

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxTemplateNameLen = 100
	maxTemplateFields  = 50
	templateActive     = "ACTIVE"
	templateInactive   = "INACTIVE"
	fieldSingleSelect  = "SINGLE_SELECT"
)

var templateFieldTypes = []string{ //nolint:gochecknoglobals // immutable lookup table
	"NAME", "DESCRIPTION", "SCHEDULED_TIME", "QUICK_CONNECT", "URL", "NUMBER", "TEXT", "TEXT_AREA", "DATE_TIME",
	"BOOLEAN", fieldSingleSelect, emailValue, "SELF_ASSIGN", "EXPIRY_DURATION",
}

// TaskTemplateFieldID names a template field.
type TaskTemplateFieldID struct {
	Name string `json:"Name"`
}

// TaskTemplateField is one field of a task template.
type TaskTemplateField struct {
	ID                  TaskTemplateFieldID `json:"Id"`
	Description         string              `json:"Description,omitempty"`
	Type                string              `json:"Type,omitempty"`
	SingleSelectOptions []string            `json:"SingleSelectOptions,omitempty"`
}

// TaskTemplateFieldRef references a field in a constraint.
type TaskTemplateFieldRef struct {
	ID TaskTemplateFieldID `json:"Id"`
}

// TaskTemplateConstraints restricts fields of a task template.
type TaskTemplateConstraints struct {
	RequiredFields  []TaskTemplateFieldRef `json:"RequiredFields,omitempty"`
	ReadOnlyFields  []TaskTemplateFieldRef `json:"ReadOnlyFields,omitempty"`
	InvisibleFields []TaskTemplateFieldRef `json:"InvisibleFields,omitempty"`
}

// TaskTemplateDefaultValue is a default for one field.
type TaskTemplateDefaultValue struct {
	ID           TaskTemplateFieldID `json:"Id"`
	DefaultValue string              `json:"DefaultValue"`
}

// TaskTemplateDefaults holds field defaults.
type TaskTemplateDefaults struct {
	DefaultFieldValues []TaskTemplateDefaultValue `json:"DefaultFieldValues,omitempty"`
}

// TaskTemplate is the GetTaskTemplate response.
type TaskTemplate struct {
	CreatedTime      awstime.EpochTime        `json:"CreatedTime"`
	LastModifiedTime awstime.EpochTime        `json:"LastModifiedTime"`
	Constraints      *TaskTemplateConstraints `json:"Constraints,omitempty"`
	Defaults         *TaskTemplateDefaults    `json:"Defaults,omitempty"`
	Tags             map[string]string        `json:"Tags,omitempty"`
	Arn              string                   `json:"Arn"`
	ContactFlowID    string                   `json:"ContactFlowId,omitempty"`
	Description      string                   `json:"Description,omitempty"`
	ID               string                   `json:"Id"`
	InstanceID       string                   `json:"InstanceId"`
	Name             string                   `json:"Name"`
	SelfAssignFlowID string                   `json:"SelfAssignFlowId,omitempty"`
	Status           string                   `json:"Status"`
	Fields           []TaskTemplateField      `json:"Fields"`
}

// TaskTemplateMetadata is the list shape of a task template.
type TaskTemplateMetadata struct {
	CreatedTime      awstime.EpochTime `json:"CreatedTime"`
	LastModifiedTime awstime.EpochTime `json:"LastModifiedTime"`
	Arn              string            `json:"Arn"`
	Description      string            `json:"Description,omitempty"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	Status           string            `json:"Status"`
}

type taskTemplateRec struct {
	InstanceID string       `json:"instanceId"`
	Template   TaskTemplate `json:"template"`
}

type taskTemplateBody struct {
	Constraints      *TaskTemplateConstraints
	Defaults         *TaskTemplateDefaults
	ContactFlowID    string `json:"ContactFlowId"`
	Description      string
	Name             string
	SelfAssignFlowID string `json:"SelfAssignFlowId"`
	Status           string
	Fields           []TaskTemplateField
}

// CreateTaskTemplateInput is the CreateTaskTemplate request.
type CreateTaskTemplateInput struct {
	ClientToken string
	instPath
	taskTemplateBody
}

// UpdateTaskTemplateInput is the UpdateTaskTemplate request.
type UpdateTaskTemplateInput struct {
	TemplateID string `json:"-" path:"TaskTemplateId"`
	instPath
	taskTemplateBody
}

// ListTaskTemplatesInput holds the ListTaskTemplates query.
type ListTaskTemplatesInput struct {
	Name      string `json:"-" query:"name"`
	Status    string `json:"-" query:"status"`
	NextToken string `json:"-" query:"nextToken"`
	instPath
	MaxResults int32 `json:"-" query:"maxResults"`
}

// UpdateContactTaskTemplateInput is the UpdateContactTaskTemplate request.
type UpdateContactTaskTemplateInput struct {
	ContactID      string `json:"ContactId"`
	TaskTemplateID string `json:"TaskTemplateId"`
	InstanceID     string `json:"InstanceId"`
}

type taskTemplateOut struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

func (b *Backend) checkTemplateFields(fields []TaskTemplateField) (map[string]bool, error) {
	if len(fields) == 0 || len(fields) > maxTemplateFields {
		return nil, invalidParamErr("Fields must contain 1-%d fields", maxTemplateFields)
	}

	names := make(map[string]bool, len(fields))

	for _, f := range fields {
		if err := checkName("Fields.Id.Name", f.ID.Name, maxNameLen); err != nil {
			return nil, err
		}

		if names[f.ID.Name] {
			return nil, invalidParamErr("field %q is duplicated", f.ID.Name)
		}

		names[f.ID.Name] = true

		if f.Type != "" {
			if err := oneOf("Fields.Type", f.Type, templateFieldTypes...); err != nil {
				return nil, err
			}
		}

		if (f.Type == fieldSingleSelect) != (len(f.SingleSelectOptions) > 0) {
			return nil, invalidParamErr("SingleSelectOptions are required exactly for SINGLE_SELECT fields")
		}
	}

	return names, nil
}

func checkTemplateRefs(names map[string]bool, c *TaskTemplateConstraints, d *TaskTemplateDefaults) error {
	var refs []string

	if c != nil {
		for _, group := range [][]TaskTemplateFieldRef{c.RequiredFields, c.ReadOnlyFields, c.InvisibleFields} {
			for _, r := range group {
				refs = append(refs, r.ID.Name)
			}
		}
	}

	if d != nil {
		for _, v := range d.DefaultFieldValues {
			refs = append(refs, v.ID.Name)
		}
	}

	for _, r := range refs {
		if !names[r] {
			return invalidParamErr("constraint or default references unknown field %q", r)
		}
	}

	return nil
}

func (b *Backend) checkTemplateBody(inst string, in *taskTemplateBody) error {
	if err := firstError(checkName("Name", in.Name, maxTemplateNameLen), checkDescription(in.Description)); err != nil {
		return err
	}

	if in.Status != "" {
		if err := oneOf("Status", in.Status, templateActive, templateInactive); err != nil {
			return err
		}
	}

	names, err := b.checkTemplateFields(in.Fields)
	if err != nil {
		return err
	}

	if err = checkTemplateRefs(names, in.Constraints, in.Defaults); err != nil {
		return err
	}

	for _, id := range []string{in.ContactFlowID, in.SelfAssignFlowID} {
		if id != "" && !b.flows.has(inst, id) {
			return notFoundErr("ContactFlow", id)
		}
	}

	return nil
}

func templateNameTaken(b *Backend, inst, name, except string) bool {
	return nameExists(b.taskTemplates, inst, func(t *taskTemplateRec) string { return t.Template.Name }, name, except)
}

// CreateTaskTemplate creates a task template.
func (b *Backend) CreateTaskTemplate(ctx context.Context, in *CreateTaskTemplateInput) (*taskTemplateOut, error) {
	return writeInstance(b, "CreateTaskTemplate", in.InstanceID, func(inst string) (*taskTemplateOut, error) {
		if err := b.checkTemplateBody(inst, &in.taskTemplateBody); err != nil {
			return nil, err
		}

		if templateNameTaken(b, inst, in.Name, "") {
			return nil, duplicateErr("TaskTemplate", in.Name)
		}

		if in.Status == "" {
			in.Status = templateActive
		}

		id := newID()
		now := awstime.NewEpochTime(b.now())
		rec := &taskTemplateRec{InstanceID: inst, Template: TaskTemplate{
			CreatedTime: now, LastModifiedTime: now, Constraints: in.Constraints, Defaults: in.Defaults,
			Arn:           b.buildARN(ctx, "instance/"+inst+"/task-template/"+id),
			ContactFlowID: in.ContactFlowID, Description: in.Description, ID: id, InstanceID: inst, Name: in.Name,
			SelfAssignFlowID: in.SelfAssignFlowID, Status: in.Status, Fields: in.Fields,
		}}
		b.taskTemplates.put(rec)

		return &taskTemplateOut{Arn: rec.Template.Arn, ID: id}, nil
	})
}

// GetTaskTemplate returns a task template.
func (b *Backend) GetTaskTemplate(inst, id string) (*TaskTemplate, error) {
	return readInstance(b, "GetTaskTemplate", inst, func(inst string) (*TaskTemplate, error) {
		t, err := b.taskTemplates.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := t.Template
		out.Tags = cloneTags(out.Tags)

		return &out, nil
	})
}

// ListTaskTemplates pages through task templates filtered by status and name.
func (b *Backend) ListTaskTemplates(in *ListTaskTemplatesInput) ([]TaskTemplateMetadata, string, error) {
	if in.Status != "" {
		if err := oneOf("status", in.Status, templateActive, templateInactive); err != nil {
			return nil, "", err
		}
	}

	return listIn(b, "ListTaskTemplates", in.InstanceID, b.taskTemplates,
		func(t *taskTemplateRec) (TaskTemplateMetadata, bool) {
			x := &t.Template
			if (in.Status != "" && x.Status != in.Status) ||
				!strings.Contains(strings.ToLower(x.Name), strings.ToLower(in.Name)) {
				return TaskTemplateMetadata{}, false
			}

			return TaskTemplateMetadata{
				CreatedTime: x.CreatedTime, LastModifiedTime: x.LastModifiedTime, Arn: x.Arn,
				Description: x.Description, ID: x.ID, Name: x.Name, Status: x.Status,
			}, true
		}, in.NextToken, in.MaxResults)
}

// UpdateTaskTemplate replaces the supplied settings of a task template.
func (b *Backend) UpdateTaskTemplate(in *UpdateTaskTemplateInput) (*TaskTemplate, error) {
	return writeInstance(b, "UpdateTaskTemplate", in.InstanceID, func(inst string) (*TaskTemplate, error) {
		t, err := b.taskTemplates.get(inst, in.TemplateID)
		if err != nil {
			return nil, err
		}

		body := mergeTemplate(&t.Template, &in.taskTemplateBody)
		if err = b.checkTemplateBody(inst, &body); err != nil {
			return nil, err
		}

		if templateNameTaken(b, inst, body.Name, in.TemplateID) {
			return nil, duplicateErr("TaskTemplate", body.Name)
		}

		x := &t.Template
		x.Name, x.Description, x.Status, x.Fields = body.Name, body.Description, body.Status, body.Fields
		x.Constraints, x.Defaults = body.Constraints, body.Defaults
		x.ContactFlowID, x.SelfAssignFlowID = body.ContactFlowID, body.SelfAssignFlowID
		x.LastModifiedTime = awstime.NewEpochTime(b.now())

		out := *x
		out.Tags = nil

		return &out, nil
	})
}

func mergeTemplate(cur *TaskTemplate, in *taskTemplateBody) taskTemplateBody {
	out := taskTemplateBody{
		Constraints: cur.Constraints, Defaults: cur.Defaults, ContactFlowID: cur.ContactFlowID,
		Description: cur.Description, Name: cur.Name, SelfAssignFlowID: cur.SelfAssignFlowID,
		Status: cur.Status, Fields: cur.Fields,
	}

	if in.Constraints != nil {
		out.Constraints = in.Constraints
	}

	if in.Defaults != nil {
		out.Defaults = in.Defaults
	}

	if len(in.Fields) > 0 {
		out.Fields = in.Fields
	}

	for dst, src := range map[*string]string{
		&out.ContactFlowID: in.ContactFlowID, &out.Description: in.Description, &out.Name: in.Name,
		&out.SelfAssignFlowID: in.SelfAssignFlowID, &out.Status: in.Status,
	} {
		if src != "" {
			*dst = src
		}
	}

	return out
}

// DeleteTaskTemplate removes a task template.
func (b *Backend) DeleteTaskTemplate(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteTaskTemplate", inst, func(inst string) (*empty, error) {
		return nil, b.taskTemplates.del(inst, id)
	})
}

// UpdateContactTaskTemplate attaches a template to a task contact.
func (b *Backend) UpdateContactTaskTemplate(in *UpdateContactTaskTemplateInput) (*empty, error) {
	return writeInstance(b, "UpdateContactTaskTemplate", in.InstanceID, func(inst string) (*empty, error) {
		c, err := b.contacts.get(inst, in.ContactID)
		if err != nil {
			return nil, contactNotFoundErr(in.ContactID)
		}

		if _, err = b.taskTemplates.get(inst, in.TaskTemplateID); err != nil {
			return nil, err
		}

		if c.Contact.Channel != channelTask {
			return nil, invalidRequestErr("contact %q is not a task", in.ContactID)
		}

		c.TaskTemplateID = in.TaskTemplateID

		return &empty{}, nil
	})
}

// applyTaskTemplate resolves the template of a StartTaskContact request and defaults its flow.
func (b *Backend) applyTaskTemplate(inst string, in *StartTaskContactInput) error {
	if in.TaskTemplateID == "" {
		return nil
	}

	t, err := b.taskTemplates.get(inst, in.TaskTemplateID)
	if err != nil {
		return err
	}

	if t.Template.Status != templateActive {
		return invalidRequestErr("task template %q is not active", in.TaskTemplateID)
	}

	if in.ContactFlowID == "" {
		in.ContactFlowID = t.Template.ContactFlowID
	}

	return nil
}
