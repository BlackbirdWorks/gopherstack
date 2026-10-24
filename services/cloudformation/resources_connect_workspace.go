package cloudformation

import (
	"encoding/json"
	"strings"

	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// ---- Workspace ----

func createConnectWorkspace(c *connectCall) (string, error) {
	if err := c.reject("Pages", "Media", "Associations"); err != nil {
		return "", err
	}

	var in connectbackend.CreateWorkspaceInput
	if err := c.decode(&in, "Visibility"); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateWorkspace(c.ctx, &in)
	if err != nil {
		return "", err
	}

	if v := c.str("Visibility"); v != "" {
		vis := connectbackend.UpdateWorkspaceVisibilityInput{Visibility: v}
		vis.InstanceID, vis.WorkspaceID = splitConnectARN(out.WorkspaceArn)

		if _, err = c.be.UpdateWorkspaceVisibility(c.ctx, &vis); err != nil {
			return "", err
		}
	}

	return out.WorkspaceArn, nil
}

func updateConnectWorkspace(c *connectCall, physicalID string) error {
	if err := c.reject("Pages", "Media", "Associations"); err != nil {
		return err
	}

	var meta connectbackend.UpdateWorkspaceMetadataInput
	if err := c.decode(&meta, connectTagsProp); err != nil {
		return err
	}

	meta.InstanceID, meta.WorkspaceID = splitConnectARN(physicalID)
	meta.Description = new(c.str("Description"))
	meta.Title = new(c.str("Title"))

	if _, err := c.be.UpdateWorkspaceMetadata(c.ctx, &meta); err != nil {
		return err
	}

	theme := connectbackend.UpdateWorkspaceThemeInput{Theme: json.RawMessage("null")}
	theme.InstanceID, theme.WorkspaceID = meta.InstanceID, meta.WorkspaceID

	if t, ok := c.wire["Theme"]; ok {
		raw, err := json.Marshal(t)
		if err != nil {
			return err
		}

		theme.Theme = raw
	}

	if _, err := c.be.UpdateWorkspaceTheme(c.ctx, &theme); err != nil {
		return err
	}

	if v := c.str("Visibility"); v != "" {
		vis := connectbackend.UpdateWorkspaceVisibilityInput{Visibility: v}
		vis.InstanceID, vis.WorkspaceID = meta.InstanceID, meta.WorkspaceID

		if _, err := c.be.UpdateWorkspaceVisibility(c.ctx, &vis); err != nil {
			return err
		}
	}

	return c.syncTags(physicalID)
}

func deleteConnectWorkspace(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteWorkspace(inst, id)

	return err
}

// ---- EvaluationForm ----

func (c *connectCall) formRequest() (*connectbackend.FormRequest, error) {
	var in connectbackend.FormRequest
	if err := c.decode(&in, connectStatusProp); err != nil {
		return nil, err
	}

	in.AsDraft = c.str(connectStatusProp) != connectStateActive

	return &in, nil
}

// createConnectEvaluationForm creates a form whose first version is active unless Status is DRAFT.
func createConnectEvaluationForm(c *connectCall) (string, error) {
	in, err := c.formRequest()
	if err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateEvaluationForm(c.ctx, in)
	if err != nil {
		return "", err
	}

	return out.Arn, nil
}

func updateConnectEvaluationForm(c *connectCall, physicalID string) error {
	in, err := c.formRequest()
	if err != nil {
		return err
	}

	in.InstanceID, in.FormID = splitConnectARN(physicalID)
	in.CreateNewVersion = new(true)

	versions, _, err := c.be.ListEvaluationFormVersions(
		&connectbackend.FormRequest{FormID: in.FormID, MaxResults: 1, InstanceID: in.InstanceID},
	)
	if err != nil {
		return err
	}

	if len(versions) > 0 {
		in.Version = versions[0].EvaluationFormVersion
	}

	if _, err = c.be.UpdateEvaluationForm(c.ctx, in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

// deleteConnectEvaluationForm deactivates the active version, which blocks deletion, then deletes the form.
func deleteConnectEvaluationForm(c *connectCall, physicalID string) error {
	req := connectbackend.FormRequest{}
	req.InstanceID, req.FormID = splitConnectARN(physicalID)

	versions, _, err := c.be.ListEvaluationFormVersions(&req)
	if err != nil {
		return err
	}

	for _, v := range versions {
		if v.Status != connectStateActive {
			continue
		}

		off := connectbackend.FormRequest{Version: v.EvaluationFormVersion}
		off.InstanceID, off.FormID = req.InstanceID, req.FormID

		if _, err = c.be.DeactivateEvaluationForm(c.ctx, &off); err != nil {
			return err
		}
	}

	_, err = c.be.DeleteEvaluationForm(&req)

	return err
}

// ---- DataTable ----

func (c *connectCall) stashDataTable(tableARN string) {
	inst, id := splitConnectARN(tableARN)

	t, err := c.be.DescribeDataTable(inst, id)
	if err != nil {
		return
	}

	attrs := map[string]string{
		"CreatedTime":        connectTime(&t.CreatedTime),
		connectLastModified:  connectTime(t.LastModifiedTime),
		connectLastModRegion: t.LastModifiedRegion,
	}
	if t.LockVersion != nil {
		attrs["LockVersion.DataTable"] = t.LockVersion.DataTable
	}

	c.stash(attrs)
}

func createConnectDataTable(c *connectCall) (string, error) {
	var in connectbackend.CreateDataTableInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateDataTable(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashDataTable(out.Arn)

	return out.Arn, nil
}

func updateConnectDataTable(c *connectCall, physicalID string) error {
	var in connectbackend.UpdateDataTableMetadataInput
	if err := c.decode(&in, connectTagsProp, connectStatusProp); err != nil {
		return err
	}

	in.InstanceID, in.TableID = splitConnectARN(physicalID)
	in.Description = new(c.str("Description"))

	if _, err := c.be.UpdateDataTableMetadata(c.ctx, &in); err != nil {
		return err
	}

	c.stashDataTable(physicalID)

	return c.syncTags(physicalID)
}

func deleteConnectDataTable(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteDataTable(inst, id)

	return err
}

// ---- DataTableAttribute ----

// createConnectDataTableAttribute adds an attribute. Ref is "<instance ID>|<data table ID>|<attribute name>".
func createConnectDataTableAttribute(c *connectCall) (string, error) {
	var in connectbackend.CreateDataTableAttributeInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = lastPathSegment(c.instance())
	in.TableID = lastPathSegment(c.str("DataTableArn"))

	out, err := c.be.CreateDataTableAttribute(c.ctx, &in)
	if err != nil {
		return "", err
	}

	id := connectCompositeID(in.InstanceID, in.TableID, out.Name)
	c.stashDataTableAttribute(id)

	return id, nil
}

func (c *connectCall) stashDataTableAttribute(physicalID string) {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return
	}

	a, err := c.be.DescribeDataTableAttribute(parts[0], parts[1], parts[2])
	if err != nil {
		return
	}

	attrs := map[string]string{
		"AttributeId":        a.AttributeID,
		connectLastModified:  connectTime(a.LastModifiedTime),
		connectLastModRegion: a.LastModifiedRegion,
	}
	if a.LockVersion != nil {
		attrs["LockVersion.Attribute"] = a.LockVersion.Attribute
		attrs["LockVersion.DataTable"] = a.LockVersion.DataTable
	}

	c.stash(attrs)
}

func updateConnectDataTableAttribute(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return nil
	}

	var in connectbackend.UpdateDataTableAttributeInput
	if err := c.decode(&in); err != nil {
		return err
	}

	in.InstanceID, in.TableID, in.AttributeName = parts[0], parts[1], parts[2]
	in.Name = parts[2]

	if _, err := c.be.UpdateDataTableAttribute(c.ctx, &in); err != nil {
		return err
	}

	c.stashDataTableAttribute(physicalID)

	return nil
}

func deleteConnectDataTableAttribute(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return nil
	}

	_, err := c.be.DeleteDataTableAttribute(c.ctx, parts[0], parts[1], parts[2])

	return err
}
