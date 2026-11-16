package cloudformation

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// ---- Workspace ----

func createConnectWorkspace(c *connectCall) (string, error) {
	extras, err := c.workspaceExtras()
	if err != nil {
		return "", err
	}

	var in connectbackend.CreateWorkspaceInput
	if err = c.decode(&in, "Visibility", "Pages", "Media", "Associations"); err != nil {
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
			c.rollbackWorkspace(out.WorkspaceArn)

			return "", err
		}
	}

	if err = c.syncWorkspaceExtras(out.WorkspaceArn, extras); err != nil {
		c.rollbackWorkspace(out.WorkspaceArn)

		return "", err
	}

	return out.WorkspaceArn, nil
}

func (c *connectCall) rollbackWorkspace(workspaceARN string) {
	inst, id := splitConnectARN(workspaceARN)
	_, _ = c.be.DeleteWorkspace(inst, id)
}

func updateConnectWorkspace(c *connectCall, physicalID string) error {
	extras, err := c.workspaceExtras()
	if err != nil {
		return err
	}

	var meta connectbackend.UpdateWorkspaceMetadataInput
	if err = c.decode(&meta, connectTagsProp); err != nil {
		return err
	}

	meta.InstanceID, meta.WorkspaceID = splitConnectARN(physicalID)
	meta.Description = new(c.str("Description"))
	meta.Title = new(c.str("Title"))

	if _, err = c.be.UpdateWorkspaceMetadata(c.ctx, &meta); err != nil {
		return err
	}

	theme := connectbackend.UpdateWorkspaceThemeInput{Theme: jsontext.Value("null")}
	theme.InstanceID, theme.WorkspaceID = meta.InstanceID, meta.WorkspaceID

	if t, ok := c.wire["Theme"]; ok {
		raw, merr := json.Marshal(t, jsonx.V1())
		if merr != nil {
			return merr
		}

		theme.Theme = raw
	}

	if _, err = c.be.UpdateWorkspaceTheme(c.ctx, &theme); err != nil {
		return err
	}

	if v := c.str("Visibility"); v != "" {
		vis := connectbackend.UpdateWorkspaceVisibilityInput{Visibility: v}
		vis.InstanceID, vis.WorkspaceID = meta.InstanceID, meta.WorkspaceID

		if _, err = c.be.UpdateWorkspaceVisibility(c.ctx, &vis); err != nil {
			return err
		}
	}

	if err = c.syncWorkspaceExtras(physicalID, extras); err != nil {
		return err
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
		connectCreatedTime:   connectTime(&t.CreatedTime),
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

// connectWorkspaceExtras are the Pages, Media and Associations properties of AWS::Connect::Workspace.
type connectWorkspaceExtras struct {
	pages []connectbackend.WorkspacePage
	media []connectbackend.MediaItem
	assoc []string
}

func connectList[T any](c *connectCall, key string) ([]T, error) {
	raw, ok := c.wire[key]
	if !ok || raw == nil {
		return nil, nil
	}

	doc, err := json.Marshal(raw, jsonx.V1())
	if err != nil {
		return nil, err
	}

	var out []T
	if err = json.Unmarshal(doc, &out, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: property %s: %w", errUnsupportedCFNValue, key, err)
	}

	return out, nil
}

func (c *connectCall) workspaceExtras() (*connectWorkspaceExtras, error) {
	pages, err := connectList[connectbackend.WorkspacePage](c, "Pages")
	if err != nil {
		return nil, err
	}

	media, err := connectList[connectbackend.MediaItem](c, "Media")
	if err != nil {
		return nil, err
	}

	assoc, err := connectList[string](c, "Associations")
	if err != nil {
		return nil, err
	}

	return &connectWorkspaceExtras{pages: pages, media: media, assoc: assoc}, nil
}

// syncWorkspaceExtras makes the workspace's pages, media and assignments equal the template's.
func (c *connectCall) syncWorkspaceExtras(workspaceARN string, ex *connectWorkspaceExtras) error {
	inst, id := splitConnectARN(workspaceARN)

	return errors.Join(
		c.syncWorkspacePages(inst, id, ex.pages),
		c.syncWorkspaceMedia(inst, id, ex.media),
		c.syncWorkspaceAssociations(inst, id, ex.assoc),
	)
}

func (c *connectCall) syncWorkspacePages(inst, id string, want []connectbackend.WorkspacePage) error {
	var have []connectbackend.WorkspacePage

	list := connectbackend.ListWorkspacePagesInput{}
	list.InstanceID, list.WorkspaceID = inst, id

	for {
		page, next, err := c.be.ListWorkspacePages(&list)
		if err != nil {
			return err
		}

		have = append(have, page...)

		if next == "" {
			break
		}

		list.NextToken = next
	}

	wanted := func(p connectbackend.WorkspacePage) bool {
		return slices.ContainsFunc(
			want,
			func(w connectbackend.WorkspacePage) bool { return strs.Equal(w.Page, p.Page) },
		)
	}

	for _, p := range have {
		if wanted(p) {
			continue
		}

		del := connectbackend.DeleteWorkspacePageInput{Page: p.Page}
		del.InstanceID, del.WorkspaceID = inst, id

		if _, err := c.be.DeleteWorkspacePage(c.ctx, &del); err != nil {
			return err
		}
	}

	for _, w := range want {
		exists := slices.ContainsFunc(
			have,
			func(p connectbackend.WorkspacePage) bool { return strs.Equal(w.Page, p.Page) },
		)
		if err := c.putWorkspacePage(inst, id, w, exists); err != nil {
			return err
		}
	}

	return nil
}

// putWorkspacePage replaces an existing page of the same name, so cleared InputData and Slug are cleared.
func (c *connectCall) putWorkspacePage(inst, id string, w connectbackend.WorkspacePage, exists bool) error {
	if exists {
		del := connectbackend.DeleteWorkspacePageInput{Page: w.Page}
		del.InstanceID, del.WorkspaceID = inst, id

		if _, err := c.be.DeleteWorkspacePage(c.ctx, &del); err != nil {
			return err
		}
	}

	in := connectbackend.CreateWorkspacePageInput{Page: w.Page}
	in.InstanceID, in.WorkspaceID = inst, id
	in.ResourceArn = &w.ResourceArn

	if w.InputData != "" {
		in.InputData = &w.InputData
	}

	if w.Slug != "" {
		in.Slug = &w.Slug
	}

	_, err := c.be.CreateWorkspacePage(c.ctx, &in)

	return err
}

func (c *connectCall) syncWorkspaceMedia(inst, id string, want []connectbackend.MediaItem) error {
	have, err := c.be.ListWorkspaceMedia(inst, id)
	if err != nil {
		return err
	}

	for _, m := range have {
		if slices.ContainsFunc(want, func(w connectbackend.MediaItem) bool { return w.Type == m.Type }) {
			continue
		}

		del := connectbackend.WorkspaceMediaInput{MediaQuery: m.Type}
		del.InstanceID, del.WorkspaceID = inst, id

		if _, err = c.be.DeleteWorkspaceMedia(c.ctx, &del); err != nil {
			return err
		}
	}

	for _, m := range want {
		in := connectbackend.WorkspaceMediaInput{MediaSource: m.Source, MediaType: m.Type}
		in.InstanceID, in.WorkspaceID = inst, id

		if _, err = c.be.ImportWorkspaceMedia(c.ctx, &in); err != nil {
			return err
		}
	}

	return nil
}

func (c *connectCall) syncWorkspaceAssociations(inst, id string, want []string) error {
	have, err := c.be.WorkspaceAssociations(inst, id)
	if err != nil {
		return err
	}

	remove := slices.DeleteFunc(slices.Clone(have), func(a string) bool { return slices.Contains(want, a) })
	add := slices.DeleteFunc(slices.Clone(want), func(a string) bool { return slices.Contains(have, a) })

	return errors.Join(
		c.workspaceLinks(inst, id, remove, c.be.DisassociateWorkspace),
		c.workspaceLinks(inst, id, add, c.be.AssociateWorkspace),
	)
}

type workspaceLinkFunc func(
	context.Context, *connectbackend.WorkspaceAssociationInput,
) (*connectbackend.BatchAssociationResult, error)

func (c *connectCall) workspaceLinks(inst, id string, arns []string, link workspaceLinkFunc) error {
	const batch = 25

	for chunk := range slices.Chunk(arns, batch) {
		in := connectbackend.WorkspaceAssociationInput{ResourceArns: chunk}
		in.InstanceID, in.WorkspaceID = inst, id

		res, err := link(c.ctx, &in)
		if err != nil {
			return err
		}

		if len(res.FailedList) > 0 {
			f := res.FailedList[0]

			return fmt.Errorf("%w: association %s: %s", errUnsupportedCFNValue, f.ResourceArn, f.ErrorMessage)
		}
	}

	return nil
}
