package connect

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxViewNameLen = 512
	maxViewActions = 50
	viewManaged    = "AWS_MANAGED"
	viewCustom     = "CUSTOMER_MANAGED"
)

var viewRefPattern = regexp.MustCompile(`\$\.([A-Za-z_][A-Za-z0-9_]*)`)

// ViewContent is the stored content of a view.
type ViewContent struct {
	InputSchema string   `json:"InputSchema,omitempty"`
	Template    string   `json:"Template,omitempty"`
	Actions     []string `json:"Actions"`
}

// View is a Connect view.
type View struct {
	CreatedTime        awstime.EpochTime `json:"CreatedTime"`
	LastModifiedTime   awstime.EpochTime `json:"LastModifiedTime"`
	Content            *ViewContent      `json:"Content,omitempty"`
	Tags               map[string]string `json:"Tags,omitempty"`
	Arn                string            `json:"Arn"`
	Description        string            `json:"Description,omitempty"`
	ID                 string            `json:"Id"`
	Name               string            `json:"Name"`
	Status             string            `json:"Status"`
	Type               string            `json:"Type"`
	VersionDescription string            `json:"VersionDescription,omitempty"`
	ViewContentSha256  string            `json:"ViewContentSha256,omitempty"`
	Version            int32             `json:"Version"`
}

// ViewSummary is the list shape of a View.
type ViewSummary struct {
	Arn         string `json:"Arn"`
	Description string `json:"Description,omitempty"`
	ID          string `json:"Id"`
	Name        string `json:"Name"`
	Status      string `json:"Status"`
	Type        string `json:"Type"`
}

// ViewVersionSummary is the list shape of a published View version.
type ViewVersionSummary struct {
	Arn                string `json:"Arn"`
	Description        string `json:"Description,omitempty"`
	ID                 string `json:"Id"`
	Name               string `json:"Name"`
	Type               string `json:"Type"`
	VersionDescription string `json:"VersionDescription,omitempty"`
	Version            int32  `json:"Version"`
}

type viewRec struct {
	InstanceID string `json:"instanceId"`
	View       View   `json:"view"`
}

type viewVersionRec struct {
	InstanceID string `json:"instanceId"`
	View       View   `json:"view"`
}

// ViewInputContent is the content supplied by a caller.
type ViewInputContent struct {
	Template string   `json:"Template"`
	Actions  []string `json:"Actions"`
}

// CreateViewInput is the CreateView request.
type CreateViewInput struct {
	Tags        map[string]string
	Content     *ViewInputContent
	Name        string
	Status      string
	Description string
	ClientToken string
	instPath
}

// ViewRequest carries the update, version and list requests of views.
type ViewRequest struct {
	Content            *ViewInputContent
	Name               *string
	Description        *string
	Status             string
	VersionDescription string
	ViewContentSha256  string
	ViewID             string `json:"-" path:"ViewId"`
	PathVersion        string `json:"-" path:"ViewVersion"`
	Type               string `json:"-"                    query:"type"`
	NextToken          string `json:"-"                    query:"nextToken"`
	instPath
	MaxResults int32 `json:"-"                    query:"maxResults"`
}

func checkViewContent(c *ViewInputContent) (*ViewContent, error) {
	if c == nil || strings.TrimSpace(c.Template) == "" {
		return nil, invalidParamErr("Content.Template is required")
	}

	if !json.Valid([]byte(c.Template)) {
		return nil, invalidParamErr("Content.Template must be valid JSON")
	}

	if len(c.Actions) > maxViewActions {
		return nil, invalidParamErr("Content.Actions must contain at most %d entries", maxViewActions)
	}

	return &ViewContent{
		Template:    c.Template,
		Actions:     orEmpty(slices.Clone(c.Actions)),
		InputSchema: viewSchema(c.Template),
	}, nil
}

// viewSchema derives a JSON schema from the $.Name references of a template.
func viewSchema(template string) string {
	props := map[string]any{}
	for _, m := range viewRefPattern.FindAllStringSubmatch(template, -1) {
		props[m[1]] = map[string]any{}
	}

	raw, _ := json.Marshal(map[string]any{
		"$schema": "http://json-schema.org/draft-07/schema#", fieldType: "object", "properties": props,
	})

	return string(raw)
}

func (b *Backend) viewVersions(inst, id string) []*viewVersionRec {
	var out []*viewVersionRec

	for _, v := range b.viewSnapshots.in(inst) {
		if v.View.ID == id {
			out = append(out, v)
		}
	}

	slices.SortFunc(out, func(a, c *viewVersionRec) int { return int(a.View.Version - c.View.Version) })

	return out
}

func snapshotKey(id string, v int32) string { return id + ":" + strconv.Itoa(int(v)) }

func (b *Backend) putViewSnapshot(inst string, v *View) {
	snap := *v
	snap.Tags = nil
	b.viewSnapshots.put(&viewVersionRec{InstanceID: inst, View: snap})
}

// splitViewRef resolves a ViewId path label that may be an ARN and/or carry a ":version" suffix.
func splitViewRef(ref string) (string, *int32) {
	if _, after, ok := strings.CutLast(ref, "/"); ok {
		ref = after
	}

	id, ver, ok := strings.Cut(ref, ":")
	if !ok {
		return id, nil
	}

	n, err := strconv.ParseInt(ver, 10, 32)
	if err != nil {
		return id, nil
	}

	v := int32(n)

	return id, &v
}

func (b *Backend) viewByRef(inst, ref string) (*viewRec, *int32, error) {
	id, ver := splitViewRef(ref)

	v, err := b.views.get(inst, id)
	if err != nil {
		return nil, nil, err
	}

	return v, ver, nil
}

// CreateView creates a view.
func (b *Backend) CreateView(ctx context.Context, in *CreateViewInput) (*View, error) {
	if err := firstError(
		checkName("Name", in.Name, maxViewNameLen), checkDescription(in.Description), checkTags(in.Tags),
		oneOf("Status", in.Status, flowPublished, flowSaved),
	); err != nil {
		return nil, err
	}

	content, err := checkViewContent(in.Content)
	if err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateView", in.InstanceID, func(inst string) (*View, error) {
		if nameExists(b.views, inst, func(v *viewRec) string { return v.View.Name }, in.Name, "") {
			return nil, duplicateErr("View", in.Name)
		}

		id := newID()
		now := awstime.NewEpochTime(b.now())
		v := View{
			CreatedTime: now, LastModifiedTime: now, Content: content, Tags: cloneTags(in.Tags),
			Arn: b.buildARN(ctx, "instance/"+inst+"/view/"+id), Description: in.Description, ID: id, Name: in.Name,
			Status: in.Status, Type: viewCustom, ViewContentSha256: sha256Hex(content.Template),
		}

		if in.Status == flowPublished {
			v.Version = 1
			b.putViewSnapshot(inst, &v)
		}

		b.views.put(&viewRec{InstanceID: inst, View: v})

		return &v, nil
	})
}

// DescribeView returns a view, or one published version when the reference carries a version.
func (b *Backend) DescribeView(inst, ref string) (*View, error) {
	return readInstance(b, "DescribeView", inst, func(inst string) (*View, error) {
		v, ver, err := b.viewByRef(inst, ref)
		if err != nil {
			return nil, err
		}

		out := v.View
		if ver != nil {
			snap, serr := b.viewSnapshots.get(inst, snapshotKey(v.View.ID, *ver))
			if serr != nil {
				return nil, notFoundErr("View version", snapshotKey(v.View.ID, *ver))
			}

			out = snap.View
		}

		out.Tags = cloneTags(v.View.Tags)

		return &out, nil
	})
}

// ListViews pages through views of one type.
func (b *Backend) ListViews(in *ViewRequest) ([]ViewSummary, string, error) {
	if in.Type != "" {
		if err := oneOf("type", in.Type, viewCustom, viewManaged); err != nil {
			return nil, "", err
		}
	}

	return listIn(b, "ListViews", in.InstanceID, b.views, func(v *viewRec) (ViewSummary, bool) {
		return ViewSummary{
			Arn: v.View.Arn, Description: v.View.Description, ID: v.View.ID, Name: v.View.Name,
			Status: v.View.Status, Type: v.View.Type,
		}, in.Type == "" || v.View.Type == in.Type
	}, in.NextToken, in.MaxResults)
}

// ListViewVersions pages through the published versions of a view.
func (b *Backend) ListViewVersions(in *ViewRequest) ([]ViewVersionSummary, string, error) {
	all, err := readInstance(b, "ListViewVersions", in.InstanceID, func(inst string) ([]ViewVersionSummary, error) {
		v, _, verr := b.viewByRef(inst, in.ViewID)
		if verr != nil {
			return nil, verr
		}

		var out []ViewVersionSummary

		for _, s := range b.viewVersions(inst, v.View.ID) {
			out = append(out, ViewVersionSummary{
				Arn: s.View.Arn + ":" + strconv.Itoa(int(s.View.Version)), Description: s.View.Description,
				ID: s.View.ID, Name: s.View.Name, Type: s.View.Type, VersionDescription: s.View.VersionDescription,
				Version: s.View.Version,
			})
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// publishView records the current view content as the next published version.
func (b *Backend) publishView(inst string, v *viewRec, desc string) {
	next := int32(1)
	if vs := b.viewVersions(inst, v.View.ID); len(vs) > 0 {
		next = vs[len(vs)-1].View.Version + 1
	}

	v.View.Version, v.View.VersionDescription, v.View.Status = next, desc, flowPublished
	b.putViewSnapshot(inst, &v.View)
}

// CreateViewVersion publishes the current content of a view as a new version.
func (b *Backend) CreateViewVersion(in *ViewRequest) (*View, error) {
	if err := checkDescription(in.VersionDescription); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateViewVersion", in.InstanceID, func(inst string) (*View, error) {
		v, _, err := b.viewByRef(inst, in.ViewID)
		if err != nil {
			return nil, err
		}

		if v.View.Type == viewManaged {
			return nil, invalidRequestErr("AWS managed views cannot be versioned")
		}

		if in.ViewContentSha256 != "" && in.ViewContentSha256 != v.View.ViewContentSha256 {
			return nil, invalidRequestErr("the content checksum does not match the latest content")
		}

		b.publishView(inst, v, in.VersionDescription)

		out := v.View
		out.Tags = cloneTags(out.Tags)

		return &out, nil
	})
}

// UpdateViewContent replaces the content of a view; PUBLISHED also publishes a new version.
func (b *Backend) UpdateViewContent(in *ViewRequest) (*View, error) {
	if err := oneOf("Status", in.Status, flowPublished, flowSaved); err != nil {
		return nil, err
	}

	content, err := checkViewContent(in.Content)
	if err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateViewContent", in.InstanceID, func(inst string) (*View, error) {
		v, _, verr := b.viewByRef(inst, in.ViewID)
		if verr != nil {
			return nil, verr
		}

		if v.View.Type == viewManaged {
			return nil, invalidRequestErr("AWS managed views cannot be changed")
		}

		v.View.Content, v.View.ViewContentSha256, v.View.Status = content, sha256Hex(content.Template), in.Status
		v.View.LastModifiedTime = awstime.NewEpochTime(b.now())

		if in.Status == flowPublished {
			b.publishView(inst, v, "")
		}

		out := v.View
		out.Tags = cloneTags(out.Tags)

		return &out, nil
	})
}

// UpdateViewMetadata renames a view or changes its description.
func (b *Backend) UpdateViewMetadata(in *ViewRequest) (*empty, error) {
	if in.Name == nil && in.Description == nil {
		return nil, invalidParamErr("Name or Description is required")
	}

	return writeInstance(b, "UpdateViewMetadata", in.InstanceID, func(inst string) (*empty, error) {
		v, _, err := b.viewByRef(inst, in.ViewID)
		if err != nil {
			return nil, err
		}

		if v.View.Type == viewManaged {
			return nil, invalidRequestErr("AWS managed views cannot be changed")
		}

		err = applyRename(b.views, v, in.Name, in.Description,
			func(x *viewRec) *string { return &x.View.Name }, func(x *viewRec) *string { return &x.View.Description })
		if err != nil {
			return nil, err
		}

		v.View.LastModifiedTime = awstime.NewEpochTime(b.now())

		return &empty{}, nil
	})
}

// DeleteView removes a view and all its versions.
func (b *Backend) DeleteView(inst, ref string) (*empty, error) {
	return writeInstance(b, "DeleteView", inst, func(inst string) (*empty, error) {
		v, _, err := b.viewByRef(inst, ref)
		if err != nil {
			return nil, err
		}

		if v.View.Type == viewManaged {
			return nil, invalidRequestErr("AWS managed views cannot be deleted")
		}

		for _, s := range b.viewVersions(inst, v.View.ID) {
			b.viewSnapshots.table.Delete(inst + "/" + snapshotKey(v.View.ID, s.View.Version))
		}

		return nil, b.views.del(inst, v.View.ID)
	})
}

// DeleteViewVersion removes one published version of a view.
func (b *Backend) DeleteViewVersion(in *ViewRequest) (*empty, error) {
	n, err := strconv.ParseInt(in.PathVersion, 10, 32)
	if err != nil {
		return nil, invalidParamErr("ViewVersion must be an integer")
	}

	version := int32(n)

	return writeInstance(b, "DeleteViewVersion", in.InstanceID, func(inst string) (*empty, error) {
		v, _, verr := b.viewByRef(inst, in.ViewID)
		if verr != nil {
			return nil, verr
		}

		key := snapshotKey(v.View.ID, version)
		if !b.viewSnapshots.has(inst, key) {
			return nil, notFoundErr("View version", key)
		}

		return nil, b.viewSnapshots.del(inst, key)
	})
}
