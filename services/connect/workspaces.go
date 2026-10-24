package connect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	labelWorkspace     = "Workspace"
	labelWorkspacePage = "WorkspacePage"
	labelWorkspaceLink = "WorkspaceAssociation"
	visibilityAll      = "ALL"
	visibilityAssigned = "ASSIGNED"
	visibilityNone     = "NONE"
	resourceUser       = "USER"
	resourceRouting    = "ROUTING_PROFILE"
	arnTypeUser        = "agent"
	arnTypeRouting     = "routing-profile"
	wsFieldTitle       = "title"
	wsFieldVisibility  = "visibility"
	wsFieldWorkspaceID = "workspaceid"
	wsFieldResourceID  = "resourceid"
	wsFieldResType     = "resourcetype"
	wsFieldResName     = "resourcename"
	maxWorkspaceSlug   = 128
	maxAssocBatch      = 25
)

func visibilities() []string { return []string{visibilityAll, visibilityAssigned, visibilityNone} }

func mediaTypes() []string {
	return []string{
		"IMAGE_LOGO_LIGHT_FAVICON", "IMAGE_LOGO_DARK_FAVICON",
		"IMAGE_LOGO_LIGHT_HORIZONTAL", "IMAGE_LOGO_DARK_HORIZONTAL",
	}
}

// WorkspacePage maps a page of a workspace to a view.
type WorkspacePage struct {
	InputData   string `json:"InputData,omitempty"`
	Page        string `json:"Page"`
	ResourceArn string `json:"ResourceArn"`
	Slug        string `json:"Slug,omitempty"`
}

// MediaItem is a media asset of a workspace.
type MediaItem struct {
	Source string `json:"Source"`
	Type   string `json:"Type"`
}

// Workspace is a Connect workspace.
type Workspace struct {
	Tags map[string]string `json:"Tags,omitempty"`
	Stamp
	Arn         string          `json:"Arn"`
	ID          string          `json:"Id"`
	Name        string          `json:"Name"`
	Description string          `json:"Description,omitempty"`
	Title       string          `json:"Title,omitempty"`
	Visibility  string          `json:"Visibility"`
	Theme       json.RawMessage `json:"Theme,omitempty"`
}

// WorkspaceSummary is the list shape of a workspace.
type WorkspaceSummary struct {
	Stamp
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// WorkspaceSearchSummary is the SearchWorkspaces result shape.
type WorkspaceSearchSummary struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Arn         string            `json:"Arn"`
	CreatedAt   awstime.EpochTime `json:"CreatedAt"`
	Description string            `json:"Description,omitempty"`
	ID          string            `json:"Id"`
	Name        string            `json:"Name"`
	Title       string            `json:"Title,omitempty"`
	Visibility  string            `json:"Visibility"`
}

// WorkspaceAssociationSummary is the SearchWorkspaceAssociations result shape.
type WorkspaceAssociationSummary struct {
	ResourceArn  string `json:"ResourceArn"`
	ResourceID   string `json:"ResourceId"`
	ResourceName string `json:"ResourceName"`
	ResourceType string `json:"ResourceType"`
	WorkspaceArn string `json:"WorkspaceArn"`
	WorkspaceID  string `json:"WorkspaceId"`
}

type workspaceRec struct {
	Created    awstime.EpochTime `json:"created"`
	InstanceID string            `json:"instanceId"`
	Workspace  Workspace         `json:"workspace"`
	Pages      []WorkspacePage   `json:"pages,omitempty"`
	Media      []MediaItem       `json:"media,omitempty"`
	Resources  []string          `json:"resources,omitempty"`
}

type wsRef struct {
	WorkspaceID string `json:"-" path:"WorkspaceId"`
	instPath
}

func checkTheme(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}

	var modes map[string]json.RawMessage
	if err := json.Unmarshal(raw, &modes); err != nil {
		return invalidParamErr("Theme must be an object")
	}

	for k, v := range modes {
		var cfg map[string]json.RawMessage
		if k != "Light" && k != "Dark" || json.Unmarshal(v, &cfg) != nil {
			return invalidParamErr("Theme may only hold Light and Dark theme configurations")
		}
	}

	return nil
}

func (r *workspaceRec) view() Workspace {
	out := r.Workspace
	out.Tags = cloneTags(out.Tags)
	out.Theme = slices.Clone(out.Theme)

	return out
}

func (b *Backend) workspaceByID(inst, id string) (*workspaceRec, error) {
	return b.workspaces.get(inst, id)
}

// CreateWorkspaceInput is the CreateWorkspace request.
type CreateWorkspaceInput struct {
	Tags        map[string]string
	Name        string
	Description string
	Title       string
	instPath
	Theme json.RawMessage
}

type createWorkspaceOut struct {
	WorkspaceArn string `json:"WorkspaceArn"`
	WorkspaceID  string `json:"WorkspaceId"`
}

// CreateWorkspace creates a workspace; new workspaces are visible to assigned users and routing profiles only.
func (b *Backend) CreateWorkspace(ctx context.Context, in *CreateWorkspaceInput) (*createWorkspaceOut, error) {
	if err := firstError(
		checkName("Name", in.Name, maxNameLen), checkDescription(in.Description), checkTags(in.Tags),
		checkOptionalName("Title", in.Title), checkTheme(in.Theme),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateWorkspace", in.InstanceID, func(inst string) (*createWorkspaceOut, error) {
		if nameExists(b.workspaces, inst, func(r *workspaceRec) string { return r.Workspace.Name }, in.Name, "") {
			return nil, duplicateErr(labelWorkspace, in.Name)
		}

		id := newID()
		rec := &workspaceRec{InstanceID: inst, Created: awstime.NewEpochTime(b.now()), Workspace: Workspace{
			Stamp: b.stamp(ctx), Tags: cloneTags(in.Tags), Theme: slices.Clone(in.Theme),
			Arn: b.buildARN(ctx, "instance/"+inst+"/workspace/"+id), ID: id, Name: in.Name,
			Description: in.Description, Title: in.Title, Visibility: visibilityAssigned,
		}}
		b.workspaces.put(rec)

		return &createWorkspaceOut{WorkspaceArn: rec.Workspace.Arn, WorkspaceID: id}, nil
	})
}

// DescribeWorkspace returns a workspace.
func (b *Backend) DescribeWorkspace(inst, id string) (*Workspace, error) {
	return readInstance(b, "DescribeWorkspace", inst, func(inst string) (*Workspace, error) {
		r, err := b.workspaceByID(inst, id)
		if err != nil {
			return nil, err
		}

		out := r.view()

		return &out, nil
	})
}

// ListWorkspaces pages through the workspaces of an instance.
func (b *Backend) ListWorkspaces(inst, token string, maxResults int32) ([]WorkspaceSummary, string, error) {
	return listIn(b, "ListWorkspaces", inst, b.workspaces, func(r *workspaceRec) (WorkspaceSummary, bool) {
		w := r.Workspace

		return WorkspaceSummary{Stamp: w.Stamp, Arn: w.Arn, ID: w.ID, Name: w.Name}, true
	}, token, maxResults)
}

func (b *Backend) updateWorkspace(
	ctx context.Context, op, instRef, id string, apply func(inst string, r *workspaceRec) error,
) (*empty, error) {
	return writeInstance(b, op, instRef, func(inst string) (*empty, error) {
		r, err := b.workspaceByID(inst, id)
		if err != nil {
			return nil, err
		}

		if err = apply(inst, r); err != nil {
			return nil, err
		}

		r.Workspace.Stamp = b.stamp(ctx)

		return &empty{}, nil
	})
}

// UpdateWorkspaceMetadataInput is the UpdateWorkspaceMetadata request.
type UpdateWorkspaceMetadataInput struct {
	Name        *string
	Description *string
	Title       *string
	wsRef
}

// UpdateWorkspaceMetadata changes the name, description or title of a workspace.
func (b *Backend) UpdateWorkspaceMetadata(ctx context.Context, in *UpdateWorkspaceMetadataInput) (*empty, error) {
	return b.updateWorkspace(ctx, "UpdateWorkspaceMetadata", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			w := &r.Workspace
			if err := applyRename(b.workspaces, r, in.Name, in.Description,
				func(x *workspaceRec) *string { return &x.Workspace.Name },
				func(x *workspaceRec) *string { return &x.Workspace.Description }); err != nil {
				return err
			}

			if in.Title != nil {
				if err := checkOptionalName("Title", *in.Title); err != nil {
					return err
				}

				w.Title = *in.Title
			}

			return nil
		})
}

// UpdateWorkspaceThemeInput is the UpdateWorkspaceTheme request.
type UpdateWorkspaceThemeInput struct {
	wsRef
	Theme json.RawMessage
}

// UpdateWorkspaceTheme replaces the theme of a workspace.
func (b *Backend) UpdateWorkspaceTheme(ctx context.Context, in *UpdateWorkspaceThemeInput) (*empty, error) {
	if err := checkTheme(in.Theme); err != nil {
		return nil, err
	}

	return b.updateWorkspace(ctx, "UpdateWorkspaceTheme", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			r.Workspace.Theme = slices.Clone(in.Theme)

			return nil
		})
}

// UpdateWorkspaceVisibilityInput is the UpdateWorkspaceVisibility request.
type UpdateWorkspaceVisibilityInput struct {
	Visibility string
	wsRef
}

// UpdateWorkspaceVisibility sets who can see a workspace.
func (b *Backend) UpdateWorkspaceVisibility(ctx context.Context, in *UpdateWorkspaceVisibilityInput) (*empty, error) {
	if err := oneOf("Visibility", in.Visibility, visibilities()...); err != nil {
		return nil, err
	}

	return b.updateWorkspace(ctx, "UpdateWorkspaceVisibility", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			r.Workspace.Visibility = in.Visibility

			return nil
		})
}

// DeleteWorkspace removes a workspace with its pages, media and assignments.
func (b *Backend) DeleteWorkspace(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteWorkspace", inst, func(inst string) (*empty, error) {
		return nil, b.workspaces.del(inst, id)
	})
}

// SearchWorkspaces searches workspaces by name, description, title, visibility and tags.
func (b *Backend) SearchWorkspaces(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName, fieldDescription, wsFieldTitle, wsFieldVisibility}}

	return runSearch(b, "SearchWorkspaces", "Workspaces", in, b.workspaces, spec,
		func(r *workspaceRec) searchSubject {
			w := r.Workspace
			f := nameDesc(w.Name, w.Description)
			f[wsFieldTitle], f[wsFieldVisibility] = w.Title, w.Visibility

			return searchSubject{fields: f, tags: w.Tags}
		},
		func(r *workspaceRec) WorkspaceSearchSummary {
			w := r.Workspace

			return WorkspaceSearchSummary{
				Tags: cloneTags(w.Tags), Arn: w.Arn, CreatedAt: r.Created, Description: w.Description, ID: w.ID,
				Name: w.Name, Title: w.Title, Visibility: w.Visibility,
			}
		})
}

type pageFields struct {
	ResourceArn *string
	InputData   *string
	Slug        *string
}

// CreateWorkspacePageInput is the CreateWorkspacePage request.
type CreateWorkspacePageInput struct {
	Page string
	pageFields
	wsRef
}

// UpdateWorkspacePageInput is the UpdateWorkspacePage request.
type UpdateWorkspacePageInput struct {
	Page    string `json:"-" path:"Page"`
	NewPage *string
	pageFields
	wsRef
}

func checkPageName(page string) error {
	if err := checkName("Page", page, maxNameLen); err != nil {
		return err
	}

	switch l := strs.Fold(page); {
	case l == "home" || l == "agent_experience":
		return nil
	case strings.HasPrefix(l, "aws:") || strings.HasPrefix(l, "connect:"):
		return invalidParamErr("custom pages must not start with aws: or connect:")
	}

	return nil
}

func (b *Backend) checkPageFields(inst string, in *pageFields) error {
	if in.ResourceArn != nil {
		if _, _, err := b.viewByRef(inst, *in.ResourceArn); err != nil {
			return err
		}
	}

	if in.InputData != nil {
		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(*in.InputData), &obj) != nil {
			return invalidParamErr("InputData must be a JSON object")
		}
	}

	if in.Slug != nil {
		return checkName("Slug", *in.Slug, maxWorkspaceSlug)
	}

	return nil
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// CreateWorkspacePage maps a page of a workspace to a view.
func (b *Backend) CreateWorkspacePage(ctx context.Context, in *CreateWorkspacePageInput) (*empty, error) {
	if err := checkPageName(in.Page); err != nil {
		return nil, err
	}

	if in.ResourceArn == nil {
		return nil, invalidParamErr("ResourceArn is required")
	}

	return b.updateWorkspace(ctx, "CreateWorkspacePage", in.InstanceID, in.WorkspaceID,
		func(inst string, r *workspaceRec) error {
			if err := b.checkPageFields(inst, &in.pageFields); err != nil {
				return err
			}

			if slices.ContainsFunc(r.Pages, func(p WorkspacePage) bool { return strs.Equal(p.Page, in.Page) }) {
				return duplicateErr(labelWorkspacePage, in.Page)
			}

			r.Pages = append(r.Pages, WorkspacePage{
				Page:        in.Page,
				ResourceArn: *in.ResourceArn,
				InputData:   strOrEmpty(in.InputData),
				Slug:        strOrEmpty(in.Slug),
			})

			return nil
		})
}

// UpdateWorkspacePage changes the view, input, slug or name of a page.
func (b *Backend) UpdateWorkspacePage(ctx context.Context, in *UpdateWorkspacePageInput) (*empty, error) {
	if in.NewPage != nil {
		if err := checkPageName(*in.NewPage); err != nil {
			return nil, err
		}
	}

	return b.updateWorkspace(ctx, "UpdateWorkspacePage", in.InstanceID, in.WorkspaceID,
		func(inst string, r *workspaceRec) error {
			i := slices.IndexFunc(r.Pages, func(p WorkspacePage) bool { return strs.Equal(p.Page, in.Page) })
			if i < 0 {
				return notFoundErr(labelWorkspacePage, in.Page)
			}

			if err := b.checkPageFields(inst, &in.pageFields); err != nil {
				return err
			}

			p := &r.Pages[i]

			if in.NewPage != nil {
				taken := slices.ContainsFunc(r.Pages, func(o WorkspacePage) bool {
					return strs.Equal(o.Page, *in.NewPage) && !strs.Equal(o.Page, p.Page)
				})
				if taken {
					return duplicateErr(labelWorkspacePage, *in.NewPage)
				}

				p.Page = *in.NewPage
			}

			if in.ResourceArn != nil {
				p.ResourceArn = *in.ResourceArn
			}

			if in.InputData != nil {
				p.InputData = *in.InputData
			}

			if in.Slug != nil {
				p.Slug = *in.Slug
			}

			return nil
		})
}

// DeleteWorkspacePageInput is the DeleteWorkspacePage request.
type DeleteWorkspacePageInput struct {
	Page string `json:"-" path:"Page"`
	wsRef
}

// DeleteWorkspacePage removes the view mapping of a page.
func (b *Backend) DeleteWorkspacePage(ctx context.Context, in *DeleteWorkspacePageInput) (*empty, error) {
	return b.updateWorkspace(ctx, "DeleteWorkspacePage", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			n := len(r.Pages)
			r.Pages = slices.DeleteFunc(r.Pages, func(p WorkspacePage) bool { return strs.Equal(p.Page, in.Page) })

			if len(r.Pages) == n {
				return notFoundErr(labelWorkspacePage, in.Page)
			}

			return nil
		})
}

// ListWorkspacePagesInput is the ListWorkspacePages request.
type ListWorkspacePagesInput struct {
	NextToken string `json:"-" query:"nextToken"`
	wsRef
	MaxResults int32 `json:"-" query:"maxResults"`
}

// ListWorkspacePages pages through the pages of a workspace.
func (b *Backend) ListWorkspacePages(in *ListWorkspacePagesInput) ([]WorkspacePage, string, error) {
	all, err := readInstance(b, "ListWorkspacePages", in.InstanceID, func(inst string) ([]WorkspacePage, error) {
		r, err := b.workspaceByID(inst, in.WorkspaceID)
		if err != nil {
			return nil, err
		}

		return slices.Clone(r.Pages), nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// WorkspaceMediaInput is the ImportWorkspaceMedia and DeleteWorkspaceMedia request.
type WorkspaceMediaInput struct {
	MediaSource string
	MediaType   string
	MediaQuery  string `json:"-" query:"mediaType"`
	wsRef
}

func checkMediaSource(src string) error {
	if src == "" {
		return invalidParamErr("MediaSource is required")
	}

	if strings.HasPrefix(src, "https://") || strings.HasPrefix(src, "http://") {
		return nil
	}

	if _, err := base64.StdEncoding.DecodeString(src); err != nil {
		return invalidParamErr("MediaSource must be a URL or a base64-encoded string")
	}

	return nil
}

// ImportWorkspaceMedia stores a media asset of a workspace; one asset is kept per media type.
func (b *Backend) ImportWorkspaceMedia(ctx context.Context, in *WorkspaceMediaInput) (*empty, error) {
	if err := firstError(
		oneOf("MediaType", in.MediaType, mediaTypes()...),
		checkMediaSource(in.MediaSource),
	); err != nil {
		return nil, err
	}

	return b.updateWorkspace(ctx, "ImportWorkspaceMedia", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			item := MediaItem{Type: in.MediaType, Source: in.MediaSource}
			if i := slices.IndexFunc(r.Media, func(m MediaItem) bool { return m.Type == in.MediaType }); i >= 0 {
				r.Media[i] = item
			} else {
				r.Media = append(r.Media, item)
			}

			return nil
		})
}

// DeleteWorkspaceMedia removes the media asset of one type.
func (b *Backend) DeleteWorkspaceMedia(ctx context.Context, in *WorkspaceMediaInput) (*empty, error) {
	if err := oneOf("MediaType", in.MediaQuery, mediaTypes()...); err != nil {
		return nil, err
	}

	return b.updateWorkspace(ctx, "DeleteWorkspaceMedia", in.InstanceID, in.WorkspaceID,
		func(_ string, r *workspaceRec) error {
			n := len(r.Media)
			r.Media = slices.DeleteFunc(r.Media, func(m MediaItem) bool { return m.Type == in.MediaQuery })

			if len(r.Media) == n {
				return notFoundErr("WorkspaceMedia", in.MediaQuery)
			}

			return nil
		})
}

// ListWorkspaceMedia returns the media assets of a workspace.
func (b *Backend) ListWorkspaceMedia(inst, id string) ([]MediaItem, error) {
	return readInstance(b, "ListWorkspaceMedia", inst, func(inst string) ([]MediaItem, error) {
		r, err := b.workspaceByID(inst, id)
		if err != nil {
			return nil, err
		}

		return slices.Clone(r.Media), nil
	})
}

// WorkspaceAssociationInput is the (Dis)AssociateWorkspace request.
type WorkspaceAssociationInput struct {
	wsRef
	ResourceArns []string
}

// BatchAssociationFailure is one resource a batch association rejected.
type BatchAssociationFailure struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
	ResourceArn  string `json:"ResourceArn"`
}

// BatchAssociationSuccess is one resource a batch association applied.
type BatchAssociationSuccess struct {
	ResourceArn string `json:"ResourceArn"`
}

// BatchAssociationResult is the body of (Dis)AssociateWorkspace.
type BatchAssociationResult struct {
	SuccessfulList []BatchAssociationSuccess `json:"SuccessfulList"`
	FailedList     []BatchAssociationFailure `json:"FailedList"`
}

// workspaceResource resolves a user or routing profile ARN to its type and name.
func (b *Backend) workspaceResource(inst, resourceARN string) (workspaceResourceInfo, bool) {
	for _, u := range b.users.in(inst) {
		if u.User.Arn == resourceARN {
			return workspaceResourceInfo{kind: resourceUser, id: u.User.ID, name: u.User.Username}, true
		}
	}

	for _, p := range b.routingProfiles.in(inst) {
		if p.Profile.RoutingProfileArn == resourceARN {
			return workspaceResourceInfo{
				kind: resourceRouting,
				id:   p.Profile.RoutingProfileID,
				name: p.Profile.Name,
			}, true
		}
	}

	return workspaceResourceInfo{}, false
}

type workspaceResourceInfo struct{ kind, id, name string }

func (b *Backend) workspaceLinks(
	ctx context.Context, op string, in *WorkspaceAssociationInput, add bool,
) (*BatchAssociationResult, error) {
	if n := len(in.ResourceArns); n == 0 || n > maxAssocBatch {
		return nil, invalidParamErr("ResourceArns must contain 1-%d entries", maxAssocBatch)
	}

	return writeInstance(b, op, in.InstanceID, func(inst string) (*BatchAssociationResult, error) {
		r, err := b.workspaceByID(inst, in.WorkspaceID)
		if err != nil {
			return nil, err
		}

		res := &BatchAssociationResult{
			SuccessfulList: []BatchAssociationSuccess{},
			FailedList:     []BatchAssociationFailure{},
		}

		for _, a := range in.ResourceArns {
			fail := func(code, msg string) {
				res.FailedList = append(
					res.FailedList,
					BatchAssociationFailure{ErrorCode: code, ErrorMessage: msg, ResourceArn: a},
				)
			}

			_, found := b.workspaceResource(inst, a)
			has := slices.Contains(r.Resources, a)

			switch {
			case !slices.Contains([]string{arnTypeUser, arnTypeRouting}, arnResourceType(a)):
				fail("InvalidParameterException", "only users and routing profiles can be associated with a workspace")
			case add && !found:
				fail("ResourceNotFoundException", "resource "+a+" was not found")
			case !add && !has:
				fail("ResourceNotFoundException", "the resource is not associated with the workspace")
			default:
				if add && !has {
					r.Resources = append(r.Resources, a)
				} else if !add {
					r.Resources = slices.DeleteFunc(r.Resources, func(x string) bool { return x == a })
				}

				res.SuccessfulList = append(res.SuccessfulList, BatchAssociationSuccess{ResourceArn: a})
			}
		}

		r.Workspace.Stamp = b.stamp(ctx)

		return res, nil
	})
}

// AssociateWorkspace assigns a workspace to users and routing profiles; assigning twice is a no-op.
func (b *Backend) AssociateWorkspace(
	ctx context.Context,
	in *WorkspaceAssociationInput,
) (*BatchAssociationResult, error) {
	return b.workspaceLinks(ctx, "AssociateWorkspace", in, true)
}

// DisassociateWorkspace removes workspace assignments.
func (b *Backend) DisassociateWorkspace(
	ctx context.Context, in *WorkspaceAssociationInput,
) (*BatchAssociationResult, error) {
	return b.workspaceLinks(ctx, "DisassociateWorkspace", in, false)
}

// SearchWorkspaceAssociations searches the user and routing profile assignments of workspaces.
func (b *Backend) SearchWorkspaceAssociations(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{wsFieldWorkspaceID, wsFieldResourceID, wsFieldResType, wsFieldResName}}

	crit, filt, err := parseSearch(in.SearchCriteria, in.SearchFilter, spec)
	if err != nil {
		return nil, err
	}

	all, err := readInstance(b, "SearchWorkspaceAssociations", in.InstanceID,
		func(inst string) ([]WorkspaceAssociationSummary, error) {
			var out []WorkspaceAssociationSummary

			for _, w := range b.workspaces.in(inst) {
				for _, a := range w.Resources {
					res, ok := b.workspaceResource(inst, a)
					if !ok {
						continue
					}

					s := searchSubject{tags: w.Workspace.Tags, fields: map[string]string{
						wsFieldWorkspaceID: w.Workspace.ID,
						wsFieldResourceID:  res.id,
						wsFieldResType:     res.kind,
						wsFieldResName:     res.name,
					}}
					if crit.matches(&s) && filt.matches(&s) {
						out = append(out, WorkspaceAssociationSummary{
							ResourceArn: a, ResourceID: res.id, ResourceName: res.name, ResourceType: res.kind,
							WorkspaceArn: w.Workspace.Arn, WorkspaceID: w.Workspace.ID,
						})
					}
				}
			}

			return out, nil
		})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	body := SearchResult{"WorkspaceAssociations": orEmpty(page), "ApproximateTotalCount": int64(len(all))}
	if next != "" {
		body["NextToken"] = next
	}

	return &body, nil
}
