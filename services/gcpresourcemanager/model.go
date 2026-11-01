package gcpresourcemanager

import (
	"maps"
	"time"

	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	kindOrganizations = "organizations"
	kindFolders       = "folders"
	kindProjects      = "projects"
	kindTagKeys       = "tagKeys"
	kindTagValues     = "tagValues"
	kindTagBindings   = "tagBindings"
)

type lifecycle struct {
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	Deleted time.Time `json:"deleted,omitzero"`
}

func (l lifecycle) requested() bool { return !l.Deleted.IsZero() }

type projectRec struct {
	lifecycle
	Labels      map[string]string `json:"labels,omitempty"`
	Number      string            `json:"number"`
	ID          string            `json:"id"`
	DisplayName string            `json:"displayName"`
	Parent      string            `json:"parent,omitempty"`
	Billing     string            `json:"billing,omitempty"`
}

type folderRec struct {
	lifecycle
	ID          string `json:"id"`
	Parent      string `json:"parent"`
	DisplayName string `json:"displayName"`
}

type orgRec struct {
	Created time.Time
	ID      string
	Domain  string
}

type tagKeyRec struct {
	lifecycle
	PurposeData map[string]string `json:"purposeData,omitempty"`
	ID          string            `json:"id"`
	Parent      string            `json:"parent"`
	ShortName   string            `json:"shortName"`
	Description string            `json:"description,omitempty"`
	Purpose     int32             `json:"purpose,omitempty"`
}

type tagValueRec struct {
	lifecycle
	ID          string `json:"id"`
	Parent      string `json:"parent"`
	ShortName   string `json:"shortName"`
	Description string `json:"description,omitempty"`
}

type bindingRec struct {
	Parent   string `json:"parent"`
	TagValue string `json:"tagValue"`
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}

	return timestamppb.New(t)
}

func (p *projectRec) view() *resourcemanagerpb.Project {
	state := resourcemanagerpb.Project_ACTIVE
	if p.requested() {
		state = resourcemanagerpb.Project_DELETE_REQUESTED
	}

	labels := map[string]string(nil)
	if len(p.Labels) > 0 {
		labels = make(map[string]string, len(p.Labels))
		maps.Copy(labels, p.Labels)
	}

	return &resourcemanagerpb.Project{
		Name: kindProjects + "/" + p.Number, Parent: p.Parent, ProjectId: p.ID, State: state,
		DisplayName: p.DisplayName, CreateTime: ts(p.Created), UpdateTime: ts(p.Updated),
		DeleteTime: ts(p.Deleted), Labels: labels, Etag: etagOf(p.Number, p.Parent, p.DisplayName, p.Updated, p.Labels),
	}
}

func (f *folderRec) view() *resourcemanagerpb.Folder {
	state := resourcemanagerpb.Folder_ACTIVE
	if f.requested() {
		state = resourcemanagerpb.Folder_DELETE_REQUESTED
	}

	return &resourcemanagerpb.Folder{
		Name: kindFolders + "/" + f.ID, Parent: f.Parent, DisplayName: f.DisplayName, State: state,
		CreateTime: ts(f.Created), UpdateTime: ts(f.Updated), DeleteTime: ts(f.Deleted),
		Etag: etagOf(f.ID, f.Parent, f.DisplayName, f.Updated, nil),
	}
}

func (o *orgRec) view() *resourcemanagerpb.Organization {
	return &resourcemanagerpb.Organization{
		Name: kindOrganizations + "/" + o.ID, DisplayName: o.Domain, State: resourcemanagerpb.Organization_ACTIVE,
		Owner:      &resourcemanagerpb.Organization_DirectoryCustomerId{DirectoryCustomerId: "C" + o.ID},
		CreateTime: ts(o.Created), UpdateTime: ts(o.Created), Etag: etagOf(o.ID, o.Domain, "", o.Created, nil),
	}
}
