package dashboard

import (
	"bytes"
	"encoding/base64"
	"context"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

const (
	gcpRMPrefix       = "/dashboard/api/v1/gcp/resourcemanager"
	gcpRMItemSegs     = 2
	gcpRMServicesSegs = 3
	gcpRMServiceSegs  = 4
	gcpRMServicesIdx  = 2
	gcpRMServiceIdx   = 3
	gcpRMMaxMember    = 256
	gcpRMKindProjects = "projects"
	gcpRMKindFolders  = "folders"
	gcpRMKindOrgs     = "organizations"
	gcpRMVerbMove     = "move"
	gcpRMVerbUndelete = "undelete"
	gcpRMVerbEnable   = "enable"
	gcpRMVerbDisable  = "disable"
	gcpRMVerbGetIAM   = "getIamPolicy"
	gcpRMVerbAddIAM   = "addMember"
	gcpRMVerbDelIAM   = "removeMember"
	gcpRMIAMVersion   = 3
)

var (
	gcpRMIDRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,62}$`)
	gcpRMParentRE  = regexp.MustCompile(`^(organizations|folders)/[A-Za-z0-9]{1,64}$`)
	gcpRMKeyRE     = regexp.MustCompile(`^(organizations|projects)/[A-Za-z0-9-]{1,64}$`)
	gcpRMValueRE   = regexp.MustCompile(`^tagKeys/[0-9]{1,32}$`)
	gcpRMBindingRE = regexp.MustCompile(`^//cloudresourcemanager\.googleapis\.com/(projects|folders|organizations)/[A-Za-z0-9-]{1,64}$`)
	gcpRMServiceRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	gcpRMRoleRE    = regexp.MustCompile(`^(roles/[A-Za-z0-9_.]{1,128}|(projects|organizations)/[A-Za-z0-9_-]{1,64}/roles/[A-Za-z0-9_.]{1,128})$`)
	gcpRMStateRE   = regexp.MustCompile(`^(ENABLED|DISABLED)$`)
)

// registerGCPResourceManagerRoutes serves the Resource Manager page by calling the control plane in-process. Reads
// cover the hierarchy, IAM policies, Service Usage and tags; writes are rebuilt from fixed fields.
func (h *DashboardHandler) registerGCPResourceManagerRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPResourceManagerOps; ops != nil {
		route = (&gcpRMProxy{be: ops.Backend, enforce: ops.Enforcement}).route
	}

	h.registerGCPRESTProxy(gcpRMPrefix, route)
}

type gcpRMProxy struct {
	be      *gcpresourcemanager.InMemoryBackend
	enforce func() gcpenforce.Settings
}

func gcpRMBad(msg string) error { return status.Error(codes.InvalidArgument, msg) }

func gcpRMKind(k string) bool {
	return k == gcpRMKindProjects || k == gcpRMKindFolders || k == gcpRMKindOrgs
}

func (p *gcpRMProxy) route(_ context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)

	switch {
	case n == 1:
		return p.collection(r)
	case n == gcpRMItemSegs && gcpRMKind(r.segs[0]) && gcpRMIDRE.MatchString(r.segs[1]):
		return p.item(r)
	case n >= gcpRMServicesSegs && r.segs[0] == gcpRMKindProjects && gcpRMIDRE.MatchString(r.segs[1]) &&
		r.segs[gcpRMServicesIdx] == "services":
		return p.services(r)
	}

	return nil, errGCPUnsupported()
}

func (p *gcpRMProxy) collection(r gcpRESTCall) (proto.Message, error) {
	if r.verb != "" {
		return nil, errGCPUnsupported()
	}

	key := r.segs[0]

	if r.method == http.MethodPost {
		switch key {
		case gcpRMKindFolders:
			return p.createFolder(r)
		case gcpRMKindProjects:
			return p.createProject(r)
		}

		return nil, errGCPUnsupported()
	}

	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	if key == "enforcement" {
		return structOf(p.enforce())
	}

	return p.list(r, key)
}

func (p *gcpRMProxy) list(r gcpRESTCall, key string) (proto.Message, error) {
	parent := r.query.Get("parent")
	size, token := r.pageSize(), r.query.Get("pageToken")

	switch {
	case key == gcpRMKindOrgs:
		return p.be.SearchOrganizations(&resourcemanagerpb.SearchOrganizationsRequest{PageSize: size, PageToken: token})
	case key == gcpRMKindFolders && gcpRMParentRE.MatchString(parent):
		return p.be.ListFolders(&resourcemanagerpb.ListFoldersRequest{
			Parent: parent, PageSize: size, PageToken: token, ShowDeleted: true,
		})
	case key == gcpRMKindProjects && parent == "":
		return p.be.SearchProjects(&resourcemanagerpb.SearchProjectsRequest{
			Query: "state:ACTIVE state:DELETE_REQUESTED", PageSize: size, PageToken: token,
		})
	case key == gcpRMKindProjects && gcpRMParentRE.MatchString(parent):
		return p.be.ListProjects(&resourcemanagerpb.ListProjectsRequest{
			Parent: parent, PageSize: size, PageToken: token, ShowDeleted: true,
		})
	case key == "tagKeys" && gcpRMKeyRE.MatchString(parent):
		return p.be.ListTagKeys(&resourcemanagerpb.ListTagKeysRequest{Parent: parent, PageSize: size, PageToken: token})
	case key == "tagValues" && gcpRMValueRE.MatchString(parent):
		return p.be.ListTagValues(&resourcemanagerpb.ListTagValuesRequest{
			Parent: parent, PageSize: size, PageToken: token,
		})
	case key == "tagBindings" && gcpRMBindingRE.MatchString(parent):
		return p.be.ListTagBindings(&resourcemanagerpb.ListTagBindingsRequest{
			Parent: parent, PageSize: size, PageToken: token,
		})
	}

	return nil, errGCPUnsupported()
}

func (p *gcpRMProxy) createFolder(r gcpRESTCall) (proto.Message, error) {
	in := &resourcemanagerpb.Folder{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	if !gcpRMParentRE.MatchString(in.GetParent()) {
		return nil, gcpRMBad("parent must be organizations/ID or folders/ID")
	}

	return p.be.CreateFolder(&resourcemanagerpb.CreateFolderRequest{
		Folder: &resourcemanagerpb.Folder{Parent: in.GetParent(), DisplayName: in.GetDisplayName()},
	})
}

func (p *gcpRMProxy) createProject(r gcpRESTCall) (proto.Message, error) {
	in := &resourcemanagerpb.Project{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	if in.GetParent() != "" && !gcpRMParentRE.MatchString(in.GetParent()) {
		return nil, gcpRMBad("parent must be organizations/ID or folders/ID")
	}

	return p.be.CreateProject(&resourcemanagerpb.CreateProjectRequest{Project: &resourcemanagerpb.Project{
		ProjectId: in.GetProjectId(), DisplayName: in.GetDisplayName(), Parent: in.GetParent(), Labels: in.GetLabels(),
	}})
}

func (p *gcpRMProxy) item(r gcpRESTCall) (proto.Message, error) {
	kind, name := r.segs[0], r.name(gcpRMItemSegs)

	switch {
	case r.verb == "" && r.method == http.MethodGet:
		return p.get(kind, name)
	case r.verb == "" && r.method == http.MethodDelete && kind != gcpRMKindOrgs:
		return p.remove(kind, name)
	case r.method != http.MethodPost:
		return nil, errGCPUnsupported()
	case r.verb == gcpRMVerbGetIAM:
		return p.be.GetIamPolicy(&iampb.GetIamPolicyRequest{
			Resource: name, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: gcpRMIAMVersion},
		})
	case r.verb == gcpRMVerbAddIAM, r.verb == gcpRMVerbDelIAM:
		return p.editMember(r, name)
	case kind == gcpRMKindOrgs:
		return nil, errGCPUnsupported()
	case r.verb == gcpRMVerbMove:
		return p.move(r, kind, name)
	case r.verb == gcpRMVerbUndelete:
		if kind == gcpRMKindProjects {
			return p.be.UndeleteProject(&resourcemanagerpb.UndeleteProjectRequest{Name: name})
		}

		return p.be.UndeleteFolder(&resourcemanagerpb.UndeleteFolderRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (p *gcpRMProxy) get(kind, name string) (proto.Message, error) {
	switch kind {
	case gcpRMKindProjects:
		return p.be.GetProject(name)
	case gcpRMKindFolders:
		return p.be.GetFolder(name)
	}

	return p.be.GetOrganization(name)
}

func (p *gcpRMProxy) remove(kind, name string) (proto.Message, error) {
	if kind == gcpRMKindProjects {
		return p.be.DeleteProject(&resourcemanagerpb.DeleteProjectRequest{Name: name})
	}

	return p.be.DeleteFolder(&resourcemanagerpb.DeleteFolderRequest{Name: name})
}

func (p *gcpRMProxy) move(r gcpRESTCall, kind, name string) (proto.Message, error) {
	in := &resourcemanagerpb.MoveProjectRequest{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	if !gcpRMParentRE.MatchString(in.GetDestinationParent()) {
		return nil, gcpRMBad("destinationParent must be organizations/ID or folders/ID")
	}

	if kind == gcpRMKindProjects {
		return p.be.MoveProject(&resourcemanagerpb.MoveProjectRequest{
			Name: name, DestinationParent: in.GetDestinationParent(),
		})
	}

	return p.be.MoveFolder(&resourcemanagerpb.MoveFolderRequest{
		Name: name, DestinationParent: in.GetDestinationParent(),
	})
}

// editMember adds or removes one member of one unconditioned binding, writing back with the caller's etag so a stale
// view is rejected by the policy store (ABORTED).
func (p *gcpRMProxy) editMember(r gcpRESTCall, resource string) (proto.Message, error) {
	in := &structpb.Struct{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	f := in.GetFields()
	role, member, etag := f["role"].GetStringValue(), f["member"].GetStringValue(), f["etag"].GetStringValue()

	switch {
	case !gcpRMRoleRE.MatchString(role):
		return nil, gcpRMBad("role must be roles/NAME or a custom role name")
	case member == "" || len(member) > gcpRMMaxMember || strings.ContainsAny(member, " \t\r\n\x00"):
		return nil, gcpRMBad("member is required")
	case etag == "":
		return nil, gcpRMBad("etag is required")
	}

	cur, err := p.be.GetIamPolicy(&iampb.GetIamPolicyRequest{
		Resource: resource, Options: &iampb.GetPolicyOptions{RequestedPolicyVersion: gcpRMIAMVersion},
	})
	if err != nil {
		return nil, err
	}

	want, err := base64.StdEncoding.DecodeString(etag)
	if err != nil {
		return nil, gcpRMBad("etag is not valid")
	}

	if !bytes.Equal(want, cur.GetEtag()) {
		return nil, status.Error(codes.Aborted, "The policy was changed by another request; reload and retry.")
	}

	pol := &iampb.Policy{Version: cur.GetVersion(), Etag: cur.GetEtag(), AuditConfigs: cur.GetAuditConfigs()}
	for _, b := range cur.GetBindings() {
		pol.Bindings = append(pol.Bindings, &iampb.Binding{
			Role: b.GetRole(), Members: slices.Clone(b.GetMembers()), Condition: b.GetCondition(),
		})
	}

	if r.verb == gcpRMVerbAddIAM {
		gcpRMAddMember(pol, role, member)
	} else if err = gcpRMRemoveMember(pol, role, member); err != nil {
		return nil, err
	}

	return p.be.SetIamPolicy(&iampb.SetIamPolicyRequest{Resource: resource, Policy: pol})
}

func gcpRMAddMember(pol *iampb.Policy, role, member string) {
	for _, b := range pol.GetBindings() {
		if b.GetRole() == role && b.GetCondition() == nil {
			if !slices.Contains(b.GetMembers(), member) {
				b.Members = append(b.Members, member)
			}

			return
		}
	}

	pol.Bindings = append(pol.Bindings, &iampb.Binding{Role: role, Members: []string{member}})
}

func gcpRMRemoveMember(pol *iampb.Policy, role, member string) error {
	for i, b := range pol.GetBindings() {
		if b.GetRole() != role || b.GetCondition() != nil || !slices.Contains(b.GetMembers(), member) {
			continue
		}

		b.Members = slices.DeleteFunc(b.Members, func(m string) bool { return m == member })
		if len(b.Members) == 0 {
			pol.Bindings = slices.Delete(pol.Bindings, i, i+1)
		}

		return nil
	}

	return status.Error(codes.NotFound, "that member does not hold that role")
}

func (p *gcpRMProxy) services(r gcpRESTCall) (proto.Message, error) {
	project := r.name(gcpRMItemSegs)

	switch len(r.segs) {
	case gcpRMServicesSegs:
		return p.listServices(r, project)
	case gcpRMServiceSegs:
		if !gcpRMServiceRE.MatchString(r.segs[gcpRMServiceIdx]) {
			return nil, errGCPUnsupported()
		}

		return p.serviceAction(r, r.name(gcpRMServiceSegs))
	}

	return nil, errGCPUnsupported()
}

func (p *gcpRMProxy) listServices(r gcpRESTCall, project string) (proto.Message, error) {
	if r.verb != "" || r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	filter := ""
	if s := r.query.Get("state"); s != "" {
		if !gcpRMStateRE.MatchString(s) {
			return nil, gcpRMBad("state must be ENABLED or DISABLED")
		}

		filter = "state:" + s
	}

	return p.be.ListServices(&serviceusagepb.ListServicesRequest{
		Parent: project, Filter: filter, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
	})
}

func (p *gcpRMProxy) serviceAction(r gcpRESTCall, name string) (proto.Message, error) {
	switch {
	case r.method == http.MethodGet && r.verb == "":
		return p.be.GetService(name)
	case r.method != http.MethodPost:
		return nil, errGCPUnsupported()
	case r.verb == gcpRMVerbEnable:
		return p.be.EnableService(&serviceusagepb.EnableServiceRequest{Name: name})
	case r.verb == gcpRMVerbDisable:
		in := &serviceusagepb.DisableServiceRequest{}
		if err := r.unmarshal(in); err != nil {
			return nil, err
		}

		return p.be.DisableService(&serviceusagepb.DisableServiceRequest{
			Name: name, DisableDependentServices: in.GetDisableDependentServices(),
		})
	}

	return nil, errGCPUnsupported()
}
