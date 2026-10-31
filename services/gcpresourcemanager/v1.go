package gcpresourcemanager

import (
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	v1Projects      = "/v1/projects"
	v1Organizations = "/v1/organizations"
	maxBody         = 1 << 20
	v1Page          = 500
	decimalBase     = 10
	verbGetIam      = ":getIamPolicy"
	verbSetIam      = ":setIamPolicy"
	verbTestIam     = ":testIamPermissions"
)

// v1JSON is the v1 Project resource, which the Terraform provider still uses for projects and project IAM.
type v1Project struct {
	Labels         map[string]string `json:"labels,omitempty"`
	Parent         *v1ResourceID     `json:"parent,omitempty"`
	ProjectNumber  string            `json:"projectNumber,omitempty"`
	ProjectID      string            `json:"projectId,omitempty"`
	LifecycleState string            `json:"lifecycleState,omitempty"`
	Name           string            `json:"name,omitempty"`
	CreateTime     string            `json:"createTime,omitempty"`
}

type v1ResourceID struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type v1Organization struct {
	Owner          map[string]string `json:"owner,omitempty"`
	Name           string            `json:"name"`
	DisplayName    string            `json:"displayName"`
	CreationTime   string            `json:"creationTime,omitempty"`
	LifecycleState string            `json:"lifecycleState"`
}

func v1Parent(parent string) *v1ResourceID {
	kind, id, ok := strings.Cut(parent, "/")
	if !ok {
		return nil
	}

	return &v1ResourceID{Type: strings.TrimSuffix(kind, "s"), ID: id}
}

func v1ParentName(r *v1ResourceID) string {
	if r == nil || r.ID == "" {
		return ""
	}

	return r.Type + "s/" + r.ID
}

func toV1Project(p *resourcemanagerpb.Project) v1Project {
	out := v1Project{
		ProjectNumber: strings.TrimPrefix(p.GetName(), kindProjects+"/"), ProjectID: p.GetProjectId(),
		LifecycleState: p.GetState().String(), Name: p.GetDisplayName(), Labels: p.GetLabels(),
		Parent: v1Parent(p.GetParent()),
	}

	if t := p.GetCreateTime(); t != nil {
		out.CreateTime = t.AsTime().Format(time.RFC3339Nano)
	}

	return out
}

type v1Handler struct{ b *InMemoryBackend }

// matchV1 selects the requests of the Cloud Resource Manager v1 API.
func matchV1(r *http.Request) bool {
	p := r.URL.Path
	if strings.Contains(p, "/services") {
		return false
	}

	return p == v1Projects || strings.HasPrefix(p, v1Projects+"/") || strings.HasPrefix(p, v1Projects+":") ||
		strings.HasPrefix(p, v1Organizations)
}

func writeV1Error(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	httpCode := gcprest.HTTPStatus(st.Code())

	details := make([]json.RawMessage, 0, len(st.Proto().GetDetails()))

	for _, d := range st.Proto().GetDetails() {
		if raw, mErr := protojson.Marshal(d); mErr == nil {
			details = append(details, raw)
		}
	}

	writeV1JSON(w, httpCode, map[string]any{"error": map[string]any{
		"code":    httpCode,
		"message": st.Message(),
		"status":  strings.ToUpper(strings.ReplaceAll(codeName(st.Code()), " ", "_")),
		"details": details,
	}})
}

func codeName(c codes.Code) string {
	switch c {
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	default:
		return "INTERNAL"
	}
}

func writeV1JSON(w http.ResponseWriter, code int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	writeV1Raw(w, code, raw)
}

func writeV1Raw(w http.ResponseWriter, code int, raw []byte) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

func writeV1Proto(w http.ResponseWriter, m proto.Message) {
	raw, err := protojson.Marshal(m)
	if err != nil {
		writeV1Error(w, errInvalid("encode response: %v", err))

		return
	}

	writeV1Raw(w, http.StatusOK, raw)
}

func readBody(r *http.Request, into any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		return errInvalid("read request body: %v", err)
	}

	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}

	if err = json.Unmarshal(raw, into); err != nil {
		return errInvalid("Invalid JSON payload: %v", err)
	}

	return nil
}

func readProtoBody(r *http.Request, into proto.Message) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		return errInvalid("read request body: %v", err)
	}

	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}

	if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, into); err != nil {
		return errInvalid("Invalid JSON payload: %v", err)
	}

	return nil
}

var v1Route = regexp.MustCompile(`^(projects|organizations)(?:/([^/:]+))?(/billingInfo)?(?::(\w+))?$`)

func (h v1Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m := v1Route.FindStringSubmatch(strings.TrimPrefix(r.URL.Path, "/v1/"))
	if m == nil {
		writeV1Error(w, status.Error(codes.NotFound, "unknown resource "+r.URL.Path))

		return
	}

	id, verb := m[2], ""
	if m[4] != "" {
		verb = ":" + m[4]
	}

	var err error

	switch {
	case m[3] != "" && m[1] == kindProjects && verb == "":
		err = h.billingInfo(w, r, id)
	case m[3] != "":
		err = methodNotAllowed(r)
	case m[1] == kindProjects:
		err = h.projects(w, r, id, verb)
	default:
		err = h.organizations(w, r, id, verb)
	}

	if err != nil {
		writeV1Error(w, err)
	}
}

func methodNotAllowed(r *http.Request) error {
	return status.Errorf(codes.NotFound, "no route for %s %s", r.Method, r.URL.Path)
}

func (h v1Handler) projects(w http.ResponseWriter, r *http.Request, id, verb string) error {
	switch {
	case verb != "" && r.Method == http.MethodPost:
		return h.projectVerb(w, r, id, verb)
	case verb != "":
		return methodNotAllowed(r)
	case id == "":
		return h.projectCollection(w, r)
	default:
		return h.projectItem(w, r, id)
	}
}

func (h v1Handler) projectCollection(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case http.MethodPost:
		return h.createProject(w, r)
	case http.MethodGet:
		return h.listProjects(w, r)
	default:
		return methodNotAllowed(r)
	}
}

func (h v1Handler) projectItem(w http.ResponseWriter, r *http.Request, id string) error {
	switch r.Method {
	case http.MethodGet:
		p, err := h.b.GetProject(id)
		if err != nil {
			return err
		}

		writeV1JSON(w, http.StatusOK, toV1Project(p))

		return nil
	case http.MethodPut:
		return h.updateProject(w, r, id)
	case http.MethodDelete:
		if _, err := h.b.DeleteProject(&resourcemanagerpb.DeleteProjectRequest{Name: id}); err != nil {
			return err
		}

		writeV1Raw(w, http.StatusOK, []byte("{}"))

		return nil
	default:
		return methodNotAllowed(r)
	}
}

func (h v1Handler) projectVerb(w http.ResponseWriter, r *http.Request, id, verb string) error {
	switch verb {
	case ":undelete":
		if _, err := h.b.UndeleteProject(&resourcemanagerpb.UndeleteProjectRequest{Name: id}); err != nil {
			return err
		}

		writeV1Raw(w, http.StatusOK, []byte("{}"))

		return nil
	case ":getAncestry":
		return h.ancestry(w, id)
	case verbGetIam, verbSetIam, verbTestIam:
		return h.iam(w, r, "projects/"+id, verb)
	default:
		return methodNotAllowed(r)
	}
}

func (h v1Handler) createProject(w http.ResponseWriter, r *http.Request) error {
	var in v1Project
	if err := readBody(r, &in); err != nil {
		return err
	}

	op, err := h.b.CreateProject(&resourcemanagerpb.CreateProjectRequest{Project: &resourcemanagerpb.Project{
		ProjectId: in.ProjectID, DisplayName: in.Name, Labels: in.Labels, Parent: v1ParentName(in.Parent),
	}})
	if err != nil {
		return err
	}

	writeV1Proto(w, op)

	return nil
}

func (h v1Handler) updateProject(w http.ResponseWriter, r *http.Request, id string) error {
	var in v1Project
	if err := readBody(r, &in); err != nil {
		return err
	}

	cur, err := h.b.GetProject(id)
	if err != nil {
		return err
	}

	if in.ProjectID != "" && in.ProjectID != cur.GetProjectId() {
		return errInvalid("project_id is immutable.")
	}

	if parent := v1ParentName(in.Parent); parent != cur.GetParent() {
		if parent == "" {
			return errInvalid("The parent of a project cannot be cleared.")
		}

		if _, err = h.b.MoveProject(
			&resourcemanagerpb.MoveProjectRequest{Name: id, DestinationParent: parent},
		); err != nil {
			return err
		}
	}

	op, err := h.b.UpdateProject(&resourcemanagerpb.UpdateProjectRequest{Project: &resourcemanagerpb.Project{
		Name: id, DisplayName: in.Name, Labels: in.Labels,
	}})
	if err != nil {
		return err
	}

	p := &resourcemanagerpb.Project{}
	if resp := op.GetResponse(); resp != nil {
		if uErr := resp.UnmarshalTo(p); uErr != nil {
			return errInvalid("decode project: %v", uErr)
		}
	}

	writeV1JSON(w, http.StatusOK, toV1Project(p))

	return nil
}

func (h v1Handler) listProjects(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()

	size := 0
	if s := q.Get("pageSize"); s != "" {
		if n, err := parseInt(s); err == nil {
			size = n
		}
	}

	h.b.mu.Lock("v1ListProjects")
	h.b.purgeLocked()

	query, err := parseQuery(q.Get("filter"), true)
	if err != nil {
		h.b.mu.Unlock()

		return err
	}

	var hits []*projectRec

	for _, p := range h.b.projects {
		ok, mErr := query.match(p.fields)
		if mErr != nil {
			h.b.mu.Unlock()

			return mErr
		}

		if ok && (query.hasState || !p.requested()) {
			hits = append(hits, p)
		}
	}

	views := viewsOf(sortProjects(hits))
	h.b.mu.Unlock()

	pg := page.New(views, q.Get("pageToken"), size, v1Page)
	out := make([]v1Project, 0, len(pg.Data))

	for _, p := range pg.Data {
		out = append(out, toV1Project(p))
	}

	resp := map[string]any{}
	if len(out) > 0 {
		resp["projects"] = out
	}

	if pg.Next != "" {
		resp["nextPageToken"] = pg.Next
	}

	writeV1JSON(w, http.StatusOK, resp)

	return nil
}

func (h v1Handler) ancestry(w http.ResponseWriter, id string) error {
	p, err := h.b.GetProject(id)
	if err != nil {
		return err
	}

	type ancestor struct {
		ResourceID v1ResourceID `json:"resourceId"`
	}

	chain := []ancestor{{v1ResourceID{Type: "project", ID: p.GetProjectId()}}}

	h.b.mu.RLock("v1Ancestry")

	for parent := p.GetParent(); parent != ""; {
		ref := v1Parent(parent)
		chain = append(chain, ancestor{*ref})

		f, ok := h.b.folders[ref.ID]
		if ref.Type != "folder" || !ok {
			break
		}

		parent = f.Parent
	}

	h.b.mu.RUnlock()

	writeV1JSON(w, http.StatusOK, map[string]any{"ancestor": chain})

	return nil
}

func (h v1Handler) organizations(w http.ResponseWriter, r *http.Request, id, verb string) error {
	switch {
	case r.Method == http.MethodPost && id == "" && verb == ":search":
		return h.searchOrganizations(w, r)
	case r.Method == http.MethodPost && id != "" && (verb == verbGetIam || verb == verbSetIam || verb == verbTestIam):
		return h.iam(w, r, "organizations/"+id, verb)
	case r.Method == http.MethodGet && id != "" && verb == "":
		o, err := h.b.GetOrganization("organizations/" + id)
		if err != nil {
			return err
		}

		writeV1JSON(w, http.StatusOK, toV1Org(o))

		return nil
	default:
		return methodNotAllowed(r)
	}
}

func (h v1Handler) searchOrganizations(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Filter    string `json:"filter"`
		PageToken string `json:"pageToken"`
		PageSize  int32  `json:"pageSize"`
	}

	if err := readBody(r, &in); err != nil {
		return err
	}

	resp, err := h.b.SearchOrganizations(&resourcemanagerpb.SearchOrganizationsRequest{
		Query: in.Filter, PageToken: in.PageToken, PageSize: in.PageSize,
	})
	if err != nil {
		return err
	}

	orgs := make([]v1Organization, 0, len(resp.GetOrganizations()))
	for _, o := range resp.GetOrganizations() {
		orgs = append(orgs, toV1Org(o))
	}

	out := map[string]any{}
	if len(orgs) > 0 {
		out["organizations"] = orgs
	}

	if resp.GetNextPageToken() != "" {
		out["nextPageToken"] = resp.GetNextPageToken()
	}

	writeV1JSON(w, http.StatusOK, out)

	return nil
}

func toV1Org(o *resourcemanagerpb.Organization) v1Organization {
	out := v1Organization{
		Name: o.GetName(), DisplayName: o.GetDisplayName(), LifecycleState: o.GetState().String(),
		Owner: map[string]string{"directoryCustomerId": o.GetDirectoryCustomerId()},
	}

	if t := o.GetCreateTime(); t != nil {
		out.CreationTime = t.AsTime().Format(time.RFC3339Nano)
	}

	return out
}

func (h v1Handler) iam(w http.ResponseWriter, r *http.Request, resource, verb string) error {
	switch verb {
	case verbGetIam:
		var in struct {
			Options struct {
				RequestedPolicyVersion int32 `json:"requestedPolicyVersion"`
			} `json:"options"`
		}

		if err := readBody(r, &in); err != nil {
			return err
		}

		p, err := h.b.GetIamPolicy(&iampb.GetIamPolicyRequest{
			Resource: resource,
			Options:  &iampb.GetPolicyOptions{RequestedPolicyVersion: in.Options.RequestedPolicyVersion},
		})
		if err != nil {
			return err
		}

		writeV1Proto(w, p)
	case verbSetIam:
		req := &iampb.SetIamPolicyRequest{}
		if err := readProtoBody(r, req); err != nil {
			return err
		}

		req.Resource = resource

		p, err := h.b.SetIamPolicy(req)
		if err != nil {
			return err
		}

		writeV1Proto(w, p)
	default:
		req := &iampb.TestIamPermissionsRequest{}
		if err := readProtoBody(r, req); err != nil {
			return err
		}

		req.Resource = resource

		resp, err := h.b.TestIamPermissions(req)
		if err != nil {
			return err
		}

		writeV1Proto(w, resp)
	}

	return nil
}

func parseInt(s string) (int, error) {
	n := 0

	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errInvalid("invalid integer %q", s)
		}

		n = n*decimalBase + int(c-'0')
	}

	return n, nil
}

type billingInfo struct {
	Name               string `json:"name"`
	ProjectID          string `json:"projectId"`
	BillingAccountName string `json:"billingAccountName"`
	BillingEnabled     bool   `json:"billingEnabled"`
}

// billingInfo serves Cloud Billing projects.getBillingInfo and projects.updateBillingInfo, which the Terraform
// google_project resource calls on every read.
func (h v1Handler) billingInfo(w http.ResponseWriter, r *http.Request, id string) error {
	var (
		info billingInfo
		err  error
	)

	switch r.Method {
	case http.MethodGet:
		info, err = h.b.BillingInfo(id)
	case http.MethodPut:
		var in billingInfo
		if err = readBody(r, &in); err != nil {
			return err
		}

		info, err = h.b.SetBillingAccount(id, in.BillingAccountName)
	default:
		return methodNotAllowed(r)
	}

	if err != nil {
		return err
	}

	writeV1JSON(w, http.StatusOK, info)

	return nil
}
