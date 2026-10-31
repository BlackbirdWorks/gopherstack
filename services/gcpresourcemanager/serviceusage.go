package gcpresourcemanager

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/genproto/googleapis/api/serviceconfig"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxBatchEnable   = 20
	maxBatchGet      = 30
	defaultSUPage    = 50
	maxSUPage        = 200
	suDomain         = "googleapis.com"
	reasonConsumer   = "CONSUMER_INVALID"
	reasonNoService  = "SERVICE_CONFIG_NOT_FOUND_OR_PERMISSION_DENIED"
	reasonDisabled   = "SERVICE_DISABLED"
	filterEnabled    = "state:ENABLED"
	filterDisabled   = "state:DISABLED"
	serviceUsageName = "serviceusage.googleapis.com"
	mdService        = "service"
)

func suError(code codes.Code, msg, reason string, md map[string]string) error {
	st := status.New(code, msg)

	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: suDomain, Metadata: md})
	if err != nil {
		return st.Err()
	}

	return withInfo.Err()
}

func errConsumer(parent string) error { return errConsumerFor(parent, serviceUsageName) }

func errConsumerFor(parent, service string) error {
	return suError(
		codes.PermissionDenied,
		"Permission denied on resource project "+strings.TrimPrefix(parent, kindProjects+"/")+".",
		reasonConsumer,
		map[string]string{"consumer": parent, mdService: service},
	)
}

func errNoService(name string) error {
	return suError(
		codes.PermissionDenied,
		"Permission denied to access service ["+name+"]; it was not found or you lack permission.",
		reasonNoService,
		map[string]string{mdService: name},
	)
}

func (b *InMemoryBackend) enableDefaultsLocked(number string) {
	now := b.now()
	set := map[string]time.Time{}

	for _, e := range catalog {
		if e.defaultOn {
			set[e.name] = now
		}
	}

	b.enabled[number] = set
}

// enableLocked enables name and, first, every service it depends on.
func (b *InMemoryBackend) enableLocked(number, name string) {
	set := b.enabled[number]
	if set == nil {
		set = map[string]time.Time{}
		b.enabled[number] = set
	}

	if _, on := set[name]; on {
		return
	}

	set[name] = b.now()

	if e, ok := lookupService(name); ok {
		for _, d := range e.depends {
			b.enableLocked(number, d)
		}
	}
}

func (b *InMemoryBackend) consumerLocked(parent string) (*projectRec, error) {
	if !strings.HasPrefix(parent, kindProjects+"/") {
		if strings.HasPrefix(parent, kindFolders+"/") || strings.HasPrefix(parent, kindOrganizations+"/") {
			return nil, errInvalid("Service Usage supports projects only, not %q.", parent)
		}

		return nil, errInvalid("Invalid parent %q: use projects/PROJECT.", parent)
	}

	p, ok := b.findProjectLocked(parent)
	if !ok || p.requested() {
		return nil, errConsumer(parent)
	}

	return p, nil
}

func splitServiceName(name string) (string, string, error) {
	parent, service, ok := strings.Cut(name, "/services/")
	if !ok || parent == "" || service == "" || strings.Contains(service, "/") || strings.Contains(parent, "/services") {
		return "", "", errInvalid("Invalid service name %q: use projects/PROJECT/services/SERVICE.", name)
	}

	return parent, service, nil
}

func (b *InMemoryBackend) serviceView(p *projectRec, e catalogEntry) *serviceusagepb.Service {
	state := serviceusagepb.State_DISABLED
	if _, on := b.enabled[p.Number][e.name]; on {
		state = serviceusagepb.State_ENABLED
	}

	return &serviceusagepb.Service{
		Name:   kindProjects + "/" + p.Number + "/services/" + e.name,
		Parent: kindProjects + "/" + p.Number,
		State:  state,
		Config: &serviceusagepb.ServiceConfig{
			Name: e.name, Title: e.title,
			Documentation: &serviceconfig.Documentation{Summary: e.summary},
			Endpoints:     []*serviceconfig.Endpoint{{Name: e.name}},
		},
	}
}

func (b *InMemoryBackend) suOpLocked(p *projectRec, names []string, resp proto.Message,
) (*longrunningpb.Operation, error) {
	resources := make([]string, len(names))
	for i, n := range names {
		resources[i] = kindProjects + "/" + p.Number + "/services/" + n
	}

	return b.opLocked("acat", &serviceusagepb.OperationMetadata{ResourceNames: resources}, resp)
}

// GetService returns a service's state for a project.
func (b *InMemoryBackend) GetService(name string) (*serviceusagepb.Service, error) {
	parent, svc, err := splitServiceName(name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetService")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.consumerLocked(parent)
	if err != nil {
		return nil, err
	}

	e, ok := lookupService(svc)
	if !ok {
		return nil, errNoService(svc)
	}

	return b.serviceView(p, e), nil
}

// BatchGetServices returns the state of up to 30 services of one project.
func (b *InMemoryBackend) BatchGetServices(in *serviceusagepb.BatchGetServicesRequest) (
	*serviceusagepb.BatchGetServicesResponse, error,
) {
	if len(in.GetNames()) == 0 {
		return nil, errInvalid("names must not be empty.")
	}

	if len(in.GetNames()) > maxBatchGet {
		return nil, errInvalid("A single request can get at most %d services.", maxBatchGet)
	}

	b.mu.Lock("BatchGetServices")
	defer b.mu.Unlock()

	b.purgeLocked()

	var (
		owner *projectRec
		out   = make([]*serviceusagepb.Service, 0, len(in.GetNames()))
	)

	for _, n := range in.GetNames() {
		parent, svc, err := splitServiceName(n)
		if err != nil {
			return nil, err
		}

		if in.GetParent() != "" && in.GetParent() != parent {
			return nil, errInvalid("Service %q does not belong to parent %q.", n, in.GetParent())
		}

		p, err := b.consumerLocked(parent)
		if err != nil {
			return nil, err
		}

		if owner != nil && owner != p {
			return nil, errInvalid("All services must belong to the same project.")
		}

		owner = p

		e, ok := lookupService(svc)
		if !ok {
			return nil, errNoService(svc)
		}

		out = append(out, b.serviceView(p, e))
	}

	return &serviceusagepb.BatchGetServicesResponse{Services: out}, nil
}

// ListServices lists the catalog's services with their state for a project, ordered by name.
func (b *InMemoryBackend) ListServices(in *serviceusagepb.ListServicesRequest) (
	*serviceusagepb.ListServicesResponse, error,
) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	filter := strings.TrimSpace(in.GetFilter())
	if filter != "" && filter != filterEnabled && filter != filterDisabled {
		return nil, errInvalid(
			"Invalid filter %q: the allowed filters are %s and %s.",
			filter,
			filterEnabled,
			filterDisabled,
		)
	}

	b.mu.Lock("ListServices")
	b.purgeLocked()

	p, err := b.consumerLocked(in.GetParent())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	all := make([]*serviceusagepb.Service, 0, len(catalog))

	for _, name := range CatalogServices() {
		e, _ := lookupService(name)
		sv := b.serviceView(p, e)

		if filter == "" || (filter == filterEnabled) == (sv.GetState() == serviceusagepb.State_ENABLED) {
			all = append(all, sv)
		}
	}

	b.mu.Unlock()

	pg := page.New(all, in.GetPageToken(), int(min(in.GetPageSize(), maxSUPage)), defaultSUPage)

	return &serviceusagepb.ListServicesResponse{Services: pg.Data, NextPageToken: pg.Next}, nil
}

// EnableService enables a service and its dependencies; enabling an enabled service succeeds.
func (b *InMemoryBackend) EnableService(in *serviceusagepb.EnableServiceRequest) (*longrunningpb.Operation, error) {
	parent, svc, err := splitServiceName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("EnableService")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.consumerLocked(parent)
	if err != nil {
		return nil, err
	}

	e, ok := lookupService(svc)
	if !ok {
		return nil, errNoService(svc)
	}

	b.enableLocked(p.Number, svc)

	return b.suOpLocked(p, []string{svc}, &serviceusagepb.EnableServiceResponse{Service: b.serviceView(p, e)})
}

// BatchEnableServices enables up to 20 services atomically: one unknown service fails the whole batch.
func (b *InMemoryBackend) BatchEnableServices(in *serviceusagepb.BatchEnableServicesRequest) (
	*longrunningpb.Operation, error,
) {
	ids := in.GetServiceIds()
	if len(ids) == 0 {
		return nil, errInvalid("service_ids must not be empty.")
	}

	if len(ids) > maxBatchEnable {
		return nil, errInvalid("A single request can enable at most %d services.", maxBatchEnable)
	}

	b.mu.Lock("BatchEnableServices")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.consumerLocked(in.GetParent())
	if err != nil {
		return nil, err
	}

	var unknown []string

	for _, id := range ids {
		if _, ok := lookupService(id); !ok {
			unknown = append(unknown, id)
		}
	}

	if len(unknown) > 0 {
		return nil, suError(codes.PermissionDenied,
			"Not found or permission denied for service(s): "+strings.Join(unknown, ", ")+".",
			reasonNoService, map[string]string{mdService: strings.Join(unknown, ",")})
	}

	out := make([]*serviceusagepb.Service, 0, len(ids))

	for _, id := range ids {
		b.enableLocked(p.Number, id)

		e, _ := lookupService(id)
		out = append(out, b.serviceView(p, e))
	}

	return b.suOpLocked(p, ids, &serviceusagepb.BatchEnableServicesResponse{Services: out})
}

// DisableService disables an enabled service, and its enabled dependents when asked to.
func (b *InMemoryBackend) DisableService(in *serviceusagepb.DisableServiceRequest) (*longrunningpb.Operation, error) {
	parent, svc, err := splitServiceName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DisableService")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.consumerLocked(parent)
	if err != nil {
		return nil, err
	}

	e, ok := lookupService(svc)
	if !ok {
		return nil, errNoService(svc)
	}

	set := b.enabled[p.Number]
	if _, on := set[svc]; !on {
		return nil, errPrecondition("Service %s is not enabled on project %s.", svc, p.Number)
	}

	var dependents []string

	for _, d := range dependentsOf(svc) {
		if _, on := set[d]; on {
			dependents = append(dependents, d)
		}
	}

	if len(dependents) > 0 && !in.GetDisableDependentServices() {
		slices.Sort(dependents)

		return nil, errPrecondition("Not ready to disable service %s: these enabled services depend on it: %s. "+
			"Set disable_dependent_services to disable them together.", svc, strings.Join(dependents, ", "))
	}

	for _, d := range dependents {
		b.disableTreeLocked(set, d)
	}

	delete(set, svc)

	return b.suOpLocked(p, []string{svc}, &serviceusagepb.DisableServiceResponse{Service: b.serviceView(p, e)})
}

func (b *InMemoryBackend) disableTreeLocked(set map[string]time.Time, name string) {
	for _, d := range dependentsOf(name) {
		if _, on := set[d]; on {
			b.disableTreeLocked(set, d)
		}
	}

	delete(set, name)
}

// CheckServiceEnabled returns the SERVICE_DISABLED error Google APIs answer with when project has not enabled
// service, and nil when it has (or the service is not in the catalog). An unknown project is CONSUMER_INVALID.
func (b *InMemoryBackend) CheckServiceEnabled(project, service string) error {
	b.mu.RLock("CheckServiceEnabled")
	defer b.mu.RUnlock()

	p, ok := b.findProjectLocked(project)
	if !ok || p.requested() {
		return errConsumerFor(kindProjects+"/"+strings.TrimPrefix(project, kindProjects+"/"), service)
	}

	e, known := lookupService(service)
	if _, on := b.enabled[p.Number][service]; on || !known {
		return nil
	}

	url := fmt.Sprintf("https://console.developers.google.com/apis/api/%s/overview?project=%s", service, p.Number)
	msg := fmt.Sprintf(
		"%s has not been used in project %s before or it is disabled. Enable it by visiting %s then retry. "+
			"If you enabled this API recently, wait a few minutes for the action to propagate to our systems and retry.",
		e.title,
		p.Number,
		url,
	)
	st := status.New(codes.PermissionDenied, msg)

	withDetails, err := st.WithDetails(
		&errdetails.ErrorInfo{Reason: reasonDisabled, Domain: suDomain, Metadata: map[string]string{
			"containerInfo": p.Number, "consumer": kindProjects + "/" + p.Number, mdService: service,
			"serviceTitle": e.title, "activationUrl": url,
		}},
		&errdetails.Help{
			Links: []*errdetails.Help_Link{{Description: "Google developers console API activation", Url: url}},
		},
	)
	if err != nil {
		return st.Err()
	}

	return withDetails.Err()
}
