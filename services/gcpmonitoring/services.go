package gcpmonitoring

import (
	"regexp"
	"sort"
	"strings"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	collServices  = "services"
	maxServiceID  = 63
	fldIdentifier = "identifier_case"
	fldTelemetry  = "telemetry"
	pathTelemetry = "telemetry.resource_name"
	genIDPrefix   = "gen-"
)

var serviceIDRe = regexp.MustCompile(`^[a-z0-9-]+$`)

func cloneService(s *monitoringpb.Service) *monitoringpb.Service {
	out, _ := proto.Clone(s).(*monitoringpb.Service)

	return out
}

func serviceName(project, id string) string {
	return projectScope + project + "/" + collServices + "/" + id
}

func validateServiceID(id string) error {
	if id == "" || len(id) > maxServiceID || !serviceIDRe.MatchString(id) {
		return errInvalid("Field service_id had an invalid value of %q: it must match [a-z0-9\\-]+ and be at most "+
			"%d characters.", id, maxServiceID)
	}

	return nil
}

func validateService(s *monitoringpb.Service) error {
	if s == nil {
		return errInvalid("service is required.")
	}

	if len(s.GetDisplayName()) > maxDisplayName {
		return errInvalid("Field service.display_name had an invalid value: at most %d characters.", maxDisplayName)
	}

	if s.GetIdentifier() == nil && s.GetBasicService() == nil {
		return errInvalid("Field service.identifier had an invalid value: a service identifier is required.")
	}

	return validateUserLabels(s.GetUserLabels())
}

// CreateService stores a service under the given or a generated id.
func (b *InMemoryBackend) CreateService(req *monitoringpb.CreateServiceRequest) (*monitoringpb.Service, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, err
	}

	id := req.GetServiceId()
	if id != "" {
		if err = validateServiceID(id); err != nil {
			return nil, err
		}
	} else {
		id = genIDPrefix + newHexSuffix() + newHexSuffix()
	}

	if err = validateService(req.GetService()); err != nil {
		return nil, err
	}

	s := cloneService(req.GetService())
	s.Name = serviceName(project, id)

	b.mu.Lock("CreateService")
	defer b.mu.Unlock()

	if _, dup := b.services[s.GetName()]; dup {
		return nil, errExists("Service %s already exists.", s.GetName())
	}

	b.services[s.GetName()] = s

	return cloneService(s), nil
}

// GetService returns one service.
func (b *InMemoryBackend) GetService(name string) (*monitoringpb.Service, error) {
	if _, _, err := splitName(name, collServices); err != nil {
		return nil, err
	}

	b.mu.RLock("GetService")
	defer b.mu.RUnlock()

	s, ok := b.services[name]
	if !ok {
		return nil, errNotFound("Service %s does not exist.", name)
	}

	return cloneService(s), nil
}

type serviceEnv struct{ s *monitoringpb.Service }

func (e serviceEnv) values(path string) []string {
	switch {
	case path == fldDisplayName:
		return []string{e.s.GetDisplayName()}
	case path == fldName:
		return []string{e.s.GetName()}
	case strings.HasPrefix(path, fldUserLabels+"."):
		return one(e.s.GetUserLabels(), strings.TrimPrefix(path, fldUserLabels+"."))
	}

	oneof := e.s.ProtoReflect().Descriptor().Oneofs().ByName("identifier")
	set := e.s.ProtoReflect().WhichOneof(oneof)

	if set == nil {
		return nil
	}

	if path == fldIdentifier {
		return []string{strings.ToUpper(string(set.Name()))}
	}

	attr, ok := strings.CutPrefix(path, string(set.Name())+".")
	if !ok {
		return nil
	}

	msg := e.s.ProtoReflect().Get(set).Message()
	if fd := msg.Descriptor().
		Fields().
		ByName(protoreflect.Name(attr)); fd != nil &&
		fd.Kind() == protoreflect.StringKind {
		return []string{msg.Get(fd).String()}
	}

	return nil
}

func validateServiceFilterField(field string) error {
	switch {
	case field == fldDisplayName, field == fldName, field == fldIdentifier,
		strings.HasPrefix(field, fldUserLabels+"."):
		return nil
	}

	root, attr, _ := strings.Cut(field, ".")
	desc := (&monitoringpb.Service{}).ProtoReflect().Descriptor()

	if fd := desc.Fields().ByName(protoreflect.Name(root)); fd != nil && fd.ContainingOneof() != nil &&
		fd.Kind() == protoreflect.MessageKind && fd.Message().Fields().ByName(protoreflect.Name(attr)) != nil {
		return nil
	}

	return errInvalid("Field filter had an invalid value of %q: unknown field.", field)
}

func parseResourceFilter(src string, check func(string) error) (*filter, error) {
	f, err := parseFilter(src)
	if err != nil {
		return nil, err
	}

	for field := range f.referenced() {
		if err = check(field); err != nil {
			return nil, err
		}
	}

	return f, nil
}

// ListServices pages the project's services ordered by name.
func (b *InMemoryBackend) ListServices(req *monitoringpb.ListServicesRequest) ([]*monitoringpb.Service, string, error) {
	project, err := projectOf(req.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	f, err := parseResourceFilter(req.GetFilter(), validateServiceFilterField)
	if err != nil {
		return nil, "", err
	}

	prefix := projectScope + project + "/" + collServices + "/"

	b.mu.RLock("ListServices")

	var all []*monitoringpb.Service

	for name, s := range b.services {
		if strings.HasPrefix(name, prefix) && f.matches(serviceEnv{s: s}) {
			all = append(all, cloneService(s))
		}
	}

	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

// UpdateService replaces the mutable fields in the mask: display_name, user_labels and telemetry.
func (b *InMemoryBackend) UpdateService(req *monitoringpb.UpdateServiceRequest) (*monitoringpb.Service, error) {
	in := req.GetService()

	if _, _, err := splitName(in.GetName(), collServices); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateService")
	defer b.mu.Unlock()

	cur, ok := b.services[in.GetName()]
	if !ok {
		return nil, errNotFound("Service %s does not exist.", in.GetName())
	}

	next := cloneService(cur)

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		paths = []string{fldDisplayName, fldUserLabels, fldTelemetry}
	}

	for _, p := range paths {
		switch p {
		case fldDisplayName:
			if len(in.GetDisplayName()) > maxDisplayName {
				return nil, errInvalid("Field service.display_name had an invalid value: at most %d characters.",
					maxDisplayName)
			}

			next.DisplayName = in.GetDisplayName()
		case fldUserLabels:
			if err := validateUserLabels(in.GetUserLabels()); err != nil {
				return nil, err
			}

			next.UserLabels = in.GetUserLabels()
		case fldTelemetry, pathTelemetry:
			next.Telemetry = in.GetTelemetry()
		default:
			return nil, errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	b.services[next.GetName()] = next

	return cloneService(next), nil
}

// DeleteService removes a service and its service level objectives.
func (b *InMemoryBackend) DeleteService(name string) error {
	if _, _, err := splitName(name, collServices); err != nil {
		return err
	}

	b.mu.Lock("DeleteService")
	defer b.mu.Unlock()

	if _, ok := b.services[name]; !ok {
		return errNotFound("Service %s does not exist.", name)
	}

	delete(b.services, name)

	for n := range b.slos {
		if strings.HasPrefix(n, name+"/") {
			delete(b.slos, n)
		}
	}

	return nil
}
