package gcpapigateway

import (
	"crypto/sha256"
	"encoding/base32"
	"strings"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const hashLen = 18

// idHash is a stable lowercase base32 digest used in generated service and host names.
func idHash(n int, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))

	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))[:n]
}

func (b *InMemoryBackend) apiViewLocked(r *apiRec) *apigatewaypb.Api {
	out := clone(r.pb)
	out.State = apigatewaypb.Api_State(r.life.state(b.now(), stateActive))

	return out
}

func (b *InMemoryBackend) liveAPILocked(name string) (*apiRec, bool) {
	r, ok := b.apis[name]
	if !ok || r.life.gone(b.now()) {
		return nil, false
	}

	return r, true
}

// CreateAPI creates an API in the global location.
func (b *InMemoryBackend) CreateAPI(in *apigatewaypb.CreateApiRequest) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if location != globalLocation {
		return nil, errInvalid("APIs are global resources: use projects/%s/locations/global.", project)
	}

	if err = checkID("api_id", in.GetApiId()); err != nil {
		return nil, err
	}

	src := in.GetApi()
	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateApi")
	defer b.mu.Unlock()

	b.sweepLocked()

	name := apiName(project, in.GetApiId())
	if _, ok := b.apis[name]; ok {
		return nil, errExists("Resource '%s' already exists.", name)
	}

	managed := src.GetManagedService()
	if managed == "" {
		managed = in.GetApiId() + "-" + idHash(hashLen, name) + ".apigateway." + project + ".cloud.goog"
	}

	now := b.ts()
	pb := &apigatewaypb.Api{
		Name: name, CreateTime: now, UpdateTime: now, Labels: src.GetLabels(), DisplayName: src.GetDisplayName(),
		ManagedService: managed, State: apigatewaypb.Api_ACTIVE,
	}
	rec := &apiRec{pb: pb, life: life{Until: b.now().Add(b.timing.Create), Transient: stateCreating}}
	b.apis[name] = rec

	return b.finishLocked(opSpec{
		project: project, location: location, target: name, verb: verbCreate, dur: b.timing.Create,
		resp: clone(pb),
	})
}

func (b *InMemoryBackend) finishLocked(s opSpec) (*longrunningpb.Operation, error) {
	o, err := b.newOpLocked(s)
	if err != nil {
		return nil, err
	}

	return b.opViewLocked(o), nil
}

// GetAPI returns one API.
func (b *InMemoryBackend) GetAPI(name string) (*apigatewaypb.Api, error) {
	if _, _, err := parseAPIName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetApi")
	defer b.mu.RUnlock()

	r, ok := b.liveAPILocked(name)
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", name)
	}

	return b.apiViewLocked(r), nil
}

// ListApis lists the APIs of a project.
func (b *InMemoryBackend) ListApis(in *apigatewaypb.ListApisRequest) (*apigatewaypb.ListApisResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if location != globalLocation && location != "-" {
		return nil, errInvalid("APIs are global resources: use projects/%s/locations/global.", project)
	}

	b.mu.RLock("ListApis")

	var all []*apigatewaypb.Api

	for _, r := range b.apis {
		if strings.HasPrefix(r.pb.GetName(), "projects/"+project+"/") && !r.life.gone(b.now()) {
			all = append(all, b.apiViewLocked(r))
		}
	}

	b.mu.RUnlock()

	pg, err := paginate(all, listArgs{
		filter: in.GetFilter(), orderBy: in.GetOrderBy(), pageToken: in.GetPageToken(), pageSize: in.GetPageSize(),
	}, listKeys[*apigatewaypb.Api]{
		name:    (*apigatewaypb.Api).GetName,
		created: func(a *apigatewaypb.Api) time.Time { return a.GetCreateTime().AsTime() },
		updated: func(a *apigatewaypb.Api) time.Time { return a.GetUpdateTime().AsTime() },
	})
	if err != nil {
		return nil, err
	}

	return &apigatewaypb.ListApisResponse{Apis: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateAPI updates the display name and labels of an API.
func (b *InMemoryBackend) UpdateAPI(in *apigatewaypb.UpdateApiRequest) (*longrunningpb.Operation, error) {
	src := in.GetApi()

	project, _, err := parseAPIName(src.GetName())
	if err != nil {
		return nil, err
	}

	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateApi")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveAPILocked(src.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", src.GetName())
	}

	next := clone(rec.pb)

	for _, p := range maskPaths(in.GetUpdateMask().GetPaths(), maskDisplay, maskLabels) {
		switch p {
		case maskAll:
			next.DisplayName, next.Labels = src.GetDisplayName(), src.GetLabels()
		case maskDisplay, maskDisplayCamel:
			next.DisplayName = src.GetDisplayName()
		case maskLabels:
			next.Labels = src.GetLabels()
		case "managed_service", "managedService":
			if src.GetManagedService() != rec.pb.GetManagedService() {
				return nil, errInvalid("managed_service is immutable.")
			}
		default:
			return nil, errInvalid("Unsupported update_mask path %q for Api.", p)
		}
	}

	next.UpdateTime = timestamppb.New(b.now())
	rec.pb = next
	rec.life.Until, rec.life.Transient = b.now().Add(b.timing.Update), stateUpdating

	return b.finishLocked(opSpec{
		project: project, location: globalLocation, target: next.GetName(), verb: verbUpdate,
		dur: b.timing.Update, resp: clone(next),
	})
}

// DeleteAPI deletes an API that has no API configs left.
func (b *InMemoryBackend) DeleteAPI(in *apigatewaypb.DeleteApiRequest) (*longrunningpb.Operation, error) {
	project, _, err := parseAPIName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteApi")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveAPILocked(in.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", in.GetName())
	}

	if rec.life.deleting() {
		return nil, errPrecondition("Resource '%s' is already being deleted.", in.GetName())
	}

	for _, c := range b.configs {
		if strings.HasPrefix(c.pb.GetName(), in.GetName()+"/") && !c.life.gone(b.now()) {
			return nil, errPrecondition("API '%s' still has API configs; delete them first.", in.GetName())
		}
	}

	op, err := b.finishLocked(opSpec{
		project: project, location: globalLocation, target: in.GetName(), verb: verbDelete,
		dur: b.timing.Delete, resp: emptyResp(),
	})
	if err != nil {
		return nil, err
	}

	rec.life.DeleteAt, rec.life.DeleteOp = b.now().Add(b.timing.Delete), op.GetName()

	return op, nil
}
