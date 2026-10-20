package gcpfunctions

import (
	"fmt"
	"sort"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func (b *InMemoryBackend) host(n resName) string {
	h := hostLabel(n) + localhostSuffix
	if b.ingressPort > 0 {
		h = fmt.Sprintf("%s:%d", h, b.ingressPort)
	}

	return h
}

// viewLocked returns a copy of the function with its serving URIs filled in.
func (b *InMemoryBackend) viewLocked(rec *fnRec) *functionspb.Function {
	v := clone(rec.pb)

	n, err := parseName(v.GetName())
	if err != nil {
		return v
	}

	if v.GetServiceConfig() == nil {
		v.ServiceConfig = &functionspb.ServiceConfig{}
	}

	v.Url = "http://" + b.host(n)
	v.ServiceConfig.Uri = v.GetUrl()
	v.ServiceConfig.Service = n.parent() + "/services/" + n.id

	return v
}

func (b *InMemoryBackend) newRecLocked(n resName, req *functionspb.Function, res *resolved) *fnRec {
	now := b.ts()
	uid := newUID()
	pb := &functionspb.Function{
		Name:        n.full(),
		Description: req.GetDescription(),
		Labels:      req.GetLabels(),
		KmsKeyName:  req.GetKmsKeyName(),
		State:       functionspb.Function_DEPLOYING,
		Environment: functionspb.Environment_GEN_2,
		CreateTime:  now,
		UpdateTime:  now,
	}

	rec := &fnRec{pb: pb, revSeq: 1, pending: 1}
	pb.BuildConfig = buildConfigFor(n, req.GetBuildConfig(), res)
	pb.ServiceConfig = applyDefaults(clone(req.GetServiceConfig()))
	pb.ServiceConfig.Revision = revisionName(n.id, rec.revSeq, uid)
	pb.EventTrigger = normalizeTrigger(n, req.GetEventTrigger())
	rec.zip = res.data

	return rec
}

func buildConfigFor(n resName, req *functionspb.BuildConfig, res *resolved) *functionspb.BuildConfig {
	bc := clone(req)
	bc.Build = fmt.Sprintf("projects/%s/locations/%s/builds/%s", n.project, n.location, uuid.NewString())
	bc.SourceProvenance = &functionspb.SourceProvenance{ResolvedStorageSource: res.src}

	return bc
}

func normalizeTrigger(n resName, et *functionspb.EventTrigger) *functionspb.EventTrigger {
	if et == nil {
		return nil
	}

	out := clone(et)
	out.Trigger = fmt.Sprintf("%s/triggers/%s-%s", n.parent(), n.id, shortHash(n.project, n.location, n.id)[:6])
	out.TriggerRegion = n.location

	if out.GetRetryPolicy() == functionspb.EventTrigger_RETRY_POLICY_UNSPECIFIED {
		out.RetryPolicy = functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY
	}

	return out
}

// CreateFunction stores a function and deploys it in the background.
func (b *InMemoryBackend) CreateFunction(in *functionspb.CreateFunctionRequest) (*longrunningpb.Operation, error) {
	p, err := parseParent(in.GetParent())
	if err != nil || p.location == allLocs {
		return nil, errInvalid("Invalid parent %q.", in.GetParent())
	}

	fn := in.GetFunction()
	if fn == nil {
		return nil, errInvalid("function is required.")
	}

	id := in.GetFunctionId()
	if id == "" && fn.GetName() != "" {
		id = lastSegment(fn.GetName())
	}

	if err = validateID(id); err != nil {
		return nil, err
	}

	if err = b.validateFunction(fn); err != nil {
		return nil, err
	}

	n := resName{project: p.project, location: p.location, id: id}

	res, err := b.resolveSource(n, fn.GetBuildConfig().GetSource())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFunction")

	if _, exists := b.functions[n.full()]; exists {
		b.mu.Unlock()

		return nil, errExists("Function %q already exists.", n.full())
	}

	rec := b.newRecLocked(n, fn, res)
	b.functions[n.full()] = rec
	j := job{name: n.full(), kind: opCreate, created: rec.pb.GetCreateTime()}
	b.mu.Unlock()

	return b.begin(n.parent(), j)
}

// GetFunction returns a function.
func (b *InMemoryBackend) GetFunction(name string) (*functionspb.Function, error) {
	n, err := parseName(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	rec, ok := b.functions[n.full()]
	if !ok {
		return nil, errNotFound(n)
	}

	return b.viewLocked(rec), nil
}

// ListFunctions lists functions under a location, or every location with "-".
func (b *InMemoryBackend) ListFunctions(in *functionspb.ListFunctionsRequest) ([]*functionspb.Function, string, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if in.GetFilter() != "" || in.GetOrderBy() != "" {
		return nil, "", errInvalid("filter and order_by are not supported by the emulator.")
	}

	prefix := "projects/" + p.project + "/locations/"
	if p.location != allLocs {
		prefix += p.location + "/"
	}

	b.mu.RLock("ListFunctions")
	names := make([]string, 0, len(b.functions))

	for name := range b.functions {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	all := make([]*functionspb.Function, 0, len(names))
	for _, name := range names {
		all = append(all, b.viewLocked(b.functions[name]))
	}
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// ListRuntimes lists the runtimes the emulator can run.
func (b *InMemoryBackend) ListRuntimes(in *functionspb.ListRuntimesRequest) (*functionspb.ListRuntimesResponse, error) {
	if _, err := parseParent(in.GetParent()); err != nil {
		return nil, err
	}

	env := functionspb.Environment_ENVIRONMENT_UNSPECIFIED

	switch f := in.GetFilter(); f {
	case "":
	case "environment=GEN_1":
		env = functionspb.Environment_GEN_1
	case "environment=GEN_2":
		env = functionspb.Environment_GEN_2
	default:
		return nil, errInvalid("Unsupported filter %q; only environment=GEN_1|GEN_2 is supported.", f)
	}

	return &functionspb.ListRuntimesResponse{Runtimes: supportedRuntimes(env)}, nil
}

// UpdateFunction applies the masked fields and redeploys in the background.
func (b *InMemoryBackend) UpdateFunction(in *functionspb.UpdateFunctionRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetFunction().GetName())
	if err != nil {
		return nil, err
	}

	cur, epoch, err := b.updatable(n)
	if err != nil {
		return nil, err
	}

	next := clone(cur)
	if err = applyMask(next, in.GetFunction(), in.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if (cur.GetEventTrigger() == nil) != (next.GetEventTrigger() == nil) {
		return nil, errInvalid("A function cannot switch between HTTP and event triggers.")
	}

	if err = b.validateFunction(next); err != nil {
		return nil, err
	}

	var res *resolved

	if !proto.Equal(next.GetBuildConfig().GetSource(), cur.GetBuildConfig().GetSource()) {
		if res, err = b.resolveSource(n, next.GetBuildConfig().GetSource()); err != nil {
			return nil, err
		}
	}

	return b.commitUpdate(n, next, res, epoch)
}

func (b *InMemoryBackend) updatable(n resName) (*functionspb.Function, int, error) {
	b.mu.RLock("updatable")
	defer b.mu.RUnlock()

	rec, ok := b.functions[n.full()]
	if !ok {
		return nil, 0, errNotFound(n)
	}

	if rec.pending > 0 {
		return nil, 0, errPrecondition("Function %q has an operation in progress.", n.full())
	}

	return clone(rec.pb), rec.epoch, nil
}

func (b *InMemoryBackend) commitUpdate(
	n resName, next *functionspb.Function, res *resolved, epoch int,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("commitUpdate")

	rec, ok := b.functions[n.full()]
	if !ok || rec.epoch != epoch || rec.pending > 0 {
		b.mu.Unlock()

		return nil, errPrecondition("Function %q was modified concurrently; retry.", n.full())
	}

	now := b.ts()
	next.ServiceConfig = applyDefaults(next.GetServiceConfig())
	next.EventTrigger = normalizeTrigger(n, next.GetEventTrigger())
	rec.revSeq++
	next.ServiceConfig.Revision = revisionName(n.id, rec.revSeq, rec.pb.GetBuildConfig().GetBuild())

	if res != nil {
		rec.zip = res.data
		next.BuildConfig = buildConfigFor(n, next.GetBuildConfig(), res)
	}

	next.State, next.UpdateTime = functionspb.Function_DEPLOYING, now
	rec.pb, rec.epoch, rec.pending = next, rec.epoch+1, 1
	j := job{name: n.full(), kind: opUpdate, created: now}
	b.mu.Unlock()

	return b.begin(n.parent(), j)
}

// DeleteFunction removes a function, its container and its trigger subscription in the background.
func (b *InMemoryBackend) DeleteFunction(in *functionspb.DeleteFunctionRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteFunction")

	rec, ok := b.functions[n.full()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound(n)
	}

	if rec.pending > 0 {
		b.mu.Unlock()

		return nil, errPrecondition("Function %q has an operation in progress.", n.full())
	}

	now := b.ts()
	rec.pending, rec.epoch = 1, rec.epoch+1
	rec.pb.State, rec.pb.UpdateTime = functionspb.Function_DELETING, now
	j := job{name: n.full(), kind: opDelete, created: now}
	b.mu.Unlock()

	return b.begin(n.parent(), j)
}
