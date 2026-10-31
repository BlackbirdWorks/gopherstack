package gcpapigateway

import (
	"strings"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const specFailMsg = "Cannot convert to service config."

func (b *InMemoryBackend) liveConfigLocked(name string) (*configRec, bool) {
	r, ok := b.configs[name]
	if !ok || r.life.gone(b.now()) {
		return nil, false
	}

	return r, true
}

// configViewLocked returns the config as clients see it; the OpenAPI contents only come with the full view.
func (b *InMemoryBackend) configViewLocked(r *configRec, full bool) *apigatewaypb.ApiConfig {
	out := clone(r.pb)
	out.State = apigatewaypb.ApiConfig_State(r.life.state(b.now(), int32(r.pb.GetState())))

	if !full {
		out.OpenapiDocuments = nil
	}

	return out
}

func validateConfigInput(src *apigatewaypb.ApiConfig) error {
	if err := validateLabels(src.GetLabels()); err != nil {
		return err
	}

	if sa := src.GetGatewayServiceAccount(); sa != "" && !validServiceAccount(sa) {
		return errInvalid("Invalid gateway_service_account %q: want a service account email or resource name.", sa)
	}

	if len(src.GetGrpcServices()) > 0 || len(src.GetManagedServiceConfigs()) > 0 {
		return errInvalid("gRPC service definitions are not supported by the emulator; use openapi_documents.")
	}

	docs := src.GetOpenapiDocuments()
	if len(docs) == 0 {
		return errInvalid("openapi_documents is required.")
	}

	if len(docs) > 1 {
		return errInvalid("Exactly one OpenAPI document is supported per API config.")
	}

	if docs[0].GetDocument().GetPath() == "" || len(docs[0].GetDocument().GetContents()) == 0 {
		return errInvalid("openapi_documents[0].document needs a path and contents.")
	}

	return nil
}

// CreateAPIConfig creates an API config; an invalid OpenAPI document leaves it FAILED with the operation failed.
func (b *InMemoryBackend) CreateAPIConfig(in *apigatewaypb.CreateApiConfigRequest) (*longrunningpb.Operation, error) {
	project, api, err := parseAPIName(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = checkID("api_config_id", in.GetApiConfigId()); err != nil {
		return nil, err
	}

	src := in.GetApiConfig()
	if err = validateConfigInput(src); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateApiConfig")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, ok := b.liveAPILocked(in.GetParent()); !ok {
		return nil, errNotFound("Resource '%s' was not found.", in.GetParent())
	}

	name := configName(project, api, in.GetApiConfigId())
	if _, ok := b.configs[name]; ok {
		return nil, errExists("Resource '%s' already exists.", name)
	}

	now := b.now()

	for _, c := range b.configs {
		if strings.HasPrefix(c.pb.GetName(), in.GetParent()+"/") && !c.life.deleting() && now.Before(c.life.Until) &&
			c.life.Transient == stateCreating {
			return nil, errPrecondition("Only one API config at a time may be created for API '%s'.", in.GetParent())
		}
	}

	spec, diags := compileSpec(src.GetOpenapiDocuments()[0].GetDocument().GetContents())

	pb := &apigatewaypb.ApiConfig{
		Name: name, CreateTime: b.ts(), UpdateTime: b.ts(), Labels: src.GetLabels(),
		DisplayName: src.GetDisplayName(), GatewayServiceAccount: src.GetGatewayServiceAccount(),
		ServiceConfigId: in.GetApiConfigId() + "-" + idHash(hashLen, name), State: apigatewaypb.ApiConfig_ACTIVE,
		OpenapiDocuments: src.GetOpenapiDocuments(),
	}
	rec := &configRec{pb: pb, spec: spec, life: life{Until: now.Add(b.timing.Create), Transient: stateCreating}}

	s := opSpec{project: project, location: globalLocation, target: name, verb: verbCreate, dur: b.timing.Create}

	if len(diags) > 0 {
		pb.State, rec.spec = apigatewaypb.ApiConfig_FAILED, nil
		s.failMsg, s.failCode, s.diags = specFailMsg+" "+diags[0].Message, codes.InvalidArgument, diags
	} else {
		resp := clone(pb)
		resp.OpenapiDocuments = nil
		s.resp = resp
	}

	b.configs[name] = rec

	return b.finishLocked(s)
}

// GetAPIConfig returns one API config; the full view includes the OpenAPI documents.
func (b *InMemoryBackend) GetAPIConfig(in *apigatewaypb.GetApiConfigRequest) (*apigatewaypb.ApiConfig, error) {
	if _, err := parseConfigName(in.GetName()); err != nil {
		return nil, err
	}

	b.mu.RLock("GetApiConfig")
	defer b.mu.RUnlock()

	r, ok := b.liveConfigLocked(in.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", in.GetName())
	}

	return b.configViewLocked(r, in.GetView() == apigatewaypb.GetApiConfigRequest_FULL), nil
}

// ListAPIConfigs lists the configs of an API.
func (b *InMemoryBackend) ListAPIConfigs(in *apigatewaypb.ListApiConfigsRequest) (
	*apigatewaypb.ListApiConfigsResponse, error,
) {
	if _, _, err := parseAPIName(in.GetParent()); err != nil {
		return nil, err
	}

	b.mu.RLock("ListApiConfigs")

	if _, ok := b.liveAPILocked(in.GetParent()); !ok {
		b.mu.RUnlock()

		return nil, errNotFound("Resource '%s' was not found.", in.GetParent())
	}

	var all []*apigatewaypb.ApiConfig

	for _, r := range b.configs {
		if strings.HasPrefix(r.pb.GetName(), in.GetParent()+"/") && !r.life.gone(b.now()) {
			all = append(all, b.configViewLocked(r, false))
		}
	}

	b.mu.RUnlock()

	pg, err := paginate(all, listArgs{
		filter: in.GetFilter(), orderBy: in.GetOrderBy(), pageToken: in.GetPageToken(), pageSize: in.GetPageSize(),
	}, listKeys[*apigatewaypb.ApiConfig]{
		name:    (*apigatewaypb.ApiConfig).GetName,
		created: func(a *apigatewaypb.ApiConfig) time.Time { return a.GetCreateTime().AsTime() },
		updated: func(a *apigatewaypb.ApiConfig) time.Time { return a.GetUpdateTime().AsTime() },
	})
	if err != nil {
		return nil, err
	}

	return &apigatewaypb.ListApiConfigsResponse{ApiConfigs: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateAPIConfig updates the display name and labels of an API config.
func (b *InMemoryBackend) UpdateAPIConfig(in *apigatewaypb.UpdateApiConfigRequest) (*longrunningpb.Operation, error) {
	src := in.GetApiConfig()

	project, err := parseConfigName(src.GetName())
	if err != nil {
		return nil, err
	}

	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateApiConfig")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveConfigLocked(src.GetName())
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
		default:
			return nil, errInvalid("Unsupported update_mask path %q for ApiConfig: the document and service account "+
				"are immutable.", p)
		}
	}

	next.UpdateTime = timestamppb.New(b.now())
	rec.pb = next
	rec.life.Until, rec.life.Transient = b.now().Add(b.timing.Update), stateUpdating

	resp := clone(next)
	resp.OpenapiDocuments = nil

	return b.finishLocked(opSpec{
		project: project, location: globalLocation, target: next.GetName(), verb: verbUpdate,
		dur: b.timing.Update, resp: resp,
	})
}

// DeleteAPIConfig deletes an API config that no gateway serves.
func (b *InMemoryBackend) DeleteAPIConfig(in *apigatewaypb.DeleteApiConfigRequest) (*longrunningpb.Operation, error) {
	project, err := parseConfigName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteApiConfig")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveConfigLocked(in.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", in.GetName())
	}

	if rec.life.deleting() {
		return nil, errPrecondition("Resource '%s' is already being deleted.", in.GetName())
	}

	for _, g := range b.gateways {
		if g.pb.GetApiConfig() == in.GetName() && !g.life.gone(b.now()) {
			return nil, errPrecondition("API config '%s' is in use by gateway '%s'.", in.GetName(), g.pb.GetName())
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
