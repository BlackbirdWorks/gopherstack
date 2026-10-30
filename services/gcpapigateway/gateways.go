package gcpapigateway

import (
	"strings"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const hostSuffix = ".gateway.dev"

// regionCodes are the short region labels of gateway hostnames.
//
//nolint:gochecknoglobals // immutable lookup table
var regionCodes = map[string]string{
	"us-central1": "uc", "us-east1": "ue", "us-east4": "ue", "us-west2": "uw", "us-west3": "uw", "us-west4": "uw",
	"europe-west1": "ew", "europe-west2": "ew", "asia-northeast1": "an", "australia-southeast1": "as",
}

func defaultHostname(id, region, name string) string {
	return id + "-" + idHash(hashLen, name) + "." + regionCodes[region] + hostSuffix
}

func (b *InMemoryBackend) liveGatewayLocked(name string) (*gatewayRec, bool) {
	r, ok := b.gateways[name]
	if !ok || r.life.gone(b.now()) {
		return nil, false
	}

	return r, true
}

func (b *InMemoryBackend) gatewayViewLocked(r *gatewayRec) *apigatewaypb.Gateway {
	out := clone(r.pb)
	out.State = apigatewaypb.Gateway_State(r.life.state(b.now(), stateActive))

	return out
}

// usableConfigLocked checks that a gateway can serve the config.
func (b *InMemoryBackend) usableConfigLocked(name string) error {
	if _, err := parseConfigName(name); err != nil {
		return errInvalid("Invalid api_config %q: %v", name, err)
	}

	c, ok := b.liveConfigLocked(name)
	if !ok {
		return errNotFound("Resource '%s' was not found.", name)
	}

	if c.life.deleting() || c.pb.GetState() == apigatewaypb.ApiConfig_FAILED {
		return errPrecondition("API config '%s' is %s and cannot be served.", name,
			apigatewaypb.ApiConfig_State(c.life.state(b.now(), int32(c.pb.GetState()))))
	}

	return nil
}

// CreateGateway creates a gateway in a region.
func (b *InMemoryBackend) CreateGateway(in *apigatewaypb.CreateGatewayRequest) (*longrunningpb.Operation, error) {
	project, region, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !knownRegion(region) {
		return nil, errInvalid("Gateways are not available in location %q.", region)
	}

	if err = checkID("gateway_id", in.GetGatewayId()); err != nil {
		return nil, err
	}

	src := in.GetGateway()
	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	if src.GetApiConfig() == "" {
		return nil, errInvalid("gateway.api_config is required.")
	}

	b.mu.Lock("CreateGateway")
	defer b.mu.Unlock()

	b.sweepLocked()

	name := childName(project, region, kindGateways, in.GetGatewayId())
	if _, ok := b.gateways[name]; ok {
		return nil, errExists("Resource '%s' already exists.", name)
	}

	if err = b.usableConfigLocked(src.GetApiConfig()); err != nil {
		return nil, err
	}

	effective := apigatewaypb.Gateway_EFFECTIVE_STREAMING_MODE_DISABLED
	if src.GetStreamingMode() == apigatewaypb.Gateway_STREAMING_MODE_ENABLED {
		effective = apigatewaypb.Gateway_EFFECTIVE_STREAMING_MODE_ENABLED
	}

	pb := &apigatewaypb.Gateway{
		Name: name, CreateTime: b.ts(), UpdateTime: b.ts(), Labels: src.GetLabels(),
		DisplayName: src.GetDisplayName(), ApiConfig: src.GetApiConfig(), State: apigatewaypb.Gateway_ACTIVE,
		DefaultHostname: defaultHostname(in.GetGatewayId(), region, name), StreamingMode: src.GetStreamingMode(),
		EffectiveStreamingMode: effective,
	}
	b.gateways[name] = &gatewayRec{pb: pb, life: life{Until: b.now().Add(b.timing.Create), Transient: stateCreating}}

	return b.finishLocked(opSpec{
		project: project, location: region, target: name, verb: verbCreate, dur: b.timing.Create, resp: clone(pb),
	})
}

// GetGateway returns one gateway.
func (b *InMemoryBackend) GetGateway(name string) (*apigatewaypb.Gateway, error) {
	if _, _, err := parseGatewayName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetGateway")
	defer b.mu.RUnlock()

	r, ok := b.liveGatewayLocked(name)
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", name)
	}

	return b.gatewayViewLocked(r), nil
}

// ListGateways lists the gateways of a location, or of every location for "-".
func (b *InMemoryBackend) ListGateways(in *apigatewaypb.ListGatewaysRequest) (
	*apigatewaypb.ListGatewaysResponse, error,
) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListGateways")

	var all []*apigatewaypb.Gateway

	for _, r := range b.gateways {
		p, l, _ := parseGatewayName(r.pb.GetName())
		if p == project && (location == "-" || l == location) && !r.life.gone(b.now()) {
			all = append(all, b.gatewayViewLocked(r))
		}
	}

	b.mu.RUnlock()

	pg, err := paginate(all, listArgs{
		filter: in.GetFilter(), orderBy: in.GetOrderBy(), pageToken: in.GetPageToken(), pageSize: in.GetPageSize(),
	}, listKeys[*apigatewaypb.Gateway]{
		name:    (*apigatewaypb.Gateway).GetName,
		created: func(a *apigatewaypb.Gateway) time.Time { return a.GetCreateTime().AsTime() },
		updated: func(a *apigatewaypb.Gateway) time.Time { return a.GetUpdateTime().AsTime() },
	})
	if err != nil {
		return nil, err
	}

	return &apigatewaypb.ListGatewaysResponse{Gateways: pg.Data, NextPageToken: pg.Next}, nil
}

// UpdateGateway updates the display name, labels and API config of a gateway.
func (b *InMemoryBackend) UpdateGateway(in *apigatewaypb.UpdateGatewayRequest) (*longrunningpb.Operation, error) {
	src := in.GetGateway()

	project, region, err := parseGatewayName(src.GetName())
	if err != nil {
		return nil, err
	}

	if err = validateLabels(src.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateGateway")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveGatewayLocked(src.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", src.GetName())
	}

	next := clone(rec.pb)

	for _, p := range maskPaths(in.GetUpdateMask().GetPaths(), maskDisplay, maskLabels, maskAPIConfig) {
		switch p {
		case maskAll:
			next.DisplayName, next.Labels = src.GetDisplayName(), src.GetLabels()
			if src.GetApiConfig() != "" {
				next.ApiConfig = src.GetApiConfig()
			}
		case maskDisplay, maskDisplayCamel:
			next.DisplayName = src.GetDisplayName()
		case maskLabels:
			next.Labels = src.GetLabels()
		case maskAPIConfig, "apiConfig":
			next.ApiConfig = src.GetApiConfig()
		case "streaming_mode", "streamingMode":
			if src.GetStreamingMode() != rec.pb.GetStreamingMode() {
				return nil, errInvalid("streaming_mode is immutable.")
			}
		default:
			return nil, errInvalid("Unsupported update_mask path %q for Gateway.", p)
		}
	}

	if next.GetApiConfig() != rec.pb.GetApiConfig() {
		if err = b.usableConfigLocked(next.GetApiConfig()); err != nil {
			return nil, err
		}
	}

	next.UpdateTime = timestamppb.New(b.now())
	rec.pb = next
	rec.life.Until, rec.life.Transient = b.now().Add(b.timing.Update), stateUpdating

	return b.finishLocked(opSpec{
		project: project, location: region, target: next.GetName(), verb: verbUpdate,
		dur: b.timing.Update, resp: clone(next),
	})
}

// DeleteGateway deletes a gateway.
func (b *InMemoryBackend) DeleteGateway(in *apigatewaypb.DeleteGatewayRequest) (*longrunningpb.Operation, error) {
	project, region, err := parseGatewayName(in.GetName())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteGateway")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, ok := b.liveGatewayLocked(in.GetName())
	if !ok {
		return nil, errNotFound("Resource '%s' was not found.", in.GetName())
	}

	if rec.life.deleting() {
		return nil, errPrecondition("Resource '%s' is already being deleted.", in.GetName())
	}

	op, err := b.finishLocked(opSpec{
		project: project, location: region, target: in.GetName(), verb: verbDelete,
		dur: b.timing.Delete, resp: emptyResp(),
	})
	if err != nil {
		return nil, err
	}

	rec.life.DeleteAt, rec.life.DeleteOp = b.now().Add(b.timing.Delete), op.GetName()

	return op, nil
}

func hostOnly(h string) string {
	if i := strings.LastIndexByte(h, ':'); i >= 0 && !strings.Contains(h[i:], "]") {
		return strings.ToLower(h[:i])
	}

	return strings.ToLower(h)
}
